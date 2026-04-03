package monitor

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (a *App) runLogFollower(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		a.setSourceConnected(false, "")

		cmd, err := a.newComposeLogCommand(ctx)
		if err != nil {
			a.setSourceConnected(false, err.Error())
			time.Sleep(DefaultRetryDelay)
			continue
		}

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			a.setSourceConnected(false, err.Error())
			time.Sleep(DefaultRetryDelay)
			continue
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			a.setSourceConnected(false, err.Error())
			time.Sleep(DefaultRetryDelay)
			continue
		}
		if err := cmd.Start(); err != nil {
			a.setSourceConnected(false, err.Error())
			time.Sleep(DefaultRetryDelay)
			continue
		}

		a.setSourceConnected(true, "")
		err = a.waitForLogStreams(ctx, cmd, stdout, stderr)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			a.setSourceConnected(false, err.Error())
		} else {
			a.setSourceConnected(false, "docker compose logs exited")
		}
		time.Sleep(DefaultRetryDelay)
	}
}

func (a *App) newComposeLogCommand(ctx context.Context) (*exec.Cmd, error) {
	composeFile := a.cfg.ComposeFile
	if !filepath.IsAbs(composeFile) {
		composeFile = filepath.Join(a.cfg.ProjectDir, composeFile)
	}

	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "logs", "-f", "--tail", strconv.Itoa(a.cfg.TailLines), "--no-color")
	cmd.Dir = a.cfg.ProjectDir
	return cmd, nil
}

func (a *App) waitForLogStreams(ctx context.Context, cmd *exec.Cmd, stdout, stderr io.Reader) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		a.consumeLines(stdout, false)
	}()
	go func() {
		defer wg.Done()
		a.consumeLines(stderr, true)
	}()

	err := cmd.Wait()
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (a *App) consumeLines(reader io.Reader, isError bool) {
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if isError && !strings.Contains(line, "ts=") {
			trimmed := strings.TrimSpace(line)
			if isLikelyFatalStderrLine(trimmed) {
				a.setSourceConnected(false, trimmed)
			}
			a.applyUnparsedLine(line, true)
			continue
		}

		parsed, ok := parseLogLine(line)
		if !ok {
			a.applyUnparsedLine(line, isError)
			continue
		}
		a.applyParsedLine(parsed)
	}
	if err := scanner.Err(); err != nil {
		a.setSourceConnected(false, err.Error())
	}
}

func isLikelyFatalStderrLine(line string) bool {
	if line == "" {
		return false
	}
	lower := strings.ToLower(line)
	fatalHints := []string{
		"error",
		"failed",
		"permission denied",
		"cannot",
		"unable",
		"no such file",
		"connection refused",
	}
	for _, hint := range fatalHints {
		if strings.Contains(lower, hint) {
			return true
		}
	}
	return false
}
