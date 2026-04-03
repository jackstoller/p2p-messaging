package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var nodeIdPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)
var containerIdPattern = regexp.MustCompile(`^[a-f0-9]{12,64}$`)

type nodeLifecycleRequest struct {
	NodeId string `json:"nodeId"`
}

type nodeLifecycleResult struct {
	Action   string `json:"action"`
	NodeId   string `json:"nodeId"`
	Ok       bool   `json:"ok"`
	ExitCode int    `json:"exitCode"`
	Output   string `json:"output"`
	Error    string `json:"error,omitempty"`
}

func (a *App) handleNodeLifecycleAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	action := strings.TrimPrefix(r.URL.Path, "/api/ops/nodes/")
	if action != "start" && action != "stop" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	var req nodeLifecycleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	nodeId := strings.TrimSpace(req.NodeId)
	ctx, cancel := nodeRequestContext(r.Context())
	defer cancel()

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	var (
		result nodeLifecycleResult
		err    error
	)
	if action == "start" {
		result, err = a.startNodeContainer(ctx, nodeId)
	} else {
		result, err = a.stopNodeContainer(ctx, nodeId)
	}

	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	_ = json.NewEncoder(w).Encode(result)
}

func (a *App) startNodeContainer(ctx context.Context, nodeId string) (nodeLifecycleResult, error) {
	if err := validateNodeId(nodeId); err != nil {
		return nodeLifecycleResult{Action: "start", NodeId: nodeId, Ok: false, ExitCode: 1, Error: err.Error()}, err
	}

	// First try native compose service start (works for predeclared services).
	result, err := a.runComposeCommand(ctx, "start", nodeId, []string{"up", "-d", nodeId})
	if err == nil {
		return result, nil
	}

	// Fallback: launch a one-off container via `docker run` using node1's image,
	// network, and mounted cert volume to avoid compose bind-path resolution issues
	// when monitor itself runs in a container.
	if certErr := ensureNodeCertsExist(a.cfg.ProjectDir, nodeId); certErr != nil {
		result.Error = certErr.Error()
		return result, certErr
	}

	return a.runDynamicNodeContainer(ctx, nodeId)
}

func (a *App) stopNodeContainer(ctx context.Context, nodeId string) (nodeLifecycleResult, error) {
	if err := validateNodeId(nodeId); err != nil {
		return nodeLifecycleResult{Action: "stop", NodeId: nodeId, Ok: false, ExitCode: 1, Error: err.Error()}, err
	}

	result, err := a.runComposeCommand(ctx, "stop", nodeId, []string{"stop", nodeId})
	if err == nil {
		return result, nil
	}

	// If container was created via compose run fallback, it may not map to a service name.
	cmd := a.execCommandContext
	if cmd == nil {
		cmd = exec.CommandContext
	}
	raw := cmd(ctx, "docker", "stop", nodeId)
	var out bytes.Buffer
	raw.Stdout = &out
	raw.Stderr = &out
	runErr := raw.Run()
	fallback := nodeLifecycleResult{
		Action: "stop",
		NodeId: nodeId,
		Ok:     runErr == nil,
		Output: strings.TrimSpace(out.String()),
	}
	if runErr != nil {
		fallback.ExitCode = extractExitCode(runErr)
		fallback.Error = runErr.Error()
		return fallback, runErr
	}
	return fallback, nil
}

func (a *App) runComposeCommand(ctx context.Context, action, nodeId string, subArgs []string) (nodeLifecycleResult, error) {
	composeFile := a.cfg.ComposeFile
	if !filepath.IsAbs(composeFile) {
		composeFile = filepath.Join(a.cfg.ProjectDir, composeFile)
	}

	cmd := a.execCommandContext
	if cmd == nil {
		cmd = exec.CommandContext
	}
	args := append([]string{"compose", "-f", composeFile}, subArgs...)
	raw := cmd(ctx, "docker", args...)
	raw.Dir = a.cfg.ProjectDir

	var out bytes.Buffer
	raw.Stdout = &out
	raw.Stderr = &out
	err := raw.Run()

	result := nodeLifecycleResult{
		Action: action,
		NodeId: nodeId,
		Ok:     err == nil,
		Output: strings.TrimSpace(out.String()),
	}
	if err != nil {
		result.ExitCode = extractExitCode(err)
		result.Error = err.Error()
		return result, err
	}
	return result, nil
}

func (a *App) runDynamicNodeContainer(ctx context.Context, nodeId string) (nodeLifecycleResult, error) {
	seed := a.bootstrapSeedNode()
	containerId, err := a.composeServiceContainerId(ctx, "node1")
	if err != nil {
		return nodeLifecycleResult{Action: "start", NodeId: nodeId, Ok: false, ExitCode: 1, Error: err.Error()}, err
	}

	image, network, err := a.inspectContainerRuntime(ctx, containerId)
	if err != nil {
		return nodeLifecycleResult{Action: "start", NodeId: nodeId, Ok: false, ExitCode: 1, Error: err.Error()}, err
	}

	cmd := a.execCommandContext
	if cmd == nil {
		cmd = exec.CommandContext
	}

	args := []string{
		"run", "-d",
		"--name", nodeId,
		"--network", network,
		"--volumes-from", containerId,
		"-v", nodeId + "-data:/data",
		image,
		"-id=" + nodeId,
		"-listen=:9000",
		"-advertise=" + nodeId + ":9000",
		"-db=/data/" + nodeId + ".db",
		"-replicas=2",
		"-ca-cert=/certs/ca.crt",
		"-node-cert=/certs/" + nodeId + ".crt",
		"-node-key=/certs/" + nodeId + ".key",
	}
	if seed != "" {
		args = append(args, "-peers="+seed)
	}

	result, runErr := a.runDockerStartContainer(ctx, nodeId, args, cmd)
	if runErr != nil {
		// If the container name already exists (often from a previously stopped
		// dynamic node), try starting it in place instead of failing lifecycle.
		if isContainerNameConflict(result.Output) {
			started, startErr := a.startExistingContainer(ctx, nodeId)
			if startErr == nil {
				return started, nil
			}

			// Self-heal stale containers that point to removed networks by deleting
			// the conflicting container and recreating it with current runtime config.
			if isMissingNetwork(started.Output) || isMissingNetwork(startErr.Error()) {
				if rmErr := a.removeContainer(ctx, nodeId, cmd); rmErr == nil {
					retried, retryErr := a.runDockerStartContainer(ctx, nodeId, args, cmd)
					if retryErr == nil {
						return retried, nil
					}
				}
			}
		}
		result.ExitCode = extractExitCode(runErr)
		result.Error = runErr.Error()
		return result, runErr
	}

	return result, nil
}

func (a *App) runDockerStartContainer(ctx context.Context, nodeId string, args []string, cmd func(context.Context, string, ...string) *exec.Cmd) (nodeLifecycleResult, error) {
	raw := cmd(ctx, "docker", args...)
	var out bytes.Buffer
	raw.Stdout = &out
	raw.Stderr = &out
	err := raw.Run()

	result := nodeLifecycleResult{
		Action: "start",
		NodeId: nodeId,
		Ok:     err == nil,
		Output: strings.TrimSpace(out.String()),
	}
	return result, err
}

func isContainerNameConflict(output string) bool {
	text := strings.ToLower(output)
	return strings.Contains(text, "is already in use by container") || strings.Contains(text, "container name") && strings.Contains(text, "already in use")
}

func isMissingNetwork(output string) bool {
	text := strings.ToLower(output)
	return strings.Contains(text, "network") && strings.Contains(text, "not found")
}

func (a *App) removeContainer(ctx context.Context, nodeId string, cmd func(context.Context, string, ...string) *exec.Cmd) error {
	raw := cmd(ctx, "docker", "rm", "-f", nodeId)
	var out bytes.Buffer
	raw.Stdout = &out
	raw.Stderr = &out
	if err := raw.Run(); err != nil {
		return fmt.Errorf("remove stale container %s: %w (%s)", nodeId, err, strings.TrimSpace(out.String()))
	}
	return nil
}

func (a *App) startExistingContainer(ctx context.Context, nodeId string) (nodeLifecycleResult, error) {
	cmd := a.execCommandContext
	if cmd == nil {
		cmd = exec.CommandContext
	}
	raw := cmd(ctx, "docker", "start", nodeId)

	var out bytes.Buffer
	raw.Stdout = &out
	raw.Stderr = &out
	err := raw.Run()

	result := nodeLifecycleResult{
		Action: "start",
		NodeId: nodeId,
		Ok:     err == nil,
		Output: strings.TrimSpace(out.String()),
	}
	if err != nil {
		result.ExitCode = extractExitCode(err)
		result.Error = err.Error()
		return result, err
	}

	return result, nil
}

func (a *App) composeServiceContainerId(ctx context.Context, service string) (string, error) {
	composeFile := a.cfg.ComposeFile
	if !filepath.IsAbs(composeFile) {
		composeFile = filepath.Join(a.cfg.ProjectDir, composeFile)
	}

	cmd := a.execCommandContext
	if cmd == nil {
		cmd = exec.CommandContext
	}
	raw := cmd(ctx, "docker", "compose", "-f", composeFile, "ps", "-q", service)
	raw.Dir = a.cfg.ProjectDir

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	raw.Stdout = &stdout
	raw.Stderr = &stderr
	if err := raw.Run(); err != nil {
		combined := strings.TrimSpace(strings.Join([]string{stdout.String(), stderr.String()}, "\n"))
		return "", fmt.Errorf("resolve container for %s: %w (%s)", service, err, combined)
	}

	containerId := parseComposeContainerId(stdout.String())
	if containerId == "" {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return "", fmt.Errorf("service %s has no running container (%s)", service, detail)
		}
		return "", fmt.Errorf("service %s has no running container", service)
	}
	return containerId, nil
}

func parseComposeContainerId(output string) string {
	for _, line := range strings.Split(output, "\n") {
		candidate := strings.TrimSpace(line)
		if containerIdPattern.MatchString(candidate) {
			return candidate
		}
	}
	return ""
}

func (a *App) inspectContainerRuntime(ctx context.Context, containerId string) (string, string, error) {
	cmd := a.execCommandContext
	if cmd == nil {
		cmd = exec.CommandContext
	}
	raw := cmd(ctx, "docker", "inspect", containerId)

	var out bytes.Buffer
	raw.Stdout = &out
	raw.Stderr = &out
	if err := raw.Run(); err != nil {
		return "", "", fmt.Errorf("inspect container %s: %w (%s)", containerId, err, strings.TrimSpace(out.String()))
	}

	type inspectNetwork struct{}
	type inspectContainer struct {
		Config struct {
			Image string `json:"Image"`
		} `json:"Config"`
		NetworkSettings struct {
			Networks map[string]inspectNetwork `json:"Networks"`
		} `json:"NetworkSettings"`
	}

	var containers []inspectContainer
	if err := json.Unmarshal(out.Bytes(), &containers); err != nil {
		return "", "", fmt.Errorf("decode inspect output: %w", err)
	}
	if len(containers) == 0 {
		return "", "", errors.New("empty inspect output")
	}

	image := strings.TrimSpace(containers[0].Config.Image)
	if image == "" {
		return "", "", errors.New("container image not found")
	}

	networks := make([]string, 0, len(containers[0].NetworkSettings.Networks))
	for name := range containers[0].NetworkSettings.Networks {
		networks = append(networks, name)
	}
	if len(networks) == 0 {
		return "", "", errors.New("container network not found")
	}
	sort.Strings(networks)

	return image, networks[0], nil
}

func validateNodeId(nodeId string) error {
	if nodeId == "" {
		return errors.New("nodeId is required")
	}
	if !nodeIdPattern.MatchString(nodeId) {
		return fmt.Errorf("invalid nodeId %q", nodeId)
	}
	return nil
}

func extractExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return 1
}

func ensureNodeCertsExist(projectDir, nodeId string) error {
	crt := filepath.Join(projectDir, "certs", nodeId+".crt")
	key := filepath.Join(projectDir, "certs", nodeId+".key")
	if _, err := os.Stat(crt); err != nil {
		return fmt.Errorf("missing certificate %s (run certs/gen.sh for %s)", crt, nodeId)
	}
	if _, err := os.Stat(key); err != nil {
		return fmt.Errorf("missing key %s (run certs/gen.sh for %s)", key, nodeId)
	}
	return nil
}

func (a *App) bootstrapSeedNode() string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for _, node := range a.nodes {
		if node == nil || node.NodeId == "" || node.NodeId == "monitor" {
			continue
		}
		if node.Status == statusOnline {
			return node.NodeId + ":9000"
		}
	}
	return "node1:9000"
}
