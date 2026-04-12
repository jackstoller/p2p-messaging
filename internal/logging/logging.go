package logging

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type Options struct {
	Verbose bool
}

var verbose atomic.Bool

func Init(opts Options) {
	verbose.Store(opts.Verbose)
}

func EnableVerbose() { verbose.Store(true) }

func Debug(format string, args ...any) {
	if verbose.Load() {
		fmt.Fprintf(os.Stdout, "[debug] "+format+"\n", args...)
	}
}

func Verbose(format string, args ...any) {
	if verbose.Load() {
		fmt.Fprintf(os.Stdout, "[verbose] "+format+"\n", args...)
	}
}

func Info(format string, args ...any) {
	fmt.Fprintf(os.Stdout, format+"\n", args...)
}

func Warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "warning: "+format+"\n", args...)
}

func Error(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
}

func DurationMillis(_ string, duration time.Duration) int64 {
	return duration.Milliseconds()
}

func ParseLevel(raw string) bool {
	return strings.EqualFold(strings.TrimSpace(raw), "verbose") || strings.EqualFold(strings.TrimSpace(raw), "debug")
}
