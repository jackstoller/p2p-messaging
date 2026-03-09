package util

import (
	"context"
	"fmt"
	"time"
)

// Config defines retry behaviour for a specific call site.
type Config struct {
	// MaxAttempts is the total number of attempts. -1 means unlimited.
	MaxAttempts int
	// InitialDelay is the pause before the second attempt.
	InitialDelay time.Duration
	// Multiplier scales the delay after each failure.
	// 1.0 = constant interval, 2.0 = exponential backoff.
	Multiplier float64
	// MaxDelay caps the delay when using exponential backoff. 0 = no cap.
	MaxDelay time.Duration
}

// Named presets to avoid ad hoc retry settings.
var (
	// HeartbeatRetry runs 3 attempts, 3 seconds apart.
	HeartbeatRetry = Config{MaxAttempts: 3, InitialDelay: 3 * time.Second, Multiplier: 1.0}

	// TransferBackoff retries forever with exponential delay up to 30 minutes.
	TransferBackoff = Config{MaxAttempts: -1, InitialDelay: 1 * time.Second, Multiplier: 2.0, MaxDelay: 30 * time.Minute}

	// TransferComplete runs 5 attempts, 500 milliseconds apart.
	TransferComplete = Config{MaxAttempts: 5, InitialDelay: 500 * time.Millisecond, Multiplier: 1.0}

	// RPC runs 3 attempts, 200 milliseconds apart for short inter-node calls.
	RPC = Config{MaxAttempts: 3, InitialDelay: 200 * time.Millisecond, Multiplier: 1.0}
)

// Do calls fn until it returns nil, ctx is cancelled, or max attempts are
// exhausted (whichever comes first). The last non-nil error is returned.
// fn should be idempotent. ctx cancellation is checked between attempts only;
// long-running fn bodies should accept and honour the ctx themselves.
func Do(ctx context.Context, cfg Config, fn func() error) error {
	delay := cfg.InitialDelay
	var lastErr error

	for attempt := 0; cfg.MaxAttempts < 0 || attempt < cfg.MaxAttempts; attempt++ {
		if lastErr = fn(); lastErr == nil {
			return nil
		}

		// Don't wait after the final attempt.
		isLastAttempt := cfg.MaxAttempts > 0 && attempt == cfg.MaxAttempts-1
		if isLastAttempt {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		if cfg.Multiplier > 1.0 {
			next := time.Duration(float64(delay) * cfg.Multiplier)
			if cfg.MaxDelay > 0 && next > cfg.MaxDelay {
				next = cfg.MaxDelay
			}
			delay = next
		}
	}

	if lastErr != nil {
		return fmt.Errorf("all %d attempt(s) failed: %w", cfg.MaxAttempts, lastErr)
	}
	return fmt.Errorf("all attempts exhausted")
}
