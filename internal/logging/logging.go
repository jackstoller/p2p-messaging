package logging

import (
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	AttrService   = "service"
	AttrComponent = "component"
	AttrNodeId    = "node_id"
	AttrNodeAddr  = "node_addr"
	AttrPeerId    = "peer_id"
	AttrPeerAddr  = "peer_addr"
	AttrVnodeId   = "vnode_id"
	AttrKey       = "key"
	AttrOutcome   = "outcome"
	AttrError     = "err"

	OutcomeStarted   = "started"
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
	OutcomeRejected  = "rejected"
	OutcomeSkipped   = "skipped"
)

type Options struct {
	Level         slog.Leveler
	NodeId        string
	AdvertiseAddr string
}

func Init(opts Options) {
	level := opts.Level
	if level == nil {
		level = slog.LevelInfo
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.TimeKey:
				return slog.String("ts", attr.Value.Time().UTC().Format(time.RFC3339Nano))
			case slog.LevelKey:
				return slog.String("level", strings.ToLower(attr.Value.String()))
			case slog.MessageKey:
				return slog.Attr{Key: "event", Value: attr.Value}
			case AttrError:
				if err, ok := attr.Value.Any().(error); ok && err != nil {
					return slog.String(AttrError, err.Error())
				}
			}
			return attr
		},
	})

	logger := slog.New(handler).With(AttrService, "p2p-mesh-node")
	if opts.NodeId != "" {
		logger = logger.With(AttrNodeId, opts.NodeId)
	}
	if opts.AdvertiseAddr != "" {
		logger = logger.With(AttrNodeAddr, opts.AdvertiseAddr)
	}

	slog.SetDefault(logger)
}

func Component(name string) *slog.Logger {
	return slog.Default().With(AttrComponent, name)
}

func Err(err error) slog.Attr {
	if err == nil {
		return slog.String(AttrError, "")
	}
	return slog.String(AttrError, err.Error())
}

func Outcome(value string) slog.Attr {
	return slog.String(AttrOutcome, value)
}

func DurationMillis(name string, duration time.Duration) slog.Attr {
	return slog.Int64(name+"_ms", duration.Milliseconds())
}

func ParseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		fallthrough
	default:
		return slog.LevelInfo
	}
}
