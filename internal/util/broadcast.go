package util

import (
	"context"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
)

// BroadcastPeer represents a peer that can receive a broadcast.
type BroadcastPeer interface {
	GetNodeId() string
}

// BroadcastSender is a function that sends a message to a single peer.
// It receives the peer, a context with timeout already applied, and should
// handle the actual RPC call. Errors are logged but not returned to the caller
// (fire-and-forget pattern).
type BroadcastSender[P BroadcastPeer] func(ctx context.Context, peer P) error

// Broadcast sends a message to all peers in parallel using the provided sender function.
// Each peer is sent to in its own goroutine with the given timeout.
// Errors are logged but do not block or affect other peers.
// If peers is empty, this is a no-op.
func Broadcast[P BroadcastPeer](ctx context.Context, peers []P, timeout time.Duration, sender BroadcastSender[P]) {
	log := logging.Component("util.broadcast")
	log.Debug("broadcast.dispatch", logging.Outcome(logging.OutcomeStarted), "peers", len(peers), logging.DurationMillis("timeout", timeout))
	for _, peer := range peers {
		go func(p P) {
			broadcastCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			log.Debug("broadcast.send", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerId, p.GetNodeId())
			if err := sender(broadcastCtx, p); err != nil {
				log.Warn("broadcast.send", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerId, p.GetNodeId(), logging.Err(err))
				return
			}
			log.Debug("broadcast.send", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, p.GetNodeId())
		}(peer)
	}
	_ = ctx
}
