package membership

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

const (
	heartbeatInterval = 5 * time.Second
	pingTimeout       = 3 * time.Second
	pingRetries       = 3
	pingRetryDelay    = 3 * time.Second
	maxConfirmPeers   = 5
)

// RunHeartbeats starts the heartbeat loop and runs until ctx is canceled.
func (m *Manager) RunHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runHeartbeatTick(ctx)
		}
	}
}

// runHeartbeatTick pings active peers except self and local suspects.
func (m *Manager) runHeartbeatTick(ctx context.Context) {
	for _, target := range m.ActivePeers() {
		go m.pingNode(ctx, target)
	}
}

// pingNode runs the full heartbeat flow for a single peer
func (m *Manager) pingNode(ctx context.Context, target Peer) {
	if m.IsDown(target.NodeId) {
		return
	}

	// Step 1 first ping attempt
	if m.sendPing(ctx, target) == nil {
		m.ClearSuspect(target.NodeId)
		return
	}

	// Step 2 mark as suspect and retry three times
	m.MarkSuspect(target.NodeId)
	slog.Warn("peer suspect", "nodeId", target.NodeId, "attempt", 0)

	for i := 0; i < pingRetries; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pingRetryDelay):
		}

		if m.IsDown(target.NodeId) {
			return
		}
		if !m.IsSuspect(target.NodeId) {
			// Peer came back.
			return
		}

		if m.sendPing(ctx, target) == nil {
			m.ClearSuspect(target.NodeId)
			slog.Info("peer recovered", "nodeId", target.NodeId)
			return
		}
		slog.Warn("peer suspect", "nodeId", target.NodeId, "attempt", i+1)
	}

	// Step 3 retries exhausted so gather peer confirmation
	m.confirmSuspect(ctx, target)
}

// confirmSuspect asks up to 5 non-suspect active peers whether they can also
// not reach the suspect. A strict majority triggers declareDown.
func (m *Manager) confirmSuspect(ctx context.Context, suspect Peer) {
	if m.IsDown(suspect.NodeId) {
		return
	}

	// Step 1 pick non suspect peers excluding self
	candidates := m.confirmerCandidates(suspect.NodeId)

	type pollResult struct {
		responsive bool
		confirmed  bool
	}
	resultCh := make(chan pollResult, len(candidates))

	// Step 2 ask peers in parallel
	for _, peer := range candidates {
		go func(p Peer) {
			if m.IsDown(suspect.NodeId) {
				// Already resolved so drain the channel
				resultCh <- pollResult{}
				return
			}
			pollCtx, cancel := context.WithTimeout(ctx, pingTimeout)
			defer cancel()

			client, err := m.MembershipClient(pollCtx, p.Address)
			if err != nil {
				resultCh <- pollResult{responsive: false}
				return
			}
			resp, err := client.ConfirmSuspect(pollCtx, &pb.ConfirmSuspectRequest{
				SuspectNodeId: suspect.NodeId,
			})
			if err != nil {
				resultCh <- pollResult{responsive: false}
				return
			}
			resultCh <- pollResult{responsive: true, confirmed: resp.Confirmed}
		}(peer)
	}

	// Collect results
	confirmations, denominator := 0, 0
	for range candidates {
		r := <-resultCh

		// Exclude unresponsive nodes
		if !r.responsive {
			continue
		}

		denominator++
		if r.confirmed {
			confirmations++
		}
	}

	if m.IsDown(suspect.NodeId) {
		return
	}

	// Step 3 determine quorum
	// On meshes of 3 nodes or fewer there may be no peers to ask.
	// If denominator is zero, repeated local failures are enough.
	quorumMet := denominator == 0 || float64(confirmations)/float64(denominator) > 0.5
	if quorumMet {
		m.declareDown(ctx, suspect)
	}
}

// declareDown marks a peer dead locally and broadcasts NodeStatus DOWN.
// The first broadcaster wins and duplicate DOWN messages are ignored.
func (m *Manager) declareDown(ctx context.Context, dead Peer) {
	// Mark locally first so IsDown is true before broadcasting
	m.MarkDown(dead.NodeId)
	slog.Warn("declaring peer down", "nodeId", dead.NodeId)

	req := &pb.NodeStatusRequest{
		NodeId: dead.NodeId,
		State:  pb.PeerState_PEER_DOWN,
	}

	// Filter out the dead peer from recipients
	var recipients []Peer
	for _, peer := range m.upPeers() {
		if peer.NodeId != dead.NodeId {
			recipients = append(recipients, peer)
		}
	}

	// Broadcast to all remaining UP peers
	util.Broadcast(ctx, recipients, pingTimeout, func(broadcastCtx context.Context, peer Peer) error {
		client, err := m.MembershipClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.NodeStatus(broadcastCtx, req)
		return err
	})
}

// Helper methods

// sendPing sends one Ping to target with a timeout.
func (m *Manager) sendPing(ctx context.Context, target Peer) error {
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	client, err := m.MembershipClient(pingCtx, target.Address)
	if err != nil {
		return err
	}
	_, err = client.Ping(pingCtx, &pb.PingRequest{NodeId: m.selfId})
	return err
}

// confirmerCandidates returns up to maxConfirmPeers shuffled non suspect peers.
// The list excludes self and the current suspect.
func (m *Manager) confirmerCandidates(suspectId string) []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.suspectsMu.RLock()
	defer m.suspectsMu.RUnlock()

	var pool []Peer
	for _, p := range m.members {
		if p.NodeId == m.selfId || p.NodeId == suspectId || p.State == PeerDown {
			continue
		}
		if _, isSuspect := m.suspects[p.NodeId]; isSuspect {
			continue
		}
		pool = append(pool, *p)
	}

	rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	if len(pool) > maxConfirmPeers {
		pool = pool[:maxConfirmPeers]
	}
	return pool
}
