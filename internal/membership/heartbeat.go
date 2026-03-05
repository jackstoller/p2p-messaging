package membership

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	pb "github.com/jackstoller/p2p-messaging/proto/mesh"
)

const (
	heartbeatInterval = 5 * time.Second
	pingTimeout       = 3 * time.Second
	pingRetries       = 3
	pingRetryDelay    = 3 * time.Second
	maxConfirmPeers   = 5
)

// RunHeartbeats starts the heartbeat loop. Blocks until ctx is cancelled.
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

// runHeartbeatTick pings all active, non-suspect, non-self peers in parallel.
func (m *Manager) runHeartbeatTick(ctx context.Context) {
	for _, target := range m.ActivePeers() {
		go m.pingNode(ctx, target)
	}
}

// pingNode runs the full heartbeat flow for a single peer
func (m *Manager) pingNode(ctx context.Context, target Peer) {
	if m.IsDown(suspect.NodeID) { return }

	// Step 1: First ping attempt.
	if m.sendPing(ctx, target) == nil {
		m.ClearSuspect(target.NodeID)
		return
	}

	// Step 2: Mark locally as suspect and retry 3 times with 3s delay.
	m.MarkSuspect(target.NodeID)
	slog.Warn("peer suspect", "nodeID", target.NodeID, "attempt", 0)

	for i := 0; i < pingRetries; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pingRetryDelay):
		}

		if m.IsDown(suspect.NodeID) { return }
		if !m.IsSuspect(target.NodeID) {
			// Peer came back.
			return
		}

		if m.sendPing(ctx, target) == nil {
			m.ClearSuspect(target.NodeID)
			slog.Info("peer recovered", "nodeID", target.NodeID)
			return
		}
		slog.Warn("peer suspect", "nodeID", target.NodeID, "attempt", i+1)
	}

	// Step 3: All retries exhausted - gather peer confirmation.
	m.confirmSuspect(ctx, target)
}

// confirmSuspect asks up to 5 non-suspect active peers whether they can also
// not reach the suspect. A strict majority triggers declareDown.
func (m *Manager) confirmSuspect(ctx context.Context, suspect Peer) {
	if m.IsDown(suspect.NodeID) { return }

	// Step 1: Get non-suspect, non-self peers.
	candidates := m.confirmerCandidates(suspect.NodeID)

	type pollResult struct {
		responsive bool
		confirmed  bool
	}
	resultCh := make(chan pollResult, len(candidates))

	// Step 2: Check with peers
	for _, peer := range candidates {
		go func(p Peer) {
			if m.IsDown(suspect.NodeID) {
				// Already resolved, drain channel
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
				SuspectNodeId: suspect.NodeID,
			})
			if err != nil {
				resultCh <- pollResult{responsive: false}
				return
			}
			resultCh <- pollResult{responsive: true, confirmed: resp.Confirmed}
		}(peer)
	}

	// Collect results.
	confirmations, denominator := 0, 0
	for range candidates {
		r := <-resultCh

		// Unresponsive nodes are excluded
		if !r.responsive { continue }
		
		denominator++
		if r.confirmed {
			confirmations++
		}
	}

	if m.IsDown(suspect.NodeID) { return }

	// Step 3: Determine quorum.
	// On meshes of 3 nodes or fewer there may be no other peers to ask
	// (denominator == 0); in that case our own repeated failures are sufficient.
	quorumMet := denominator == 0 || float64(confirmations)/float64(denominator) > 0.5
	if quorumMet {
		m.declareDown(ctx, suspect)
	}
}

// declareDown marks a peer dead locally and broadcasts NodeStatus(DOWN) to all
// UP peers. The first node to broadcast wins; duplicate DOWN messages are
// ignored by recipients.
func (m *Manager) declareDown(ctx context.Context, dead Peer) {
	// Mark locally first so IsDown() is true before we broadcast.
	m.MarkDown(dead.NodeID)
	slog.Warn("declaring peer down", "nodeID", dead.NodeID)

	req := &pb.NodeStatusRequest{
		NodeId: dead.NodeID,
		State:  pb.PeerState_PEER_DOWN,
	}
	for _, peer := range m.upPeers() { // TODO: add a broadcast util
		if peer.NodeID == dead.NodeID {
			continue
		}
		go func(p Peer) {
			broadcastCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			defer cancel()
			client, err := m.MembershipClient(broadcastCtx, p.Address)
			if err != nil {
				return
			}
			if _, err = client.NodeStatus(broadcastCtx, req); err != nil {
				slog.Debug("DOWN broadcast failed", "to", p.NodeID, "err", err)
			}
		}(peer)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// sendPing sends a single Ping to target's address with a pingTimeout deadline.
func (m *Manager) sendPing(ctx context.Context, target Peer) error {
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	client, err := m.MembershipClient(pingCtx, target.Address)
	if err != nil {
		return err
	}
	_, err = client.Ping(pingCtx, &pb.PingRequest{NodeId: m.selfID})
	return err
}

// confirmerCandidates returns up to maxConfirmPeers shuffled UP non-suspect
// peers, excluding self and the suspect.
func (m *Manager) confirmerCandidates(suspectID string) []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.suspectsMu.Lock()
	defer m.suspectsMu.Unlock()

	var pool []Peer
	for _, p := range m.members {
		if p.NodeID == m.selfID || p.NodeID == suspectID || p.State == PeerDown {
			continue
		}
		if _, isSuspect := m.suspects[p.NodeID]; isSuspect {
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
