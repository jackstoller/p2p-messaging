package membership

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

const (
	heartbeatInterval = 1 * time.Second
	pingTimeout       = 3 * time.Second
	pingRetries       = 3
	pingRetryDelay    = 3 * time.Second
	maxConfirmPeers   = 5
)

// RunHeartbeats starts the heartbeat loop and runs until ctx is canceled.
func (m *Manager) RunHeartbeats(ctx context.Context) {
	log := logging.Component("membership.heartbeat")
	log.Info("heartbeat.loop", logging.Outcome(logging.OutcomeStarted), logging.DurationMillis("interval", heartbeatInterval))
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("heartbeat.loop", logging.Outcome(logging.OutcomeSucceeded), "reason", "context_cancelled")
			return
		case <-ticker.C:
			m.runHeartbeatTick(ctx)
		}
	}
}

// runHeartbeatTick pings active peers except self and local suspects.
func (m *Manager) runHeartbeatTick(ctx context.Context) {
	log := logging.Component("membership.heartbeat")
	targets := m.ActivePeers()
	log.Debug("heartbeat.tick", logging.Outcome(logging.OutcomeStarted), "targets", len(targets))
	for _, target := range targets {
		go m.pingNode(ctx, target)
	}
}

// pingNode runs the full heartbeat flow for a single peer
func (m *Manager) pingNode(ctx context.Context, target Peer) {
	log := logging.Component("membership.heartbeat")
	if !m.beginHeartbeatProbe(target.NodeId) {
		log.Debug("heartbeat.ping", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, "reason", "probe_inflight")
		return
	}
	defer m.endHeartbeatProbe(target.NodeId)

	if m.IsDown(target.NodeId) {
		log.Debug("heartbeat.ping", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, "reason", "peer_down")
		return
	}
	log.Debug("heartbeat.ping", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address)

	// Step 1 first ping attempt
	if m.sendPing(ctx, target) == nil {
		m.ClearSuspect(target.NodeId)
		log.Debug("heartbeat.ping", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, "attempt", 1)
		return
	}

	// Step 2 mark as suspect and retry three times
	m.MarkSuspect(target.NodeId)
	log.Warn("heartbeat.suspect", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, "attempt", 0)

	for i := 0; i < pingRetries; i++ {
		select {
		case <-ctx.Done():
			log.Info("heartbeat.ping", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, target.NodeId, "reason", "context_cancelled")
			return
		case <-time.After(pingRetryDelay):
		}

		if m.IsDown(target.NodeId) {
			log.Info("heartbeat.ping", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, target.NodeId, "reason", "peer_marked_down")
			return
		}
		if !m.IsSuspect(target.NodeId) {
			// Peer came back.
			log.Info("heartbeat.ping", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, target.NodeId, "reason", "suspect_cleared")
			return
		}

		if m.sendPing(ctx, target) == nil {
			m.ClearSuspect(target.NodeId)
			log.Info("heartbeat.recovered", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, "attempt", i+2)
			return
		}
		log.Warn("heartbeat.suspect", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, "attempt", i+1)
	}

	// Step 3 retries exhausted so gather peer confirmation
	log.Warn("heartbeat.confirm", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address)
	m.confirmSuspect(ctx, target)
}

// confirmSuspect asks up to 5 non-suspect active peers whether they can also
// not reach the suspect. A strict majority triggers declareDown.
func (m *Manager) confirmSuspect(ctx context.Context, suspect Peer) {
	log := logging.Component("membership.heartbeat")
	if m.IsDown(suspect.NodeId) {
		log.Debug("heartbeat.confirm", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, suspect.NodeId, "reason", "peer_down")
		return
	}

	// Step 1 pick non suspect peers excluding self
	candidates := m.confirmerCandidates(suspect.NodeId)
	log.Info("heartbeat.confirm", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerId, suspect.NodeId, "candidates", len(candidates))

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
				log.Warn("heartbeat.confirm.poll", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerId, p.NodeId, logging.AttrPeerAddr, p.Address, "suspect_id", suspect.NodeId, logging.Err(err))
				resultCh <- pollResult{responsive: false}
				return
			}
			resp, err := client.ConfirmSuspect(pollCtx, &pb.ConfirmSuspectRequest{
				SuspectNodeId: suspect.NodeId,
			})
			if err != nil {
				log.Warn("heartbeat.confirm.poll", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerId, p.NodeId, logging.AttrPeerAddr, p.Address, "suspect_id", suspect.NodeId, logging.Err(err))
				resultCh <- pollResult{responsive: false}
				return
			}
			log.Debug("heartbeat.confirm.poll", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, p.NodeId, logging.AttrPeerAddr, p.Address, "suspect_id", suspect.NodeId, "confirmed", resp.Confirmed)
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
		log.Debug("heartbeat.confirm", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, suspect.NodeId, "reason", "peer_down_after_poll")
		return
	}

	// Step 3 determine quorum
	// On meshes of 3 nodes or fewer there may be no peers to ask.
	// If denominator is zero, repeated local failures are enough.
	quorumMet := denominator == 0 || float64(confirmations)/float64(denominator) > 0.5
	log.Info("heartbeat.confirm", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, suspect.NodeId, "confirmations", confirmations, "responsive_peers", denominator, "quorum_met", quorumMet)
	if quorumMet {
		m.declareDown(ctx, suspect)
	}
}

// declareDown marks a peer dead locally and broadcasts NodeStatus DOWN.
// The first broadcaster wins and duplicate DOWN messages are ignored.
func (m *Manager) declareDown(ctx context.Context, dead Peer) {
	log := logging.Component("membership.heartbeat")
	// Mark locally first so IsDown is true before broadcasting
	m.MarkDown(dead.NodeId)
	log.Warn("heartbeat.declare_down", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, dead.NodeId, logging.AttrPeerAddr, dead.Address)

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
	log.Info("heartbeat.declare_down.broadcast", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerId, dead.NodeId, "recipients", len(recipients))
	util.Broadcast(ctx, recipients, pingTimeout, func(broadcastCtx context.Context, peer Peer) error {
		client, err := m.MembershipClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.NodeStatus(broadcastCtx, req)
		return err
	})
	log.Info("heartbeat.declare_down.broadcast", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, dead.NodeId, "recipients", len(recipients))
}

// Helper methods

// sendPing sends one Ping to target with a timeout.
func (m *Manager) sendPing(ctx context.Context, target Peer) error {
	log := logging.Component("membership.heartbeat")
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	client, err := m.MembershipClient(pingCtx, target.Address)
	if err != nil {
		log.Warn("heartbeat.ping.rpc", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, logging.Err(err))
		return err
	}
	_, err = client.Ping(pingCtx, &pb.PingRequest{NodeId: m.selfId})
	if err != nil {
		log.Warn("heartbeat.ping.rpc", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address, logging.Err(err))
		return err
	}
	log.Debug("heartbeat.ping.rpc", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, target.NodeId, logging.AttrPeerAddr, target.Address)
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
	logging.Component("membership.heartbeat").Debug("heartbeat.confirm.candidates", logging.Outcome(logging.OutcomeSucceeded), "suspect_id", suspectId, "candidates", len(pool))
	return pool
}

func (m *Manager) beginHeartbeatProbe(nodeId string) bool {
	m.heartbeatMu.Lock()
	defer m.heartbeatMu.Unlock()

	if _, exists := m.heartbeatInFlight[nodeId]; exists {
		return false
	}
	m.heartbeatInFlight[nodeId] = struct{}{}
	return true
}

func (m *Manager) endHeartbeatProbe(nodeId string) {
	m.heartbeatMu.Lock()
	delete(m.heartbeatInFlight, nodeId)
	m.heartbeatMu.Unlock()
}
