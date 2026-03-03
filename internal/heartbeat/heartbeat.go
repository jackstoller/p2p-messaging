package heartbeat

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	pb "github.com/jackstoller/p2p-messaging/proto/node"
)

const (
	Interval         = 2 * time.Second
	Timeout          = 3 * time.Second
	MissesForSuspect = 3 // consecutive missed acks before suspecting
)

// Monitor runs the heartbeat loop for this node.
// It pings all ACTIVE peers. On repeated failure it gossips a suspect
// report. When quorum agrees, it calls onConfirmDead.
type Monitor struct {
	mgr           *membership.Manager
	missCount     map[string]int // nodeID → consecutive misses
	onConfirmDead func(nodeID string)
}

// New creates a Monitor.
// onConfirmDead is called (once) when a node is confirmed dead by quorum.
func New(mgr *membership.Manager, onConfirmDead func(nodeID string)) *Monitor {
	return &Monitor{
		mgr:           mgr,
		missCount:     map[string]int{},
		onConfirmDead: onConfirmDead,
	}
}

// Run starts the heartbeat loop. Blocks until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

func (m *Monitor) tick(ctx context.Context) {
	peers := m.mgr.ActiveMembers()
	self := m.mgr.Self()

	for _, peer := range peers {
		go m.ping(ctx, self, peer)
	}
}

func (m *Monitor) ping(ctx context.Context, self membership.Member, peer membership.Member) {
	pingCtx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	client, err := m.mgr.MembershipClient(pingCtx, peer.Address)
	if err != nil {
		m.recordMiss(ctx, self, peer.NodeID)
		return
	}

	_, err = client.Heartbeat(pingCtx, &pb.HeartbeatRequest{
		NodeId: self.NodeID,
		SentAt: time.Now().UnixMilli(),
	})
	if err != nil {
		m.recordMiss(ctx, self, peer.NodeID)
		return
	}

	// Successful pong - clear any miss history.
	m.missCount[peer.NodeID] = 0
	m.mgr.ClearSuspect(peer.NodeID)
}

func (m *Monitor) recordMiss(ctx context.Context, self membership.Member, nodeID string) {
	m.missCount[nodeID]++
	misses := m.missCount[nodeID]

	slog.Warn("heartbeat miss", "node", nodeID, "consecutive", misses)

	if misses < MissesForSuspect {
		return
	}

	// Broadcast suspect to all peers.
	m.gossipSuspect(ctx, self, nodeID)

	// Also record our own suspicion locally.
	if quorum := m.mgr.RecordSuspect(nodeID, self.NodeID); quorum {
		slog.Warn("quorum reached, confirming dead", "node", nodeID)
		m.mgr.MarkDead(nodeID)
		m.gossipConfirmDead(ctx, self, nodeID)
		if m.onConfirmDead != nil {
			m.onConfirmDead(nodeID)
		}
		delete(m.missCount, nodeID)
	}
}

func (m *Monitor) gossipSuspect(ctx context.Context, self membership.Member, nodeID string) {
	peers := m.mgr.ActiveMembers()
	for _, peer := range peers {
		if peer.NodeID == nodeID {
			continue // don't tell the dead node it's dead
		}
		go func(peer membership.Member) {
			gCtx, cancel := context.WithTimeout(ctx, Timeout)
			defer cancel()
			client, err := m.mgr.MembershipClient(gCtx, peer.Address)
			if err != nil {
				return
			}
			_, _ = client.SuspectNode(gCtx, &pb.SuspectRequest{
				SuspectedNodeId: nodeID,
				ReporterNodeId:  self.NodeID,
				ReportedAt:      time.Now().UnixMilli(),
			})
		}(peer)
	}
}

func (m *Monitor) gossipConfirmDead(ctx context.Context, self membership.Member, nodeID string) {
	peers := m.mgr.ActiveMembers()
	confirmedBy := []string{self.NodeID}
	for _, peer := range peers {
		if peer.NodeID == nodeID {
			continue
		}
		go func(peer membership.Member) {
			gCtx, cancel := context.WithTimeout(ctx, Timeout)
			defer cancel()
			client, err := m.mgr.MembershipClient(gCtx, peer.Address)
			if err != nil {
				return
			}
			_, _ = client.ConfirmDead(gCtx, &pb.ConfirmDeadRequest{
				DeadNodeId:  nodeID,
				ConfirmedBy: confirmedBy,
			})
		}(peer)
	}
}
