package membership

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// Join runs bootstrap registration, full member sync, and UP announcement.
func (m *Manager) Join(ctx context.Context, bootstrapPeers []string) error {
	bootstrapAddr, err := m.stepRegisterWithBootstrap(ctx, bootstrapPeers)
	if err != nil {
		return err
	}
	if err := m.stepSyncMembersFromBootstrap(ctx, bootstrapAddr); err != nil {
		return err
	}

	// Announce up (all vnodes are still disabled)
	m.stepAnnounceUp(ctx)

	slog.Info("joined mesh", "nodeId", m.selfId, "peers", len(m.ActivePeers()))
	return nil
}

func (m *Manager) stepRegisterWithBootstrap(ctx context.Context, peers []string) (string, error) {
	req := &pb.RegisterNodeRequest{Member: m.SelfProto()}

	// Try all provided bootstrap nodes
	for _, addr := range peers {
		client, err := m.MembershipClient(ctx, addr)
		if err != nil {
			slog.Warn("bootstrap unreachable", "addr", addr, "err", err)
			continue
		}

		var resp *pb.RegisterNodeResponse
		err = util.Do(ctx, util.RPC, func() error {
			var callErr error
			resp, callErr = client.RegisterNode(ctx, req)
			return callErr
		})
		if err != nil {
			slog.Warn("RegisterNode failed", "addr", addr, "err", err)
			continue
		}
		if !resp.Accepted {
			slog.Warn("RegisterNode rejected", "addr", addr)
			continue
		}

		slog.Info("registered with bootstrap", "addr", addr)

		// Exit on the first successful registration
		return addr, nil
	}

	return "", errors.New("membership: no bootstrap peer accepted registration")
}

func (m *Manager) stepSyncMembersFromBootstrap(ctx context.Context, addr string) error {
	client, err := m.MembershipClient(ctx, addr)
	if err != nil {
		return fmt.Errorf("membership: dial bootstrap for sync: %w", err)
	}

	resp, err := client.ListNodes(ctx, &pb.ListNodesRequest{RequestingNodeId: m.selfId})
	if err != nil {
		return fmt.Errorf("membership: ListNodes from %s: %w", addr, err)
	}

	m.mu.Lock()
	for _, pm := range resp.Members {
		if pm.NodeId == m.selfId {
			continue
		}
		peer := protoToPeer(pm)
		m.members[peer.NodeId] = &peer
	}
	m.rebuildRingLocked()
	m.mu.Unlock()

	slog.Info("synced member list", "from", addr, "count", len(resp.Members))
	return nil
}

func (m *Manager) stepAnnounceUp(ctx context.Context) {
	m.broadcastStatus(ctx, pb.PeerState_PEER_UP)
}

func (m *Manager) broadcastStatus(ctx context.Context, state pb.PeerState) {
	req := &pb.NodeStatusRequest{NodeId: m.selfId, State: state}
	if state == pb.PeerState_PEER_UP {
		req.Vnodes = m.selfVnodesProto()
	}

	util.Broadcast(ctx, m.upPeers(), 3*time.Second, func(broadcastCtx context.Context, peer Peer) error {
		client, err := m.MembershipClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.NodeStatus(broadcastCtx, req)
		return err
	})
}
