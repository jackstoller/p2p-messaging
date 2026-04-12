package membership

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// Join runs bootstrap registration, full member sync, and UP announcement.
func (m *Manager) Join(ctx context.Context, bootstrapPeers []string) error {
	bootstrapAddr, err := m.stepRegisterWithBootstrap(ctx, bootstrapPeers)
	if err != nil {
		logging.Error("Bootstrap join failed with error=%v.", err)
		return err
	}
	if err := m.stepSyncMembersFromBootstrap(ctx, bootstrapAddr); err != nil {
		logging.Error("Bootstrap member sync failed with bootstrap addr=%v, error=%v.", bootstrapAddr, err)
		return err
	}

	// Announce up (all vnodes are still disabled)
	m.stepAnnounceUp(ctx)

	return nil
}

func (m *Manager) stepRegisterWithBootstrap(ctx context.Context, peers []string) (string, error) {
	req := &pb.RegisterNodeRequest{Member: m.SelfProto()}

	// Try all provided bootstrap nodes
	for _, addr := range peers {
		client, err := m.MembershipClient(ctx, addr)
		if err != nil {
			continue
		}

		var resp *pb.RegisterNodeResponse
		err = util.Do(ctx, util.RPC, func() error {
			var callErr error
			resp, callErr = client.RegisterNode(ctx, req)
			return callErr
		})
		if err != nil {
			continue
		}
		if !resp.Accepted {
			continue
		}


		// Exit on the first successful registration
		return addr, nil
	}

	logging.Error("No bootstrap peer accepted registration with bootstrap peers=%v.", len(peers))
	return "", errors.New("membership: no bootstrap peer accepted registration")
}

func (m *Manager) stepSyncMembersFromBootstrap(ctx context.Context, addr string) error {
	client, err := m.MembershipClient(ctx, addr)
	if err != nil {
		logging.Error("Could not dial bootstrap peer for member sync with peer addr=%v, error=%v.", addr, err)
		return fmt.Errorf("membership: dial bootstrap for sync: %w", err)
	}

	resp, err := client.ListNodes(ctx, &pb.ListNodesRequest{RequestingNodeId: m.selfId})
	if err != nil {
		logging.Error("Member sync RPC failed with peer addr=%v, error=%v.", addr, err)
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
