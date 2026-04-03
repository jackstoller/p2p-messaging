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
	log := logging.Component("membership.join")
	log.Info("membership.join", logging.Outcome(logging.OutcomeStarted), "bootstrap_peers", len(bootstrapPeers))
	bootstrapAddr, err := m.stepRegisterWithBootstrap(ctx, bootstrapPeers)
	if err != nil {
		log.Error("membership.join", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return err
	}
	if err := m.stepSyncMembersFromBootstrap(ctx, bootstrapAddr); err != nil {
		log.Error("membership.join.sync", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerAddr, bootstrapAddr, logging.Err(err))
		return err
	}

	// Announce up (all vnodes are still disabled)
	m.stepAnnounceUp(ctx)

	log.Info("membership.join", logging.Outcome(logging.OutcomeSucceeded), "bootstrap_addr", bootstrapAddr, "active_peers", len(m.ActivePeers()))
	return nil
}

func (m *Manager) stepRegisterWithBootstrap(ctx context.Context, peers []string) (string, error) {
	log := logging.Component("membership.join")
	req := &pb.RegisterNodeRequest{Member: m.SelfProto()}

	// Try all provided bootstrap nodes
	for _, addr := range peers {
		log.Info("membership.bootstrap.register", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerAddr, addr)
		client, err := m.MembershipClient(ctx, addr)
		if err != nil {
			log.Warn("membership.bootstrap.register", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerAddr, addr, logging.Err(err))
			continue
		}

		var resp *pb.RegisterNodeResponse
		err = util.Do(ctx, util.RPC, func() error {
			var callErr error
			resp, callErr = client.RegisterNode(ctx, req)
			return callErr
		})
		if err != nil {
			log.Warn("membership.bootstrap.register", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerAddr, addr, logging.Err(err))
			continue
		}
		if !resp.Accepted {
			log.Warn("membership.bootstrap.register", logging.Outcome(logging.OutcomeRejected), logging.AttrPeerAddr, addr)
			continue
		}

		log.Info("membership.bootstrap.register", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerAddr, addr)

		// Exit on the first successful registration
		return addr, nil
	}

	log.Error("membership.bootstrap.register", logging.Outcome(logging.OutcomeFailed), "bootstrap_peers", len(peers))
	return "", errors.New("membership: no bootstrap peer accepted registration")
}

func (m *Manager) stepSyncMembersFromBootstrap(ctx context.Context, addr string) error {
	log := logging.Component("membership.join")
	log.Info("membership.bootstrap.sync", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerAddr, addr)
	client, err := m.MembershipClient(ctx, addr)
	if err != nil {
		log.Error("membership.bootstrap.sync", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerAddr, addr, logging.Err(err))
		return fmt.Errorf("membership: dial bootstrap for sync: %w", err)
	}

	resp, err := client.ListNodes(ctx, &pb.ListNodesRequest{RequestingNodeId: m.selfId})
	if err != nil {
		log.Error("membership.bootstrap.sync", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerAddr, addr, logging.Err(err))
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

	log.Info("membership.bootstrap.sync", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerAddr, addr, "members", len(resp.Members), "ring_entries", m.Ring.Len())
	return nil
}

func (m *Manager) stepAnnounceUp(ctx context.Context) {
	logging.Component("membership.join").Info("membership.status.broadcast", logging.Outcome(logging.OutcomeStarted), "state", pb.PeerState_PEER_UP.String())
	m.broadcastStatus(ctx, pb.PeerState_PEER_UP)
}

func (m *Manager) broadcastStatus(ctx context.Context, state pb.PeerState) {
	log := logging.Component("membership.join")
	req := &pb.NodeStatusRequest{NodeId: m.selfId, State: state}
	if state == pb.PeerState_PEER_UP {
		req.Vnodes = m.selfVnodesProto()
	}
	log.Info("membership.status.broadcast", logging.Outcome(logging.OutcomeStarted), "state", state.String(), "recipients", len(m.upPeers()))

	util.Broadcast(ctx, m.upPeers(), 3*time.Second, func(broadcastCtx context.Context, peer Peer) error {
		client, err := m.MembershipClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.NodeStatus(broadcastCtx, req)
		return err
	})
	log.Info("membership.status.broadcast", logging.Outcome(logging.OutcomeSucceeded), "state", state.String(), "recipients", len(m.upPeers()))
}
