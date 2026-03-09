package transfer

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

const streamChunkSize = 128

// Manager coordinates transfer claim operations and transfer RPC handlers.
type Manager struct {
	mgr   *membership.Manager
	store *storage.Store

	mu         sync.Mutex
	inProgress map[string]*transferState // transfer key -> owner-side transfer state
}

type transferState struct {
	requestor   string
	targetVnode string
	sourceVnode string
	rangeStart  uint64
	rangeEnd    uint64
}

func NewManager(mgr *membership.Manager, store *storage.Store) *Manager {
	return &Manager{
		mgr:        mgr,
		store:      store,
		inProgress: make(map[string]*transferState),
	}
}

// RecoverOwnedVnodes restores active vnode ownership persisted on disk.
func (m *Manager) RecoverOwnedVnodes(ctx context.Context) error {
	owned, err := m.store.GetOwnedVnodes()
	if err != nil {
		return fmt.Errorf("transfer: load owned vnodes: %w", err)
	}

	selfId := m.mgr.SelfId()
	for _, ov := range owned {
		if ov.State != storage.OwnedVnodeStateActive {
			continue
		}
		m.mgr.SetVnodeState(selfId, ov.Id, membership.VnodeActive)
		m.broadcastRangeStatus(ctx, ov.Id, selfId, pb.VnodeState_VNODE_ACTIVE)
	}
	return nil
}

func (m *Manager) activateTargetRange(ctx context.Context, ownerId, vnodeId string, vnodePos uint64) error {
	if err := m.store.SetVnodeState(vnodeId, vnodePos, storage.OwnedVnodeStateActive); err != nil {
		return err
	}
	m.mgr.SetVnodeState(ownerId, vnodeId, membership.VnodeActive)
	m.broadcastRangeStatus(ctx, vnodeId, ownerId, pb.VnodeState_VNODE_ACTIVE)
	return nil
}

func (m *Manager) broadcastRangeStatus(ctx context.Context, vnodeId, ownerId string, state pb.VnodeState) {
	req := &pb.RangeStatusUpdateRequest{TargetVnodeId: vnodeId, OwnerId: ownerId, State: state}
	util.Broadcast(ctx, m.mgr.UpPeers(), util.RPC.InitialDelay, func(broadcastCtx context.Context, peer membership.Peer) error {
		client, err := m.mgr.TransferClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.RangeStatusUpdate(broadcastCtx, req)
		return err
	})
}

func transferKey(sourceVnodeId string, start, end uint64) string {
	return fmt.Sprintf("%s:%d:%d", sourceVnodeId, start, end)
}
