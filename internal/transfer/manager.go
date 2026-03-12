package transfer

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

const streamChunkSize = 128

var errRangeTransferClosed = errors.New("transfer: range ownership is changing")

// Manager coordinates transfer claim operations and transfer RPC handlers.
type Manager struct {
	mgr   *membership.Manager
	store *storage.Store

	mu                     sync.Mutex
	ownerTransfers         map[string]*ownerTransferState
	ownerTransfersByStream map[int32]string
	claimTransfers         map[string]*claimTransferState
}

type ownerTransferState struct {
	TransferID      string
	StreamID        int32
	RequestorNodeID string
	RequestorAddr   string
	SourceVnodeID   string
	TargetVnodeID   string
	Range           ring.OwnedRange
	LiveForwarding  bool
	CutoverGranted  bool
	BufferedWrites  []storage.Record
}

type claimTransferState struct {
	TransferID    string
	StreamID      int32
	OwnerNodeID   string
	OwnerAddr     string
	TargetVnodeID string
	TargetPos     uint64
	Range         ring.OwnedRange
}

func NewManager(mgr *membership.Manager, store *storage.Store) *Manager {
	return &Manager{
		mgr:                    mgr,
		store:                  store,
		ownerTransfers:         make(map[string]*ownerTransferState),
		ownerTransfersByStream: make(map[int32]string),
		claimTransfers:         make(map[string]*claimTransferState),
	}
}

func (m *Manager) activateTargetRange(ctx context.Context, ownerId, vnodeId string, vnodePos uint64) error {
	if err := m.store.SetVnodeState(vnodeId, vnodePos, storage.OwnedVnodeStateActive); err != nil {
		return err
	}
	m.mgr.SetVnodeState(ownerId, vnodeId, membership.VnodeActive)
	m.broadcastVNodeStatus(ctx, vnodeId, ownerId, pb.VnodeState_VNODE_ACTIVE)
	return nil
}

func (m *Manager) broadcastVNodeStatus(ctx context.Context, vnodeId, ownerId string, state pb.VnodeState) {
	req := &pb.VNodeStatusUpdateRequest{TargetVnodeId: vnodeId, NodeId: ownerId, State: state}
	util.Broadcast(ctx, m.mgr.UpPeers(), util.RPC.InitialDelay, func(broadcastCtx context.Context, peer membership.Peer) error {
		client, err := m.mgr.TransferClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.VNodeStatusUpdate(broadcastCtx, req)
		return err
	})
}

func transferKey(sourceVnodeId string, start, end uint64) string {
	return fmt.Sprintf("%s:%d:%d", sourceVnodeId, start, end)
}

func streamIDForTransfer(transferID string) int32 {
	streamID := int32(crc32.ChecksumIEEE([]byte(transferID)) & 0x7fffffff)
	if streamID == 0 {
		return 1
	}
	return streamID
}

func (m *Manager) registerClaim(plan rangeTransferPlan, vnode membership.Vnode) {
	m.mu.Lock()
	m.claimTransfers[plan.TransferID] = &claimTransferState{
		TransferID:    plan.TransferID,
		StreamID:      plan.StreamID,
		OwnerNodeID:   plan.OwnerNodeID,
		OwnerAddr:     plan.OwnerAddr,
		TargetVnodeID: vnode.Id,
		TargetPos:     vnode.Position,
		Range:         plan.Range,
	}
	m.mu.Unlock()
}

func (m *Manager) unregisterClaim(transferID string) {
	m.mu.Lock()
	delete(m.claimTransfers, transferID)
	m.mu.Unlock()
}

func (m *Manager) claimForKey(key string) (claimTransferState, bool) {
	keyPos := ring.KeyPosition(key)

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, claim := range m.claimTransfers {
		if claim.Range.InRange(keyPos) {
			return *claim, true
		}
	}

	return claimTransferState{}, false
}

func (m *Manager) startOwnerTransfer(state ownerTransferState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingID, exists := m.ownerTransfersByStream[state.StreamID]; exists && existingID != state.TransferID {
		return false
	}
	if _, exists := m.ownerTransfers[state.TransferID]; exists {
		return false
	}

	for _, active := range m.ownerTransfers {
		if active.TargetVnodeID == state.TargetVnodeID {
			return false
		}
	}

	transfer := state
	m.ownerTransfers[state.TransferID] = &transfer
	m.ownerTransfersByStream[state.StreamID] = state.TransferID
	return true
}

func (m *Manager) ownerTransferByID(transferID string) (ownerTransferState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferID]
	if !ok {
		return ownerTransferState{}, false
	}
	return *state, true
}

func (m *Manager) ownerTransferByStreamID(streamID int32) (ownerTransferState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	transferID, ok := m.ownerTransfersByStream[streamID]
	if !ok {
		return ownerTransferState{}, false
	}

	state, ok := m.ownerTransfers[transferID]
	if !ok {
		return ownerTransferState{}, false
	}
	return *state, true
}

func (m *Manager) setOwnerTransferLive(transferID string) {
	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferID]; ok {
		state.LiveForwarding = true
	}
	m.mu.Unlock()
}

func (m *Manager) setOwnerTransferCutover(transferID string, granted bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferID]
	if !ok {
		return false
	}
	state.CutoverGranted = granted
	return true
}

func (m *Manager) matchingOwnerTransfer(vnodeID, key string) (ownerTransferState, bool) {
	keyPos := ring.KeyPosition(key)

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, state := range m.ownerTransfers {
		if state.SourceVnodeID != vnodeID {
			continue
		}
		if state.Range.InRange(keyPos) {
			return *state, true
		}
	}

	return ownerTransferState{}, false
}

func (m *Manager) appendBufferedWrite(transferID string, rec storage.Record) {
	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferID]; ok {
		state.BufferedWrites = append(state.BufferedWrites, rec)
	}
	m.mu.Unlock()
}

func (m *Manager) takeBufferedWrites(transferID string) (ownerTransferState, []storage.Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferID]
	if !ok {
		return ownerTransferState{}, nil, false
	}

	buffered := append([]storage.Record(nil), state.BufferedWrites...)
	state.BufferedWrites = nil
	return *state, buffered, true
}

func (m *Manager) restoreBufferedWrites(transferID string, writes []storage.Record) {
	if len(writes) == 0 {
		return
	}

	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferID]; ok {
		state.BufferedWrites = append(append([]storage.Record(nil), writes...), state.BufferedWrites...)
	}
	m.mu.Unlock()
}

func (m *Manager) bufferedWriteCount(transferID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferID]
	if !ok {
		return 0
	}
	return len(state.BufferedWrites)
}

func (m *Manager) ClearCompletedTransfer(targetVnodeID, ownerNodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for transferID, state := range m.ownerTransfers {
		if state.TargetVnodeID != targetVnodeID || state.RequestorNodeID != ownerNodeID {
			continue
		}
		delete(m.ownerTransfersByStream, state.StreamID)
		delete(m.ownerTransfers, transferID)
	}
}

func (m *Manager) RejectPrimaryWrite(vnodeID, key string) error {
	state, ok := m.matchingOwnerTransfer(vnodeID, key)
	if !ok {
		return nil
	}
	if state.CutoverGranted {
		return errRangeTransferClosed
	}
	return nil
}

func (m *Manager) ForwardPrimaryWrite(ctx context.Context, vnodeID string, rec storage.Record) error {
	state, ok := m.matchingOwnerTransfer(vnodeID, rec.Key)
	if !ok {
		return nil
	}
	if state.CutoverGranted {
		return errRangeTransferClosed
	}
	if !state.LiveForwarding {
		m.appendBufferedWrite(state.TransferID, rec)
		return nil
	}

	if err := m.forwardRecord(ctx, state.RequestorAddr, rec); err != nil {
		m.appendBufferedWrite(state.TransferID, rec)
	}
	return nil
}

func (m *Manager) flushBufferedWrites(ctx context.Context, transferID string) error {
	for {
		state, buffered, ok := m.takeBufferedWrites(transferID)
		if !ok || len(buffered) == 0 {
			return nil
		}

		for i, rec := range buffered {
			if err := m.forwardRecord(ctx, state.RequestorAddr, rec); err != nil {
				m.restoreBufferedWrites(transferID, buffered[i:])
				return err
			}
		}
	}
}

func (m *Manager) forwardRecord(ctx context.Context, requestorAddr string, rec storage.Record) error {
	var resp *pb.ForwardWriteResponse
	request := &pb.ForwardWriteRequest{
		WriteId: fmt.Sprintf("%s:%d", rec.Key, rec.Timestamp),
		Record: &pb.DataRecord{
			Key:       rec.Key,
			Value:     rec.Value,
			Timestamp: rec.Timestamp,
		},
	}

	if err := util.Do(ctx, util.RPC, func() error {
		client, err := m.mgr.TransferClient(ctx, requestorAddr)
		if err != nil {
			return err
		}

		resp, err = client.ForwardWrite(ctx, request)
		return err
	}); err != nil {
		return err
	}

	if resp.GetWriteId() != request.WriteId {
		return fmt.Errorf("transfer: forwarded write ack mismatch for key %s", rec.Key)
	}

	return nil
}
