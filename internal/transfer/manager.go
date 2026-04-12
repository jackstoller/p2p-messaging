package transfer

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/jackstoller/p2p-messaging/internal/logging"
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
	TransferId      string
	StreamId        int32
	RequestorNodeId string
	RequestorAddr   string
	SourceVnodeId   string
	TargetVnodeId   string
	Range           ring.OwnedRange
	LiveForwarding  bool
	CutoverGranted  bool
	BufferedWrites  []storage.Record
}

type claimTransferState struct {
	TransferId    string
	StreamId      int32
	OwnerNodeId   string
	OwnerAddr     string
	TargetVnodeId string
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
		logging.Error("Could not activate transferred range with owner=%v, vnode=%v, error=%v.", ownerId, vnodeId, err)
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

func streamIdForTransfer(transferId string) int32 {
	streamId := int32(crc32.ChecksumIEEE([]byte(transferId)) & 0x7fffffff)
	if streamId == 0 {
		return 1
	}
	return streamId
}

func (m *Manager) registerClaim(plan rangeTransferPlan, vnode membership.Vnode) {
	m.mu.Lock()
	m.claimTransfers[plan.TransferId] = &claimTransferState{
		TransferId:    plan.TransferId,
		StreamId:      plan.StreamId,
		OwnerNodeId:   plan.OwnerNodeId,
		OwnerAddr:     plan.OwnerAddr,
		TargetVnodeId: vnode.Id,
		TargetPos:     vnode.Position,
		Range:         plan.Range,
	}
	m.mu.Unlock()
}

func (m *Manager) unregisterClaim(transferId string) {
	m.mu.Lock()
	delete(m.claimTransfers, transferId)
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

	if existingId, exists := m.ownerTransfersByStream[state.StreamId]; exists && existingId != state.TransferId {
		logging.Warn("Cannot start owner transfer because stream id is already in use with transfer id=%v, stream id=%v, existing transfer id=%v.", state.TransferId, state.StreamId, existingId)
		return false
	}
	if _, exists := m.ownerTransfers[state.TransferId]; exists {
		logging.Warn("Cannot start owner transfer because transfer id already exists with transfer id=%v.", state.TransferId)
		return false
	}

	for _, active := range m.ownerTransfers {
		if active.TargetVnodeId == state.TargetVnodeId {
			logging.Warn("Cannot start owner transfer because target vnode is already busy with transfer id=%v, target vnode=%v.", state.TransferId, state.TargetVnodeId)
			return false
		}
	}

	transfer := state
	m.ownerTransfers[state.TransferId] = &transfer
	m.ownerTransfersByStream[state.StreamId] = state.TransferId
	return true
}

func (m *Manager) ownerTransferById(transferId string) (ownerTransferState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferId]
	if !ok {
		return ownerTransferState{}, false
	}
	return *state, true
}

func (m *Manager) ownerTransferByStreamId(streamId int32) (ownerTransferState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	transferId, ok := m.ownerTransfersByStream[streamId]
	if !ok {
		return ownerTransferState{}, false
	}

	state, ok := m.ownerTransfers[transferId]
	if !ok {
		return ownerTransferState{}, false
	}
	return *state, true
}

func (m *Manager) setOwnerTransferLive(transferId string) {
	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferId]; ok {
		state.LiveForwarding = true
	}
	m.mu.Unlock()
}

func (m *Manager) setOwnerTransferCutover(transferId string, granted bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferId]
	if !ok {
		logging.Warn("Could not update cutover state because transfer was not found with transfer id=%v.", transferId)
		return false
	}
	state.CutoverGranted = granted
	return true
}

func (m *Manager) matchingOwnerTransfer(vnodeId, key string) (ownerTransferState, bool) {
	keyPos := ring.KeyPosition(key)

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, state := range m.ownerTransfers {
		if state.SourceVnodeId != vnodeId {
			continue
		}
		if state.Range.InRange(keyPos) {
			return *state, true
		}
	}

	return ownerTransferState{}, false
}

func (m *Manager) appendBufferedWrite(transferId string, rec storage.Record) {
	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferId]; ok {
		state.BufferedWrites = append(state.BufferedWrites, rec)
	}
	m.mu.Unlock()
}

func (m *Manager) takeBufferedWrites(transferId string) (ownerTransferState, []storage.Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferId]
	if !ok {
		return ownerTransferState{}, nil, false
	}

	buffered := append([]storage.Record(nil), state.BufferedWrites...)
	state.BufferedWrites = nil
	return *state, buffered, true
}

func (m *Manager) restoreBufferedWrites(transferId string, writes []storage.Record) {
	if len(writes) == 0 {
		return
	}

	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferId]; ok {
		state.BufferedWrites = append(append([]storage.Record(nil), writes...), state.BufferedWrites...)
	}
	m.mu.Unlock()
}

func (m *Manager) bufferedWriteCount(transferId string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferId]
	if !ok {
		return 0
	}
	return len(state.BufferedWrites)
}

func (m *Manager) ClearCompletedTransfer(targetVnodeId, ownerNodeId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for transferId, state := range m.ownerTransfers {
		if state.TargetVnodeId != targetVnodeId || state.RequestorNodeId != ownerNodeId {
			continue
		}
		delete(m.ownerTransfersByStream, state.StreamId)
		delete(m.ownerTransfers, transferId)
	}
}

func (m *Manager) RejectPrimaryWrite(vnodeId, key string) error {
	state, ok := m.matchingOwnerTransfer(vnodeId, key)
	if !ok {
		return nil
	}
	if state.CutoverGranted {
		logging.Warn("Rejecting primary write because transfer cutover is already granted with vnode=%v, key=%v, transfer id=%v.", vnodeId, key, state.TransferId)
		return errRangeTransferClosed
	}
	return nil
}

func (m *Manager) ForwardPrimaryWrite(ctx context.Context, vnodeId string, rec storage.Record) error {
	state, ok := m.matchingOwnerTransfer(vnodeId, rec.Key)
	if !ok {
		return nil
	}
	if state.CutoverGranted {
		logging.Warn("Rejecting primary write forwarding because transfer cutover is already granted with vnode=%v, key=%v, transfer id=%v.", vnodeId, rec.Key, state.TransferId)
		return errRangeTransferClosed
	}
	if !state.LiveForwarding {
		m.appendBufferedWrite(state.TransferId, rec)
		return nil
	}

	if err := m.forwardRecord(ctx, state.RequestorAddr, rec); err != nil {
		m.appendBufferedWrite(state.TransferId, rec)
		logging.Warn("Forwarding primary write failed; write was re-buffered with transfer id=%v, key=%v, error=%v.", state.TransferId, rec.Key, err)
		return nil
	}
	return nil
}

func (m *Manager) flushBufferedWrites(ctx context.Context, transferId string) error {
	for {
		state, buffered, ok := m.takeBufferedWrites(transferId)
		if !ok || len(buffered) == 0 {
			return nil
		}

		for i, rec := range buffered {
			if err := m.forwardRecord(ctx, state.RequestorAddr, rec); err != nil {
				m.restoreBufferedWrites(transferId, buffered[i:])
				logging.Warn("Buffered write flush failed and remaining writes were restored with transfer id=%v, key=%v, error=%v.", transferId, rec.Key, err)
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
		logging.Warn("Forwarding transfer record failed with key=%v, peer addr=%v, error=%v.", rec.Key, requestorAddr, err)
		return err
	}

	if resp.GetWriteId() != request.WriteId {
		logging.Error("Forwarding transfer record returned mismatched ack id with key=%v, write id=%v, ack write id=%v.", rec.Key, request.WriteId, resp.GetWriteId())
		return fmt.Errorf("transfer: forwarded write ack mismatch for key %s", rec.Key)
	}

	return nil
}
