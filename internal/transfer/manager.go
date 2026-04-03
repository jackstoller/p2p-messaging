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
	logging.Component("transfer").Info("transfer.manager.init", logging.Outcome(logging.OutcomeSucceeded))
	return &Manager{
		mgr:                    mgr,
		store:                  store,
		ownerTransfers:         make(map[string]*ownerTransferState),
		ownerTransfersByStream: make(map[int32]string),
		claimTransfers:         make(map[string]*claimTransferState),
	}
}

func (m *Manager) activateTargetRange(ctx context.Context, ownerId, vnodeId string, vnodePos uint64) error {
	log := logging.Component("transfer")
	log.Info("transfer.activate_range", logging.Outcome(logging.OutcomeStarted), logging.AttrNodeId, ownerId, logging.AttrVnodeId, vnodeId, "position", vnodePos)
	if err := m.store.SetVnodeState(vnodeId, vnodePos, storage.OwnedVnodeStateActive); err != nil {
		log.Error("transfer.activate_range", logging.Outcome(logging.OutcomeFailed), logging.AttrNodeId, ownerId, logging.AttrVnodeId, vnodeId, logging.Err(err))
		return err
	}
	m.mgr.SetVnodeState(ownerId, vnodeId, membership.VnodeActive)
	m.broadcastVNodeStatus(ctx, vnodeId, ownerId, pb.VnodeState_VNODE_ACTIVE)
	log.Info("transfer.activate_range", logging.Outcome(logging.OutcomeSucceeded), logging.AttrNodeId, ownerId, logging.AttrVnodeId, vnodeId, "position", vnodePos)
	return nil
}

func (m *Manager) broadcastVNodeStatus(ctx context.Context, vnodeId, ownerId string, state pb.VnodeState) {
	log := logging.Component("transfer")
	req := &pb.VNodeStatusUpdateRequest{TargetVnodeId: vnodeId, NodeId: ownerId, State: state}
	log.Info("transfer.vnode_status.broadcast", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, vnodeId, logging.AttrNodeId, ownerId, "state", state.String(), "recipients", len(m.mgr.UpPeers()))
	util.Broadcast(ctx, m.mgr.UpPeers(), util.RPC.InitialDelay, func(broadcastCtx context.Context, peer membership.Peer) error {
		client, err := m.mgr.TransferClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.VNodeStatusUpdate(broadcastCtx, req)
		return err
	})
	log.Info("transfer.vnode_status.broadcast", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, logging.AttrNodeId, ownerId, "state", state.String(), "recipients", len(m.mgr.UpPeers()))
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
	logging.Component("transfer").Info("transfer.claim.register", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", plan.TransferId, logging.AttrVnodeId, vnode.Id, "owner_node_id", plan.OwnerNodeId, "range_start", plan.Range.Start, "range_end", plan.Range.End)
}

func (m *Manager) unregisterClaim(transferId string) {
	m.mu.Lock()
	delete(m.claimTransfers, transferId)
	m.mu.Unlock()
	logging.Component("transfer").Info("transfer.claim.unregister", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId)
}

func (m *Manager) claimForKey(key string) (claimTransferState, bool) {
	log := logging.Component("transfer")
	keyPos := ring.KeyPosition(key)

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, claim := range m.claimTransfers {
		if claim.Range.InRange(keyPos) {
			log.Debug("transfer.claim.lookup", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, key, "transfer_id", claim.TransferId, logging.AttrVnodeId, claim.TargetVnodeId)
			return *claim, true
		}
	}

	log.Debug("transfer.claim.lookup", logging.Outcome(logging.OutcomeSkipped), logging.AttrKey, key, "reason", "no_active_claim")
	return claimTransferState{}, false
}

func (m *Manager) startOwnerTransfer(state ownerTransferState) bool {
	log := logging.Component("transfer")
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingId, exists := m.ownerTransfersByStream[state.StreamId]; exists && existingId != state.TransferId {
		log.Warn("transfer.owner.start", logging.Outcome(logging.OutcomeRejected), "transfer_id", state.TransferId, "stream_id", state.StreamId, "reason", "stream_conflict", "existing_transfer_id", existingId)
		return false
	}
	if _, exists := m.ownerTransfers[state.TransferId]; exists {
		log.Warn("transfer.owner.start", logging.Outcome(logging.OutcomeRejected), "transfer_id", state.TransferId, "reason", "transfer_exists")
		return false
	}

	for _, active := range m.ownerTransfers {
		if active.TargetVnodeId == state.TargetVnodeId {
			log.Warn("transfer.owner.start", logging.Outcome(logging.OutcomeRejected), "transfer_id", state.TransferId, logging.AttrVnodeId, state.TargetVnodeId, "reason", "target_vnode_busy")
			return false
		}
	}

	transfer := state
	m.ownerTransfers[state.TransferId] = &transfer
	m.ownerTransfersByStream[state.StreamId] = state.TransferId
	log.Info("transfer.owner.start", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", state.TransferId, "stream_id", state.StreamId, "requestor_id", state.RequestorNodeId, "source_vnode_id", state.SourceVnodeId, "target_vnode_id", state.TargetVnodeId, "range_start", state.Range.Start, "range_end", state.Range.End)
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
	logging.Component("transfer").Info("transfer.owner.live_forwarding", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId)
}

func (m *Manager) setOwnerTransferCutover(transferId string, granted bool) bool {
	log := logging.Component("transfer")
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferId]
	if !ok {
		log.Warn("transfer.owner.cutover", logging.Outcome(logging.OutcomeRejected), "transfer_id", transferId, "granted", granted, "reason", "transfer_not_found")
		return false
	}
	state.CutoverGranted = granted
	log.Info("transfer.owner.cutover", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId, "granted", granted)
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
	log := logging.Component("transfer")
	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferId]; ok {
		state.BufferedWrites = append(state.BufferedWrites, rec)
		log.Info("transfer.buffered_write.append", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId, logging.AttrKey, rec.Key, "buffered_writes", len(state.BufferedWrites))
	}
	m.mu.Unlock()
}

func (m *Manager) takeBufferedWrites(transferId string) (ownerTransferState, []storage.Record, bool) {
	log := logging.Component("transfer")
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.ownerTransfers[transferId]
	if !ok {
		log.Debug("transfer.buffered_write.take", logging.Outcome(logging.OutcomeSkipped), "transfer_id", transferId, "reason", "transfer_not_found")
		return ownerTransferState{}, nil, false
	}

	buffered := append([]storage.Record(nil), state.BufferedWrites...)
	state.BufferedWrites = nil
	log.Debug("transfer.buffered_write.take", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId, "buffered_writes", len(buffered))
	return *state, buffered, true
}

func (m *Manager) restoreBufferedWrites(transferId string, writes []storage.Record) {
	if len(writes) == 0 {
		return
	}

	log := logging.Component("transfer")
	m.mu.Lock()
	if state, ok := m.ownerTransfers[transferId]; ok {
		state.BufferedWrites = append(append([]storage.Record(nil), writes...), state.BufferedWrites...)
		log.Warn("transfer.buffered_write.restore", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId, "restored_writes", len(writes), "buffered_writes", len(state.BufferedWrites))
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
	log := logging.Component("transfer")
	m.mu.Lock()
	defer m.mu.Unlock()

	for transferId, state := range m.ownerTransfers {
		if state.TargetVnodeId != targetVnodeId || state.RequestorNodeId != ownerNodeId {
			continue
		}
		delete(m.ownerTransfersByStream, state.StreamId)
		delete(m.ownerTransfers, transferId)
		log.Info("transfer.owner.clear_completed", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId, "target_vnode_id", targetVnodeId, "owner_node_id", ownerNodeId)
	}
}

func (m *Manager) RejectPrimaryWrite(vnodeId, key string) error {
	log := logging.Component("transfer")
	state, ok := m.matchingOwnerTransfer(vnodeId, key)
	if !ok {
		log.Debug("transfer.primary_write.reject", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, logging.AttrKey, key, "reason", "no_transfer")
		return nil
	}
	if state.CutoverGranted {
		log.Warn("transfer.primary_write.reject", logging.Outcome(logging.OutcomeRejected), logging.AttrVnodeId, vnodeId, logging.AttrKey, key, "transfer_id", state.TransferId, "reason", "cutover_granted")
		return errRangeTransferClosed
	}
	log.Debug("transfer.primary_write.reject", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, logging.AttrKey, key, "transfer_id", state.TransferId, "reason", "transfer_open")
	return nil
}

func (m *Manager) ForwardPrimaryWrite(ctx context.Context, vnodeId string, rec storage.Record) error {
	log := logging.Component("transfer")
	state, ok := m.matchingOwnerTransfer(vnodeId, rec.Key)
	if !ok {
		log.Debug("transfer.primary_write.forward", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, "reason", "no_transfer")
		return nil
	}
	if state.CutoverGranted {
		log.Warn("transfer.primary_write.forward", logging.Outcome(logging.OutcomeRejected), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, "transfer_id", state.TransferId, "reason", "cutover_granted")
		return errRangeTransferClosed
	}
	if !state.LiveForwarding {
		m.appendBufferedWrite(state.TransferId, rec)
		log.Info("transfer.primary_write.forward", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, "transfer_id", state.TransferId, "reason", "buffered_until_live")
		return nil
	}

	if err := m.forwardRecord(ctx, state.RequestorAddr, rec); err != nil {
		m.appendBufferedWrite(state.TransferId, rec)
		log.Warn("transfer.primary_write.forward", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, "transfer_id", state.TransferId, logging.AttrPeerAddr, state.RequestorAddr, logging.Err(err))
		return nil
	}
	log.Info("transfer.primary_write.forward", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, "transfer_id", state.TransferId, logging.AttrPeerAddr, state.RequestorAddr)
	return nil
}

func (m *Manager) flushBufferedWrites(ctx context.Context, transferId string) error {
	log := logging.Component("transfer")
	log.Info("transfer.buffered_write.flush", logging.Outcome(logging.OutcomeStarted), "transfer_id", transferId)
	for {
		state, buffered, ok := m.takeBufferedWrites(transferId)
		if !ok || len(buffered) == 0 {
			log.Info("transfer.buffered_write.flush", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId)
			return nil
		}

		for i, rec := range buffered {
			if err := m.forwardRecord(ctx, state.RequestorAddr, rec); err != nil {
				m.restoreBufferedWrites(transferId, buffered[i:])
				log.Warn("transfer.buffered_write.flush", logging.Outcome(logging.OutcomeFailed), "transfer_id", transferId, logging.AttrKey, rec.Key, logging.AttrPeerAddr, state.RequestorAddr, logging.Err(err))
				return err
			}
			log.Debug("transfer.buffered_write.flush.record", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", transferId, logging.AttrKey, rec.Key, logging.AttrPeerAddr, state.RequestorAddr)
		}
	}
}

func (m *Manager) forwardRecord(ctx context.Context, requestorAddr string, rec storage.Record) error {
	log := logging.Component("transfer")
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
		log.Warn("transfer.forward_record", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, rec.Key, logging.AttrPeerAddr, requestorAddr, logging.Err(err))
		return err
	}

	if resp.GetWriteId() != request.WriteId {
		log.Error("transfer.forward_record", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, rec.Key, logging.AttrPeerAddr, requestorAddr, "write_id", request.WriteId, "ack_write_id", resp.GetWriteId())
		return fmt.Errorf("transfer: forwarded write ack mismatch for key %s", rec.Key)
	}

	log.Info("transfer.forward_record", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, rec.Key, logging.AttrPeerAddr, requestorAddr, "write_id", request.WriteId)
	return nil
}
