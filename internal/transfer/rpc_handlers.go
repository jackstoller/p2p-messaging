package transfer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc"
)

func (m *Manager) RequestRangeTransfer(_ context.Context, req *pb.RequestRangeTransferRequest) (*pb.RequestRangeTransferResponse, error) {
	if req.TargetRange == nil || req.TransferId == "" || req.Requestor == "" {
		logging.Warn("Rejected range transfer request because required fields are missing with transfer id=%v, requestor=%v.", req.GetTransferId(), req.GetRequestor())
		return nil, errors.New("transfer: target_range, transfer_id, and requestor are required")
	}

	requestor, ok := m.mgr.PeerById(req.Requestor)
	if !ok {
		logging.Warn("Rejected range transfer request because requestor is unknown with transfer id=%v, requestor=%v.", req.TransferId, req.Requestor)
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	targetVnode, ok := findVnodeByPosition(requestor, req.TargetRange.End)
	if !ok {
		logging.Warn("Rejected range transfer request because target vnode was not found with transfer id=%v, requestor=%v, range end=%v.", req.TransferId, requestor.NodeId, req.TargetRange.End)
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	owner, rangeStart, ok := m.currentOwnerForPosition(targetVnode.Position)
	if !ok || owner.NodeId != m.mgr.SelfId() {
		logging.Warn("Rejected range transfer request because this node is not the current owner with transfer id=%v, target vnode=%v.", req.TransferId, targetVnode.Id)
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}
	if req.TargetRange.Start != rangeStart || req.TargetRange.End != targetVnode.Position {
		logging.Warn("Rejected range transfer request because range did not match current ownership with transfer id=%v, target vnode=%v.", req.TransferId, targetVnode.Id)
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	self := m.mgr.Self()
	ownedActive := false
	for _, vn := range self.Vnodes {
		if vn.Id == owner.Id && vn.State == membership.VnodeActive {
			ownedActive = true
			break
		}
	}
	if !ownedActive {
		logging.Warn("Rejected range transfer request because source vnode is inactive with transfer id=%v, source vnode=%v.", req.TransferId, owner.Id)
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	state := ownerTransferState{
		TransferId:      req.TransferId,
		StreamId:        streamIdForTransfer(req.TransferId),
		RequestorNodeId: requestor.NodeId,
		RequestorAddr:   requestor.Address,
		SourceVnodeId:   owner.Id,
		TargetVnodeId:   targetVnode.Id,
		Range: ring.OwnedRange{
			Start:   rangeStart,
			End:     targetVnode.Position,
			NodeId:  owner.NodeId,
			Address: owner.Address,
		},
	}

	if !m.startOwnerTransfer(state) {
		logging.Warn("Rejected range transfer request because owner transfer state could not be started with transfer id=%v, source vnode=%v, target vnode=%v.", req.TransferId, owner.Id, targetVnode.Id)
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	return &pb.RequestRangeTransferResponse{Accepted: true}, nil
}

func findVnodeByPosition(peer membership.Peer, position uint64) (membership.Vnode, bool) {
	for _, vnode := range peer.Vnodes {
		if vnode.Position == position {
			return vnode, true
		}
	}
	return membership.Vnode{}, false
}

func (m *Manager) StreamRange(req *pb.StreamRangeRequest, stream grpc.ServerStreamingServer[pb.RangeDataChunk]) error {
	if req.TransferId == 0 {
		logging.Warn("Rejected range stream because transfer id is missing.")
		return errors.New("transfer: transfer_id is required")
	}

	state, ok := m.ownerTransferByStreamId(req.TransferId)
	if !ok {
		logging.Warn("Rejected range stream because transfer was not found with stream id=%v.", req.TransferId)
		return errors.New("transfer: stream requested without accepted transfer")
	}

	records, err := m.store.GetRecordsByVnode(state.SourceVnodeId)
	if err != nil {
		logging.Error("Failed to load records for range stream with transfer id=%v, source vnode=%v, error=%v.", state.TransferId, state.SourceVnodeId, err)
		return err
	}

	filtered := make([]storage.Record, 0, len(records))
	for _, rec := range records {
		if state.Range.InRange(ring.KeyPosition(rec.Key)) {
			filtered = append(filtered, rec)
		}
	}

	seq := int32(0)
	if len(filtered) == 0 {
		if err := stream.Send(finalRangeChunk(state.TransferId, seq)); err != nil {
			logging.Error("Failed sending final empty range chunk with transfer id=%v, seq=%v, error=%v.", state.TransferId, seq, err)
			return err
		}
		m.setOwnerTransferLive(state.TransferId)
		_ = m.flushBufferedWrites(stream.Context(), state.TransferId)
		return nil
	}

	for start := 0; start < len(filtered); start += streamChunkSize {
		end := start + streamChunkSize
		if end > len(filtered) {
			end = len(filtered)
		}

		chunkRecords := make([]*pb.DataRecord, 0, end-start)
		for _, r := range filtered[start:end] {
			chunkRecords = append(chunkRecords, &pb.DataRecord{Key: r.Key, Value: r.Value, Timestamp: r.Timestamp})
		}

		if err := stream.Send(&pb.RangeDataChunk{
			TransferId: state.TransferId,
			Seq:        seq,
			IsFinal:    end == len(filtered),
			Records:    chunkRecords,
		}); err != nil {
			logging.Error("Failed sending range chunk with transfer id=%v, seq=%v, records=%v, error=%v.", state.TransferId, seq, len(chunkRecords), err)
			return err
		}
		seq++
	}

	m.setOwnerTransferLive(state.TransferId)
	_ = m.flushBufferedWrites(stream.Context(), state.TransferId)
	return nil
}

func (m *Manager) ForwardWrite(_ context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	if req.Record == nil {
		logging.Warn("Rejected forwarded write because record payload is missing with write id=%v.", req.GetWriteId())
		return nil, errors.New("transfer: record is required")
	}

	claim, ok := m.claimForKey(req.Record.Key)
	if !ok {
		logging.Warn("Rejected forwarded write because no local claim is active for key with write id=%v, key=%v.", req.GetWriteId(), req.Record.Key)
		return nil, fmt.Errorf("transfer: no local claim for key %s", req.Record.Key)
	}

	_, err := m.store.UpsertRecord(storage.Record{
		Key:       req.Record.Key,
		Value:     req.Record.Value,
		VnodeId:   claim.TargetVnodeId,
		Timestamp: req.Record.Timestamp,
	})
	if err != nil {
		logging.Error("Forwarded write failed with write id=%v, key=%v, vnode=%v, error=%v.", req.GetWriteId(), req.Record.Key, claim.TargetVnodeId, err)
		return nil, err
	}
	return &pb.ForwardWriteResponse{WriteId: req.WriteId}, nil
}

func (m *Manager) CompleteRangeTransfer(ctx context.Context, req *pb.CompleteRangeTransferRequest) (*pb.CompleteRangeTransferResponse, error) {
	if req.TransferId == "" {
		logging.Warn("Rejected transfer completion because transfer id is missing.")
		return nil, errors.New("transfer: transfer_id is required")
	}

	state, ok := m.ownerTransferById(req.TransferId)
	if !ok {
		logging.Warn("Rejected transfer completion because transfer was not found with transfer id=%v.", req.TransferId)
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if !state.LiveForwarding {
		logging.Warn("Rejected transfer completion because live forwarding is not ready with transfer id=%v.", req.TransferId)
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}

	if err := m.flushBufferedWrites(ctx, req.TransferId); err != nil {
		logging.Warn("Rejected transfer completion because buffered writes could not flush with transfer id=%v, error=%v.", req.TransferId, err)
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if m.bufferedWriteCount(req.TransferId) > 0 {
		logging.Warn("Rejected transfer completion because buffered writes are still pending with transfer id=%v, buffered writes=%v.", req.TransferId, m.bufferedWriteCount(req.TransferId))
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if !m.setOwnerTransferCutover(req.TransferId, true) {
		logging.Warn("Rejected transfer completion because cutover state was not set with transfer id=%v.", req.TransferId)
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}

	if err := m.store.DeleteRecordsInVnodeRange(state.SourceVnodeId, state.Range.Start, state.Range.End); err != nil {
		m.setOwnerTransferCutover(req.TransferId, false)
		logging.Error("Transfer completion failed while deleting source range records with transfer id=%v, source vnode=%v, error=%v.", req.TransferId, state.SourceVnodeId, err)
		return nil, err
	}
	return &pb.CompleteRangeTransferResponse{Accepted: true}, nil
}

func finalRangeChunk(transferId string, seq int32) *pb.RangeDataChunk {
	return &pb.RangeDataChunk{
		TransferId: transferId,
		Seq:        seq,
		IsFinal:    true,
	}
}
