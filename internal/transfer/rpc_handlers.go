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
	log := logging.Component("transfer")
	log.Info("transfer.request_range", logging.Outcome(logging.OutcomeStarted), "transfer_id", req.GetTransferId(), "requestor_id", req.GetRequestor())
	if req.TargetRange == nil || req.TransferId == "" || req.Requestor == "" {
		log.Warn("transfer.request_range", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.GetTransferId(), "requestor_id", req.GetRequestor(), "reason", "missing_required_fields")
		return nil, errors.New("transfer: target_range, transfer_id, and requestor are required")
	}

	requestor, ok := m.mgr.PeerById(req.Requestor)
	if !ok {
		log.Warn("transfer.request_range", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "requestor_id", req.Requestor, "reason", "requestor_not_found")
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	targetVnode, ok := findVnodeByPosition(requestor, req.TargetRange.End)
	if !ok {
		log.Warn("transfer.request_range", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, logging.AttrPeerId, requestor.NodeId, "reason", "target_vnode_not_found", "range_end", req.TargetRange.End)
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	owner, rangeStart, ok := m.currentOwnerForPosition(targetVnode.Position)
	if !ok || owner.NodeId != m.mgr.SelfId() {
		log.Warn("transfer.request_range", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, logging.AttrVnodeId, targetVnode.Id, "reason", "not_current_owner")
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}
	if req.TargetRange.Start != rangeStart || req.TargetRange.End != targetVnode.Position {
		log.Warn("transfer.request_range", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, logging.AttrVnodeId, targetVnode.Id, "reason", "range_mismatch", "expected_start", rangeStart, "expected_end", targetVnode.Position, "requested_start", req.TargetRange.Start, "requested_end", req.TargetRange.End)
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
		log.Warn("transfer.request_range", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "source_vnode_id", owner.Id, "reason", "source_inactive")
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
		log.Warn("transfer.request_range", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "source_vnode_id", owner.Id, logging.AttrVnodeId, targetVnode.Id, "reason", "owner_transfer_not_started")
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	log.Info("transfer.request_range", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", req.TransferId, "stream_id", state.StreamId, "requestor_id", requestor.NodeId, "source_vnode_id", owner.Id, "target_vnode_id", targetVnode.Id, "range_start", state.Range.Start, "range_end", state.Range.End)
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
	log := logging.Component("transfer")
	log.Info("transfer.stream", logging.Outcome(logging.OutcomeStarted), "stream_id", req.GetTransferId())
	if req.TransferId == 0 {
		log.Warn("transfer.stream", logging.Outcome(logging.OutcomeRejected), "reason", "transfer_id_required")
		return errors.New("transfer: transfer_id is required")
	}

	state, ok := m.ownerTransferByStreamId(req.TransferId)
	if !ok {
		log.Warn("transfer.stream", logging.Outcome(logging.OutcomeRejected), "stream_id", req.TransferId, "reason", "transfer_not_found")
		return errors.New("transfer: stream requested without accepted transfer")
	}

	records, err := m.store.GetRecordsByVnode(state.SourceVnodeId)
	if err != nil {
		log.Error("transfer.stream", logging.Outcome(logging.OutcomeFailed), "transfer_id", state.TransferId, "source_vnode_id", state.SourceVnodeId, logging.Err(err))
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
			log.Error("transfer.stream.final", logging.Outcome(logging.OutcomeFailed), "transfer_id", state.TransferId, "seq", seq, logging.Err(err))
			return err
		}
		m.setOwnerTransferLive(state.TransferId)
		_ = m.flushBufferedWrites(stream.Context(), state.TransferId)
		log.Info("transfer.stream", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", state.TransferId, "records", 0, "chunks", 1)
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
			log.Error("transfer.stream.chunk", logging.Outcome(logging.OutcomeFailed), "transfer_id", state.TransferId, "seq", seq, "records", len(chunkRecords), logging.Err(err))
			return err
		}
		log.Debug("transfer.stream.chunk", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", state.TransferId, "seq", seq, "records", len(chunkRecords), "is_final", end == len(filtered))
		seq++
	}

	m.setOwnerTransferLive(state.TransferId)
	_ = m.flushBufferedWrites(stream.Context(), state.TransferId)
	log.Info("transfer.stream", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", state.TransferId, "records", len(filtered), "chunks", seq)
	return nil
}

func (m *Manager) ForwardWrite(_ context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	log := logging.Component("transfer")
	log.Info("transfer.forward_write", logging.Outcome(logging.OutcomeStarted), "write_id", req.GetWriteId(), logging.AttrKey, req.GetRecord().GetKey())
	if req.Record == nil {
		log.Warn("transfer.forward_write", logging.Outcome(logging.OutcomeRejected), "write_id", req.GetWriteId(), "reason", "record_required")
		return nil, errors.New("transfer: record is required")
	}

	claim, ok := m.claimForKey(req.Record.Key)
	if !ok {
		log.Warn("transfer.forward_write", logging.Outcome(logging.OutcomeRejected), "write_id", req.GetWriteId(), logging.AttrKey, req.Record.Key, "reason", "claim_not_found")
		return nil, fmt.Errorf("transfer: no local claim for key %s", req.Record.Key)
	}

	_, err := m.store.UpsertRecord(storage.Record{
		Key:       req.Record.Key,
		Value:     req.Record.Value,
		VnodeId:   claim.TargetVnodeId,
		Timestamp: req.Record.Timestamp,
	})
	if err != nil {
		log.Error("transfer.forward_write", logging.Outcome(logging.OutcomeFailed), "write_id", req.GetWriteId(), logging.AttrKey, req.Record.Key, logging.AttrVnodeId, claim.TargetVnodeId, logging.Err(err))
		return nil, err
	}
	log.Info("transfer.forward_write", logging.Outcome(logging.OutcomeSucceeded), "write_id", req.GetWriteId(), logging.AttrKey, req.Record.Key, logging.AttrVnodeId, claim.TargetVnodeId)
	return &pb.ForwardWriteResponse{WriteId: req.WriteId}, nil
}

func (m *Manager) CompleteRangeTransfer(ctx context.Context, req *pb.CompleteRangeTransferRequest) (*pb.CompleteRangeTransferResponse, error) {
	log := logging.Component("transfer")
	log.Info("transfer.complete", logging.Outcome(logging.OutcomeStarted), "transfer_id", req.GetTransferId())
	if req.TransferId == "" {
		log.Warn("transfer.complete", logging.Outcome(logging.OutcomeRejected), "reason", "transfer_id_required")
		return nil, errors.New("transfer: transfer_id is required")
	}

	state, ok := m.ownerTransferById(req.TransferId)
	if !ok {
		log.Warn("transfer.complete", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "reason", "transfer_not_found")
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if !state.LiveForwarding {
		log.Warn("transfer.complete", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "reason", "live_forwarding_not_ready")
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}

	if err := m.flushBufferedWrites(ctx, req.TransferId); err != nil {
		log.Warn("transfer.complete", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "reason", "flush_failed", logging.Err(err))
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if m.bufferedWriteCount(req.TransferId) > 0 {
		log.Warn("transfer.complete", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "reason", "buffer_not_empty", "buffered_writes", m.bufferedWriteCount(req.TransferId))
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if !m.setOwnerTransferCutover(req.TransferId, true) {
		log.Warn("transfer.complete", logging.Outcome(logging.OutcomeRejected), "transfer_id", req.TransferId, "reason", "cutover_not_set")
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}

	if err := m.store.DeleteRecordsInVnodeRange(state.SourceVnodeId, state.Range.Start, state.Range.End); err != nil {
		m.setOwnerTransferCutover(req.TransferId, false)
		log.Error("transfer.complete", logging.Outcome(logging.OutcomeFailed), "transfer_id", req.TransferId, "source_vnode_id", state.SourceVnodeId, logging.Err(err))
		return nil, err
	}
	log.Info("transfer.complete", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", req.TransferId, "source_vnode_id", state.SourceVnodeId, "target_vnode_id", state.TargetVnodeId, "range_start", state.Range.Start, "range_end", state.Range.End)
	return &pb.CompleteRangeTransferResponse{Accepted: true}, nil
}

func finalRangeChunk(transferId string, seq int32) *pb.RangeDataChunk {
	return &pb.RangeDataChunk{
		TransferId: transferId,
		Seq:        seq,
		IsFinal:    true,
	}
}
