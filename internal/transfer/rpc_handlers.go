package transfer

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc"
)

func (m *Manager) RequestRangeTransfer(_ context.Context, req *pb.RequestRangeTransferRequest) (*pb.RequestRangeTransferResponse, error) {
	if req.TargetRange == nil || req.TransferId == "" || req.Requestor == "" {
		return nil, errors.New("transfer: target_range, transfer_id, and requestor are required")
	}

	requestor, ok := m.mgr.PeerById(req.Requestor)
	if !ok {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	targetVnode, ok := findVnodeByPosition(requestor, req.TargetRange.End)
	if !ok {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	owner, rangeStart, ok := m.currentOwnerForPosition(targetVnode.Position)
	if !ok || owner.NodeId != m.mgr.SelfId() {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}
	if req.TargetRange.Start != rangeStart || req.TargetRange.End != targetVnode.Position {
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
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	state := ownerTransferState{
		TransferID:      req.TransferId,
		StreamID:        streamIDForTransfer(req.TransferId),
		RequestorNodeID: requestor.NodeId,
		RequestorAddr:   requestor.Address,
		SourceVnodeID:   owner.Id,
		TargetVnodeID:   targetVnode.Id,
		Range: ring.OwnedRange{
			Start:   rangeStart,
			End:     targetVnode.Position,
			NodeId:  owner.NodeId,
			Address: owner.Address,
		},
	}

	if !m.startOwnerTransfer(state) {
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
		return errors.New("transfer: transfer_id is required")
	}

	state, ok := m.ownerTransferByStreamID(req.TransferId)
	if !ok {
		return errors.New("transfer: stream requested without accepted transfer")
	}

	records, err := m.store.GetRecordsByVnode(state.SourceVnodeID)
	if err != nil {
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
		if err := stream.Send(finalRangeChunk(state.TransferID, seq)); err != nil {
			return err
		}
		m.setOwnerTransferLive(state.TransferID)
		_ = m.flushBufferedWrites(stream.Context(), state.TransferID)
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
			TransferId: state.TransferID,
			Seq:        seq,
			IsFinal:    end == len(filtered),
			Records:    chunkRecords,
		}); err != nil {
			return err
		}
		seq++
	}

	m.setOwnerTransferLive(state.TransferID)
	_ = m.flushBufferedWrites(stream.Context(), state.TransferID)
	return nil
}

func (m *Manager) ForwardWrite(_ context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	if req.Record == nil {
		return nil, errors.New("transfer: record is required")
	}

	claim, ok := m.claimForKey(req.Record.Key)
	if !ok {
		return nil, fmt.Errorf("transfer: no local claim for key %s", req.Record.Key)
	}

	_, err := m.store.UpsertRecord(storage.Record{
		Key:       req.Record.Key,
		Value:     req.Record.Value,
		VnodeId:   claim.TargetVnodeID,
		Timestamp: req.Record.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	return &pb.ForwardWriteResponse{WriteId: req.WriteId}, nil
}

func (m *Manager) CompleteRangeTransfer(ctx context.Context, req *pb.CompleteRangeTransferRequest) (*pb.CompleteRangeTransferResponse, error) {
	if req.TransferId == "" {
		return nil, errors.New("transfer: transfer_id is required")
	}

	state, ok := m.ownerTransferByID(req.TransferId)
	if !ok {
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if !state.LiveForwarding {
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}

	if err := m.flushBufferedWrites(ctx, req.TransferId); err != nil {
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if m.bufferedWriteCount(req.TransferId) > 0 {
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}
	if !m.setOwnerTransferCutover(req.TransferId, true) {
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}

	if err := m.store.DeleteRecordsInVnodeRange(state.SourceVnodeID, state.Range.Start, state.Range.End); err != nil {
		m.setOwnerTransferCutover(req.TransferId, false)
		return nil, err
	}
	return &pb.CompleteRangeTransferResponse{Accepted: true}, nil
}

func finalRangeChunk(transferID string, seq int32) *pb.RangeDataChunk {
	return &pb.RangeDataChunk{
		TransferId: transferID,
		Seq:        seq,
		IsFinal:    true,
	}
}
