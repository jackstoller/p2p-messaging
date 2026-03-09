package transfer

import (
	"context"
	"errors"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc"
)

func (m *Manager) RequestRangeTransfer(_ context.Context, req *pb.RequestRangeTransferRequest) (*pb.RequestRangeTransferResponse, error) {
	if req.TargetVnodeId == "" || req.Requestor == "" {
		return nil, errors.New("transfer: vnode_id and requestor are required")
	}

	requestor, ok := m.mgr.PeerById(req.Requestor)
	if !ok {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	var targetPos uint64
	foundTarget := false
	for _, vn := range requestor.Vnodes {
		if vn.Id != req.TargetVnodeId {
			continue
		}
		targetPos = vn.Position
		foundTarget = true
		break
	}
	if !foundTarget {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	plan, ok := m.currentOwnerRangeForTarget(targetPos, req.Requestor)
	if !ok || plan.owner.NodeId != m.mgr.SelfId() {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	self := m.mgr.Self()
	ownedActive := false
	for _, vn := range self.Vnodes {
		if vn.Id == plan.sourceVnodeId && vn.State == membership.VnodeActive {
			ownedActive = true
			break
		}
	}
	if !ownedActive {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}

	key := transferKey(plan.sourceVnodeId, plan.rangeStart, plan.rangeEnd)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.inProgress[key]; exists {
		return &pb.RequestRangeTransferResponse{Accepted: false}, nil
	}
	for _, active := range m.inProgress {
		if active.targetVnode == req.TargetVnodeId {
			return &pb.RequestRangeTransferResponse{Accepted: false}, nil
		}
	}

	m.inProgress[key] = &transferState{
		requestor:   req.Requestor,
		targetVnode: req.TargetVnodeId,
		sourceVnode: plan.sourceVnodeId,
		rangeStart:  plan.rangeStart,
		rangeEnd:    plan.rangeEnd,
	}

	return &pb.RequestRangeTransferResponse{Accepted: true}, nil
}

func (m *Manager) getTransferByTarget(targetVnodeId string) (string, *transferState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, state := range m.inProgress {
		if state.targetVnode == targetVnodeId {
			return key, state, true
		}
	}
	return "", nil, false
}

func (m *Manager) StreamRange(req *pb.StreamRangeRequest, stream grpc.ServerStreamingServer[pb.RangeDataChunk]) error {
	if req.TargetVnodeId == "" {
		return errors.New("transfer: vnode_id is required")
	}

	_, state, ok := m.getTransferByTarget(req.TargetVnodeId)
	if !ok {
		return errors.New("transfer: stream requested without accepted transfer")
	}

	records, err := m.store.GetRecordsByVnode(state.sourceVnode)
	if err != nil {
		return err
	}

	rng := ring.OwnedRange{Start: state.rangeStart, End: state.rangeEnd}
	filtered := make([]storage.Record, 0, len(records))
	for _, rec := range records {
		if rng.InRange(ring.KeyPosition(rec.Key)) {
			filtered = append(filtered, rec)
		}
	}

	seq := int32(0)
	if len(filtered) == 0 {
		return stream.Send(&pb.RangeDataChunk{TargetVnodeId: req.TargetVnodeId, Seq: seq, Final: true})
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
			TargetVnodeId: req.TargetVnodeId,
			Seq:           seq,
			Final:         end == len(filtered),
			Records:       chunkRecords,
		}); err != nil {
			return err
		}
		seq++
	}

	return nil
}

func (m *Manager) ForwardWrite(_ context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	if req.Record == nil {
		return nil, errors.New("transfer: record is required")
	}

	_, err := m.store.UpsertRecord(storage.Record{
		Key:       req.Record.Key,
		Value:     req.Record.Value,
		VnodeId:   "",
		Timestamp: req.Record.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	return &pb.ForwardWriteResponse{WriteId: req.WriteId}, nil
}

func (m *Manager) CompleteRangeTransfer(_ context.Context, req *pb.CompleteRangeTransferRequest) (*pb.CompleteRangeTransferResponse, error) {
	if req.TargetVnodeId == "" {
		return nil, errors.New("transfer: vnode_id is required")
	}

	m.mu.Lock()
	var (
		state *transferState
		key   string
	)
	for k, inFlight := range m.inProgress {
		if inFlight.targetVnode == req.TargetVnodeId {
			key = k
			state = inFlight
			break
		}
	}
	if state != nil {
		delete(m.inProgress, key)
	}
	m.mu.Unlock()
	if state == nil {
		return &pb.CompleteRangeTransferResponse{Accepted: false}, nil
	}

	if err := m.store.DeleteRecordsInVnodeRange(state.sourceVnode, state.rangeStart, state.rangeEnd); err != nil {
		return nil, err
	}
	return &pb.CompleteRangeTransferResponse{Accepted: true}, nil
}
