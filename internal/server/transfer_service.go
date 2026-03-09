package server

import (
	"context"
	"log/slog"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// RangeStatusUpdate announces that a target vnode range is now active.
func (s *Server) RangeStatusUpdate(_ context.Context, req *pb.RangeStatusUpdateRequest) (*pb.RangeStatusUpdateResponse, error) {
	if req.State != pb.VnodeState_VNODE_ACTIVE {
		return &pb.RangeStatusUpdateResponse{}, nil
	}

	s.mgr.SetVnodeState(req.OwnerId, req.TargetVnodeId, membership.VnodeActive)
	if req.OwnerId == s.mgr.SelfId() {
		s.repl.OnVnodeActive(context.Background(), req.TargetVnodeId)
	}

	slog.Info("RangeStatusUpdate ACTIVE", "targetVnodeId", req.TargetVnodeId, "owner", req.OwnerId)
	return &pb.RangeStatusUpdateResponse{}, nil
}

func (s *Server) RequestRangeTransfer(ctx context.Context, req *pb.RequestRangeTransferRequest) (*pb.RequestRangeTransferResponse, error) {
	return s.xfer.RequestRangeTransfer(ctx, req)
}

func (s *Server) StreamRange(req *pb.StreamRangeRequest, stream pb.TransferService_StreamRangeServer) error {
	return s.xfer.StreamRange(req, stream)
}

func (s *Server) ForwardWrite(ctx context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	return s.xfer.ForwardWrite(ctx, req)
}

func (s *Server) CompleteRangeTransfer(ctx context.Context, req *pb.CompleteRangeTransferRequest) (*pb.CompleteRangeTransferResponse, error) {
	return s.xfer.CompleteRangeTransfer(ctx, req)
}

func (s *Server) UpdateReplicaRank(ctx context.Context, req *pb.UpdateReplicaRankRequest) (*pb.UpdateReplicaRankResponse, error) {
	return s.repl.UpdateReplicaRank(ctx, req)
}

func (s *Server) CreateReplica(ctx context.Context, req *pb.CreateReplicaRequest) (*pb.CreateReplicaResponse, error) {
	return s.repl.CreateReplica(ctx, req)
}
