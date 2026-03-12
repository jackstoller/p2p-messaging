package server

import (
	"context"
	"log/slog"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// VNodeStatusUpdate announces that a target vnode is now active.
func (s *Server) VNodeStatusUpdate(_ context.Context, req *pb.VNodeStatusUpdateRequest) (*pb.VNodeStatusUpdateResponse, error) {
	if req.State != pb.VnodeState_VNODE_ACTIVE {
		return &pb.VNodeStatusUpdateResponse{}, nil
	}

	s.mgr.SetVnodeState(req.NodeId, req.TargetVnodeId, membership.VnodeActive)
	s.xfer.ClearCompletedTransfer(req.TargetVnodeId, req.NodeId)
	if req.NodeId == s.mgr.SelfId() {
		s.repl.OnVnodeActive(context.Background(), req.TargetVnodeId)
	}

	slog.Info("VNodeStatusUpdate ACTIVE", "targetVnodeId", req.TargetVnodeId, "owner", req.NodeId)
	return &pb.VNodeStatusUpdateResponse{}, nil
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
