package server

import (
	"context"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// VNodeStatusUpdate announces that a target vnode is now active.
func (s *Server) VNodeStatusUpdate(_ context.Context, req *pb.VNodeStatusUpdateRequest) (*pb.VNodeStatusUpdateResponse, error) {
	log := logging.Component("server.transfer")
	if req.State != pb.VnodeState_VNODE_ACTIVE {
		log.Debug("rpc.transfer.vnode_status", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, req.TargetVnodeId, logging.AttrNodeId, req.NodeId, "state", req.State.String())
		return &pb.VNodeStatusUpdateResponse{}, nil
	}

	s.mgr.SetVnodeState(req.NodeId, req.TargetVnodeId, membership.VnodeActive)
	s.xfer.ClearCompletedTransfer(req.TargetVnodeId, req.NodeId)
	s.repl.OnTopologyChanged(context.Background(), req.TargetVnodeId)

	log.Info("rpc.transfer.vnode_status", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, req.TargetVnodeId, logging.AttrNodeId, req.NodeId, "state", req.State.String())
	return &pb.VNodeStatusUpdateResponse{}, nil
}

func (s *Server) RequestRangeTransfer(ctx context.Context, req *pb.RequestRangeTransferRequest) (*pb.RequestRangeTransferResponse, error) {
	logging.Component("server.transfer").Info("rpc.transfer.request_range", logging.Outcome(logging.OutcomeStarted), "transfer_id", req.GetTransferId(), "requestor_id", req.GetRequestor())
	return s.xfer.RequestRangeTransfer(ctx, req)
}

func (s *Server) StreamRange(req *pb.StreamRangeRequest, stream pb.TransferService_StreamRangeServer) error {
	logging.Component("server.transfer").Info("rpc.transfer.stream_range", logging.Outcome(logging.OutcomeStarted), "stream_id", req.GetTransferId())
	return s.xfer.StreamRange(req, stream)
}

func (s *Server) ForwardWrite(ctx context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	logging.Component("server.transfer").Info("rpc.transfer.forward_write", logging.Outcome(logging.OutcomeStarted), "write_id", req.GetWriteId(), logging.AttrKey, req.GetRecord().GetKey())
	return s.xfer.ForwardWrite(ctx, req)
}

func (s *Server) CompleteRangeTransfer(ctx context.Context, req *pb.CompleteRangeTransferRequest) (*pb.CompleteRangeTransferResponse, error) {
	logging.Component("server.transfer").Info("rpc.transfer.complete_range", logging.Outcome(logging.OutcomeStarted), "transfer_id", req.GetTransferId())
	return s.xfer.CompleteRangeTransfer(ctx, req)
}

func (s *Server) UpdateReplicaRank(ctx context.Context, req *pb.UpdateReplicaRankRequest) (*pb.UpdateReplicaRankResponse, error) {
	logging.Component("server.transfer").Info("rpc.replica.update_rank", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, req.GetVnodeId(), "primary_id", req.GetPrimaryId(), "new_rank", req.GetNewRank())
	return s.repl.UpdateReplicaRank(ctx, req)
}

func (s *Server) CreateReplica(ctx context.Context, req *pb.CreateReplicaRequest) (*pb.CreateReplicaResponse, error) {
	logging.Component("server.transfer").Info("rpc.replica.create", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, req.GetVnodeId(), logging.AttrKey, req.GetRecord().GetKey())
	return s.repl.CreateReplica(ctx, req)
}
