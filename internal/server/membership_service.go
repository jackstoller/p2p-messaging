package server

import (
	"context"
	"log/slog"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterNode handles a new node introducing itself to this node.
func (s *Server) RegisterNode(_ context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if req.Member == nil {
		return nil, status.Error(codes.InvalidArgument, "member is required")
	}

	s.mgr.AddOrUpdatePeer(req.Member.NodeId, req.Member.Address, protosToVnodes(req.Member.Vnodes))
	slog.Info("RegisterNode", "nodeId", req.Member.NodeId, "addr", req.Member.Address)
	return &pb.RegisterNodeResponse{Accepted: true}, nil
}

// ListNodes returns all known UP members so joiners can build their ring.
func (s *Server) ListNodes(_ context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	members := s.mgr.AllMembers()
	resp := &pb.ListNodesResponse{}
	for _, m := range members {
		if m.State == membership.PeerDown {
			continue
		}
		resp.Members = append(resp.Members, peerToProto(m))
	}
	slog.Debug("ListNodes", "requester", req.RequestingNodeId, "returned", len(resp.Members))
	return resp, nil
}

// NodeStatus applies membership state updates broadcast by peers.
func (s *Server) NodeStatus(_ context.Context, req *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
	switch req.State {
	case pb.PeerState_PEER_UP:
		s.mgr.AddOrUpdatePeer(req.NodeId, addressFromVnodes(req.Vnodes), protosToVnodes(req.Vnodes))
		slog.Info("NodeStatus UP", "nodeId", req.NodeId)
	case pb.PeerState_PEER_DOWN:
		slog.Warn("NodeStatus DOWN received", "nodeId", req.NodeId)
		s.mgr.MarkDown(req.NodeId)
	}
	return &pb.NodeStatusResponse{}, nil
}

// Ping responds to liveness probes.
func (s *Server) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{NodeId: s.mgr.Self().NodeId}, nil
}

// ConfirmSuspect returns whether this node currently suspects the target.
func (s *Server) ConfirmSuspect(_ context.Context, req *pb.ConfirmSuspectRequest) (*pb.ConfirmSuspectResponse, error) {
	return &pb.ConfirmSuspectResponse{Confirmed: s.mgr.IsSuspect(req.SuspectNodeId)}, nil
}
