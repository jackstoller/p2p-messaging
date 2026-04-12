package server

import (
	"context"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterNode handles a new node introducing itself to this node.
func (s *Server) RegisterNode(_ context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if req.Member == nil {
		logging.Warn("Rejected membership registration because member payload was missing.")
		return nil, status.Error(codes.InvalidArgument, "member is required")
	}

	s.mgr.AddOrUpdatePeer(req.Member.NodeId, req.Member.Address, protosToVnodes(req.Member.Vnodes))
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
	return resp, nil
}

// NodeStatus applies membership state updates broadcast by peers.
func (s *Server) NodeStatus(_ context.Context, req *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
	switch req.State {
	case pb.PeerState_PEER_UP:
		s.mgr.AddOrUpdatePeer(req.NodeId, addressFromVnodes(req.Vnodes), protosToVnodes(req.Vnodes))
	case pb.PeerState_PEER_DOWN:
		s.mgr.MarkDownQuiet(req.NodeId)
	default:
		logging.Warn("Rejected membership status update with unknown state with peer=%v, state=%v.", req.NodeId, req.State.String())
	}
	return &pb.NodeStatusResponse{}, nil
}

// Ping responds to liveness probes.
func (s *Server) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{NodeId: s.mgr.Self().NodeId}, nil
}

// ConfirmSuspect returns whether this node can also verify the target is unreachable.
func (s *Server) ConfirmSuspect(ctx context.Context, req *pb.ConfirmSuspectRequest) (*pb.ConfirmSuspectResponse, error) {
	confirmed := s.mgr.ConfirmPeerUnreachable(ctx, req.SuspectNodeId)
	if confirmed {
		logging.Warn("Confirmed node is down with peer=%v.", req.SuspectNodeId)
	} else {
	}
	return &pb.ConfirmSuspectResponse{Confirmed: confirmed}, nil
}
