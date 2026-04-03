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
	log := logging.Component("server.membership")
	if req.Member == nil {
		log.Warn("rpc.membership.register", logging.Outcome(logging.OutcomeRejected), "reason", "member_required")
		return nil, status.Error(codes.InvalidArgument, "member is required")
	}

	s.mgr.AddOrUpdatePeer(req.Member.NodeId, req.Member.Address, protosToVnodes(req.Member.Vnodes))
	log.Info("rpc.membership.register", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, req.Member.NodeId, logging.AttrPeerAddr, req.Member.Address, "vnodes", len(req.Member.Vnodes))
	return &pb.RegisterNodeResponse{Accepted: true}, nil
}

// ListNodes returns all known UP members so joiners can build their ring.
func (s *Server) ListNodes(_ context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	log := logging.Component("server.membership")
	members := s.mgr.AllMembers()
	resp := &pb.ListNodesResponse{}
	for _, m := range members {
		if m.State == membership.PeerDown {
			continue
		}
		resp.Members = append(resp.Members, peerToProto(m))
	}
	log.Info("rpc.membership.list", logging.Outcome(logging.OutcomeSucceeded), "requester_id", req.RequestingNodeId, "returned", len(resp.Members))
	return resp, nil
}

// NodeStatus applies membership state updates broadcast by peers.
func (s *Server) NodeStatus(_ context.Context, req *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
	log := logging.Component("server.membership")
	switch req.State {
	case pb.PeerState_PEER_UP:
		s.mgr.AddOrUpdatePeer(req.NodeId, addressFromVnodes(req.Vnodes), protosToVnodes(req.Vnodes))
		log.Info("rpc.membership.status", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, req.NodeId, "state", req.State.String(), "vnodes", len(req.Vnodes))
	case pb.PeerState_PEER_DOWN:
		log.Warn("rpc.membership.status", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, req.NodeId, "state", req.State.String())
		s.mgr.MarkDown(req.NodeId)
	default:
		log.Warn("rpc.membership.status", logging.Outcome(logging.OutcomeRejected), logging.AttrPeerId, req.NodeId, "state", req.State.String(), "reason", "unknown_state")
	}
	return &pb.NodeStatusResponse{}, nil
}

// Ping responds to liveness probes.
func (s *Server) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	logging.Component("server.membership").Debug("rpc.membership.ping", logging.Outcome(logging.OutcomeSucceeded), "responder_id", s.mgr.Self().NodeId)
	return &pb.PingResponse{NodeId: s.mgr.Self().NodeId}, nil
}

// ConfirmSuspect returns whether this node currently suspects the target.
func (s *Server) ConfirmSuspect(_ context.Context, req *pb.ConfirmSuspectRequest) (*pb.ConfirmSuspectResponse, error) {
	confirmed := s.mgr.IsSuspect(req.SuspectNodeId)
	logging.Component("server.membership").Info("rpc.membership.confirm_suspect", logging.Outcome(logging.OutcomeSucceeded), "suspect_id", req.SuspectNodeId, "confirmed", confirmed)
	return &pb.ConfirmSuspectResponse{Confirmed: confirmed}, nil
}
