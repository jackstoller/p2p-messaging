package server

import (
	"context"
	"log/slog"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	pb "github.com/jackstoller/p2p-messaging/proto/mesh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements all four gRPC services.
// Phase 1: MembershipService is fully implemented.
// Phase 2: TransferService (VnodeStatusUpdate implemented; rest stubbed).
// Phase 3: ReplicaService and DataService are stubbed.
type Server struct {
	pb.UnimplementedMembershipServiceServer
	pb.UnimplementedTransferServiceServer
	pb.UnimplementedReplicaServiceServer
	pb.UnimplementedDataServiceServer

	mgr   *membership.Manager
	store *storage.Store
}

// New creates a Server.
func New(mgr *membership.Manager, store *storage.Store) *Server {
	return &Server{mgr: mgr, store: store}
}

// ─── MembershipService ────────────────────────────────────────────────────────

// RegisterNode handles a new node introducing itself to this node.
// The new node is added to the member list with all vnodes INACTIVE.
func (s *Server) RegisterNode(_ context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if req.Member == nil {
		return nil, status.Error(codes.InvalidArgument, "member is required")
	}
	vnodes := protosToVnodes(req.Member.Vnodes)
	s.mgr.AddOrUpdatePeer(req.Member.NodeId, req.Member.Address, vnodes)
	slog.Info("RegisterNode", "nodeID", req.Member.NodeId, "addr", req.Member.Address)
	return &pb.RegisterNodeResponse{Accepted: true}, nil
}

// ListNodes returns all known UP members, used by a joining node to build its
// initial ring. DOWN members are omitted since they are no longer routing.
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

// NodeStatus handles a broadcast state change from another node.
// PEER_UP: add/update the node in the member list.
// PEER_DOWN: mark the node dead (idempotent; duplicate DOWN messages ignored).
func (s *Server) NodeStatus(_ context.Context, req *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
	switch req.State {
	case pb.PeerState_PEER_UP:
		vnodes := protosToVnodes(req.Vnodes)
		s.mgr.AddOrUpdatePeer(req.NodeId, addressFromVnodes(req.Vnodes), vnodes)
		slog.Info("NodeStatus UP", "nodeID", req.NodeId)

	case pb.PeerState_PEER_DOWN:
		slog.Warn("NodeStatus DOWN received", "nodeID", req.NodeId)
		s.mgr.MarkDown(req.NodeId)
	}
	return &pb.NodeStatusResponse{}, nil
}

// Ping responds to a liveness probe.
func (s *Server) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{NodeId: s.mgr.Self().NodeID}, nil
}

// ConfirmSuspect reports whether this node has also failed to reach the suspect.
// Returns confirmed=true if and only if the suspect is in our local suspect set.
func (s *Server) ConfirmSuspect(_ context.Context, req *pb.ConfirmSuspectRequest) (*pb.ConfirmSuspectResponse, error) {
	confirmed := s.mgr.IsSuspect(req.SuspectNodeId)
	return &pb.ConfirmSuspectResponse{Confirmed: confirmed}, nil
}

// ─── TransferService (Phase 2) ────────────────────────────────────────────────

// VnodeStatusUpdate is handled here (rather than left as a stub) because it is
// the final step of any transfer and must update the ring immediately.
func (s *Server) VnodeStatusUpdate(_ context.Context, req *pb.VnodeStatusUpdateRequest) (*pb.VnodeStatusUpdateResponse, error) {
	if req.State == pb.VnodeState_VNODE_ACTIVE {
		s.mgr.ActivateVnode(req.OwnerId, req.VnodeId)
		slog.Info("VnodeStatusUpdate ACTIVE", "vnodeID", req.VnodeId, "owner", req.OwnerId)
	}
	return &pb.VnodeStatusUpdateResponse{}, nil
}

// RequestTransfer, StreamSlice, ForwardWrite, CompleteTransfer — Phase 2.
// Handled by UnimplementedTransferServiceServer until Phase 2 is wired.

// ─── ReplicaService (Phase 3) ─────────────────────────────────────────────────
// UpdateReplicaRank, CreateReplica — Phase 3.
// Handled by UnimplementedReplicaServiceServer until Phase 3 is wired.

// ─── DataService (Phase 3) ────────────────────────────────────────────────────
// Write, Read — Phase 3.
// Handled by UnimplementedDataServiceServer until Phase 3 is wired.

// ─── Helpers ─────────────────────────────────────────────────────────────────

func protosToVnodes(pvs []*pb.VirtualNode) []membership.Vnode {
	vnodes := make([]membership.Vnode, 0, len(pvs))
	for _, pv := range pvs {
		state := membership.VnodeInactive
		if pv.State == pb.VnodeState_VNODE_ACTIVE {
			state = membership.VnodeActive
		}
		vnodes = append(vnodes, membership.Vnode{
			ID:       pv.Id,
			Position: pv.Position,
			State:    state,
		})
	}
	return vnodes
}

func peerToProto(p membership.Peer) *pb.Member {
	pbState := pb.PeerState_PEER_UP
	if p.State == membership.PeerDown {
		pbState = pb.PeerState_PEER_DOWN
	}
	pvs := make([]*pb.VirtualNode, 0, len(p.Vnodes))
	for _, vn := range p.Vnodes {
		pbVnState := pb.VnodeState_VNODE_INACTIVE
		if vn.State == membership.VnodeActive {
			pbVnState = pb.VnodeState_VNODE_ACTIVE
		}
		pvs = append(pvs, &pb.VirtualNode{
			Id:       vn.ID,
			NodeId:   p.NodeID,
			Position: vn.Position,
			Address:  p.Address,
			State:    pbVnState,
		})
	}
	return &pb.Member{
		NodeId:  p.NodeID,
		Address: p.Address,
		Vnodes:  pvs,
		State:   pbState,
	}
}

// addressFromVnodes extracts the physical node address from vnode protos.
// All vnodes for one physical node share the same address.
func addressFromVnodes(pvs []*pb.VirtualNode) string {
	for _, pv := range pvs {
		if pv.Address != "" {
			return pv.Address
		}
	}
	return ""
}
