package server

import (
	"github.com/jackstoller/p2p-messaging/internal/membership"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

func protosToVnodes(pvs []*pb.VirtualNode) []membership.Vnode {
	vnodes := make([]membership.Vnode, 0, len(pvs))
	for _, pv := range pvs {
		state := membership.VnodeInactive
		if pv.State == pb.VnodeState_VNODE_ACTIVE {
			state = membership.VnodeActive
		}
		vnodes = append(vnodes, membership.Vnode{
			Id:       pv.Id,
			Position: pv.Position,
			State:    state,
		})
	}
	return vnodes
}

func peerToProto(p membership.Peer) *pb.Member {
	state := pb.PeerState_PEER_UP
	if p.State == membership.PeerDown {
		state = pb.PeerState_PEER_DOWN
	}

	vnodes := make([]*pb.VirtualNode, 0, len(p.Vnodes))
	for _, vn := range p.Vnodes {
		vnodeState := pb.VnodeState_VNODE_INACTIVE
		if vn.State == membership.VnodeActive {
			vnodeState = pb.VnodeState_VNODE_ACTIVE
		}
		vnodes = append(vnodes, &pb.VirtualNode{
			Id:       vn.Id,
			NodeId:   p.NodeId,
			Position: vn.Position,
			Address:  p.Address,
			State:    vnodeState,
		})
	}

	return &pb.Member{
		NodeId:  p.NodeId,
		Address: p.Address,
		Vnodes:  vnodes,
		State:   state,
	}
}

func addressFromVnodes(pvs []*pb.VirtualNode) string {
	for _, pv := range pvs {
		if pv.Address != "" {
			return pv.Address
		}
	}
	return ""
}
