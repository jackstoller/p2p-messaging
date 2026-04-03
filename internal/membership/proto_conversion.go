package membership

import pb "github.com/jackstoller/p2p-messaging/proto"

func protoToPeer(pm *pb.Member) Peer {
	vnodes := make([]Vnode, 0, len(pm.Vnodes))
	for _, pv := range pm.Vnodes {
		state := VnodeInactive
		if pv.State == pb.VnodeState_VNODE_ACTIVE {
			state = VnodeActive
		}
		vnodes = append(vnodes, Vnode{Id: pv.Id, Position: pv.Position, State: state})
	}

	state := PeerUp
	if pm.State == pb.PeerState_PEER_DOWN {
		state = PeerDown
	}
	return Peer{NodeId: pm.NodeId, Address: pm.Address, Vnodes: vnodes, State: state}
}

func peerToProto(p Peer) *pb.Member {
	state := pb.PeerState_PEER_UP
	if p.State == PeerDown {
		state = pb.PeerState_PEER_DOWN
	}
	return &pb.Member{
		NodeId:  p.NodeId,
		Address: p.Address,
		Vnodes:  vnodesProto(p.NodeId, p.Address, p.Vnodes),
		State:   state,
	}
}

func vnodesProto(nodeId, address string, vnodes []Vnode) []*pb.VirtualNode {
	result := make([]*pb.VirtualNode, len(vnodes))
	for i, vn := range vnodes {
		state := pb.VnodeState_VNODE_INACTIVE
		if vn.State == VnodeActive {
			state = pb.VnodeState_VNODE_ACTIVE
		}
		result[i] = &pb.VirtualNode{Id: vn.Id, NodeId: nodeId, Position: vn.Position, Address: address, State: state}
	}
	return result
}
