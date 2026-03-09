package membership

import pb "github.com/jackstoller/p2p-messaging/proto"

func (m *Manager) Self() Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return *m.members[m.selfId]
}

func (m *Manager) SelfProto() *pb.Member {
	return peerToProto(m.Self())
}

// ActivePeers returns UP, non-suspect peers excluding self.
func (m *Manager) ActivePeers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.suspectsMu.RLock()
	defer m.suspectsMu.RUnlock()

	var result []Peer
	for _, p := range m.members {
		if p.NodeId == m.selfId || p.State == PeerDown {
			continue
		}
		if _, suspect := m.suspects[p.NodeId]; suspect {
			continue
		}
		result = append(result, *p)
	}
	return result
}

func (m *Manager) AllMembers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Peer, 0, len(m.members))
	for _, p := range m.members {
		result = append(result, *p)
	}
	return result
}

func (m *Manager) UpPeers() []Peer { return m.upPeers() }

func (m *Manager) ReplicaCount() int { return m.replicaCount }

func (m *Manager) upPeers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []Peer
	for _, p := range m.members {
		if p.NodeId != m.selfId && p.State == PeerUp {
			result = append(result, *p)
		}
	}
	return result
}

func (m *Manager) selfVnodesProto() []*pb.VirtualNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	self := m.members[m.selfId]
	return vnodesProto(self.NodeId, self.Address, self.Vnodes)
}
