package membership

import "github.com/jackstoller/p2p-messaging/internal/logging"

// Upserts a peer into the member list and rebuilds the ring.
func (m *Manager) AddOrUpdatePeer(nodeId, address string, vnodes []Vnode) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if nodeId == m.selfId {
		return
	}

	existing, ok := m.members[nodeId]
	if ok {
		existing.Address = address
		existing.Vnodes = append(existing.Vnodes[:0], vnodes...)
		existing.State = PeerUp
	} else {
		copied := append([]Vnode(nil), vnodes...)
		m.members[nodeId] = &Peer{NodeId: nodeId, Address: address, Vnodes: copied, State: PeerUp}
	}

	m.rebuildRingLocked()
}

// Marks a peer as dead and cleans up
func (m *Manager) MarkDown(nodeId string) {
	m.markDown(nodeId, true)
}

// MarkDownQuiet marks a peer dead without emitting the local down confirmation logging.
// Used when applying a down state that another node already confirmed and broadcast.
func (m *Manager) MarkDownQuiet(nodeId string) {
	m.markDown(nodeId, false)
}

func (m *Manager) markDown(nodeId string, emitLog bool) {
	m.mu.Lock()
	peer, ok := m.members[nodeId]
	if !ok || peer.State == PeerDown {
		m.mu.Unlock()
		if emitLog {
		}
		return
	}
	peer.State = PeerDown
	deadAddr := peer.Address
	m.rebuildRingLocked()
	ringEntries := m.Ring.Len()
	m.mu.Unlock()

	m.clientsMu.Lock()
	if c, ok := m.clients[deadAddr]; ok {
		_ = c.conn.Close()
		delete(m.clients, deadAddr)
	}
	m.clientsMu.Unlock()

	m.ClearSuspect(nodeId)
	if emitLog {
		logging.Warn("Confirmed node is down with peer id=%v, peer addr=%v, ring entries=%v.", nodeId, deadAddr, ringEntries)
	}

	if m.OnPeerDown != nil {
		go m.OnPeerDown(nodeId)
	}
}

func (m *Manager) MarkSuspect(nodeId string) bool {
	m.suspectsMu.Lock()
	if _, exists := m.suspects[nodeId]; exists {
		m.suspectsMu.Unlock()
		return false
	}
	m.suspects[nodeId] = struct{}{}
	m.suspectsMu.Unlock()
	logging.Warn("Suspecting node is down with peer id=%v.", nodeId)
	return true
}

func (m *Manager) ClearSuspect(nodeId string) {
	m.suspectsMu.Lock()
	delete(m.suspects, nodeId)
	m.suspectsMu.Unlock()
}

func (m *Manager) IsDown(nodeId string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	peer, ok := m.members[nodeId]
	return ok && peer.State == PeerDown
}

func (m *Manager) IsSuspect(nodeId string) bool {
	m.suspectsMu.RLock()
	defer m.suspectsMu.RUnlock()
	_, ok := m.suspects[nodeId]
	return ok
}
