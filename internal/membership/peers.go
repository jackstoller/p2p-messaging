package membership

import "log/slog"

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
		existing.Vnodes = mergeVnodes(existing.Vnodes, vnodes) // TODO: Should we just overwrite?
		existing.State = PeerUp
	} else {
		m.members[nodeId] = &Peer{NodeId: nodeId, Address: address, Vnodes: vnodes, State: PeerUp}
	}

	m.rebuildRingLocked()
	slog.Info("peer added/updated", "nodeId", nodeId)
}

// Marks a peer as dead and cleans up
func (m *Manager) MarkDown(nodeId string) {
	m.mu.Lock()
	peer, ok := m.members[nodeId]
	if !ok || peer.State == PeerDown {
		m.mu.Unlock()
		return
	}
	peer.State = PeerDown
	deadAddr := peer.Address
	m.rebuildRingLocked()
	m.mu.Unlock()

	m.clientsMu.Lock()
	if c, ok := m.clients[deadAddr]; ok {
		_ = c.conn.Close()
		delete(m.clients, deadAddr)
	}
	m.clientsMu.Unlock()

	m.ClearSuspect(nodeId)
	slog.Warn("peer marked dead", "nodeId", nodeId)

	if m.OnPeerDown != nil {
		go m.OnPeerDown(nodeId)
	}
}

func (m *Manager) MarkSuspect(nodeId string) {
	m.suspectsMu.Lock()
	m.suspects[nodeId] = struct{}{}
	m.suspectsMu.Unlock()
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
