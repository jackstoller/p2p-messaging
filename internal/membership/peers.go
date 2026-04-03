package membership

import "github.com/jackstoller/p2p-messaging/internal/logging"

// Upserts a peer into the member list and rebuilds the ring.
func (m *Manager) AddOrUpdatePeer(nodeId, address string, vnodes []Vnode) {
	log := logging.Component("membership.peers")
	m.mu.Lock()
	defer m.mu.Unlock()

	if nodeId == m.selfId {
		log.Debug("membership.peer.upsert", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, nodeId, "reason", "self")
		return
	}

	existing, ok := m.members[nodeId]
	action := "created"
	if ok {
		existing.Address = address
		existing.Vnodes = append(existing.Vnodes[:0], vnodes...)
		existing.State = PeerUp
		action = "updated"
	} else {
		copied := append([]Vnode(nil), vnodes...)
		m.members[nodeId] = &Peer{NodeId: nodeId, Address: address, Vnodes: copied, State: PeerUp}
	}

	m.rebuildRingLocked()
	log.Info("membership.peer.upsert", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, nodeId, logging.AttrPeerAddr, address, "action", action, "vnodes", len(vnodes), "ring_entries", m.Ring.Len())
}

// Marks a peer as dead and cleans up
func (m *Manager) MarkDown(nodeId string) {
	log := logging.Component("membership.peers")
	m.mu.Lock()
	peer, ok := m.members[nodeId]
	if !ok || peer.State == PeerDown {
		m.mu.Unlock()
		log.Debug("membership.peer.down", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerId, nodeId, "reason", "missing_or_already_down")
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
	log.Warn("membership.peer.down", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, nodeId, logging.AttrPeerAddr, deadAddr, "ring_entries", m.Ring.Len())

	if m.OnPeerDown != nil {
		go m.OnPeerDown(nodeId)
	}
}

func (m *Manager) MarkSuspect(nodeId string) {
	m.suspectsMu.Lock()
	m.suspects[nodeId] = struct{}{}
	m.suspectsMu.Unlock()
	logging.Component("membership.peers").Warn("membership.peer.suspect", logging.Outcome(logging.OutcomeStarted), logging.AttrPeerId, nodeId)
}

func (m *Manager) ClearSuspect(nodeId string) {
	m.suspectsMu.Lock()
	delete(m.suspects, nodeId)
	m.suspectsMu.Unlock()
	logging.Component("membership.peers").Debug("membership.peer.suspect.clear", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, nodeId)
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
