package membership

import (
	"crypto/tls"
	"log/slog"

	"github.com/jackstoller/p2p-messaging/internal/ring"
)

// Initializes membership with local vnodes starting INACTIVE.
func NewManager(nodeId, address string, replicaCount int, tlsCfg *tls.Config) *Manager {
	self := buildSelf(nodeId, address)
	return &Manager{
		selfId:       nodeId,
		members:      map[string]*Peer{nodeId: &self},
		Ring:         ring.New(),
		suspects:     map[string]struct{}{},
		replicaCount: replicaCount,
		tlsCfg:       tlsCfg,
		clients:      map[string]*cachedConn{},
	}
}

func buildSelf(nodeId, address string) Peer {
	vnodes := make([]Vnode, ring.DefaultVnodeCount)
	for i := range vnodes {
		vnodes[i] = Vnode{
			Id:       ring.VnodeId(nodeId, i),
			Position: ring.VnodePosition(nodeId, i),
			State:    VnodeInactive,
		}
	}
	return Peer{NodeId: nodeId, Address: address, Vnodes: vnodes, State: PeerUp}
}

// Return all ACTIVE vnodes on UP peers.
// Caller must hold m.mu.
func (m *Manager) getActiveVnodeEntriesLocked() []ring.VnodeEntry {
	var entries []ring.VnodeEntry
	for _, peer := range m.members {
		if peer.State == PeerDown {
			continue
		}
		for _, vn := range peer.Vnodes {
			if vn.State != VnodeActive {
				continue
			}
			entries = append(entries, ring.VnodeEntry{
				Id:       vn.Id,
				Position: vn.Position,
				NodeId: peer.NodeId,
				Address: peer.Address,
			})
		}
	}
	return entries
}

// Rebuilds the ring from m.members
// Caller must hold m.mu.
func (m *Manager) rebuildRingLocked() {
	m.Ring.Rebuild(m.getActiveVnodeEntriesLocked())
}

// Marks every local vnode ACTIVE.
// Note: This is used when the very first node joins.
func (m *Manager) ActivateSelf() {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer := m.members[m.selfId]
	for i := range peer.Vnodes {
		peer.Vnodes[i].State = VnodeActive
	}

	m.rebuildRingLocked()
	slog.Info("self-activated all vnodes", "nodeId", m.selfId, "vnodes", len(peer.Vnodes))
}

// SetVnodeState updates one vnode state and rebuilds the ring if changed.
// It returns true when the vnode exists on the target node.
func (m *Manager) SetVnodeState(nodeId, vnodeId string, state VnodeState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.members[nodeId]
	if !ok {
		return false
	}
	for i := range peer.Vnodes {
		if peer.Vnodes[i].Id == vnodeId { // TODO: Could we just use indexes?
			if peer.Vnodes[i].State == state {
				return true
			}
			peer.Vnodes[i].State = state
			m.rebuildRingLocked()
			return true
		}
	}
	return false
}

func (m *Manager) SelfId() string { return m.selfId }

func (m *Manager) PeerById(nodeId string) (Peer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.members[nodeId]
	if !ok {
		return Peer{}, false
	}
	return *p, true
}
