package membership

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto/mesh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ─── Types ───────────────────────────────────────────────────────────────────

// VnodeState mirrors the proto enum but lives in the membership layer to
// decouple internal state from the wire format.
type VnodeState int

const (
	VnodeInactive VnodeState = iota
	VnodeActive
)

// PeerState is the broadcast state of a physical node; SUSPECT is local-only
// and is tracked separately in the suspects set.
type PeerState int

const (
	PeerUp PeerState = iota
	PeerDown
)

// Vnode is one virtual-node position belonging to a physical node.
type Vnode struct {
	ID       string // "nodeID:index"
	Position uint64
	State    VnodeState
}

// Peer is a known physical node in the mesh.
type Peer struct {
	NodeID  string
	Address string
	Vnodes  []Vnode
	State   PeerState
}

// GetNodeID returns the node ID of this peer (implements BroadcastPeer interface).
func (p Peer) GetNodeID() string {
	return p.NodeID
}

// ─── Manager ─────────────────────────────────────────────────────────────────

// Manager owns the member list, routing ring, and gRPC client cache.
// It is the single source of truth for mesh topology.
type Manager struct {
	mu      sync.RWMutex
	selfID  string
	self    Peer
	members map[string]*Peer // nodeID → peer, includes self

	Ring *ring.Ring

	// suspects tracks nodes this instance has locally failed to ping.
	// Suspect state is never broadcast; only UP/DOWN are shared.
	suspectsMu sync.Mutex
	suspects   map[string]struct{}

	replicaCount int
	tlsCfg       *tls.Config

	clientsMu sync.Mutex
	clients   map[string]*cachedConn

	// OnPeerDown is called (in a goroutine) when a peer is confirmed dead.
	// Wire this up in main for Phase 2/3 handlers.
	OnPeerDown func(nodeID string)
}

type cachedConn struct {
	conn   *grpc.ClientConn
	expiry time.Time
}

const connTTL = 5 * time.Minute

// NewManager creates a Manager for this node. The node starts with all its own
// vnodes INACTIVE; call ActivateSelf() if this is the first node in the network,
// or let the transfer flow activate them as ranges are claimed.
func NewManager(nodeID, address string, replicaCount int, tlsCfg *tls.Config) *Manager {
	self := buildSelf(nodeID, address)
	m := &Manager{
		selfID:       nodeID,
		self:         self,
		members:      map[string]*Peer{nodeID: &self},
		Ring:         ring.New(),
		suspects:     map[string]struct{}{},
		replicaCount: replicaCount,
		tlsCfg:       tlsCfg,
		clients:      map[string]*cachedConn{},
	}
	return m
}

// buildSelf constructs this node's Peer descriptor. Virtual node positions
// are derived deterministically from nodeID so they survive restarts.
func buildSelf(nodeID, address string) Peer {
	vnodes := make([]Vnode, ring.DefaultVnodeCount)
	for i := range vnodes {
		vnodes[i] = Vnode{
			ID:       ring.VnodeID(nodeID, i),
			Position: ring.VnodePosition(nodeID, i),
			State:    VnodeInactive,
		}
	}
	return Peer{
		NodeID:  nodeID,
		Address: address,
		Vnodes:  vnodes,
		State:   PeerUp,
	}
}

// ─── Join flow ────────────────────────────────────────────────────────────────

// Join runs steps 1–3 of the join flow:
//  1. Register with any reachable bootstrap peer.
//  2. Fetch the full member list from that bootstrap and rebuild the ring.
//  3. Announce presence (UP, all vnodes INACTIVE) to all known peers.
func (m *Manager) Join(ctx context.Context, bootstrapPeers []string) error {
	// Step 1: Register — try each bootstrap address until one accepts.
	bootstrapAddr, err := m.registerWithBootstrap(ctx, bootstrapPeers)
	if err != nil {
		return err
	}

	// Step 2: Fetch full member list and build the ring.
	if err := m.syncMembersFrom(ctx, bootstrapAddr); err != nil {
		return err
	}

	// Step 3: Announce self (UP, all vnodes INACTIVE) to all known peers.
	m.broadcastStatus(ctx, pb.PeerState_PEER_UP)

	slog.Info("joined mesh", "nodeID", m.selfID, "peers", len(m.ActivePeers()))
	return nil
}

func (m *Manager) registerWithBootstrap(ctx context.Context, peers []string) (string, error) {
	req := &pb.RegisterNodeRequest{Member: m.SelfProto()}
	for _, addr := range peers {
		client, err := m.MembershipClient(ctx, addr)
		if err != nil {
			slog.Warn("bootstrap unreachable", "addr", addr, "err", err)
			continue
		}
		resp, err := client.RegisterNode(ctx, req)
		if err != nil {
			slog.Warn("RegisterNode failed", "addr", addr, "err", err)
			continue
		}
		if !resp.Accepted {
			slog.Warn("RegisterNode rejected", "addr", addr)
			continue
		}
		slog.Info("registered with bootstrap", "addr", addr)
		return addr, nil
	}
	return "", errors.New("membership: no bootstrap peer accepted registration")
}

func (m *Manager) syncMembersFrom(ctx context.Context, addr string) error {
	client, err := m.MembershipClient(ctx, addr)
	if err != nil {
		return fmt.Errorf("membership: dial bootstrap for sync: %w", err)
	}
	resp, err := client.ListNodes(ctx, &pb.ListNodesRequest{RequestingNodeId: m.selfID})
	if err != nil {
		return fmt.Errorf("membership: ListNodes from %s: %w", addr, err)
	}

	m.mu.Lock()
	for _, pm := range resp.Members {
		if pm.NodeId == m.selfID {
			continue // skip self to avoid overwriting our own state
		}
		peer := protoToPeer(pm)
		m.members[peer.NodeID] = &peer
	}
	m.rebuildRingLocked()
	m.mu.Unlock()

	slog.Info("synced member list", "from", addr, "count", len(resp.Members))
	return nil
}

// broadcastStatus sends a NodeStatus RPC to all known UP peers (fire-and-forget).
// For PEER_UP the announcement includes all own vnodes (marked INACTIVE).
func (m *Manager) broadcastStatus(ctx context.Context, state pb.PeerState) {
	req := &pb.NodeStatusRequest{
		NodeId: m.selfID,
		State:  state,
	}
	if state == pb.PeerState_PEER_UP {
		req.Vnodes = m.selfVnodesProto()
	}

	// Broadcast to all UP peers.
	util.Broadcast(ctx, m.upPeers(), 3*time.Second, func(broadcastCtx context.Context, peer Peer) error {
		client, err := m.MembershipClient(broadcastCtx, peer.Address)
		if err != nil {
			return err
		}
		_, err = client.NodeStatus(broadcastCtx, req)
		return err
	})
}

// ─── State: self ─────────────────────────────────────────────────────────────

// ActivateSelf activates all own vnodes immediately. Used by the first node in
// the network, which has no predecessor to transfer ranges from.
func (m *Manager) ActivateSelf_NEVER_CALL() {
	m.mu.Lock()
	defer m.mu.Unlock()
	peer := m.members[m.selfID]
	for i := range peer.Vnodes {
		peer.Vnodes[i].State = VnodeActive
	}
	m.rebuildRingLocked()
	slog.Info("self-activated all vnodes", "nodeID", m.selfID, "vnodes", len(peer.Vnodes))
}

// ActivateVnode marks a single vnode ACTIVE and rebuilds the ring.
// Called by the server handler when a VnodeStatusUpdate(ACTIVE) is received.
func (m *Manager) ActivateVnode(nodeID, vnodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.members[nodeID]
	if !ok {
		return
	}
	for i := range peer.Vnodes {
		if peer.Vnodes[i].ID == vnodeID {
			peer.Vnodes[i].State = VnodeActive
			break
		}
	}
	m.rebuildRingLocked()
}

// ─── State: peers ─────────────────────────────────────────────────────────────

// AddOrUpdatePeer upserts a peer into the member list and rebuilds the ring.
// Called by the server when a NodeStatus(UP) is received.
func (m *Manager) AddOrUpdatePeer(nodeID, address string, vnodes []Vnode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID == m.selfID {
		return
	}
	existing, ok := m.members[nodeID]
	if ok {
		// Update address and vnodes; preserve any ACTIVE vnode states we
		// already know about (the remote might send INACTIVE in the initial
		// announcement but we may have since learned they're active).
		// QUESTION: I think this should always overwrite right???
		existing.Address = address
		existing.Vnodes = mergeVnodes(existing.Vnodes, vnodes)
		existing.State = PeerUp
	} else {
		m.members[nodeID] = &Peer{
			NodeID:  nodeID,
			Address: address,
			Vnodes:  vnodes,
			State:   PeerUp,
		}
	}
	m.rebuildRingLocked()
	slog.Info("peer added/updated", "nodeID", nodeID)
}

// MarkDown marks a peer as dead, removes it from the routing ring, evicts its
// cached gRPC connection, and fires OnPeerDown if wired.
// Called both internally (via the heartbeat flow) and by the server handler
// when a NodeStatus(DOWN) is received. Safe to call multiple times.
func (m *Manager) MarkDown(nodeID string) {
	m.mu.Lock()
	peer, ok := m.members[nodeID]
	if !ok || peer.State == PeerDown {
		m.mu.Unlock()
		return
	}
	peer.State = PeerDown
	deadAddr := peer.Address
	m.rebuildRingLocked()
	m.mu.Unlock()

	// Evict the cached connection so we don't keep dialling a dead node.
	m.clientsMu.Lock()
	if c, ok := m.clients[deadAddr]; ok {
		_ = c.conn.Close()
		delete(m.clients, deadAddr)
	}
	m.clientsMu.Unlock()

	m.ClearSuspect(nodeID)
	slog.Warn("peer marked dead", "nodeID", nodeID)

	if m.OnPeerDown != nil {
		go m.OnPeerDown(nodeID)
	}
}

// MarkSuspect flags a peer as locally suspected. Does not affect routing.
func (m *Manager) MarkSuspect(nodeID string) {
	m.suspectsMu.Lock()
	m.suspects[nodeID] = struct{}{}
	m.suspectsMu.Unlock()
}

// ClearSuspect removes the suspect flag for a peer.
func (m *Manager) ClearSuspect(nodeID string) {
	m.suspectsMu.Lock()
	delete(m.suspects, nodeID)
	m.suspectsMu.Unlock()
}

// IsDown reports whether a peer has been confirmed dead.
func (m *Manager) IsDown(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	peer, ok := m.members[nodeID]
	return ok && peer.State == PeerDown
}

// IsSuspect reports whether this node has locally flagged a peer as suspect.
func (m *Manager) IsSuspect(nodeID string) bool {
	m.suspectsMu.Lock()
	defer m.suspectsMu.Unlock()
	_, ok := m.suspects[nodeID]
	return ok
}

// ─── Queries ─────────────────────────────────────────────────────────────────

// Self returns this node's Peer descriptor.
func (m *Manager) Self() Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return *m.members[m.selfID]
}

// SelfProto converts self to the proto wire type.
func (m *Manager) SelfProto() *pb.Member {
	return peerToProto(m.Self())
}

// ActivePeers returns UP, non-suspect peers excluding self. Used by the
// heartbeat loop as the set of nodes to ping this tick.
func (m *Manager) ActivePeers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.suspectsMu.Lock()
	defer m.suspectsMu.Unlock()

	var result []Peer
	for _, p := range m.members {
		if p.NodeID == m.selfID || p.State == PeerDown {
			continue
		}
		if _, suspect := m.suspects[p.NodeID]; suspect {
			continue
		}
		result = append(result, *p)
	}
	return result
}

// AllMembers returns every known peer regardless of state, including self.
func (m *Manager) AllMembers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Peer, 0, len(m.members))
	for _, p := range m.members {
		result = append(result, *p)
	}
	return result
}

// ReplicaCount returns the configured replication factor.
func (m *Manager) ReplicaCount() int { return m.replicaCount }

// upPeers returns all UP peers excluding self. Holds no locks; intended for
// internal use where the lock situation is already managed.
func (m *Manager) upPeers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []Peer
	for _, p := range m.members {
		if p.NodeID != m.selfID && p.State == PeerUp {
			result = append(result, *p)
		}
	}
	return result
}

// ─── Ring ────────────────────────────────────────────────────────────────────

// rebuildRingLocked rebuilds the routing ring from all ACTIVE vnodes across
// all UP peers (including self). Caller must hold m.mu.
func (m *Manager) rebuildRingLocked() {
	var entries []ring.VnodeEntry
	for _, peer := range m.members {
		if peer.State == PeerDown {
			continue
		}
		for _, vn := range peer.Vnodes {
			if vn.State == VnodeActive {
				entries = append(entries, ring.VnodeEntry{
					Position: vn.Position,
					NodeID:   peer.NodeID,
					Address:  peer.Address,
				})
			}
		}
	}
	m.Ring.Rebuild(entries)
}

// ─── gRPC client vending ─────────────────────────────────────────────────────

func (m *Manager) MembershipClient(ctx context.Context, addr string) (pb.MembershipServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewMembershipServiceClient(conn), nil
}

func (m *Manager) TransferClient(ctx context.Context, addr string) (pb.TransferServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewTransferServiceClient(conn), nil
}

func (m *Manager) ReplicaClient(ctx context.Context, addr string) (pb.ReplicaServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewReplicaServiceClient(conn), nil
}

func (m *Manager) DataClient(ctx context.Context, addr string) (pb.DataServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewDataServiceClient(conn), nil
}

func (m *Manager) dial(addr string) (*grpc.ClientConn, error) {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()

	if c, ok := m.clients[addr]; ok && time.Now().Before(c.expiry) {
		return c.conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(m.tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("membership: dial %s: %w", addr, err)
	}
	m.clients[addr] = &cachedConn{conn: conn, expiry: time.Now().Add(connTTL)}
	return conn, nil
}

// ─── Proto conversion ─────────────────────────────────────────────────────────

func (m *Manager) selfVnodesProto() []*pb.VirtualNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	self := m.members[m.selfID]
	return vnodesProto(self.NodeID, self.Address, self.Vnodes)
}

func protoToPeer(pm *pb.Member) Peer {
	vnodes := make([]Vnode, 0, len(pm.Vnodes))
	for _, pv := range pm.Vnodes {
		state := VnodeInactive
		if pv.State == pb.VnodeState_VNODE_ACTIVE {
			state = VnodeActive
		}
		vnodes = append(vnodes, Vnode{
			ID:       pv.Id,
			Position: pv.Position,
			State:    state,
		})
	}
	state := PeerUp
	if pm.State == pb.PeerState_PEER_DOWN {
		state = PeerDown
	}
	return Peer{
		NodeID:  pm.NodeId,
		Address: pm.Address,
		Vnodes:  vnodes,
		State:   state,
	}
}

func peerToProto(p Peer) *pb.Member {
	state := pb.PeerState_PEER_UP
	if p.State == PeerDown {
		state = pb.PeerState_PEER_DOWN
	}
	return &pb.Member{
		NodeId:  p.NodeID,
		Address: p.Address,
		Vnodes:  vnodesProto(p.NodeID, p.Address, p.Vnodes),
		State:   state,
	}
}

func vnodesProto(nodeID, address string, vnodes []Vnode) []*pb.VirtualNode {
	result := make([]*pb.VirtualNode, len(vnodes))
	for i, vn := range vnodes {
		state := pb.VnodeState_VNODE_INACTIVE
		if vn.State == VnodeActive {
			state = pb.VnodeState_VNODE_ACTIVE
		}
		result[i] = &pb.VirtualNode{
			Id:       vn.ID,
			NodeId:   nodeID,
			Position: vn.Position,
			Address:  address,
			State:    state,
		}
	}
	return result
}

// mergeVnodes updates the existing vnode list with incoming states, preserving
// any ACTIVE state we already know about (incoming may send INACTIVE for a vnode
// we've already seen go ACTIVE via VnodeStatusUpdate).
// TODO: I think we should be eliminating this
func mergeVnodes(existing, incoming []Vnode) []Vnode {
	existingByID := make(map[string]VnodeState, len(existing))
	for _, vn := range existing {
		existingByID[vn.ID] = vn.State
	}
	merged := make([]Vnode, len(incoming))
	for i, vn := range incoming {
		if existing, ok := existingByID[vn.ID]; ok && existing == VnodeActive {
			vn.State = VnodeActive // don't downgrade
		}
		merged[i] = vn
	}
	return merged
}
