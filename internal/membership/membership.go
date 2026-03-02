package membership

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/ring"
	pb "github.com/jackstoller/p2p-messaging/proto/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Member represents a known node in the network.
type Member struct {
	NodeID  string
	Address string
	Vnodes  []VirtualNode
	Status  pb.MemberStatus
}

// VirtualNode is one ring position owned by a physical node.
type VirtualNode struct {
	Position uint64
	NodeID   string
	Address  string
}

// Manager owns the member list and the ring, and vends gRPC clients
// to peer nodes. It is the single source of truth for "who is alive."
type Manager struct {
	mu      sync.RWMutex
	self    Member
	members map[string]*Member // keyed by NodeID
	Ring    *ring.Ring

	tlsCfg        *tls.Config // mTLS config for outbound connections
	replicaCount  int
	clientCache   map[string]*clientEntry
	clientCacheMu sync.Mutex

	// Suspect tracking: nodeID → set of reporter IDs
	suspects   map[string]map[string]struct{}
	suspectsMu sync.Mutex
}

type clientEntry struct {
	conn   *grpc.ClientConn
	expiry time.Time
}

// New creates a Manager for this node.
func New(self Member, replicaCount int, tlsCfg *tls.Config) *Manager {
	m := &Manager{
		self:         self,
		members:      map[string]*Member{self.NodeID: &self},
		Ring:         ring.New(),
		tlsCfg:       tlsCfg,
		replicaCount: replicaCount,
		clientCache:  map[string]*clientEntry{},
		suspects:     map[string]map[string]struct{}{},
	}
	return m
}

// ─── member list mutations ────────────────────────────────────────────────────

// AddPending adds a node as PENDING (excluded from routing ring).
func (m *Manager) AddPending(member Member) {
	m.mu.Lock()
	defer m.mu.Unlock()
	member.Status = pb.MemberStatus_PENDING
	m.members[member.NodeID] = &member
	// Ring NOT rebuilt — pending nodes invisible to routing.
}

// Activate marks a node ACTIVE and rebuilds the ring.
func (m *Manager) Activate(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	mem, ok := m.members[nodeID]
	if !ok {
		return fmt.Errorf("membership: activate unknown node %s", nodeID)
	}
	mem.Status = pb.MemberStatus_ACTIVE
	m.rebuildRingLocked()
	return nil
}

// MarkDead removes a node from the active ring and marks it DEAD.
func (m *Manager) MarkDead(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mem, ok := m.members[nodeID]; ok {
		mem.Status = pb.MemberStatus_DEAD
	}
	m.rebuildRingLocked()
	// Drop cached client so we don't try to dial a dead node.
	m.clientCacheMu.Lock()
	if e, ok := m.clientCache[nodeID]; ok {
		_ = e.conn.Close()
		delete(m.clientCache, nodeID)
	}
	m.clientCacheMu.Unlock()
}

// ApplySnapshot replaces the member list from a SyncMembers response.
// Used on first join before this node has any peers.
func (m *Manager) ApplySnapshot(members []*pb.Member) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Keep self.
	m.members = map[string]*Member{m.self.NodeID: &m.self}
	for _, pm := range members {
		mem := protoToMember(pm)
		m.members[mem.NodeID] = &mem
	}
	m.rebuildRingLocked()
}

// Self returns this node's member descriptor.
func (m *Manager) Self() Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.self
}

// ActiveMembers returns all ACTIVE members excluding self.
func (m *Manager) ActiveMembers() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []Member
	for _, mem := range m.members {
		if mem.NodeID != m.self.NodeID && mem.Status == pb.MemberStatus_ACTIVE {
			result = append(result, *mem)
		}
	}
	return result
}

// AllMembers returns all known members (any status) including self.
func (m *Manager) AllMembers() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Member, 0, len(m.members))
	for _, mem := range m.members {
		result = append(result, *mem)
	}
	return result
}

// ReplicaCount returns the configured replication factor.
func (m *Manager) ReplicaCount() int {
	return m.replicaCount
}

// ─── suspect / dead tracking ─────────────────────────────────────────────────

// RecordSuspect notes that reporterID suspects nodeID. Returns true when
// quorum is reached and the node should be confirmed dead.
func (m *Manager) RecordSuspect(nodeID, reporterID string) (quorum bool) {
	m.suspectsMu.Lock()
	defer m.suspectsMu.Unlock()

	if _, ok := m.suspects[nodeID]; !ok {
		m.suspects[nodeID] = map[string]struct{}{}
	}
	m.suspects[nodeID][reporterID] = struct{}{}

	active := m.activeCountLocked()
	threshold := (active / 2) + 1 // simple majority
	return len(m.suspects[nodeID]) >= threshold
}

// ClearSuspect removes suspicion records for a node (it came back).
func (m *Manager) ClearSuspect(nodeID string) {
	m.suspectsMu.Lock()
	delete(m.suspects, nodeID)
	m.suspectsMu.Unlock()
}

// ─── gRPC client vending ──────────────────────────────────────────────────────

// MembershipClient returns a gRPC MembershipServiceClient for the given address.
func (m *Manager) MembershipClient(ctx context.Context, address string) (pb.MembershipServiceClient, *grpc.ClientConn, error) {
	conn, err := m.dial(address)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewMembershipServiceClient(conn), conn, nil
}

// TransferClient returns a gRPC TransferServiceClient for the given address.
func (m *Manager) TransferClient(ctx context.Context, address string) (pb.TransferServiceClient, *grpc.ClientConn, error) {
	conn, err := m.dial(address)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewTransferServiceClient(conn), conn, nil
}

// ReplicationClient returns a gRPC ReplicationServiceClient for the given address.
func (m *Manager) ReplicationClient(ctx context.Context, address string) (pb.ReplicationServiceClient, *grpc.ClientConn, error) {
	conn, err := m.dial(address)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewReplicationServiceClient(conn), conn, nil
}

// DataClient returns a gRPC DataServiceClient for the given address.
func (m *Manager) DataClient(ctx context.Context, address string) (pb.DataServiceClient, *grpc.ClientConn, error) {
	conn, err := m.dial(address)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewDataServiceClient(conn), conn, nil
}

// ─── internal ────────────────────────────────────────────────────────────────

func (m *Manager) dial(address string) (*grpc.ClientConn, error) {
	m.clientCacheMu.Lock()
	defer m.clientCacheMu.Unlock()

	if e, ok := m.clientCache[address]; ok && time.Now().Before(e.expiry) {
		return e.conn, nil
	}

	creds := credentials.NewTLS(m.tlsCfg)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("membership: dial %s: %w", address, err)
	}
	m.clientCache[address] = &clientEntry{
		conn:   conn,
		expiry: time.Now().Add(5 * time.Minute),
	}
	return conn, nil
}

func (m *Manager) rebuildRingLocked() {
	entries := make([]ring.NodeEntry, 0, len(m.members))
	for _, mem := range m.members {
		if mem.Status != pb.MemberStatus_ACTIVE {
			continue
		}
		entries = append(entries, ring.NodeEntry{
			NodeID:  mem.NodeID,
			Address: mem.Address,
			Vnodes:  len(mem.Vnodes),
		})
	}
	m.Ring.Rebuild(entries)
}

func (m *Manager) activeCountLocked() int {
	// Called from suspect tracking — doesn't hold m.mu, keep it that way.
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, mem := range m.members {
		if mem.Status == pb.MemberStatus_ACTIVE {
			count++
		}
	}
	return count
}

// ─── proto conversion helpers ─────────────────────────────────────────────────

func protoToMember(pm *pb.Member) Member {
	mem := Member{
		NodeID:  pm.NodeId,
		Address: pm.Address,
		Status:  pm.Status,
	}
	for _, vn := range pm.Vnodes {
		mem.Vnodes = append(mem.Vnodes, VirtualNode{
			Position: vn.Position,
			NodeID:   vn.NodeId,
			Address:  vn.Address,
		})
	}
	return mem
}

func MemberToProto(mem Member) *pb.Member {
	pm := &pb.Member{
		NodeId:  mem.NodeID,
		Address: mem.Address,
		Status:  mem.Status,
	}
	for _, vn := range mem.Vnodes {
		pm.Vnodes = append(pm.Vnodes, &pb.VirtualNode{
			NodeId:   vn.NodeID,
			Position: vn.Position,
			Address:  vn.Address,
		})
	}
	return pm
}
