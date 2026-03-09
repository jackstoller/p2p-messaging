package membership

import (
	"crypto/tls"
	"sync"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/ring"
	"google.golang.org/grpc"
)

// VnodeState is local vnode readiness state.
type VnodeState int

const (
	VnodeInactive VnodeState = iota
	VnodeActive
)

// PeerState is the broadcast state for a physical node.
type PeerState int

const (
	PeerUp PeerState = iota
	PeerDown
)

// Vnode is one virtual-node position belonging to a physical node.
type Vnode struct {
	Id       string
	Position uint64
	State    VnodeState // TODO: Remove?
}

// Peer is a known physical node in the mesh.
type Peer struct {
	NodeId  string
	Address string
	Vnodes  []Vnode
	State   PeerState
}

func (p Peer) GetNodeId() string { return p.NodeId }

// Manager owns membership state, routing ring, and outbound client cache.
type Manager struct {
	mu      sync.RWMutex
	selfId  string
	members map[string]*Peer // includes self

	Ring *ring.Ring

	suspectsMu sync.RWMutex
	suspects   map[string]struct{}

	replicaCount int
	tlsCfg       *tls.Config

	clientsMu sync.Mutex
	clients   map[string]*cachedConn

	// OnPeerDown fires asynchronously when a peer is newly marked down.
	OnPeerDown func(nodeId string)
}

type cachedConn struct {
	conn   *grpc.ClientConn
	expiry time.Time
}

const connTTL = 5 * time.Minute
