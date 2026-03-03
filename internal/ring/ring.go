package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const DefaultVirtualNodes = 5

// Point is one virtual node's position on the ring.
type Point struct {
	Position uint64
	NodeID   string
	Address  string
}

// Ring is a sorted, read-optimised consistent hash ring.
// All mutation rebuilds the internal slice; reads are lock-free after a
// pointer swap so hot-path lookups never contend with membership changes.
type Ring struct {
	mu     sync.RWMutex
	points []Point // sorted ascending by Position
}

// New returns an empty ring.
func New() *Ring {
	return &Ring{}
}

// NodeEntry is the input used to (re)build the ring.
type NodeEntry struct {
	NodeID   string
	Address  string
	Vnodes   int // how many virtual nodes to place; 0 → DefaultVirtualNodes
}

// Rebuild replaces the entire ring from a fresh member list.
// Call this whenever the ACTIVE member list changes.
func (r *Ring) Rebuild(members []NodeEntry) {
	pts := make([]Point, 0, len(members)*DefaultVirtualNodes)
	for _, m := range members {
		count := m.Vnodes
		if count <= 0 {
			count = DefaultVirtualNodes
		}
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("%s:%d", m.NodeID, i)
			pos := hashKey(key)
			pts = append(pts, Point{
				Position: pos,
				NodeID:   m.NodeID,
				Address:  m.Address,
			})
		}
	}
	sort.Slice(pts, func(i, j int) bool {
		return pts[i].Position < pts[j].Position
	})

	r.mu.Lock()
	r.points = pts
	r.mu.Unlock()
}

// ResponsibleNodes returns the primary and up to replicaCount additional
// distinct physical nodes for a given key, walking clockwise from hash(key).
func (r *Ring) ResponsibleNodes(key string, replicaCount int) []Point {
	r.mu.RLock()
	pts := r.points
	r.mu.RUnlock()

	if len(pts) == 0 {
		return nil
	}

	pos := hashKey(key)
	idx := sort.Search(len(pts), func(i int) bool {
		return pts[i].Position >= pos
	})
	if idx == len(pts) {
		idx = 0 // wrap around
	}

	// Collect up to 1 + replicaCount distinct physical nodes.
	want := 1 + replicaCount
	seen := make(map[string]struct{}, want)
	result := make([]Point, 0, want)

	for i := 0; i < len(pts) && len(result) < want; i++ {
		p := pts[(idx+i)%len(pts)]
		if _, dup := seen[p.NodeID]; !dup {
			seen[p.NodeID] = struct{}{}
			result = append(result, p)
		}
	}
	return result
}

// Primary returns the single primary node for a key.
func (r *Ring) Primary(key string) (Point, bool) {
	nodes := r.ResponsibleNodes(key, 0)
	if len(nodes) == 0 {
		return Point{}, false
	}
	return nodes[0], true
}

// OwnedRanges returns the ranges of the ring for which nodeID is primary.
// A node owns every segment that ends at one of its virtual node positions.
func (r *Ring) OwnedRanges(nodeID string) []OwnedRange {
	r.mu.RLock()
	pts := r.points
	r.mu.RUnlock()

	var owned []OwnedRange
	for i, p := range pts {
		if p.NodeID != nodeID {
			continue
		}
		prev := uint64(0)
		if i > 0 {
			prev = pts[i-1].Position
		}
		owned = append(owned, OwnedRange{
			Start:   prev, // exclusive
			End:     p.Position, // inclusive
			VnodeID: fmt.Sprintf("%s:%d", nodeID, i),
		})
	}
	// Handle wrap-around: the first segment's start is actually the last
	// ring position, not 0, if the first point is not nodeID's.
	if len(pts) > 0 && pts[0].NodeID == nodeID {
		last := pts[len(pts)-1].Position
		owned[0].Start = last
	}
	return owned
}

// Predecessor returns the physical node currently authoritative for the range
// (prev, end] — i.e. the node just before end on the current ring, excluding
// excludeID (the joining node itself).
func (r *Ring) Predecessor(endPosition uint64, excludeID string) (Point, bool) {
	r.mu.RLock()
	pts := r.points
	r.mu.RUnlock()

	if len(pts) == 0 {
		return Point{}, false
	}

	// Find the point at or before endPosition.
	idx := sort.Search(len(pts), func(i int) bool {
		return pts[i].Position >= endPosition
	})
	// Walk backwards skipping excludeID.
	for i := 1; i <= len(pts); i++ {
		p := pts[(idx-i+len(pts))%len(pts)]
		if p.NodeID != excludeID {
			return p, true
		}
	}
	return Point{}, false
}

// KeyPosition returns the ring position for a user-facing key.
// Exposed so the storage layer can record it alongside each row.
func KeyPosition(key string) uint64 {
	return hashKey(key)
}

// OwnedRange describes a contiguous arc of the ring owned by one virtual node.
type OwnedRange struct {
	Start   uint64 // exclusive lower bound
	End     uint64 // inclusive upper bound
	VnodeID string
}

// InRange reports whether pos falls in (start, end], handling ring wrap.
func (o OwnedRange) InRange(pos uint64) bool {
	if o.Start < o.End {
		return pos > o.Start && pos <= o.End
	}
	// Wrap-around segment: (start, maxUint64] ∪ [0, end]
	return pos > o.Start || pos <= o.End
}

func hashKey(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}