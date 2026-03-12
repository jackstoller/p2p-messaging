package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// The number of virtual nodes per physical node.
const DefaultVnodeCount = 5

// VnodeEntry is one active virtual-node position on the ring.
// The ring only ever holds entries for ACTIVE vnodes; the membership layer
// is responsible for filtering before calling Rebuild.
type VnodeEntry struct {
	Id       string
	Position uint64
	NodeId   string
	Address  string
}

// OwnedRange describes a contiguous arc [Start, End] of the ring that one
// physical node is primary for. Start is exclusive, End is inclusive.
type OwnedRange struct {
	VnodeId string // set by the caller; the ring stores it opaquely
	Start   uint64
	End     uint64
	NodeId  string
	Address string
}

// InRange reports whether pos falls in (Start, End], handling wrap-around.
func (o OwnedRange) InRange(pos uint64) bool {
	if o.Start < o.End {
		return pos > o.Start && pos <= o.End
	}
	// Wrap-around segment: (start, maxUint64] ∪ [0, end]
	return pos > o.Start || pos <= o.End
}

// Ring is a sorted consistent hash ring. The internal slice is replaced
// atomically on every Rebuild; concurrent reads never block writes.
type Ring struct {
	mu     sync.RWMutex
	points []VnodeEntry // sorted ascending by Position
}

// New returns an empty ring.
func New() *Ring { return &Ring{} }

// Rebuild atomically replaces the ring with the given set of active entries.
// Call this whenever the set of active vnodes changes.
func (r *Ring) Rebuild(entries []VnodeEntry) {
	sorted := make([]VnodeEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Position < sorted[j].Position
	})
	r.mu.Lock()
	r.points = sorted
	r.mu.Unlock()
}

// Len returns the number of active vnode entries.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.points)
}

// Primary returns the single primary node for key.
func (r *Ring) Primary(key string) (VnodeEntry, bool) {
	nodes := r.ResponsibleNodes(key, 1)
	if len(nodes) == 0 {
		return VnodeEntry{}, false
	}
	return nodes[0], true
}

// ResponsibleNodes returns up to n distinct physical nodes for key, walking
// clockwise from hash(key). The first entry is the primary.
func (r *Ring) ResponsibleNodes(key string, n int) []VnodeEntry {
	r.mu.RLock()
	pts := r.points
	r.mu.RUnlock()

	if len(pts) == 0 || n == 0 {
		return nil
	}

	pos := KeyPosition(key)
	idx := sort.Search(len(pts), func(i int) bool { return pts[i].Position >= pos })
	if idx == len(pts) {
		idx = 0
	}

	seen := make(map[string]struct{}, n)
	result := make([]VnodeEntry, 0, n)
	for i := 0; i < len(pts) && len(result) < n; i++ {
		p := pts[(idx+i)%len(pts)]
		if _, dup := seen[p.NodeId]; !dup {
			seen[p.NodeId] = struct{}{}
			result = append(result, p)
		}
	}
	return result
}

// Returns the Vnodes behind and in-front of the position. If no vnodes exist, returns false.
func (r *Ring) GetVnodesBetweenPosition(pos uint64) (VnodeEntry, VnodeEntry, bool) {
	r.mu.RLock()
	pts := r.points
	r.mu.RUnlock()

	if len(r.points) == 0 {
		return VnodeEntry{}, VnodeEntry{}, false
	}

	idx := sort.Search(len(pts), func(i int) bool { return r.points[i].Position >= pos })
	prev := r.points[(idx-1+len(pts))%len(pts)]

	return prev, r.points[idx%len(pts)], true
}

// Predecessor returns the nearest distinct physical node counterclockwise from
// position, excluding excludeNodeId. Used by the transfer flow to find the
// node that currently owns a range before the joining node takes it over.
func (r *Ring) Predecessor(position uint64, excludeNodeId string) (VnodeEntry, bool) {
	r.mu.RLock()
	pts := r.points
	r.mu.RUnlock()

	if len(pts) == 0 {
		return VnodeEntry{}, false
	}

	idx := sort.Search(len(pts), func(i int) bool { return pts[i].Position >= position })
	for i := 1; i <= len(pts); i++ {
		p := pts[(idx-i+len(pts))%len(pts)]
		if p.NodeId != excludeNodeId {
			return p, true
		}
	}
	return VnodeEntry{}, false
}

// OwnedRanges returns the arcs of the ring for which nodeId is the clockwise
// endpoint, i.e. the ranges the node is primary for. The VnodeId field on
// each returned OwnedRange is empty; callers may populate it themselves.
func (r *Ring) OwnedRanges(nodeId string) []OwnedRange {
	r.mu.RLock()
	pts := r.points
	r.mu.RUnlock()

	var owned []OwnedRange
	for i, p := range pts {
		if p.NodeId != nodeId {
			continue
		}
		start := uint64(0)
		if i > 0 {
			start = pts[i-1].Position
		}
		owned = append(owned, OwnedRange{
			Start:   start,
			End:     p.Position,
			NodeId:  p.NodeId,
			Address: p.Address,
		})
	}

	// Handle wrap-around: the first ring point owned by nodeId has its start
	// at the last ring position rather than 0.
	if len(pts) > 0 && len(owned) > 0 && pts[0].NodeId == nodeId {
		owned[0].Start = pts[len(pts)-1].Position
	}
	return owned
}

// Position helpers

// KeyPosition returns the ring position for an arbitrary string key.
func KeyPosition(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}

// VnodePosition returns the deterministic ring position for a virtual node
// identified by its physical node Id and index. This is the canonical
// formula; every participant must use it to agree on ring layout.
func VnodePosition(nodeId string, index int) uint64 {
	return KeyPosition(fmt.Sprintf("%s:%d", nodeId, index))
}

// VnodeId returns the string identifier for a virtual node.
func VnodeId(nodeId string, index int) string {
	return fmt.Sprintf("%s:%d", nodeId, index)
}
