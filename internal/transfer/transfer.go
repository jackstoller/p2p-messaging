package transfer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	pb "github.com/jackstoller/p2p-messaging/proto/node"
)

const chunkSize = 100 // records per StreamDataSlice chunk

// Manager handles all data transfer: join handoff and crash recovery.
type Manager struct {
	mgr    *membership.Manager
	store  *storage.Store
	nodeID string

	// Active inbound transfers this node is receiving (as joiner).
	inbound   map[string]*inboundState
	inboundMu sync.Mutex

	// Active outbound transfers this node is sending (as predecessor).
	outbound   map[string]*outboundState
	outboundMu sync.Mutex
}

type inboundState struct {
	transferID string
	rangeStart uint64
	rangeEnd   uint64
	// Forwarded writes received during transfer, keyed by writeID.
	forwarded   map[string]*pb.UserRecord
	forwardedMu sync.Mutex
}

type outboundState struct {
	transferID string
	newNodeID  string
	rangeStart uint64
	rangeEnd   uint64
	// Unacked forwarded writes.
	pendingWrites   map[string]struct{}
	pendingWritesMu sync.Mutex
	readyC          chan struct{} // closed when ConfirmReady arrives
}

func New(mgr *membership.Manager, store *storage.Store, nodeID string) *Manager {
	return &Manager{
		mgr:      mgr,
		store:    store,
		nodeID:   nodeID,
		inbound:  map[string]*inboundState{},
		outbound: map[string]*outboundState{},
	}
}

// ─── JOIN: new node drives this ──────────────────────────────────────────────

// ExecuteJoin runs the full join sequence for all virtual node ranges
// this node now owns. Called once, after ring positions are chosen.
// Blocks until all ranges are handed off or returns an error.
func (m *Manager) ExecuteJoin(ctx context.Context) error {
	ownedRanges := m.mgr.Ring.OwnedRanges(m.nodeID)
	if len(ownedRanges) == 0 {
		// First node in the network — nothing to transfer.
		slog.Info("no ranges to claim, first node in network")
		return nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(ownedRanges))

	for _, r := range ownedRanges {
		wg.Add(1)
		go func(r ring.OwnedRange) {
			defer wg.Done()
			if err := m.claimRange(ctx, r); err != nil {
				errs <- fmt.Errorf("range (%d,%d]: %w", r.Start, r.End, err)
			}
		}(r)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err // return first error; caller can retry
		}
	}
	return nil
}

func (m *Manager) claimRange(ctx context.Context, r ring.OwnedRange) error {
	predecessor, ok := m.mgr.Ring.Predecessor(r.End, m.nodeID)
	if !ok {
		slog.Info("no predecessor for range, already owner", "end", r.End)
		return nil
	}

	transferID := uuid.New().String()
	slog.Info("claiming range", "transferID", transferID,
		"start", r.Start, "end", r.End, "from", predecessor.NodeID)

	// Register inbound state.
	state := &inboundState{
		transferID: transferID,
		rangeStart: r.Start,
		rangeEnd:   r.End,
		forwarded:  map[string]*pb.UserRecord{},
	}
	m.inboundMu.Lock()
	m.inbound[transferID] = state
	m.inboundMu.Unlock()

	defer func() {
		m.inboundMu.Lock()
		delete(m.inbound, transferID)
		m.inboundMu.Unlock()
	}()

	// Mark range as TRANSFERRING in local DB.
	_ = m.store.SetRange(r.VnodeID, r.Start, r.End, storage.RoleTransferring)

	client, conn, err := m.mgr.TransferClient(ctx, predecessor.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Step 1: InitiateTransfer
	resp, err := client.InitiateTransfer(ctx, &pb.InitiateTransferRequest{
		TransferId: transferID,
		NewNodeId:  m.nodeID,
		Range:      &pb.Range{Start: r.Start, End: r.End},
	})
	if err != nil || !resp.Accepted {
		return fmt.Errorf("InitiateTransfer rejected: %w", err)
	}

	// Step 2: StreamDataSlice (server-streaming RPC)
	stream, err := client.StreamDataSlice(ctx, &pb.StreamDataSliceRequest{
		TransferId: transferID,
		Range:      &pb.Range{Start: r.Start, End: r.End},
	})
	if err != nil {
		return fmt.Errorf("StreamDataSlice: %w", err)
	}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("StreamDataSlice recv: %w", err)
		}
		for _, rec := range chunk.Records {
			_, _ = m.store.Upsert(storage.UserRecord{
				Username:  rec.Username,
				NodeID:    rec.NodeId,
				RingPos:   ring.KeyPosition(rec.Username),
				Version:   int64(chunk.ChunkIndex), // temporary; real version set on upsert
				UpdatedAt: rec.UpdatedAt,
			})
		}
		slog.Debug("received chunk", "transferID", transferID,
			"index", chunk.ChunkIndex, "final", chunk.IsFinal)
	}

	// Step 3: Drain any ForwardWrite messages that arrived during streaming.
	// (They were applied immediately in ForwardWrite handler - just confirm all acked.)
	// In practice we wait briefly for any in-flight forwards to land.
	time.Sleep(50 * time.Millisecond)

	// Step 4: ConfirmReady
	readyResp, err := client.ConfirmReady(ctx, &pb.ConfirmReadyRequest{
		TransferId: transferID,
	})
	if err != nil || !readyResp.Proceed {
		return fmt.Errorf("ConfirmReady failed: %w", err)
	}

	// Step 5: Predecessor sends HandoffAuthority (we receive it via the
	// HandoffAuthority RPC handler - mark range as primary there).
	// ConfirmReady response implicitly triggers predecessor to send it.
	// We block briefly waiting for the HandoffAuthority RPC to arrive.
	// In a production impl this would be a channel; here we poll the DB.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ranges, _ := m.store.GetOwnedRanges()
		for _, or_ := range ranges {
			if or_.Start == r.Start && or_.End == r.End && or_.Role == storage.RolePrimary {
				slog.Info("handoff complete", "transferID", transferID,
					"start", r.Start, "end", r.End)
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for HandoffAuthority for range (%d,%d]", r.Start, r.End)
}

// ─── PREDECESSOR: handles inbound transfer requests ───────────────────────────

// HandleInitiateTransfer is called by the gRPC server when a new node wants our data.
func (m *Manager) HandleInitiateTransfer(req *pb.InitiateTransferRequest) error {
	state := &outboundState{
		transferID:    req.TransferId,
		newNodeID:     req.NewNodeId,
		rangeStart:    req.Range.Start,
		rangeEnd:      req.Range.End,
		pendingWrites: map[string]struct{}{},
		readyC:        make(chan struct{}),
	}
	m.outboundMu.Lock()
	m.outbound[req.TransferId] = state
	m.outboundMu.Unlock()

	// Mark range as TRANSFERRING — we keep serving it, but now also forward writes.
	_ = m.store.SetRange(req.TransferId, req.Range.Start, req.Range.End, storage.RoleTransferring)
	slog.Info("transfer initiated", "transferID", req.TransferId,
		"newNode", req.NewNodeId, "start", req.Range.Start, "end", req.Range.End)
	return nil
}

// HandleStreamDataSlice is called by the gRPC server to stream records to the joiner.
func (m *Manager) HandleStreamDataSlice(
	req *pb.StreamDataSliceRequest,
	stream pb.TransferService_StreamDataSliceServer,
) error {
	records, err := m.store.GetRange(req.Range.Start, req.Range.End)
	if err != nil {
		return err
	}

	// Send in chunks.
	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]

		var pbRecords []*pb.UserRecord
		for _, r := range chunk {
			pbRecords = append(pbRecords, &pb.UserRecord{
				Username:  r.Username,
				NodeId:    r.NodeID,
				UpdatedAt: r.UpdatedAt,
			})
		}
		if err := stream.Send(&pb.DataChunk{
			TransferId: req.TransferId,
			ChunkIndex: int32(i / chunkSize),
			IsFinal:    end == len(records),
			Records:    pbRecords,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ForwardWrite is called by the predecessor's write path: if a write lands
// for a range being transferred, forward it to the new node immediately.
func (m *Manager) ForwardWrite(ctx context.Context, transferID string, rec storage.UserRecord) error {
	m.outboundMu.Lock()
	state, ok := m.outbound[transferID]
	m.outboundMu.Unlock()
	if !ok {
		return nil // transfer already complete
	}

	writeID := uuid.New().String()
	state.pendingWritesMu.Lock()
	state.pendingWrites[writeID] = struct{}{}
	state.pendingWritesMu.Unlock()

	// Get the new node's address from the member list.
	var newNodeAddr string
	for _, mem := range m.mgr.AllMembers() {
		if mem.NodeID == state.newNodeID {
			newNodeAddr = mem.Address
			break
		}
	}
	if newNodeAddr == "" {
		return fmt.Errorf("transfer: new node %s not found", state.newNodeID)
	}

	client, conn, err := m.mgr.TransferClient(ctx, newNodeAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = client.ForwardWrite(ctx, &pb.ForwardWriteRequest{
		TransferId: transferID,
		WriteId:    writeID,
		Record: &pb.UserRecord{
			Username:  rec.Username,
			NodeId:    rec.NodeID,
			UpdatedAt: rec.UpdatedAt,
		},
	})
	if err != nil {
		return err
	}

	state.pendingWritesMu.Lock()
	delete(state.pendingWrites, writeID)
	state.pendingWritesMu.Unlock()
	return nil
}

// HandleConfirmReady is called when the joiner says it's ready to take over.
// Returns true when safe to send HandoffAuthority.
func (m *Manager) HandleConfirmReady(transferID string) (proceed bool) {
	m.outboundMu.Lock()
	state, ok := m.outbound[transferID]
	m.outboundMu.Unlock()
	if !ok {
		return false
	}

	// Check all forwarded writes are acked.
	state.pendingWritesMu.Lock()
	pending := len(state.pendingWrites)
	state.pendingWritesMu.Unlock()

	if pending > 0 {
		slog.Warn("ConfirmReady received but pending writes unacked",
			"transferID", transferID, "pending", pending)
		return false
	}
	close(state.readyC)
	return true
}

// HandleHandoffAck is called after we send HandoffAuthority and the joiner acks.
// We transition to REPLICA for the range.
func (m *Manager) HandleHandoffAck(transferID string) {
	m.outboundMu.Lock()
	state, ok := m.outbound[transferID]
	if ok {
		delete(m.outbound, transferID)
	}
	m.outboundMu.Unlock()
	if !ok {
		return
	}

	// Downgrade to replica — we still hold the data as a warm backup.
	_ = m.store.SetRange(transferID, state.rangeStart, state.rangeEnd, storage.RoleReplica)
	slog.Info("handed off authority, now replica",
		"transferID", transferID, "start", state.rangeStart, "end", state.rangeEnd)
}

// ─── CRASH RECOVERY ──────────────────────────────────────────────────────────

// RecoverFromDead runs when deadNodeID has been confirmed dead.
// This node promotes itself for any ranges it holds as replica where
// the dead node was primary, then restores the replication factor.
func (m *Manager) RecoverFromDead(ctx context.Context, deadNodeID string) error {
	ownedRanges, err := m.store.GetOwnedRanges()
	if err != nil {
		return err
	}

	for _, r := range ownedRanges {
		if r.Role != storage.RoleReplica {
			continue
		}
		// Check if the primary for this range was the dead node.
		// We compute this from the ring as it was BEFORE the dead node was removed.
		// After MarkDead, the ring is already recomputed — so we check if we are
		// now the primary.
		testKey := fmt.Sprintf("__rangecheck_%d_%d", r.Start, r.End)
		primary, ok := m.mgr.Ring.Primary(testKey)
		if !ok || primary.NodeID != m.nodeID {
			continue
		}

		slog.Info("promoting self to primary after crash",
			"deadNode", deadNodeID, "start", r.Start, "end", r.End)
		if err := m.promoteRange(ctx, r); err != nil {
			slog.Error("promote failed", "err", err, "start", r.Start, "end", r.End)
		}
	}
	return nil
}

func (m *Manager) promoteRange(ctx context.Context, r storage.OwnedRangeRow) error {
	// 1. Mark self as primary.
	_ = m.store.SetRange(r.VnodeID, r.Start, r.End, storage.RolePrimary)

	// 2. Determine new replica set from updated ring.
	// Use a representative key in the range to look up responsible nodes.
	// Representative key: hash to the midpoint of the range.
	midKey := fmt.Sprintf("__recovery_%d", (r.Start+r.End)/2)
	responsible := m.mgr.Ring.ResponsibleNodes(midKey, m.mgr.ReplicaCount())

	// 3. Push our data to new replicas (skip self).
	records, err := m.store.GetRange(r.Start, r.End)
	if err != nil {
		return err
	}

	for _, node := range responsible {
		if node.NodeID == m.nodeID {
			continue
		}
		go m.replicateRangeToNode(ctx, node.Address, records, r)
	}
	return nil
}

func (m *Manager) replicateRangeToNode(
	ctx context.Context,
	address string,
	records []storage.UserRecord,
	r storage.OwnedRangeRow,
) {
	client, conn, err := m.mgr.ReplicationClient(ctx, address)
	if err != nil {
		slog.Error("replication: dial failed", "address", address, "err", err)
		return
	}
	defer conn.Close()

	for _, rec := range records {
		_, err := client.Replicate(ctx, &pb.ReplicateRequest{
			Record: &pb.UserRecord{
				Username:  rec.Username,
				NodeId:    rec.NodeID,
				UpdatedAt: rec.UpdatedAt,
			},
			PrimaryId: m.nodeID,
			Version:   rec.Version,
		})
		if err != nil {
			slog.Error("replication: record failed", "username", rec.Username, "err", err)
		}
	}
}
