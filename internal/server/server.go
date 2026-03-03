package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/transfer"
	pb "github.com/jackstoller/p2p-messaging/proto/node"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Node implements all four gRPC services.
// It is registered once against a grpc.Server in main.
type Node struct {
	pb.UnimplementedMembershipServiceServer
	pb.UnimplementedTransferServiceServer
	pb.UnimplementedReplicationServiceServer
	pb.UnimplementedDataServiceServer

	mgr    *membership.Manager
	store  *storage.Store
	xfer   *transfer.Manager
	nodeID string

	// onDead is called when a peer is confirmed dead — triggers recovery.
	onDead func(nodeID string)
}

func New(
	mgr *membership.Manager,
	store *storage.Store,
	xfer *transfer.Manager,
	nodeID string,
	onDead func(string),
) *Node {
	return &Node{
		mgr:    mgr,
		store:  store,
		xfer:   xfer,
		nodeID: nodeID,
		onDead: onDead,
	}
}

// ─── MembershipService ────────────────────────────────────────────────────────

// Announce handles a new node introducing itself.
func (n *Node) Announce(ctx context.Context, req *pb.AnnounceRequest) (*pb.AnnounceResponse, error) {
	if req.Member == nil {
		return &pb.AnnounceResponse{Accepted: false}, nil
	}
	mem := membership.Member{
		NodeID:  req.Member.NodeId,
		Address: req.Member.Address,
		Status:  pb.MemberStatus_PENDING,
	}
	for _, vn := range req.Member.Vnodes {
		mem.Vnodes = append(mem.Vnodes, membership.VirtualNode{
			Position: vn.Position,
			NodeID:   vn.NodeId,
			Address:  vn.Address,
		})
	}
	n.mgr.AddPending(mem)
	slog.Info("node announced", "nodeID", req.Member.NodeId, "address", req.Member.Address)
	return &pb.AnnounceResponse{Accepted: true}, nil
}

// SyncMembers returns the current ACTIVE member list to a joining node.
func (n *Node) SyncMembers(_ context.Context, req *pb.SyncMembersRequest) (*pb.SyncMembersResponse, error) {
	members := n.mgr.AllMembers()
	resp := &pb.SyncMembersResponse{}
	for _, m := range members {
		resp.Members = append(resp.Members, membership.MemberToProto(m))
	}
	return resp, nil
}

// Heartbeat responds to a liveness ping.
func (n *Node) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{
		NodeId:     n.nodeID,
		SentAt:     req.SentAt,
		ReceivedAt: time.Now().UnixMilli(),
	}, nil
}

// SuspectNode records an incoming suspicion report and potentially declares dead.
func (n *Node) SuspectNode(_ context.Context, req *pb.SuspectRequest) (*pb.SuspectResponse, error) {
	slog.Info("received suspect report", "suspect", req.SuspectedNodeId, "from", req.ReporterNodeId)
	if quorum := n.mgr.RecordSuspect(req.SuspectedNodeId, req.ReporterNodeId); quorum {
		slog.Warn("quorum via gossip, marking dead", "nodeID", req.SuspectedNodeId)
		n.mgr.MarkDead(req.SuspectedNodeId)
		if n.onDead != nil {
			go n.onDead(req.SuspectedNodeId)
		}
	}
	return &pb.SuspectResponse{}, nil
}

// ConfirmDead handles a broadcast that quorum has confirmed a node dead.
func (n *Node) ConfirmDead(_ context.Context, req *pb.ConfirmDeadRequest) (*pb.ConfirmDeadResponse, error) {
	slog.Warn("confirmed dead received", "nodeID", req.DeadNodeId)
	n.mgr.MarkDead(req.DeadNodeId)
	if n.onDead != nil {
		go n.onDead(req.DeadNodeId)
	}
	return &pb.ConfirmDeadResponse{}, nil
}

// MembershipUpdate handles incremental join/leave/dead events.
func (n *Node) MembershipUpdate(_ context.Context, req *pb.MembershipUpdateRequest) (*pb.MembershipUpdateResponse, error) {
	switch req.UpdateType {
	case pb.MembershipUpdateRequest_JOIN:
		n.mgr.AddPending(*protoMember(req.Member))
		if err := n.mgr.Activate(req.Member.NodeId); err != nil {
			slog.Warn("failed to activate joined node", "nodeID", req.Member.NodeId, "err", err)
		}
	case pb.MembershipUpdateRequest_DEAD:
		n.mgr.MarkDead(req.Member.NodeId)
		if n.onDead != nil {
			go n.onDead(req.Member.NodeId)
		}
	}
	return &pb.MembershipUpdateResponse{}, nil
}

// ─── TransferService ──────────────────────────────────────────────────────────

// InitiateTransfer is called by a joining node claiming our range.
func (n *Node) InitiateTransfer(_ context.Context, req *pb.InitiateTransferRequest) (*pb.InitiateTransferResponse, error) {
	if err := n.xfer.HandleInitiateTransfer(req); err != nil {
		return &pb.InitiateTransferResponse{Accepted: false}, err
	}
	return &pb.InitiateTransferResponse{Accepted: true}, nil
}

// StreamDataSlice streams all records in the requested range to the joiner.
func (n *Node) StreamDataSlice(req *pb.StreamDataSliceRequest, stream pb.TransferService_StreamDataSliceServer) error {
	return n.xfer.HandleStreamDataSlice(req, stream)
}

// ForwardWrite applies a live write forwarded from the predecessor.
func (n *Node) ForwardWrite(_ context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteResponse, error) {
	rec := storage.UserRecord{
		Username:  req.Record.Username,
		NodeID:    req.Record.NodeId,
		RingPos:   ring.KeyPosition(req.Record.Username),
		Version:   req.Record.UpdatedAt, // use timestamp as version during transfer
		UpdatedAt: req.Record.UpdatedAt,
	}
	if _, err := n.store.Upsert(rec); err != nil {
		return nil, fmt.Errorf("ForwardWrite: upsert: %w", err)
	}
	return &pb.ForwardWriteResponse{WriteId: req.WriteId}, nil
}

// ConfirmReady is called when the joiner has all data and is ready to take over.
func (n *Node) ConfirmReady(ctx context.Context, req *pb.ConfirmReadyRequest) (*pb.ConfirmReadyResponse, error) {
	proceed := n.xfer.HandleConfirmReady(req.TransferId)
	if !proceed {
		return &pb.ConfirmReadyResponse{Proceed: false}, nil
	}

	// Send HandoffAuthority back.
	// In this design ConfirmReady → HandoffAuthority is synchronous (same RPC chain).
	// The joiner's claimRange polls for the DB state change that HandoffAck triggers.
	// Here we do the handoff inline: joiner gets Proceed=true and knows to expect
	// HandoffAuthority on the same connection flow.
	//
	// Simpler: we handle HandoffAuthority as a separate outbound RPC from the predecessor
	// after returning Proceed=true. The joiner waits for it.
	go n.sendHandoffAuthority(req.TransferId)

	return &pb.ConfirmReadyResponse{Proceed: true}, nil
}

func (n *Node) sendHandoffAuthority(transferID string) {
	info, ok := n.xfer.OutboundInfo(transferID)
	if !ok {
		slog.Warn("handoff: transfer not found", "transferID", transferID)
		return
	}

	var newNodeAddr string
	for _, mem := range n.mgr.AllMembers() {
		if mem.NodeID == info.NewNodeID {
			newNodeAddr = mem.Address
			break
		}
	}
	if newNodeAddr == "" {
		slog.Error("handoff: new node address not found", "transferID", transferID, "newNodeID", info.NewNodeID)
		return
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		attemptCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)

		client, err := n.mgr.TransferClient(attemptCtx, newNodeAddr)
		if err == nil {
			resp, callErr := client.HandoffAuthority(attemptCtx, &pb.HandoffAuthorityRequest{
				TransferId: transferID,
				Range: &pb.Range{
					Start: info.RangeStart,
					End:   info.RangeEnd,
				},
			})
			cancel()

			if callErr == nil {
				if !resp.Acknowledged {
					slog.Warn("handoff: not acknowledged", "transferID", transferID)
					return
				}
				n.xfer.HandleHandoffAck(transferID)
				slog.Info("handoff complete", "transferID", transferID, "to", info.NewNodeID)
				return
			}

			if st, ok := status.FromError(callErr); ok {
				if st.Code() != codes.Canceled && st.Code() != codes.DeadlineExceeded && st.Code() != codes.Unavailable {
					slog.Error("handoff: rpc failed", "transferID", transferID, "err", callErr)
					return
				}
			}
			err = callErr
		} else {
			cancel()
		}

		if time.Now().After(deadline) {
			slog.Error("handoff: rpc failed after retries", "transferID", transferID, "address", newNodeAddr, "err", err)
			return
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// HandoffAuthority is received by the JOINER from the predecessor.
func (n *Node) HandoffAuthority(_ context.Context, req *pb.HandoffAuthorityRequest) (*pb.HandoffAuthorityResponse, error) {
	// Upgrade this range to primary in our local DB.
	vnodeID := fmt.Sprintf("vnode_%d_%d", req.Range.Start, req.Range.End)
	if err := n.store.SetRange(vnodeID, req.Range.Start, req.Range.End, storage.RolePrimary); err != nil {
		return &pb.HandoffAuthorityResponse{Acknowledged: false}, err
	}
	slog.Info("HandoffAuthority received, now primary",
		"start", req.Range.Start, "end", req.Range.End)

	// Activate self in the membership view — ring is now updated.
	_ = n.mgr.Activate(n.nodeID)

	// Notify the predecessor that handoff is complete (they will drop to replica).
	return &pb.HandoffAuthorityResponse{Acknowledged: true}, nil
}

// ─── ReplicationService ───────────────────────────────────────────────────────

// Replicate stores a record sent by a primary.
func (n *Node) Replicate(_ context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	rec := storage.UserRecord{
		Username:  req.Record.Username,
		NodeID:    req.Record.NodeId,
		RingPos:   ring.KeyPosition(req.Record.Username),
		Version:   req.Version,
		UpdatedAt: req.Record.UpdatedAt,
	}
	applied, err := n.store.Upsert(rec)
	if err != nil {
		return &pb.ReplicateResponse{Success: false}, err
	}
	_ = applied
	return &pb.ReplicateResponse{Success: true, Version: req.Version}, nil
}

// DropReplica removes records in a range we're no longer responsible for.
func (n *Node) DropReplica(_ context.Context, req *pb.DropReplicaRequest) (*pb.DropReplicaResponse, error) {
	slog.Info("dropping replica range", "start", req.Range.Start, "end", req.Range.End)
	if err := n.store.DeleteRange(req.Range.Start, req.Range.End); err != nil {
		return &pb.DropReplicaResponse{Success: false}, err
	}
	return &pb.DropReplicaResponse{Success: true}, nil
}

// PromoteReplica upgrades a replica range to primary (crash recovery path).
func (n *Node) PromoteReplica(_ context.Context, req *pb.PromoteReplicaRequest) (*pb.PromoteReplicaResponse, error) {
	vnodeID := fmt.Sprintf("vnode_%d_%d", req.Range.Start, req.Range.End)
	if err := n.store.SetRange(vnodeID, req.Range.Start, req.Range.End, storage.RolePrimary); err != nil {
		return &pb.PromoteReplicaResponse{Success: false}, err
	}
	slog.Info("promoted to primary", "start", req.Range.Start, "end", req.Range.End)
	return &pb.PromoteReplicaResponse{Success: true}, nil
}

// ─── DataService ──────────────────────────────────────────────────────────────

// ReadRecord serves a record read, forwarding to the correct node if needed.
func (n *Node) ReadRecord(ctx context.Context, req *pb.ReadRecordRequest) (*pb.ReadRecordResponse, error) {
	primary, ok := n.mgr.Ring.Primary(req.Username)
	if !ok {
		return nil, fmt.Errorf("ring is empty")
	}

	// Are we the primary?
	if primary.NodeID == n.nodeID {
		rec, found, err := n.store.Get(req.Username)
		if err != nil {
			return nil, err
		}
		if !found {
			return &pb.ReadRecordResponse{Found: false}, nil
		}
		return &pb.ReadRecordResponse{
			Found: true,
			Record: &pb.UserRecord{
				Username:  rec.Username,
				NodeId:    rec.NodeID,
				UpdatedAt: rec.UpdatedAt,
			},
		}, nil
	}

	// Forward to primary.
	client, err := n.mgr.DataClient(ctx, primary.Address)
	if err != nil {
		// Primary unreachable — try a replica and flag as stale.
		return n.readFromReplica(ctx, req)
	}

	resp, err := client.ReadRecord(ctx, req)
	if err != nil {
		return n.readFromReplica(ctx, req)
	}
	return resp, nil
}

func (n *Node) readFromReplica(ctx context.Context, req *pb.ReadRecordRequest) (*pb.ReadRecordResponse, error) {
	nodes := n.mgr.Ring.ResponsibleNodes(req.Username, n.mgr.ReplicaCount())
	for _, node := range nodes[1:] { // skip primary (index 0), already failed
		if node.NodeID == n.nodeID {
			// We are a replica — serve locally.
			rec, found, err := n.store.Get(req.Username)
			if err != nil || !found {
				continue
			}
			return &pb.ReadRecordResponse{
				Found: true,
				Stale: true,
				Record: &pb.UserRecord{
					Username:  rec.Username,
					NodeId:    rec.NodeID,
					UpdatedAt: rec.UpdatedAt,
				},
			}, nil
		}
		client, err := n.mgr.DataClient(ctx, node.Address)
		if err != nil {
			continue
		}
		resp, err := client.ReadRecord(ctx, req)
		if err == nil {
			resp.Stale = true
			return resp, nil
		}
	}
	return &pb.ReadRecordResponse{Found: false}, nil
}

// WriteRecord writes a record to the primary, with quorum replication.
func (n *Node) WriteRecord(ctx context.Context, req *pb.WriteRecordRequest) (*pb.WriteRecordResponse, error) {
	primary, ok := n.mgr.Ring.Primary(req.Record.Username)
	if !ok {
		return nil, fmt.Errorf("ring is empty")
	}

	// Forward to primary if we're not it.
	if primary.NodeID != n.nodeID {
		client, err := n.mgr.DataClient(ctx, primary.Address)
		if err != nil {
			return nil, fmt.Errorf("WriteRecord: cannot reach primary: %w", err)
		}
		return client.WriteRecord(ctx, req)
	}

	// We are primary — write locally then replicate.
	version := time.Now().UnixMilli()
	rec := storage.UserRecord{
		Username:  req.Record.Username,
		NodeID:    req.Record.NodeId,
		RingPos:   ring.KeyPosition(req.Record.Username),
		Version:   version,
		UpdatedAt: version,
	}
	if _, err := n.store.Upsert(rec); err != nil {
		return nil, err
	}

	// Replicate to all replicas (quorum = all R must ack).
	replicas := n.mgr.Ring.ResponsibleNodes(req.Record.Username, n.mgr.ReplicaCount())
	ackCh := make(chan error, len(replicas))

	for _, node := range replicas[1:] { // [0] is self
		go func(node ring.Point) {
			client, err := n.mgr.ReplicationClient(ctx, node.Address)
			if err != nil {
				ackCh <- err
				return
			}
			_, err = client.Replicate(ctx, &pb.ReplicateRequest{
				Record: &pb.UserRecord{
					Username:  rec.Username,
					NodeId:    rec.NodeID,
					UpdatedAt: rec.UpdatedAt,
				},
				PrimaryId: n.nodeID,
				Version:   version,
			})
			ackCh <- err
		}(node)
	}

	// Also forward to any node currently mid-transfer for this key's range.
	// (transfer.Manager.ForwardWrite handles the lookup internally.)
	go n.xfer.ForwardWrite(ctx, fmt.Sprintf("range_%d", ring.KeyPosition(req.Record.Username)), rec)

	// Wait for all replica acks.
	replicaCount := len(replicas) - 1
	for i := 0; i < replicaCount; i++ {
		if err := <-ackCh; err != nil {
			slog.Warn("replica ack failed", "err", err)
			// Continue — partial failure is logged but write succeeds.
			// TODO: track under-replicated keys for repair.
		}
	}

	return &pb.WriteRecordResponse{Success: true, Version: version}, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func protoMember(pm *pb.Member) *membership.Member {
	mem := &membership.Member{
		NodeID:  pm.NodeId,
		Address: pm.Address,
		Status:  pm.Status,
	}
	for _, vn := range pm.Vnodes {
		mem.Vnodes = append(mem.Vnodes, membership.VirtualNode{
			Position: vn.Position,
			NodeID:   vn.NodeId,
			Address:  vn.Address,
		})
	}
	return mem
}
