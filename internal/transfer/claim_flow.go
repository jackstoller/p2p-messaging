package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

var errAlreadyOwner = errors.New("transfer: vnode already owned by self")

const maxConcurrentClaims = 8

// Attempts to activate all inactive virtual nodes by transferring
// ranges to itself and activating
func (m *Manager) ClaimVirtualNodes(ctx context.Context) error {
	self := m.mgr.Self()
	targets := m.getInactiveVnodes(self)
	if len(targets) == 0 {
		return nil
	}
	logging.Info(fmt.Sprintf("Starting vnode claim flow with %d targets.", len(targets)))

	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	sema := make(chan struct{}, claimConcurrency(len(targets)))
	for _, vnode := range targets {
		wg.Add(1)
		go func(v membership.Vnode) {
			defer wg.Done()
			sema <- struct{}{}
			defer func() { <-sema }()
			if err := m.claimVnode(ctx, v); err != nil {
				errCh <- err
			}
		}(vnode)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			logging.Error("Vnode claim flow failed with error=%v.", err)
			return err
		}
	}
	logging.Info(fmt.Sprintf("Completed vnode claim flow with %d targets.", len(targets)))
	return nil
}

func claimConcurrency(targets int) int {
	if targets <= 1 {
		return 1
	}
	limit := runtime.GOMAXPROCS(0)
	if limit < 2 {
		limit = 2
	}
	if limit > maxConcurrentClaims {
		limit = maxConcurrentClaims
	}
	if targets < limit {
		return targets
	}
	return limit
}

func (m *Manager) ActivateVirtualNodes(ctx context.Context) error {
	return m.ClaimVirtualNodes(ctx)
}

func (m *Manager) getInactiveVnodes(self membership.Peer) []membership.Vnode {
	inactive := make([]membership.Vnode, 0)
	for _, vn := range self.Vnodes {
		if vn.State != membership.VnodeActive {
			inactive = append(inactive, vn)
		}
	}

	return inactive
}

func (m *Manager) claimVnode(ctx context.Context, vnode membership.Vnode) error {

	if owner, _, ok := m.currentOwnerForPosition(vnode.Position); ok && owner.NodeId == m.mgr.SelfId() {
		if err := m.activateTargetRange(ctx, m.mgr.SelfId(), vnode.Id, vnode.Position); err != nil {
			logging.Error("Failed to activate vnode without transfer with vnode id=%v, error=%v.", vnode.Id, err)
			return err
		}
		return nil
	}

	// Compute the range necessary to activate the vnode
	plan, err := m.acquireAcceptedTransferPlan(ctx, vnode)
	if err != nil {
		if errors.Is(err, errAlreadyOwner) {
			if activateErr := m.activateTargetRange(ctx, m.mgr.SelfId(), vnode.Id, vnode.Position); activateErr != nil {
				logging.Error("Failed to activate vnode after owner retry with vnode id=%v, error=%v.", vnode.Id, activateErr)
				return activateErr
			}
			return nil
		}
		logging.Error("Failed to acquire transfer plan for vnode with vnode id=%v, error=%v.", vnode.Id, err)
		return err
	}
	m.registerClaim(plan, vnode)
	defer m.unregisterClaim(plan.TransferId)

	if err := m.streamSnapshot(ctx, plan); err != nil {
		logging.Error("Snapshot transfer failed during vnode claim with vnode id=%v, transfer id=%v, error=%v.", vnode.Id, plan.TransferId, err)
		return err
	}

	if err := m.completeTransferWithRetry(ctx, plan); err != nil {
		logging.Error("Failed to finalize transfer during vnode claim with vnode id=%v, transfer id=%v, error=%v.", vnode.Id, plan.TransferId, err)
		return err
	}

	if err := m.activateTargetRange(ctx, m.mgr.SelfId(), vnode.Id, vnode.Position); err != nil {
		logging.Error("Failed to activate vnode after transfer with vnode id=%v, transfer id=%v, error=%v.", vnode.Id, plan.TransferId, err)
		return err
	}

	return nil
}

// Returns an accepted transfer plan. Retires on error until
// accepted or failure
func (m *Manager) acquireAcceptedTransferPlan(ctx context.Context, vnode membership.Vnode) (rangeTransferPlan, error) {
	var plan rangeTransferPlan

	err := util.Do(ctx, util.RangeClaimBackoff, func() error {
		// Compute the most current plan.
		computedPlan, err := m.computeRangeTransferPlan(vnode)
		if err != nil {
			if errors.Is(err, errAlreadyOwner) {
				return err
			}
			return err
		}
		plan = computedPlan

		// Get the owner's address
		peer, success := m.mgr.PeerById(plan.OwnerNodeId)
		if !success {
			return fmt.Errorf("owner peer %s not found", plan.OwnerNodeId)
		}
		plan.OwnerAddr = peer.Address

		// Ensure the owner has a current membership record for this node before
		// requesting a range transfer. During startup, UP broadcasts may still
		// be in flight and would otherwise cause transient "requestor unknown"
		// rejections.
		if err := m.ensureRegisteredWithOwner(ctx, peer.Address); err != nil {
			return fmt.Errorf("register with owner %s: %w", plan.OwnerNodeId, err)
		}

		client, err := m.mgr.TransferClient(ctx, peer.Address)
		if err != nil {
			m.mgr.HandlePeerUnreachable(context.Background(), peer.NodeId, err)
			return err
		}
		resp, err := client.RequestRangeTransfer(ctx, &pb.RequestRangeTransferRequest{
			TargetRange: &pb.Range{
				Start: plan.Range.Start,
				End:   plan.Range.End,
			},
			TransferId: plan.TransferId,
			Requestor:  m.mgr.SelfId(),
		})
		if err != nil {
			m.mgr.HandlePeerUnreachable(context.Background(), peer.NodeId, err)
			return err
		}
		if !resp.Accepted {
			return errors.New("transfer request rejected")
		}

		return nil
	})

	return plan, err
}

func (m *Manager) ensureRegisteredWithOwner(ctx context.Context, ownerAddr string) error {
	client, err := m.mgr.MembershipClient(ctx, ownerAddr)
	if err != nil {
		return err
	}

	resp, err := client.RegisterNode(ctx, &pb.RegisterNodeRequest{Member: m.mgr.SelfProto()})
	if err != nil {
		return err
	}
	if !resp.GetAccepted() {
		return errors.New("membership registration rejected by owner")
	}

	return nil
}

type rangeTransferPlan struct {
	TransferId    string
	StreamId      int32
	OwnerNodeId   string
	OwnerAddr     string
	OwnerVnodeId  string
	TargetVnodeId string
	Range         ring.OwnedRange
}

func (m *Manager) computeRangeTransferPlan(vnode membership.Vnode) (rangeTransferPlan, error) {
	owner, rangeStart, success := m.currentOwnerForPosition(vnode.Position)
	if !success {
		// No current owner, error
		return rangeTransferPlan{}, fmt.Errorf("no current owner found for vnode %s", vnode.Id)
	}

	if owner.NodeId == m.mgr.SelfId() {
		// Self is already the owner, error
		return rangeTransferPlan{}, errAlreadyOwner
	}

	transferId := transferKey(owner.Id, rangeStart, vnode.Position)

	// Return plan
	plan := rangeTransferPlan{
		TransferId:    transferId,
		StreamId:      streamIdForTransfer(transferId),
		OwnerNodeId:   owner.NodeId,
		OwnerVnodeId:  owner.Id,
		TargetVnodeId: vnode.Id,
		Range: ring.OwnedRange{
			VnodeId: vnode.Id,
			Start:   rangeStart,
			End:     vnode.Position,
			NodeId:  m.mgr.SelfId(),
		},
	}
	return plan, nil
}

func (m *Manager) currentOwnerForPosition(pos uint64) (ring.VnodeEntry, uint64, bool) {
	behind, ahead, success := m.mgr.Ring.GetVnodesBetweenPosition(pos)
	if !success {
		return ring.VnodeEntry{}, 0, false
	}
	return ahead, behind.Position, true
}

func (m *Manager) streamSnapshot(ctx context.Context, plan rangeTransferPlan) error {
	client, err := m.mgr.TransferClient(ctx, plan.OwnerAddr)
	if err != nil {
		m.mgr.HandlePeerUnreachable(context.Background(), plan.OwnerNodeId, err)
		logging.Error("Failed to open transfer client for snapshot with transfer id=%v, peer addr=%v, error=%v.", plan.TransferId, plan.OwnerAddr, err)
		return err
	}
	stream, err := client.StreamRange(ctx, &pb.StreamRangeRequest{TransferId: plan.StreamId})
	if err != nil {
		m.mgr.HandlePeerUnreachable(context.Background(), plan.OwnerNodeId, err)
		logging.Error("Snapshot stream RPC failed with transfer id=%v, error=%v.", plan.TransferId, err)
		return fmt.Errorf("stream range %s: %w", plan.TransferId, err)
	}
	recordsApplied := 0

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			m.mgr.HandlePeerUnreachable(context.Background(), plan.OwnerNodeId, err)
			logging.Error("Snapshot stream receive failed with transfer id=%v, error=%v.", plan.TransferId, err)
			return fmt.Errorf("stream recv %s: %w", plan.TransferId, err)
		}
		if chunk.TransferId != plan.TransferId {
			logging.Error("Snapshot stream transfer id mismatch with transfer id=%v, received transfer id=%v.", plan.TransferId, chunk.TransferId)
			return fmt.Errorf("streamed transfer mismatch: expected %s, got %s", plan.TransferId, chunk.TransferId)
		}
		for _, rec := range chunk.Records {
			_, err := m.store.UpsertRecord(storage.Record{
				Key:       rec.Key,
				Value:     rec.Value,
				VnodeId:   plan.TargetVnodeId,
				Timestamp: rec.Timestamp,
			})
			if err != nil {
				logging.Error("Failed applying streamed record with transfer id=%v, key=%v, error=%v.", plan.TransferId, rec.Key, err)
				return fmt.Errorf("apply streamed record %s: %w", rec.Key, err)
			}
			recordsApplied++
		}
		if chunk.IsFinal {
			break
		}
	}
	return nil
}

func (m *Manager) completeTransferWithRetry(ctx context.Context, plan rangeTransferPlan) error {
	client, err := m.mgr.TransferClient(ctx, plan.OwnerAddr)
	if err != nil {
		m.mgr.HandlePeerUnreachable(context.Background(), plan.OwnerNodeId, err)
		logging.Error("Failed to open transfer client for completion with transfer id=%v, error=%v.", plan.TransferId, err)
		return err
	}

	if err := util.Do(ctx, util.TransferComplete, func() error {
		resp, err := client.CompleteRangeTransfer(ctx, &pb.CompleteRangeTransferRequest{TransferId: plan.TransferId})
		if err != nil {
			m.mgr.HandlePeerUnreachable(context.Background(), plan.OwnerNodeId, err)
			return err
		}
		if !resp.Accepted {
			return errors.New("transfer complete rejected")
		}
		return nil
	}); err != nil {
		logging.Error("Failed to complete transfer after retries with transfer id=%v, error=%v.", plan.TransferId, err)
		return fmt.Errorf("complete transfer %s: %w", plan.TransferId, err)
	}
	return nil
}

type ownerCandidate struct {
	entry   ring.VnodeEntry
	vnodeId string
}
