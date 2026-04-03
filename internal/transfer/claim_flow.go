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
	log := logging.Component("transfer.claim")
	self := m.mgr.Self()
	targets := m.getInactiveVnodes(self)
	if len(targets) == 0 {
		log.Info("transfer.claim.virtual_nodes", logging.Outcome(logging.OutcomeSkipped), "reason", "no_inactive_vnodes")
		return nil
	}
	log.Info("transfer.claim.virtual_nodes", logging.Outcome(logging.OutcomeStarted), "targets", len(targets))

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
			log.Error("transfer.claim.virtual_nodes", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
			return err
		}
	}
	log.Info("transfer.claim.virtual_nodes", logging.Outcome(logging.OutcomeSucceeded), "targets", len(targets))
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
	log := logging.Component("transfer.claim")
	log.Info("transfer.claim.vnode", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, vnode.Id, "position", vnode.Position)

	if owner, _, ok := m.currentOwnerForPosition(vnode.Position); ok && owner.NodeId == m.mgr.SelfId() {
		log.Info("transfer.claim.vnode", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnode.Id, "reason", "already_owned_after_rebuild")
		if err := m.activateTargetRange(ctx, m.mgr.SelfId(), vnode.Id, vnode.Position); err != nil {
			log.Error("transfer.claim.activate", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, logging.Err(err))
			return err
		}
		log.Info("transfer.claim.vnode", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnode.Id, "reason", "activated_without_transfer")
		return nil
	}

	// Compute the range necessary to activate the vnode
	plan, err := m.acquireAcceptedTransferPlan(ctx, vnode)
	if err != nil {
		if errors.Is(err, errAlreadyOwner) {
			if activateErr := m.activateTargetRange(ctx, m.mgr.SelfId(), vnode.Id, vnode.Position); activateErr != nil {
				log.Error("transfer.claim.activate", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, logging.Err(activateErr))
				return activateErr
			}
			log.Info("transfer.claim.vnode", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnode.Id, "reason", "activated_after_retry")
			return nil
		}
		log.Error("transfer.claim.vnode", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, logging.Err(err))
		return err
	}
	m.registerClaim(plan, vnode)
	defer m.unregisterClaim(plan.TransferId)

	if err := m.streamSnapshot(ctx, plan); err != nil {
		log.Error("transfer.claim.snapshot", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, logging.Err(err))
		return err
	}

	if err := m.completeTransferWithRetry(ctx, plan); err != nil {
		log.Error("transfer.claim.complete", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, logging.Err(err))
		return err
	}

	if err := m.activateTargetRange(ctx, m.mgr.SelfId(), vnode.Id, vnode.Position); err != nil {
		log.Error("transfer.claim.activate", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, logging.Err(err))
		return err
	}

	log.Info("transfer.claim.vnode", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, "owner_node_id", plan.OwnerNodeId, "range_start", plan.Range.Start, "range_end", plan.Range.End)
	return nil
}

// Returns an accepted transfer plan. Retires on error until
// accepted or failure
func (m *Manager) acquireAcceptedTransferPlan(ctx context.Context, vnode membership.Vnode) (rangeTransferPlan, error) {
	log := logging.Component("transfer.claim")
	var plan rangeTransferPlan

	err := util.Do(ctx, util.RangeClaimBackoff, func() error {
		// Compute the most current plan.
		computedPlan, err := m.computeRangeTransferPlan(vnode)
		if err != nil {
			if errors.Is(err, errAlreadyOwner) {
				return err
			}
			log.Warn("transfer.claim.plan", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, logging.Err(err))
			return err
		}
		plan = computedPlan
		log.Info("transfer.claim.plan", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, "owner_node_id", plan.OwnerNodeId, "range_start", plan.Range.Start, "range_end", plan.Range.End)

		// Get the owner's address
		peer, success := m.mgr.PeerById(plan.OwnerNodeId)
		if !success {
			return fmt.Errorf("owner peer %s not found", plan.OwnerNodeId)
		}
		plan.OwnerAddr = peer.Address

		client, err := m.mgr.TransferClient(ctx, peer.Address)
		if err != nil {
			log.Warn("transfer.claim.plan", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, logging.AttrPeerId, peer.NodeId, logging.AttrPeerAddr, peer.Address, logging.Err(err))
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
			log.Warn("transfer.claim.plan", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, logging.Err(err))
			return err
		}
		if !resp.Accepted {
			log.Warn("transfer.claim.plan", logging.Outcome(logging.OutcomeRejected), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, logging.AttrPeerId, peer.NodeId)
			return errors.New("transfer request rejected")
		}
		log.Info("transfer.claim.plan", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, logging.AttrPeerId, peer.NodeId, logging.AttrPeerAddr, peer.Address)

		return nil
	})

	return plan, err
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
	log := logging.Component("transfer.claim")
	owner, rangeStart, success := m.currentOwnerForPosition(vnode.Position)
	if !success {
		// No current owner, error
		log.Warn("transfer.claim.plan.compute", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnode.Id, "reason", "no_current_owner")
		return rangeTransferPlan{}, fmt.Errorf("no current owner found for vnode %s", vnode.Id)
	}

	if owner.NodeId == m.mgr.SelfId() {
		// Self is already the owner, error
		log.Warn("transfer.claim.plan.compute", logging.Outcome(logging.OutcomeRejected), logging.AttrVnodeId, vnode.Id, "reason", "self_already_owner")
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
	log.Info("transfer.claim.plan.compute", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnode.Id, "transfer_id", plan.TransferId, "owner_node_id", plan.OwnerNodeId, "owner_vnode_id", plan.OwnerVnodeId, "range_start", plan.Range.Start, "range_end", plan.Range.End)
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
	log := logging.Component("transfer.claim")
	log.Info("transfer.claim.snapshot", logging.Outcome(logging.OutcomeStarted), "transfer_id", plan.TransferId, logging.AttrPeerId, plan.OwnerNodeId, logging.AttrPeerAddr, plan.OwnerAddr, "target_vnode_id", plan.TargetVnodeId)
	client, err := m.mgr.TransferClient(ctx, plan.OwnerAddr)
	if err != nil {
		log.Error("transfer.claim.snapshot", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, logging.AttrPeerAddr, plan.OwnerAddr, logging.Err(err))
		return err
	}
	stream, err := client.StreamRange(ctx, &pb.StreamRangeRequest{TransferId: plan.StreamId})
	if err != nil {
		log.Error("transfer.claim.snapshot", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, logging.Err(err))
		return fmt.Errorf("stream range %s: %w", plan.TransferId, err)
	}
	recordsApplied := 0

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			log.Debug("transfer.claim.snapshot.recv", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", plan.TransferId, "reason", "eof")
			break
		}
		if err != nil {
			log.Error("transfer.claim.snapshot.recv", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, logging.Err(err))
			return fmt.Errorf("stream recv %s: %w", plan.TransferId, err)
		}
		if chunk.TransferId != plan.TransferId {
			log.Error("transfer.claim.snapshot.recv", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, "received_transfer_id", chunk.TransferId, "reason", "transfer_id_mismatch")
			return fmt.Errorf("streamed transfer mismatch: expected %s, got %s", plan.TransferId, chunk.TransferId)
		}
		log.Debug("transfer.claim.snapshot.chunk", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", plan.TransferId, "seq", chunk.Seq, "records", len(chunk.Records), "is_final", chunk.IsFinal)
		for _, rec := range chunk.Records {
			_, err := m.store.UpsertRecord(storage.Record{
				Key:       rec.Key,
				Value:     rec.Value,
				VnodeId:   plan.TargetVnodeId,
				Timestamp: rec.Timestamp,
			})
			if err != nil {
				log.Error("transfer.claim.snapshot.apply", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, logging.AttrKey, rec.Key, logging.Err(err))
				return fmt.Errorf("apply streamed record %s: %w", rec.Key, err)
			}
			recordsApplied++
		}
		if chunk.IsFinal {
			break
		}
	}
	log.Info("transfer.claim.snapshot", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", plan.TransferId, "records_applied", recordsApplied)
	return nil
}

func (m *Manager) completeTransferWithRetry(ctx context.Context, plan rangeTransferPlan) error {
	log := logging.Component("transfer.claim")
	log.Info("transfer.claim.complete", logging.Outcome(logging.OutcomeStarted), "transfer_id", plan.TransferId, logging.AttrPeerId, plan.OwnerNodeId, logging.AttrPeerAddr, plan.OwnerAddr)
	client, err := m.mgr.TransferClient(ctx, plan.OwnerAddr)
	if err != nil {
		log.Error("transfer.claim.complete", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, logging.Err(err))
		return err
	}

	if err := util.Do(ctx, util.TransferComplete, func() error {
		resp, err := client.CompleteRangeTransfer(ctx, &pb.CompleteRangeTransferRequest{TransferId: plan.TransferId})
		if err != nil {
			log.Warn("transfer.claim.complete", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, logging.Err(err))
			return err
		}
		if !resp.Accepted {
			log.Warn("transfer.claim.complete", logging.Outcome(logging.OutcomeRejected), "transfer_id", plan.TransferId)
			return errors.New("transfer complete rejected")
		}
		log.Info("transfer.claim.complete", logging.Outcome(logging.OutcomeSucceeded), "transfer_id", plan.TransferId)
		return nil
	}); err != nil {
		log.Error("transfer.claim.complete", logging.Outcome(logging.OutcomeFailed), "transfer_id", plan.TransferId, logging.Err(err))
		return fmt.Errorf("complete transfer %s: %w", plan.TransferId, err)
	}
	return nil
}

type ownerCandidate struct {
	entry   ring.VnodeEntry
	vnodeId string
}
