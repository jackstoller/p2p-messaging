package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// Attempts to activate all inactive virtual nodes by transferring
// ranges to itself and activating
func (m *Manager) ClaimVirtualNodes(ctx context.Context) error {
	self := m.mgr.Self()
	targets := m.getInactiveVnodes(self)
	if len(targets) == 0 {
		return nil
	}

	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	for _, vnode := range targets {
		wg.Add(1)
		go func(v membership.Vnode) {
			defer wg.Done()
			if err := m.claimVnode(ctx, v); err != nil {
				errCh <- err
			}
		}(vnode)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
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
	// Compute the range necessary to activate the vnode
	plan, err := m.acquireAcceptedTransferPlan(ctx, vnode)
	if err != nil {
		return err
	}
	m.registerClaim(plan, vnode)
	defer m.unregisterClaim(plan.TransferID)

	if err := m.streamSnapshot(ctx, plan); err != nil {
		return err
	}

	if err := m.completeTransferWithRetry(ctx, plan); err != nil {
		return err
	}

	if err := m.activateTargetRange(ctx, m.mgr.SelfId(), vnode.Id, vnode.Position); err != nil {
		return err
	}

	return nil
}

// Returns an accepted transfer plan. Retires on error until
// accepted or failure
func (m *Manager) acquireAcceptedTransferPlan(ctx context.Context, vnode membership.Vnode) (rangeTransferPlan, error) {
	var plan rangeTransferPlan

	err := util.Do(ctx, util.TransferBackoff, func() error {
		// Compute the most current plan.
		computedPlan, err := m.computeRangeTransferPlan(vnode)
		if err != nil {
			return err
		}
		plan = computedPlan

		// Get the owner's address
		peer, success := m.mgr.PeerById(plan.OwnerNodeID)
		if !success {
			return fmt.Errorf("owner peer %s not found", plan.OwnerNodeID)
		}
		plan.OwnerAddr = peer.Address

		client, err := m.mgr.TransferClient(ctx, peer.Address)
		if err != nil {
			return err
		}
		resp, err := client.RequestRangeTransfer(ctx, &pb.RequestRangeTransferRequest{
			TargetRange: &pb.Range{
				Start: plan.Range.Start,
				End:   plan.Range.End,
			},
			TransferId: plan.TransferID,
			Requestor:  m.mgr.SelfId(),
		})
		if err != nil {
			return err
		}
		if !resp.Accepted {
			return errors.New("transfer request rejected")
		}

		return nil
	})

	return plan, err
}

type rangeTransferPlan struct {
	TransferID    string
	StreamID      int32
	OwnerNodeID   string
	OwnerAddr     string
	OwnerVnodeID  string
	TargetVnodeID string
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
		return rangeTransferPlan{}, fmt.Errorf("self is already owner of vnode %s", vnode.Id)
	}

	transferID := transferKey(owner.Id, rangeStart, vnode.Position)

	// Return plan
	return rangeTransferPlan{
		TransferID:    transferID,
		StreamID:      streamIDForTransfer(transferID),
		OwnerNodeID:   owner.NodeId,
		OwnerVnodeID:  owner.Id,
		TargetVnodeID: vnode.Id,
		Range: ring.OwnedRange{
			VnodeId: vnode.Id,
			Start:   rangeStart,
			End:     vnode.Position,
			NodeId:  m.mgr.SelfId(),
		},
	}, nil
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
		return err
	}
	stream, err := client.StreamRange(ctx, &pb.StreamRangeRequest{TransferId: plan.StreamID})
	if err != nil {
		return fmt.Errorf("stream range %s: %w", plan.TransferID, err)
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("stream recv %s: %w", plan.TransferID, err)
		}
		if chunk.TransferId != plan.TransferID {
			return fmt.Errorf("streamed transfer mismatch: expected %s, got %s", plan.TransferID, chunk.TransferId)
		}
		for _, rec := range chunk.Records {
			_, err := m.store.UpsertRecord(storage.Record{
				Key:       rec.Key,
				Value:     rec.Value,
				VnodeId:   plan.TargetVnodeID,
				Timestamp: rec.Timestamp,
			})
			if err != nil {
				return fmt.Errorf("apply streamed record %s: %w", rec.Key, err)
			}
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
		return err
	}

	if err := util.Do(ctx, util.TransferComplete, func() error {
		resp, err := client.CompleteRangeTransfer(ctx, &pb.CompleteRangeTransferRequest{TransferId: plan.TransferID})
		if err != nil {
			return err
		}
		if !resp.Accepted {
			return errors.New("transfer complete rejected")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("complete transfer %s: %w", plan.TransferID, err)
	}
	return nil
}

type ownerCandidate struct {
	entry   ring.VnodeEntry
	vnodeId string
}
