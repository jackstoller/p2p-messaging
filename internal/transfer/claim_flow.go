package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// ClaimVirtualNodes computes the virtual nodes this node should own as primary,
// then claims each missing vnode concurrently from its current owner.
func (m *Manager) ClaimVirtualNodes(ctx context.Context) error {
	self := m.mgr.Self()
	targets := m.computeTargetVnodes(self)
	if len(targets) == 0 {
		return nil
	}

	alreadyActive := map[string]struct{}{}
	for _, vn := range self.Vnodes {
		if vn.State == membership.VnodeActive {
			alreadyActive[vn.Id] = struct{}{}
		}
	}

	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	for _, vnodeId := range targets {
		if _, ok := alreadyActive[vnodeId]; ok {
			continue
		}

		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := m.claimVnode(ctx, id); err != nil {
				errCh <- err
			}
		}(vnodeId)
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

// ClaimTargetRanges is kept as a compatibility alias for existing callers.
func (m *Manager) ClaimTargetRanges(ctx context.Context) error {
	return m.ClaimVirtualNodes(ctx)
}

func (m *Manager) computeTargetVnodes(self membership.Peer) []string {
	entries := make([]ring.VnodeEntry, 0)
	for _, p := range m.mgr.AllMembers() {
		if p.State == membership.PeerDown {
			continue
		}
		for _, vn := range p.Vnodes {
			if vn.State != membership.VnodeActive {
				continue
			}
			entries = append(entries, ring.VnodeEntry{Position: vn.Position, NodeId: p.NodeId, Address: p.Address})
		}
	}

	selfPos := make(map[uint64]string, len(self.Vnodes))
	for _, vn := range self.Vnodes {
		selfPos[vn.Position] = vn.Id
		if vn.State == membership.VnodeActive {
			continue
		}
		entries = append(entries, ring.VnodeEntry{Position: vn.Position, NodeId: self.NodeId, Address: self.Address})
	}

	scratch := ring.New()
	scratch.Rebuild(entries)

	ranges := scratch.OwnedRanges(self.NodeId)
	result := make([]string, 0, len(ranges))
	for _, r := range ranges {
		if id, ok := selfPos[r.End]; ok {
			result = append(result, id)
		}
	}
	return result
}

type targetRangePlan struct {
	owner         ring.VnodeEntry
	sourceVnodeId string
	rangeStart    uint64
	rangeEnd      uint64
}

func (m *Manager) claimVnode(ctx context.Context, targetVnodeId string) error {
	self := m.mgr.Self()
	vnodePos, err := m.localVnodePosition(self, targetVnodeId)
	if err != nil {
		return err
	}

	plan, hasOwner, err := m.requestTransferWithBackoff(ctx, self.NodeId, targetVnodeId, vnodePos)
	if err != nil {
		return err
	}
	if !hasOwner {
		return m.activateTargetRange(ctx, self.NodeId, targetVnodeId, vnodePos)
	}

	if err := m.streamSnapshot(ctx, plan.owner.Address, targetVnodeId); err != nil {
		return err
	}

	if err := m.completeTransferWithRetry(ctx, plan.owner.Address, targetVnodeId); err != nil {
		return err
	}

	if err := m.activateTargetRange(ctx, self.NodeId, targetVnodeId, vnodePos); err != nil {
		return fmt.Errorf("persist vnode %s active: %w", targetVnodeId, err)
	}

	slog.Info(
		"claimed target range",
		"targetVnodeId", targetVnodeId,
		"from", plan.owner.NodeId,
		"sourceVnodeId", plan.sourceVnodeId,
		"start", plan.rangeStart,
		"end", plan.rangeEnd,
	)
	return nil
}

func (m *Manager) localVnodePosition(self membership.Peer, targetVnodeId string) (uint64, error) {
	for _, vn := range self.Vnodes {
		if vn.Id == targetVnodeId {
			return vn.Position, nil
		}
	}
	return 0, fmt.Errorf("transfer: unknown local vnode %s", targetVnodeId)
}

// requestTransferWithBackoff retries transfer request with exponential backoff,
// recomputing current owner each attempt to reflect topology changes.
func (m *Manager) requestTransferWithBackoff(ctx context.Context, selfId, targetVnodeId string, vnodePos uint64) (*targetRangePlan, bool, error) {
	var (
		planned  *targetRangePlan
		accepted bool
	)

	if err := util.Do(ctx, util.TransferBackoff, func() error {
		resolvedPlan, resolved := m.currentOwnerRangeForTarget(vnodePos, selfId)
		if !resolved {
			accepted = true
			planned = nil
			return nil
		}

		planned = resolvedPlan
		client, err := m.mgr.TransferClient(ctx, planned.owner.Address)
		if err != nil {
			return err
		}
		resp, err := client.RequestRangeTransfer(ctx, &pb.RequestRangeTransferRequest{TargetVnodeId: targetVnodeId, Requestor: selfId})
		if err != nil {
			return err
		}
		if !resp.Accepted {
			return errors.New("transfer request rejected")
		}
		accepted = true
		return nil
	}); err != nil {
		return nil, false, fmt.Errorf("transfer request %s: %w", targetVnodeId, err)
	}
	if !accepted {
		return nil, false, fmt.Errorf("transfer request %s not accepted", targetVnodeId)
	}
	if planned == nil {
		return nil, false, nil
	}
	return planned, true, nil
}

func (m *Manager) streamSnapshot(ctx context.Context, ownerAddr, targetVnodeId string) error {
	client, err := m.mgr.TransferClient(ctx, ownerAddr)
	if err != nil {
		return err
	}
	stream, err := client.StreamRange(ctx, &pb.StreamRangeRequest{TargetVnodeId: targetVnodeId})
	if err != nil {
		return fmt.Errorf("stream slice %s: %w", targetVnodeId, err)
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("stream recv %s: %w", targetVnodeId, err)
		}
		for _, rec := range chunk.Records {
			_, err := m.store.UpsertRecord(storage.Record{
				Key:       rec.Key,
				Value:     rec.Value,
				VnodeId:   targetVnodeId,
				Timestamp: rec.Timestamp,
			})
			if err != nil {
				return fmt.Errorf("apply streamed record %s: %w", rec.Key, err)
			}
		}
		if chunk.Final {
			break
		}
	}
	return nil
}

func (m *Manager) completeTransferWithRetry(ctx context.Context, ownerAddr, targetVnodeId string) error {
	client, err := m.mgr.TransferClient(ctx, ownerAddr)
	if err != nil {
		return err
	}

	if err := util.Do(ctx, util.TransferComplete, func() error {
		resp, err := client.CompleteRangeTransfer(ctx, &pb.CompleteRangeTransferRequest{TargetVnodeId: targetVnodeId})
		if err != nil {
			return err
		}
		if !resp.Accepted {
			return errors.New("transfer complete rejected")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("complete transfer %s: %w", targetVnodeId, err)
	}
	return nil
}

type ownerCandidate struct {
	entry   ring.VnodeEntry
	vnodeId string
}

// currentOwnerRangeForTarget finds the current owner and transfer sub-range for
// a target vnode position being activated. Returned range is (start, end].
func (m *Manager) currentOwnerRangeForTarget(position uint64, excludeNodeId string) (*targetRangePlan, bool) {
	candidates := make([]ownerCandidate, 0)
	for _, peer := range m.mgr.AllMembers() {
		if peer.State == membership.PeerDown || peer.NodeId == excludeNodeId {
			continue
		}
		for _, vn := range peer.Vnodes {
			if vn.State != membership.VnodeActive {
				continue
			}
			candidates = append(candidates, ownerCandidate{
				entry:   ring.VnodeEntry{Position: vn.Position, NodeId: peer.NodeId, Address: peer.Address},
				vnodeId: vn.Id,
			})
		}
	}

	if len(candidates) == 0 {
		return nil, false
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].entry.Position < candidates[j].entry.Position
	})

	idx := sort.Search(len(candidates), func(i int) bool {
		return candidates[i].entry.Position >= position
	})
	if idx == len(candidates) {
		idx = 0
	}

	owner := candidates[idx]
	prev := candidates[(idx-1+len(candidates))%len(candidates)]

	return &targetRangePlan{
		owner:         owner.entry,
		sourceVnodeId: owner.vnodeId,
		rangeStart:    prev.entry.Position,
		rangeEnd:      position,
	}, true
}
