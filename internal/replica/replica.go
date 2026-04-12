package replica

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

type replicaMeta struct {
	PrimaryId string
	Rank      int32
}

// Handles replica metadata updates and record replication
type Manager struct {
	mgr   *membership.Manager
	store *storage.Store

	mu   sync.Mutex
	meta map[string]replicaMeta // vnode Id -> replica metadata
}

func NewManager(mgr *membership.Manager, store *storage.Store) *Manager {
	return &Manager{
		mgr:   mgr,
		store: store,
		meta:  make(map[string]replicaMeta),
	}
}

func (m *Manager) UpdateReplicaRank(_ context.Context, req *pb.UpdateReplicaRankRequest) (*pb.UpdateReplicaRankResponse, error) {
	m.mu.Lock()
	m.meta[req.VnodeId] = replicaMeta{PrimaryId: req.PrimaryId, Rank: req.NewRank}
	m.mu.Unlock()
	return &pb.UpdateReplicaRankResponse{}, nil
}

func (m *Manager) CreateReplica(_ context.Context, req *pb.CreateReplicaRequest) (*pb.CreateReplicaResponse, error) {
	if req.Record == nil {
		logging.Warn("Rejected replica create because record payload is missing with vnode=%v.", req.VnodeId)
		return nil, fmt.Errorf("replica: record is required")
	}
	_, err := m.store.UpsertRecord(storage.Record{
		Key:       req.Record.Key,
		Value:     req.Record.Value,
		VnodeId:   req.VnodeId,
		Timestamp: req.Record.Timestamp,
	})
	if err != nil {
		logging.Error("Replica create failed with vnode=%v, key=%v, error=%v.", req.VnodeId, req.Record.Key, err)
		return nil, err
	}
	return &pb.CreateReplicaResponse{Ack: true}, nil
}

// ReplicateRecord fans out a primary write to configured replica nodes.
func (m *Manager) ReplicateRecord(ctx context.Context, vnodeId string, rec storage.Record) {
	nodes := m.mgr.Ring.ResponsibleNodes(rec.Key, m.mgr.ReplicaCount()+1)
	if len(nodes) <= 1 {
		return
	}

	for rank, n := range nodes[1:] {
		if n.NodeId == m.mgr.SelfId() {
			continue
		}

		peer, ok := m.mgr.PeerById(n.NodeId)
		if !ok || peer.State == membership.PeerDown {
			continue
		}

		go func(rank int, peer membership.Peer) {
			req := &pb.CreateReplicaRequest{
				VnodeId: vnodeId,
				Record: &pb.DataRecord{
					Key:       rec.Key,
					Value:     rec.Value,
					Timestamp: rec.Timestamp,
				},
			}
			err := util.Do(ctx, util.RPC, func() error {
				client, err := m.mgr.ReplicaClient(ctx, peer.Address)
				if err != nil {
					return err
				}
				_, err = client.CreateReplica(ctx, req)
				return err
			})
			if err != nil {
				m.mgr.HandlePeerUnreachable(context.Background(), peer.NodeId, err)
				logging.Warn("Replica write to peer failed with vnode=%v, key=%v, peer=%v, rank=%v, error=%v.", vnodeId, rec.Key, peer.NodeId, rank+1, err)
				return
			}
		}(rank, peer)
	}
}

// OnVnodeDown runs best-effort replica repair for one local vnode after
// membership marks a peer down and the ring has been rebuilt.
func (m *Manager) OnVnodeDown(ctx context.Context, vnodeId string) {
	m.repairReplicaSet(ctx, vnodeId)
}

// OnVnodeActive runs repair when a vnode becomes ACTIVE after range claim.
func (m *Manager) OnVnodeActive(ctx context.Context, vnodeId string) {
	m.repairReplicaSet(ctx, vnodeId)
}

// OnTopologyChanged triggers best-effort replica repair from all currently
// active local vnodes after the ring changes.
func (m *Manager) OnTopologyChanged(ctx context.Context, _ string) {
	m.repairAllLocalRecords(ctx)
}

// repairReplicaSet replays current records for vnodeId to the currently
// selected replica targets under the latest ring topology.
func (m *Manager) repairReplicaSet(ctx context.Context, vnodeId string) {
	records, err := m.store.GetRecordsByVnode(vnodeId)
	if err != nil {
		logging.Error("Replica repair failed while loading vnode records with vnode=%v, error=%v.", vnodeId, err)
		return
	}
	for _, r := range records {
		m.ReplicateRecord(ctx, vnodeId, r)
	}
}

// OnPeerDown triggers best-effort replica repair from currently active local vnodes.
func (m *Manager) OnPeerDown(ctx context.Context, _ string) {
	m.repairAllLocalRecords(ctx)
}

func (m *Manager) repairAllLocalRecords(ctx context.Context) int {
	records, err := m.store.GetAllRecords()
	if err != nil {
		logging.Error("Replica repair-all failed while loading records with error=%v.", err)
		return 0
	}

	repaired := 0
	for _, record := range records {
		m.ReplicateRecord(ctx, record.VnodeId, record)
		repaired++
	}

	return repaired
}
