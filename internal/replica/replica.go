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
	logging.Component("replica").Info("replica.manager.init", logging.Outcome(logging.OutcomeSucceeded))
	return &Manager{
		mgr:   mgr,
		store: store,
		meta:  make(map[string]replicaMeta),
	}
}

func (m *Manager) UpdateReplicaRank(_ context.Context, req *pb.UpdateReplicaRankRequest) (*pb.UpdateReplicaRankResponse, error) {
	log := logging.Component("replica")
	m.mu.Lock()
	m.meta[req.VnodeId] = replicaMeta{PrimaryId: req.PrimaryId, Rank: req.NewRank}
	m.mu.Unlock()
	log.Info("replica.rank.update", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, req.VnodeId, "primary_id", req.PrimaryId, "new_rank", req.NewRank)
	return &pb.UpdateReplicaRankResponse{}, nil
}

func (m *Manager) CreateReplica(_ context.Context, req *pb.CreateReplicaRequest) (*pb.CreateReplicaResponse, error) {
	log := logging.Component("replica")
	if req.Record == nil {
		log.Warn("replica.create", logging.Outcome(logging.OutcomeRejected), logging.AttrVnodeId, req.VnodeId, "reason", "record_required")
		return nil, fmt.Errorf("replica: record is required")
	}
	_, err := m.store.UpsertRecord(storage.Record{
		Key:       req.Record.Key,
		Value:     req.Record.Value,
		VnodeId:   req.VnodeId,
		Timestamp: req.Record.Timestamp,
	})
	if err != nil {
		log.Error("replica.create", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, req.VnodeId, logging.AttrKey, req.Record.Key, logging.Err(err))
		return nil, err
	}
	log.Info("replica.create", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, req.VnodeId, logging.AttrKey, req.Record.Key, "timestamp", req.Record.Timestamp)
	return &pb.CreateReplicaResponse{Ack: true}, nil
}

// ReplicateRecord fans out a primary write to configured replica nodes.
func (m *Manager) ReplicateRecord(ctx context.Context, vnodeId string, rec storage.Record) {
	log := logging.Component("replica")
	nodes := m.mgr.Ring.ResponsibleNodes(rec.Key, m.mgr.ReplicaCount()+1)
	if len(nodes) <= 1 {
		log.Debug("replica.repair.dispatch", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, "reason", "no_replica_targets")
		return
	}
	log.Info("replica.dispatch", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, "targets", len(nodes)-1)

	for rank, n := range nodes[1:] {
		if n.NodeId == m.mgr.SelfId() {
			log.Debug("replica.dispatch.target", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, logging.AttrPeerId, n.NodeId, "reason", "self")
			continue
		}

		peer, ok := m.mgr.PeerById(n.NodeId)
		if !ok || peer.State == membership.PeerDown {
			log.Warn("replica.dispatch.target", logging.Outcome(logging.OutcomeSkipped), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, logging.AttrPeerId, n.NodeId, "reason", "peer_missing_or_down")
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
				log.Error("replica.dispatch.target", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, logging.AttrPeerId, peer.NodeId, logging.AttrPeerAddr, peer.Address, "rank", rank+1, logging.Err(err))
				return
			}
			log.Info("replica.dispatch.target", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, logging.AttrKey, rec.Key, logging.AttrPeerId, peer.NodeId, logging.AttrPeerAddr, peer.Address, "rank", rank+1)
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
func (m *Manager) OnTopologyChanged(ctx context.Context, reason string) {
	log := logging.Component("replica")
	log.Info("replica.topology_changed", logging.Outcome(logging.OutcomeStarted), "reason", reason)
	repaired := m.repairAllLocalRecords(ctx)
	log.Info("replica.topology_changed", logging.Outcome(logging.OutcomeSucceeded), "reason", reason, "repaired_vnodes", repaired)
}

// repairReplicaSet replays current records for vnodeId to the currently
// selected replica targets under the latest ring topology.
func (m *Manager) repairReplicaSet(ctx context.Context, vnodeId string) {
	log := logging.Component("replica")
	log.Info("replica.repair", logging.Outcome(logging.OutcomeStarted), logging.AttrVnodeId, vnodeId)
	records, err := m.store.GetRecordsByVnode(vnodeId)
	if err != nil {
		log.Error("replica.repair", logging.Outcome(logging.OutcomeFailed), logging.AttrVnodeId, vnodeId, logging.Err(err))
		return
	}
	for _, r := range records {
		m.ReplicateRecord(ctx, vnodeId, r)
	}
	log.Info("replica.repair", logging.Outcome(logging.OutcomeSucceeded), logging.AttrVnodeId, vnodeId, "records", len(records))
}

// OnPeerDown triggers best-effort replica repair from currently active local vnodes.
func (m *Manager) OnPeerDown(ctx context.Context, deadNodeId string) {
	log := logging.Component("replica")
	log.Warn("replica.peer_down", logging.Outcome(logging.OutcomeStarted), "dead_node_id", deadNodeId)
	repaired := m.repairAllLocalRecords(ctx)
	log.Info("replica.peer_down", logging.Outcome(logging.OutcomeSucceeded), "dead_node_id", deadNodeId, "repaired_vnodes", repaired)
}

func (m *Manager) repairAllLocalRecords(ctx context.Context) int {
	log := logging.Component("replica")
	records, err := m.store.GetAllRecords()
	if err != nil {
		log.Error("replica.repair_all", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return 0
	}

	repaired := 0
	for _, record := range records {
		m.ReplicateRecord(ctx, record.VnodeId, record)
		repaired++
	}

	log.Info("replica.repair_all", logging.Outcome(logging.OutcomeSucceeded), "records", repaired)
	return repaired
}
