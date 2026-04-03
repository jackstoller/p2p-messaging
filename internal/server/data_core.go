package server

import (
	"context"
	"fmt"

	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type writePlan struct {
	primaryNodeId string
	primaryPos    uint64
	local         bool
}

type localWriteResult struct {
	response *pb.WriteResponse
	record   storage.Record
	vnodeId  string
}

func (s *Server) resolveWritePlan(key string) (writePlan, error) {
	primary, ok := s.mgr.Ring.Primary(key)
	if !ok {
		return writePlan{}, status.Error(codes.Unavailable, "ring is empty")
	}

	return writePlan{
		primaryNodeId: primary.NodeId,
		primaryPos:    primary.Position,
		local:         primary.NodeId == s.mgr.SelfId(),
	}, nil
}

func (s *Server) executeLocalWrite(ctx context.Context, req *pb.WriteRequest, plan writePlan) (localWriteResult, error) {
	vnodeId, err := s.vnodeIdFor(plan.primaryNodeId, plan.primaryPos)
	if err != nil {
		return localWriteResult{}, status.Error(codes.Internal, err.Error())
	}
	if err := s.xfer.RejectPrimaryWrite(vnodeId, req.Key); err != nil {
		return localWriteResult{}, status.Error(codes.Unavailable, err.Error())
	}

	record := storage.Record{
		Key:       req.Key,
		Value:     req.Value,
		VnodeId:   vnodeId,
		Timestamp: storage.NowMillis(),
	}
	if _, err := s.store.UpsertRecord(record); err != nil {
		return localWriteResult{}, status.Error(codes.Internal, err.Error())
	}
	if err := s.xfer.ForwardPrimaryWrite(ctx, vnodeId, record); err != nil {
		return localWriteResult{}, status.Error(codes.Unavailable, err.Error())
	}

	return localWriteResult{
		response: &pb.WriteResponse{Ok: true, Timestamp: record.Timestamp},
		record:   record,
		vnodeId:  vnodeId,
	}, nil
}

func (s *Server) replicateLocalWrite(result localWriteResult) {
	go s.repl.ReplicateRecord(context.Background(), result.vnodeId, result.record)
}

func (s *Server) readLocally(key string) (*pb.ReadResponse, storage.Record, bool, error) {
	record, found, err := s.store.GetRecord(key)
	if err != nil {
		return nil, storage.Record{}, false, err
	}
	if !found {
		return &pb.ReadResponse{Found: false}, storage.Record{}, false, nil
	}
	return &pb.ReadResponse{Value: record.Value, Found: true}, record, true, nil
}

func (s *Server) readFromResponsibleNodes(ctx context.Context, req *pb.ReadRequest, primaryNodeId string) (*pb.ReadResponse, string, string, error) {
	if resp, err := s.readRemoteWithRetry(ctx, primaryNodeId, req, 2); err == nil {
		return resp, "primary", primaryNodeId, nil
	}

	replicas := s.mgr.Ring.ResponsibleNodes(req.Key, s.mgr.ReplicaCount()+1)
	for _, replicaNode := range replicas[1:] {
		resp, err := s.readRemoteWithRetry(ctx, replicaNode.NodeId, req, 1)
		if err == nil {
			return resp, "replica", replicaNode.NodeId, nil
		}
	}

	return &pb.ReadResponse{Found: false}, "", "", nil
}

func (s *Server) dataPeer(nodeId string) (string, error) {
	peer, ok := s.mgr.PeerById(nodeId)
	if !ok {
		return "", fmt.Errorf("peer %s not found", nodeId)
	}
	return peer.Address, nil
}

func (s *Server) withDataClient(ctx context.Context, nodeId string, attempts int, fn func(pb.DataServiceClient) error) error {
	address, err := s.dataPeer(nodeId)
	if err != nil {
		return err
	}

	return util.Do(ctx, util.Config{
		MaxAttempts:  attempts,
		InitialDelay: util.RPC.InitialDelay,
		Multiplier:   1.0,
	}, func() error {
		client, err := s.mgr.DataClient(ctx, address)
		if err != nil {
			return err
		}
		return fn(client)
	})
}
