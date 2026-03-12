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

func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	// Step 1 locate the primary for this key
	primary, ok := s.mgr.Ring.Primary(req.Key)
	if !ok {
		return nil, status.Error(codes.Unavailable, "ring is empty")
	}

	// Step 2 forward when this node is not primary
	if primary.NodeId != s.mgr.SelfId() {
		return s.forwardWriteToPrimary(ctx, primary.NodeId, req)
	}

	// Step 3 write locally with the primary timestamp
	timestamp := storage.NowMillis()
	vnodeId, err := s.vnodeIdFor(primary.NodeId, primary.Position)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.xfer.RejectPrimaryWrite(vnodeId, req.Key); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	rec := storage.Record{Key: req.Key, Value: req.Value, VnodeId: vnodeId, Timestamp: timestamp} // TODO: Why store vnode id over position?
	if _, err := s.store.UpsertRecord(rec); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.xfer.ForwardPrimaryWrite(ctx, vnodeId, rec); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	// Step 4 continue async replica fanout after ACK
	go s.repl.ReplicateRecord(context.Background(), vnodeId, rec)
	return &pb.WriteResponse{Ok: true, Timestamp: timestamp}, nil
}

func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	// Step 1 locate the primary for this key
	primary, ok := s.mgr.Ring.Primary(req.Key)
	if !ok {
		return nil, status.Error(codes.Unavailable, "ring is empty")
	}

	if primary.NodeId == s.mgr.SelfId() {
		rec, found, err := s.store.GetRecord(req.Key)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if !found {
			return &pb.ReadResponse{Found: false}, nil
		}
		return &pb.ReadResponse{Value: rec.Value, Found: true}, nil
	}

	// Step 2 try primary first with one retry
	if resp, err := s.readRemoteWithRetry(ctx, primary.NodeId, req, 2); err == nil {
		return resp, nil
	}

	// Step 3 fall back to replicas in ring order
	replicas := s.mgr.Ring.ResponsibleNodes(req.Key, s.mgr.ReplicaCount()+1)
	for _, replicaNode := range replicas[1:] {
		resp, err := s.readRemoteWithRetry(ctx, replicaNode.NodeId, req, 1)
		if err == nil {
			return resp, nil
		}
	}

	return &pb.ReadResponse{Found: false}, nil
}

func (s *Server) forwardWriteToPrimary(ctx context.Context, primaryNodeId string, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	peer, ok := s.mgr.PeerById(primaryNodeId)
	if !ok {
		return nil, status.Error(codes.Unavailable, "primary peer not found")
	}

	var resp *pb.WriteResponse
	err := util.Do(ctx, util.RPC, func() error {
		client, err := s.mgr.DataClient(ctx, peer.Address)
		if err != nil {
			return err
		}
		resp, err = client.Write(ctx, req)
		return err
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return resp, nil
}

func (s *Server) vnodeIdFor(nodeId string, position uint64) (string, error) {
	peer, ok := s.mgr.PeerById(nodeId)
	if !ok {
		return "", fmt.Errorf("peer %s not found", nodeId)
	}
	for _, vn := range peer.Vnodes {
		if vn.Position == position {
			return vn.Id, nil
		}
	}
	return "", fmt.Errorf("vnode not found for node %s position %d", nodeId, position)
}

func (s *Server) readRemoteWithRetry(ctx context.Context, nodeId string, req *pb.ReadRequest, attempts int) (*pb.ReadResponse, error) {
	peer, ok := s.mgr.PeerById(nodeId)
	if !ok {
		return nil, fmt.Errorf("peer %s not found", nodeId)
	}

	var resp *pb.ReadResponse
	err := util.Do(ctx, util.Config{MaxAttempts: attempts, InitialDelay: util.RPC.InitialDelay, Multiplier: 1.0}, func() error {
		client, err := s.mgr.DataClient(ctx, peer.Address)
		if err != nil {
			return err
		}
		resp, err = client.Read(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
