package server

import (
	"context"
	"fmt"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	"github.com/jackstoller/p2p-messaging/internal/util"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	log := logging.Component("server.data")
	log.Info("rpc.data.write", logging.Outcome(logging.OutcomeStarted), logging.AttrKey, req.Key, "value_bytes", len(req.Value))
	plan, err := s.resolveWritePlan(req.Key)
	if err != nil {
		log.Error("rpc.data.write", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, req.Key, "reason", "ring_empty")
		return nil, err
	}
	log.Debug("rpc.data.write.route", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, req.Key, "primary_node_id", plan.primaryNodeId, "primary_position", plan.primaryPos)

	if !plan.local {
		log.Info("rpc.data.write.forward", logging.Outcome(logging.OutcomeStarted), logging.AttrKey, req.Key, "primary_node_id", plan.primaryNodeId)
		return s.forwardWriteToPrimary(ctx, plan.primaryNodeId, req)
	}

	result, err := s.executeLocalWrite(ctx, req, plan)
	if err != nil {
		return nil, err
	}

	s.replicateLocalWrite(result)
	log.Info("rpc.data.write", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, req.Key, logging.AttrVnodeId, result.vnodeId, "timestamp", result.record.Timestamp)
	return result.response, nil
}

func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log := logging.Component("server.data")
	log.Info("rpc.data.read", logging.Outcome(logging.OutcomeStarted), logging.AttrKey, req.Key)
	// Step 1 locate the primary for this key
	primary, ok := s.mgr.Ring.Primary(req.Key)
	if !ok {
		log.Error("rpc.data.read", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, req.Key, "reason", "ring_empty")
		return nil, status.Error(codes.Unavailable, "ring is empty")
	}

	if primary.NodeId == s.mgr.SelfId() {
		resp, rec, found, err := s.readLocally(req.Key)
		if err != nil {
			log.Error("rpc.data.read.local", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, req.Key, logging.Err(err))
			return nil, status.Error(codes.Internal, err.Error())
		}
		if !found {
			log.Info("rpc.data.read.local", logging.Outcome(logging.OutcomeSkipped), logging.AttrKey, req.Key, "found", false)
			return resp, nil
		}
		log.Info("rpc.data.read.local", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, req.Key, "found", true, logging.AttrVnodeId, rec.VnodeId, "timestamp", rec.Timestamp)
		return resp, nil
	}

	resp, source, nodeId, err := s.readFromResponsibleNodes(ctx, req, primary.NodeId)
	if err == nil && source != "" {
		log.Info("rpc.data.read.remote", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, req.Key, "source", source, "node_id", nodeId, "found", resp.GetFound())
		return resp, nil
	}

	log.Info("rpc.data.read", logging.Outcome(logging.OutcomeSkipped), logging.AttrKey, req.Key, "found", false)
	return resp, nil
}

func (s *Server) forwardWriteToPrimary(ctx context.Context, primaryNodeId string, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	log := logging.Component("server.data")
	address, err := s.dataPeer(primaryNodeId)
	if err != nil {
		log.Warn("rpc.data.write.forward", logging.Outcome(logging.OutcomeRejected), logging.AttrKey, req.Key, "primary_node_id", primaryNodeId, "reason", "peer_not_found")
		return nil, status.Error(codes.Unavailable, "primary peer not found")
	}

	var resp *pb.WriteResponse
	err = s.withDataClient(ctx, primaryNodeId, util.RPC.MaxAttempts, func(client pb.DataServiceClient) error {
		resp, err = client.Write(ctx, req)
		return err
	})
	if err != nil {
		log.Warn("rpc.data.write.forward", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, req.Key, "primary_node_id", primaryNodeId, logging.AttrPeerAddr, address, logging.Err(err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	log.Info("rpc.data.write.forward", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, req.Key, "primary_node_id", primaryNodeId, logging.AttrPeerAddr, address, "timestamp", resp.GetTimestamp())
	return resp, nil
}

func (s *Server) vnodeIdFor(nodeId string, position uint64) (string, error) {
	log := logging.Component("server.data")
	peer, ok := s.mgr.PeerById(nodeId)
	if !ok {
		log.Warn("rpc.data.vnode_lookup", logging.Outcome(logging.OutcomeRejected), logging.AttrPeerId, nodeId, "position", position, "reason", "peer_not_found")
		return "", fmt.Errorf("peer %s not found", nodeId)
	}
	for _, vn := range peer.Vnodes {
		if vn.Position == position {
			log.Debug("rpc.data.vnode_lookup", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerId, nodeId, "position", position, logging.AttrVnodeId, vn.Id)
			return vn.Id, nil
		}
	}
	log.Warn("rpc.data.vnode_lookup", logging.Outcome(logging.OutcomeRejected), logging.AttrPeerId, nodeId, "position", position, "reason", "vnode_not_found")
	return "", fmt.Errorf("vnode not found for node %s position %d", nodeId, position)
}

func (s *Server) readRemoteWithRetry(ctx context.Context, nodeId string, req *pb.ReadRequest, attempts int) (*pb.ReadResponse, error) {
	log := logging.Component("server.data")
	address, err := s.dataPeer(nodeId)
	if err != nil {
		log.Warn("rpc.data.read.forward", logging.Outcome(logging.OutcomeRejected), logging.AttrKey, req.Key, logging.AttrPeerId, nodeId, "reason", "peer_not_found")
		return nil, fmt.Errorf("peer %s not found", nodeId)
	}

	var resp *pb.ReadResponse
	err = s.withDataClient(ctx, nodeId, attempts, func(client pb.DataServiceClient) error {
		resp, err = client.Read(ctx, req)
		return err
	})
	if err != nil {
		log.Warn("rpc.data.read.forward", logging.Outcome(logging.OutcomeFailed), logging.AttrKey, req.Key, logging.AttrPeerId, nodeId, logging.AttrPeerAddr, address, "attempts", attempts, logging.Err(err))
		return nil, err
	}
	log.Debug("rpc.data.read.forward", logging.Outcome(logging.OutcomeSucceeded), logging.AttrKey, req.Key, logging.AttrPeerId, nodeId, logging.AttrPeerAddr, address, "attempts", attempts, "found", resp.GetFound())
	return resp, nil
}
