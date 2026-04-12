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
	plan, err := s.resolveWritePlan(req.Key)
	if err != nil {
		logging.Error("Write failed because ring has no active ranges with key=%v.", req.Key)
		return nil, err
	}

	if !plan.local {
		resp, err := s.forwardWriteToPrimary(ctx, plan.primaryNodeId, req)
		if err == nil {
			return resp, nil
		}

		retryPlan, ok := s.failoverWritePlan(ctx, req.Key, plan.primaryNodeId, err)
		if !ok {
			return nil, err
		}

		logging.Warn("Primary write failed; trying failover primary with key=%v, failed primary=%v, new primary=%v.", req.Key, plan.primaryNodeId, retryPlan.primaryNodeId)
		if retryPlan.local {
			result, retryErr := s.executeLocalWrite(ctx, req, retryPlan)
			if retryErr != nil {
				return nil, retryErr
			}
			s.replicateLocalWrite(result)
			return result.response, nil
		}

		resp, retryErr := s.forwardWriteToPrimary(ctx, retryPlan.primaryNodeId, req)
		if retryErr != nil {
			return nil, retryErr
		}
		return resp, nil
	}

	result, err := s.executeLocalWrite(ctx, req, plan)
	if err != nil {
		return nil, err
	}

	s.replicateLocalWrite(result)
	return result.response, nil
}

func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	// Step 1 locate the primary for this key
	primary, ok := s.mgr.Ring.Primary(req.Key)
	if !ok {
		logging.Error("Read failed because ring has no active ranges with key=%v.", req.Key)
		return nil, status.Error(codes.Unavailable, "ring is empty")
	}

	if primary.NodeId == s.mgr.SelfId() {
		resp, _, found, err := s.readLocally(req.Key)
		if err != nil {
			logging.Error("Local read failed with key=%v, error=%v.", req.Key, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		if !found {
			return resp, nil
		}
		return resp, nil
	}

	resp, source, _, err := s.readFromResponsibleNodes(ctx, req, primary.NodeId)
	if err == nil && source != "" {
		return resp, nil
	}

	return resp, nil
}

func (s *Server) forwardWriteToPrimary(ctx context.Context, primaryNodeId string, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	address, err := s.dataPeer(primaryNodeId)
	if err != nil {
		logging.Warn("Could not forward write because primary peer was not found with key=%v, primary node=%v.", req.Key, primaryNodeId)
		return nil, status.Error(codes.Unavailable, "primary peer not found")
	}

	var resp *pb.WriteResponse
	err = s.withDataClient(ctx, primaryNodeId, util.RPC.MaxAttempts, func(client pb.DataServiceClient) error {
		resp, err = client.Write(ctx, req)
		return err
	})
	if err != nil {
		logging.Warn("Write forward to primary failed with key=%v, primary node=%v, peer addr=%v, error=%v.", req.Key, primaryNodeId, address, err)
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return resp, nil
}

func (s *Server) vnodeIdFor(nodeId string, position uint64) (string, error) {
	peer, ok := s.mgr.PeerById(nodeId)
	if !ok {
		logging.Warn("Could not resolve vnode because peer was not found with peer=%v, position=%v.", nodeId, position)
		return "", fmt.Errorf("peer %s not found", nodeId)
	}
	for _, vn := range peer.Vnodes {
		if vn.Position == position {
			return vn.Id, nil
		}
	}
	logging.Warn("Could not resolve vnode because position was not found on peer with peer=%v, position=%v.", nodeId, position)
	return "", fmt.Errorf("vnode not found for node %s position %d", nodeId, position)
}

func (s *Server) readRemoteWithRetry(ctx context.Context, nodeId string, req *pb.ReadRequest, attempts int) (*pb.ReadResponse, error) {
	address, err := s.dataPeer(nodeId)
	if err != nil {
		logging.Warn("Could not forward read because peer was not found with key=%v, peer=%v.", req.Key, nodeId)
		return nil, fmt.Errorf("peer %s not found", nodeId)
	}

	var resp *pb.ReadResponse
	err = s.withDataClient(ctx, nodeId, attempts, func(client pb.DataServiceClient) error {
		resp, err = client.Read(ctx, req)
		return err
	})
	if err != nil {
		s.mgr.HandlePeerUnreachable(context.Background(), nodeId, err)
		logging.Warn("Read forward failed with key=%v, peer=%v, peer addr=%v, attempts=%v, error=%v.", req.Key, nodeId, address, attempts, err)
		return nil, err
	}
	return resp, nil
}
