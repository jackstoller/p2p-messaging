package membership

import (
	"context"
	"fmt"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/logging"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func (m *Manager) MembershipClient(_ context.Context, addr string) (pb.MembershipServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewMembershipServiceClient(conn), nil
}

func (m *Manager) TransferClient(_ context.Context, addr string) (pb.TransferServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewTransferServiceClient(conn), nil
}

func (m *Manager) ReplicaClient(_ context.Context, addr string) (pb.ReplicaServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewReplicaServiceClient(conn), nil
}

func (m *Manager) DataClient(_ context.Context, addr string) (pb.DataServiceClient, error) {
	conn, err := m.dial(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewDataServiceClient(conn), nil
}

func (m *Manager) dial(addr string) (*grpc.ClientConn, error) {
	log := logging.Component("membership.clients")
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()

	if cached, ok := m.clients[addr]; ok && time.Now().Before(cached.expiry) {
		log.Debug("membership.client.dial", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerAddr, addr, "cache", "hit")
		return cached.conn, nil
	}
	if _, ok := m.clients[addr]; ok {
		log.Debug("membership.client.dial", logging.Outcome(logging.OutcomeSkipped), logging.AttrPeerAddr, addr, "cache", "expired")
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(m.tlsCfg)))
	if err != nil {
		log.Error("membership.client.dial", logging.Outcome(logging.OutcomeFailed), logging.AttrPeerAddr, addr, logging.Err(err))
		return nil, fmt.Errorf("membership: dial %s: %w", addr, err)
	}
	m.clients[addr] = &cachedConn{conn: conn, expiry: time.Now().Add(connTTL)}
	log.Info("membership.client.dial", logging.Outcome(logging.OutcomeSucceeded), logging.AttrPeerAddr, addr, "cache", "miss", logging.DurationMillis("ttl", connTTL))
	return conn, nil
}
