package membership

import (
	"context"
	"fmt"
	"time"

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
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()

	if cached, ok := m.clients[addr]; ok && time.Now().Before(cached.expiry) {
		return cached.conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(m.tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("membership: dial %s: %w", addr, err)
	}
	m.clients[addr] = &cachedConn{conn: conn, expiry: time.Now().Add(connTTL)}
	return conn, nil
}
