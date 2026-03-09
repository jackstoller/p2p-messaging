package server

import (
	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/replica"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/transfer"
	pb "github.com/jackstoller/p2p-messaging/proto"
)

// Server implements all gRPC services for the mesh node.
type Server struct {
	pb.UnimplementedMembershipServiceServer
	pb.UnimplementedTransferServiceServer
	pb.UnimplementedReplicaServiceServer
	pb.UnimplementedDataServiceServer

	mgr   *membership.Manager
	store *storage.Store
	xfer  *transfer.Manager
	repl  *replica.Manager
}

// New creates a Server.
func New(mgr *membership.Manager, store *storage.Store, xfer *transfer.Manager, repl *replica.Manager) *Server {
	return &Server{mgr: mgr, store: store, xfer: xfer, repl: repl}
}
