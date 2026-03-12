package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/jackstoller/p2p-messaging/internal/config"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/replica"
	"github.com/jackstoller/p2p-messaging/internal/server"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/transfer"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	initLogger()
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		slog.Error("node failed", "err", err)
		os.Exit(1)
	}
}

func initLogger() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

func run(cfg config.NodeConfig) error {
	slog.Info("starting node", "nodeId", cfg.NodeId, "addr", cfg.AdvertiseAddr)

	tlsCfg, err := config.TLSConfig(cfg.CACertPath, cfg.NodeCertPath, cfg.NodeKeyPath)
	if err != nil {
		return fmt.Errorf("TLS config: %w", err)
	}

	store, err := storage.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("storage open: %w", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			slog.Warn("storage close failed", "err", closeErr)
		}
	}()

	mgr := membership.NewManager(cfg.NodeId, cfg.AdvertiseAddr, cfg.ReplicaCount, tlsCfg)
	xfer := transfer.NewManager(mgr, store)
	repl := replica.NewManager(mgr, store)
	mgr.OnPeerDown = func(nodeId string) {
		repl.OnPeerDown(context.Background(), nodeId)
	}

	grpcServer, err := startGRPCServer(cfg.ListenAddr, tlsCfg, server.New(mgr, store, xfer, repl))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := initializeMembership(ctx, cfg, mgr, store); err != nil {
		return err
	}
	if err := claimPrimaryVirtualNodes(ctx, cfg, xfer); err != nil {
		return err
	}

	startPeerMonitoring(ctx, mgr)

	waitForShutdownSignal()
	slog.Info("shutting down")
	cancel()
	grpcServer.GracefulStop()
	return nil
}

func claimPrimaryVirtualNodes(ctx context.Context, cfg config.NodeConfig, xfer *transfer.Manager) error {
	if len(cfg.BootstrapPeers) == 0 {
		return nil
	}
	if err := xfer.ClaimVirtualNodes(ctx); err != nil {
		return fmt.Errorf("claim target ranges: %w", err)
	}
	return nil
}

func startPeerMonitoring(ctx context.Context, mgr *membership.Manager) {
	go mgr.RunHeartbeats(ctx)
}

func startGRPCServer(listenAddr string, tlsCfg *tls.Config, node *server.Server) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	pb.RegisterMembershipServiceServer(grpcServer, node)
	pb.RegisterTransferServiceServer(grpcServer, node)
	pb.RegisterReplicaServiceServer(grpcServer, node)
	pb.RegisterDataServiceServer(grpcServer, node)

	go func() {
		slog.Info("gRPC server listening", "addr", listenAddr)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			slog.Error("gRPC serve error", "err", serveErr)
		}
	}()

	return grpcServer, nil
}

func initializeMembership(ctx context.Context, cfg config.NodeConfig, mgr *membership.Manager, store *storage.Store) error {
	if len(cfg.BootstrapPeers) > 0 {
		if err := mgr.Join(ctx, cfg.BootstrapPeers); err != nil {
			return fmt.Errorf("join mesh: %w", err)
		}

		// If no active ranges exist yet, this node is effectively the first live node.
		// Activate local vnodes immediately so the ring can begin serving traffic.
		if mgr.Ring.Len() == 0 {
			mgr.ActivateSelf()
			for _, vn := range mgr.Self().Vnodes {
				if err := store.SetVnodeState(vn.Id, vn.Position, storage.OwnedVnodeStateActive); err != nil {
					return fmt.Errorf("persist self vnode %s: %w", vn.Id, err)
				}
			}
		}
		return nil
	}

	mgr.ActivateSelf()
	for _, vn := range mgr.Self().Vnodes {
		if err := store.SetVnodeState(vn.Id, vn.Position, storage.OwnedVnodeStateActive); err != nil {
			return fmt.Errorf("persist self vnode %s: %w", vn.Id, err)
		}
	}
	return nil
}

func waitForShutdownSignal() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

func parseFlags() config.NodeConfig {
	nodeId := flag.String("id", "", "unique node Id (generated if empty)")
	listenAddr := flag.String("listen", ":9000", "gRPC listen address")
	advertiseAddr := flag.String("advertise", "", "address peers use to dial this node (required)")
	bootstrapPeers := flag.String("peers", "", "comma-separated bootstrap peer addresses")
	dbPath := flag.String("db", "node.db", "SQLite DB path (use :memory: for ephemeral)")
	replicaCount := flag.Int("replicas", 2, "number of replicas per key (min 1)")
	caCert := flag.String("ca-cert", "certs/ca.crt", "path to CA certificate")
	nodeCert := flag.String("node-cert", "certs/node.crt", "path to this node's certificate")
	nodeKey := flag.String("node-key", "certs/node.key", "path to this node's private key")

	flag.Parse()

	if *nodeId == "" {
		*nodeId = uuid.New().String()
		slog.Info("generated node Id", "nodeId", *nodeId)
	}
	if *advertiseAddr == "" {
		slog.Error("-advertise is required")
		os.Exit(1)
	}
	if *replicaCount < 1 {
		slog.Error("replica count must be at least 1")
		os.Exit(1)
	}

	var peers []string
	if *bootstrapPeers != "" {
		peers = strings.Split(*bootstrapPeers, ",")
	}

	return config.NodeConfig{
		NodeId:         *nodeId,
		ListenAddr:     *listenAddr,
		AdvertiseAddr:  *advertiseAddr,
		BootstrapPeers: peers,
		DBPath:         *dbPath,
		ReplicaCount:   *replicaCount,
		CACertPath:     *caCert,
		NodeCertPath:   *nodeCert,
		NodeKeyPath:    *nodeKey,
	}
}
