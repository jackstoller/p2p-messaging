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
	"github.com/jackstoller/p2p-messaging/internal/heartbeat"
	"github.com/jackstoller/p2p-messaging/internal/membership"
	"github.com/jackstoller/p2p-messaging/internal/ring"
	"github.com/jackstoller/p2p-messaging/internal/server"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	"github.com/jackstoller/p2p-messaging/internal/transfer"
	pb "github.com/jackstoller/p2p-messaging/proto/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	cfg := parseFlags()

	// ── Logging ──────────────────────────────────────────────────────────────
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	slog.Info("starting node", "nodeID", cfg.NodeID, "addr", cfg.AdvertiseAddr)

	// ── mTLS ─────────────────────────────────────────────────────────────────
	tlsCfg, err := config.TLSConfig(cfg.CACertPath, cfg.NodeCertPath, cfg.NodeKeyPath)
	if err != nil {
		slog.Error("TLS config failed", "err", err)
		os.Exit(1)
	}

	// ── Storage ───────────────────────────────────────────────────────────────
	store, err := storage.Open(cfg.DBPath)
	if err != nil {
		slog.Error("storage open failed", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	// ── Self member descriptor ────────────────────────────────────────────────
	// Generate virtual node positions deterministically from nodeID.
	self := buildSelfMember(cfg.NodeID, cfg.AdvertiseAddr)

	// ── Membership manager ────────────────────────────────────────────────────
	mgr := membership.New(self, cfg.ReplicaCount, tlsCfg)

	// ── Transfer manager ──────────────────────────────────────────────────────
	xfer := transfer.New(mgr, store, cfg.NodeID)

	// ── gRPC server ───────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		slog.Error("listen failed", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}
	serverCreds := credentials.NewTLS(tlsCfg)
	grpcServer := grpc.NewServer(grpc.Creds(serverCreds))

	onDead := func(nodeID string) {
		slog.Warn("running recovery for dead node", "nodeID", nodeID)
		if err := xfer.RecoverFromDead(context.Background(), nodeID); err != nil {
			slog.Error("recovery failed", "deadNode", nodeID, "err", err)
		}
	}

	node := server.New(mgr, store, xfer, cfg.NodeID, onDead)
	pb.RegisterMembershipServiceServer(grpcServer, node)
	pb.RegisterTransferServiceServer(grpcServer, node)
	pb.RegisterReplicationServiceServer(grpcServer, node)
	pb.RegisterDataServiceServer(grpcServer, node)

	go func() {
		slog.Info("gRPC server listening", "addr", cfg.ListenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC serve error", "err", err)
		}
	}()

	// ── Bootstrap / Join ─────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(cfg.BootstrapPeers) > 0 {
		if err := joinNetwork(ctx, cfg, mgr, xfer, tlsCfg, self); err != nil {
			slog.Error("join failed", "err", err)
			os.Exit(1)
		}
	} else {
		// First node — immediately activate self.
		slog.Info("first node in network, activating self")
		_ = mgr.Activate(cfg.NodeID)
	}

	// ── Heartbeat ────────────────────────────────────────────────────────────
	monitor := heartbeat.New(mgr, onDead)
	go monitor.Run(ctx)

	// ── Shutdown ──────────────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
	cancel()
	grpcServer.GracefulStop()
}

// joinNetwork runs the full join sequence:
//  1. Dial a bootstrap seed and sync the member list.
//  2. Announce self to all known nodes.
//  3. Execute the ring claim/transfer protocol.
//  4. Broadcast ACTIVE status.
func joinNetwork(
	ctx context.Context,
	cfg config.NodeConfig,
	mgr *membership.Manager,
	xfer *transfer.Manager,
	tlsCfg *tls.Config,
	self membership.Member,
) error {
	// Step 1: Find a live seed and sync members.
	var seedAddr string
	for _, peer := range cfg.BootstrapPeers {
		creds := credentials.NewTLS(tlsCfg)
		conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(creds))
		if err != nil {
			slog.Warn("bootstrap peer unreachable", "peer", peer, "err", err)
			continue
		}
		client := pb.NewMembershipServiceClient(conn)
		resp, err := client.SyncMembers(ctx, &pb.SyncMembersRequest{RequestingNodeId: cfg.NodeID})
		conn.Close()
		if err != nil {
			slog.Warn("SyncMembers failed", "peer", peer, "err", err)
			continue
		}
		mgr.ApplySnapshot(resp.Members)
		seedAddr = peer
		slog.Info("synced members from seed", "seed", peer, "count", len(resp.Members))
		break
	}
	if seedAddr == "" {
		return fmt.Errorf("no bootstrap peer available")
	}

	// Step 2: Announce self as PENDING to all known nodes.
	selfProto := membership.MemberToProto(self)
	for _, peer := range mgr.ActiveMembers() {
		go func(peer membership.Member) {
			client, err := mgr.MembershipClient(ctx, peer.Address)
			if err != nil {
				return
			}
			_, _ = client.Announce(ctx, &pb.AnnounceRequest{Member: selfProto})
		}(peer)
	}

	// Step 3: Rebuild ring with self as PENDING, then run transfer for each range.
	// Ring is rebuilt inside Activate after transfer completes.
	// At this point self is PENDING — ring doesn't include us yet.
	// We compute what our ranges *will be* once we're active.
	// Temporarily add self as ACTIVE in a scratch ring to compute owned ranges.
	peers := mgr.ActiveMembers()
	scratchEntries := make([]ring.NodeEntry, 0, len(peers)+1)
	for _, p := range peers {
		scratchEntries = append(scratchEntries, ring.NodeEntry{
			NodeID:  p.NodeID,
			Address: p.Address,
			Vnodes:  len(p.Vnodes),
		})
	}
	scratchEntries = append(scratchEntries, ring.NodeEntry{
		NodeID:  cfg.NodeID,
		Address: cfg.AdvertiseAddr,
		Vnodes:  ring.DefaultVirtualNodes,
	})
	scratchRing := ring.New()
	scratchRing.Rebuild(scratchEntries)

	// Temporarily swap the ring so transfer.ExecuteJoin sees the future state.
	mgr.Ring.Rebuild(scratchEntries)

	if err := xfer.ExecuteJoin(ctx); err != nil {
		return fmt.Errorf("join transfer failed: %w", err)
	}

	// Step 4: Broadcast that we're now ACTIVE.
	selfProto.Status = pb.MemberStatus_ACTIVE
	for _, peer := range mgr.ActiveMembers() {
		go func(peer membership.Member) {
			client, err := mgr.MembershipClient(ctx, peer.Address)
			if err != nil {
				return
			}
			_, _ = client.MembershipUpdate(ctx, &pb.MembershipUpdateRequest{
				UpdateType: pb.MembershipUpdateRequest_JOIN,
				Member:     selfProto,
			})
		}(peer)
	}

	slog.Info("join sequence complete, node is ACTIVE")
	return nil
}

// buildSelfMember constructs this node's Member struct with virtual node positions.
// Positions are derived deterministically from nodeID so they're stable across restarts.
func buildSelfMember(nodeID, advertiseAddr string) membership.Member {
	vnodes := make([]membership.VirtualNode, ring.DefaultVirtualNodes)
	for i := 0; i < ring.DefaultVirtualNodes; i++ {
		key := fmt.Sprintf("%s:%d", nodeID, i)
		pos := ring.KeyPosition(key)
		vnodes[i] = membership.VirtualNode{
			Position: pos,
			NodeID:   nodeID,
			Address:  advertiseAddr,
		}
	}
	return membership.Member{
		NodeID:  nodeID,
		Address: advertiseAddr,
		Vnodes:  vnodes,
		Status:  pb.MemberStatus_PENDING,
	}
}

// ─── flags ────────────────────────────────────────────────────────────────────

func parseFlags() config.NodeConfig {
	nodeID := flag.String("id", "", "unique node ID (generated if empty)")
	listenAddr := flag.String("listen", ":9000", "gRPC listen address")
	advertiseAddr := flag.String("advertise", "", "address peers use to dial this node (required)")
	bootstrapPeers := flag.String("peers", "", "comma-separated bootstrap peer addresses")
	dbPath := flag.String("db", "node.db", "SQLite DB path (use :memory: for ephemeral)")
	replicaCount := flag.Int("replicas", 2, "number of replicas per key (min 1)")
	caCert := flag.String("ca-cert", "certs/ca.crt", "path to CA certificate")
	nodeCert := flag.String("node-cert", "certs/node.crt", "path to this node's certificate")
	nodeKey := flag.String("node-key", "certs/node.key", "path to this node's private key")

	flag.Parse()

	if *nodeID == "" {
		*nodeID = uuid.New().String()
		slog.Info("generated node ID", "nodeID", *nodeID)
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
		NodeID:         *nodeID,
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
