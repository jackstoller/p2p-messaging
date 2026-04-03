package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackstoller/p2p-messaging/internal/config"
	"github.com/jackstoller/p2p-messaging/internal/httpapi"
	"github.com/jackstoller/p2p-messaging/internal/logging"
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
	cfg := parseFlags()
	initLogger(cfg)

	if err := run(cfg); err != nil {
		logging.Component("main").Error("node.run", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		os.Exit(1)
	}
}

func initLogger(cfg config.NodeConfig) {
	logging.Init(logging.Options{
		Level:         logging.ParseLevel(cfg.LogLevel),
		NodeId:        cfg.NodeId,
		AdvertiseAddr: cfg.AdvertiseAddr,
	})
}

func run(cfg config.NodeConfig) error {
	log := logging.Component("main")
	log.Info("node.start", logging.Outcome(logging.OutcomeStarted), "listen_addr", cfg.ListenAddr, "http_listen_addr", cfg.HTTPListenAddr, "bootstrap_peers", len(cfg.BootstrapPeers), "replica_count", cfg.ReplicaCount, "db_path", cfg.DBPath, "log_level", cfg.LogLevel)

	tlsCfg, err := config.TLSConfig(cfg.CACertPath, cfg.NodeCertPath, cfg.NodeKeyPath)
	if err != nil {
		log.Error("node.tls.init", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return fmt.Errorf("TLS config: %w", err)
	}
	log.Info("node.tls.init", logging.Outcome(logging.OutcomeSucceeded))

	store, err := storage.Open(cfg.DBPath)
	if err != nil {
		log.Error("node.storage.open", logging.Outcome(logging.OutcomeFailed), "db_path", cfg.DBPath, logging.Err(err))
		return fmt.Errorf("storage open: %w", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			log.Warn("node.storage.close", logging.Outcome(logging.OutcomeFailed), logging.Err(closeErr))
			return
		}
		log.Info("node.storage.close", logging.Outcome(logging.OutcomeSucceeded))
	}()

	mgr := membership.NewManager(cfg.NodeId, cfg.AdvertiseAddr, cfg.ReplicaCount, tlsCfg)
	xfer := transfer.NewManager(mgr, store)
	repl := replica.NewManager(mgr, store)
	mgr.OnPeerDown = func(nodeId string) {
		repl.OnPeerDown(context.Background(), nodeId)
	}
	nodeServer := server.New(mgr, store, xfer, repl)
	api := httpapi.New(nodeServer, mgr, store, cfg.NodeId, cfg.HTTPAdvertise)

	grpcServer, err := startGRPCServer(cfg.ListenAddr, tlsCfg, nodeServer)
	if err != nil {
		log.Error("node.grpc.start", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return err
	}
	httpServer := startHTTPServer(cfg.HTTPListenAddr, api.Handler())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := initializeMembership(ctx, cfg, mgr, store); err != nil {
		log.Error("node.membership.init", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return err
	}
	log.Info("node.membership.init", logging.Outcome(logging.OutcomeSucceeded))
	if err := claimPrimaryVirtualNodes(ctx, cfg, xfer); err != nil {
		log.Error("node.vnode.claim", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		return err
	}
	log.Info("node.vnode.claim", logging.Outcome(logging.OutcomeSucceeded))
	if err := api.PublishNodeInfo(ctx); err != nil {
		log.Warn("node.http.publish_info", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
	} else {
		log.Info("node.http.publish_info", logging.Outcome(logging.OutcomeSucceeded), "http_advertise", cfg.HTTPAdvertise)
	}

	startPeerMonitoring(ctx, mgr)

	waitForShutdownSignal()
	log.Info("node.shutdown", logging.Outcome(logging.OutcomeStarted))
	cancel()
	httpShutdownCtx, httpShutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpShutdownCancel()
	if err := httpServer.Shutdown(httpShutdownCtx); err != nil {
		log.Warn("node.http.shutdown", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
	}
	grpcServer.GracefulStop()
	log.Info("node.shutdown", logging.Outcome(logging.OutcomeSucceeded))
	return nil
}

func claimPrimaryVirtualNodes(ctx context.Context, cfg config.NodeConfig, xfer *transfer.Manager) error {
	log := logging.Component("main")
	if len(cfg.BootstrapPeers) == 0 {
		log.Debug("node.vnode.claim.skipped", logging.Outcome(logging.OutcomeSkipped), "reason", "no_bootstrap_peers")
		return nil
	}
	if err := xfer.ClaimVirtualNodes(ctx); err != nil {
		return fmt.Errorf("claim target ranges: %w", err)
	}
	return nil
}

func startPeerMonitoring(ctx context.Context, mgr *membership.Manager) {
	logging.Component("main").Info("node.heartbeat.loop", logging.Outcome(logging.OutcomeStarted))
	go mgr.RunHeartbeats(ctx)
}

func startGRPCServer(listenAddr string, tlsCfg *tls.Config, node *server.Server) (*grpc.Server, error) {
	log := logging.Component("main")
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
		log.Info("node.grpc.listen", logging.Outcome(logging.OutcomeSucceeded), "listen_addr", listenAddr)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Error("node.grpc.serve", logging.Outcome(logging.OutcomeFailed), logging.Err(serveErr))
		}
	}()

	return grpcServer, nil
}

func startHTTPServer(listenAddr string, handler http.Handler) *http.Server {
	log := logging.Component("main")
	httpServer := &http.Server{
		Addr:              listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("node.http.listen", logging.Outcome(logging.OutcomeSucceeded), "listen_addr", listenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("node.http.serve", logging.Outcome(logging.OutcomeFailed), logging.Err(err))
		}
	}()

	return httpServer
}

func initializeMembership(ctx context.Context, cfg config.NodeConfig, mgr *membership.Manager, store *storage.Store) error {
	log := logging.Component("main")
	if len(cfg.BootstrapPeers) > 0 {
		log.Info("node.membership.join", logging.Outcome(logging.OutcomeStarted), "bootstrap_peers", len(cfg.BootstrapPeers))
		if err := mgr.Join(ctx, cfg.BootstrapPeers); err != nil {
			return fmt.Errorf("join mesh: %w", err)
		}

		// If no active ranges exist yet, this node is effectively the first live node.
		// Activate local vnodes so the ring can begin serving traffic.
		if mgr.Ring.Len() == 0 {
			log.Info("node.membership.first_live_node", logging.Outcome(logging.OutcomeStarted))
			mgr.ActivateSelf()
			for _, vn := range mgr.Self().Vnodes {
				if err := store.SetVnodeState(vn.Id, vn.Position, storage.OwnedVnodeStateActive); err != nil {
					return fmt.Errorf("persist self vnode %s: %w", vn.Id, err)
				}
			}
			log.Info("node.membership.first_live_node", logging.Outcome(logging.OutcomeSucceeded), "activated_vnodes", len(mgr.Self().Vnodes))
		}
		return nil
	}

	log.Info("node.membership.bootstrap_absent", logging.Outcome(logging.OutcomeStarted))
	mgr.ActivateSelf()
	for _, vn := range mgr.Self().Vnodes {
		if err := store.SetVnodeState(vn.Id, vn.Position, storage.OwnedVnodeStateActive); err != nil {
			return fmt.Errorf("persist self vnode %s: %w", vn.Id, err)
		}
	}
	log.Info("node.membership.bootstrap_absent", logging.Outcome(logging.OutcomeSucceeded), "activated_vnodes", len(mgr.Self().Vnodes))
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
	httpListenAddr := flag.String("http-listen", ":8081", "HTTP API listen address")
	httpAdvertiseAddr := flag.String("http-advertise", "", "public HTTP address/base URL for browsers and inter-node signaling")
	advertiseAddr := flag.String("advertise", "", "address peers use to dial this node (required)")
	bootstrapPeers := flag.String("peers", "", "comma-separated bootstrap peer addresses")
	dbPath := flag.String("db", "node.db", "SQLite DB path (use :memory: for ephemeral)")
	replicaCount := flag.Int("replicas", 2, "number of replicas per key (min 1)")
	logLevel := flag.String("log-level", "debug", "log verbosity: debug, info, warn, error")
	caCert := flag.String("ca-cert", "certs/ca.crt", "path to CA certificate")
	nodeCert := flag.String("node-cert", "certs/node.crt", "path to this node's certificate")
	nodeKey := flag.String("node-key", "certs/node.key", "path to this node's private key")

	flag.Parse()

	if *nodeId == "" {
		*nodeId = uuid.New().String()
	}
	if *advertiseAddr == "" {
		fmt.Fprintln(os.Stderr, "-advertise is required")
		os.Exit(1)
	}
	if *replicaCount < 1 {
		fmt.Fprintln(os.Stderr, "replica count must be at least 1")
		os.Exit(1)
	}
	if *httpAdvertiseAddr == "" {
		*httpAdvertiseAddr = deriveHTTPAdvertiseAddr(*advertiseAddr, *httpListenAddr)
	}

	var peers []string
	if *bootstrapPeers != "" {
		peers = strings.Split(*bootstrapPeers, ",")
	}

	return config.NodeConfig{
		NodeId:         *nodeId,
		ListenAddr:     *listenAddr,
		HTTPListenAddr: *httpListenAddr,
		HTTPAdvertise:  *httpAdvertiseAddr,
		AdvertiseAddr:  *advertiseAddr,
		BootstrapPeers: peers,
		DBPath:         *dbPath,
		ReplicaCount:   *replicaCount,
		LogLevel:       *logLevel,
		CACertPath:     *caCert,
		NodeCertPath:   *nodeCert,
		NodeKeyPath:    *nodeKey,
	}
}

func deriveHTTPAdvertiseAddr(advertiseAddr, httpListenAddr string) string {
	host, _, err := net.SplitHostPort(advertiseAddr)
	if err != nil || host == "" {
		return httpListenAddr
	}

	httpHost, httpPort, err := net.SplitHostPort(httpListenAddr)
	if err != nil {
		return net.JoinHostPort(host, "8081")
	}
	if httpHost == "" || httpHost == "0.0.0.0" || httpHost == "::" {
		return net.JoinHostPort(host, httpPort)
	}
	if _, portErr := strconv.Atoi(httpPort); portErr != nil {
		return net.JoinHostPort(host, "8081")
	}
	return net.JoinHostPort(httpHost, httpPort)
}
