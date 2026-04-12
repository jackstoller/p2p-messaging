package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	neturl "net/url"
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
	"github.com/jackstoller/p2p-messaging/internal/stun"
	"github.com/jackstoller/p2p-messaging/internal/transfer"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	cfg := parseFlags()
	initLogger(cfg)

	if err := run(cfg); err != nil {
		logging.Error("Node stopped because startup failed with error=%v.", err)
		os.Exit(1)
	}
}

func initLogger(cfg config.NodeConfig) {
	logging.Init(logging.Options{
		Verbose: cfg.Verbose,
	})
}

func run(cfg config.NodeConfig) error {
	logging.Info("Starting up node.")

	tlsCfg, err := config.TLSConfig(cfg.CACertPath, cfg.NodeCertPath, cfg.NodeKeyPath)
	if err != nil {
		logging.Error("TLS setup failed with error=%v.", err)
		return fmt.Errorf("TLS config: %w", err)
	}

	store, err := storage.Open(cfg.DBPath)
	if err != nil {
		logging.Error("Could not open local storage with db path=%v, error=%v.", cfg.DBPath, err)
		return fmt.Errorf("storage open: %w", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			logging.Warn("Storage close reported an error with error=%v.", closeErr)
			return
		}
	}()

	mgr := membership.NewManager(cfg.NodeId, cfg.AdvertiseAddr, cfg.ReplicaCount, tlsCfg)
	xfer := transfer.NewManager(mgr, store)
	repl := replica.NewManager(mgr, store)
	mgr.OnPeerDown = func(nodeId string) {
		repl.OnPeerDown(context.Background(), nodeId)
	}
	nodeServer := server.New(mgr, store, xfer, repl)
	api := httpapi.New(nodeServer, mgr, store, cfg.NodeId, cfg.HTTPAdvertise, localICEServerURLs(cfg.STUNAdvertise))

	grpcServer, err := startGRPCServer(cfg.ListenAddr, tlsCfg, nodeServer)
	if err != nil {
		logging.Error("Could not start gRPC server with error=%v.", err)
		return err
	}
	httpServer := startHTTPServer(cfg.HTTPListenAddr, api.Handler())
	stunServer, err := startSTUNServer(cfg.STUNListenAddr)
	if err != nil {
		logging.Error("Could not start STUN server with error=%v.", err)
		return err
	}
	defer func() {
		if closeErr := stunServer.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			logging.Warn("STUN server close reported an error with error=%v.", closeErr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := initializeMembership(ctx, cfg, mgr, store); err != nil {
		logging.Error("Membership setup failed with error=%v.", err)
		return err
	}
	if err := claimPrimaryVirtualNodes(ctx, cfg, xfer); err != nil {
		logging.Error("Could not claim initial ranges with error=%v.", err)
		return err
	}
	if err := api.PublishNodeInfo(ctx); err != nil {
		logging.Warn("Could not publish node info to peers yet with error=%v.", err)
	} else {
	}

	go mgr.RunHeartbeats(ctx)
	logging.Info(fmt.Sprintf("Node has completed setup with %d active ranges.", mgr.Ring.Len()))

	waitForShutdownSignal()
	logging.Info("Shutting down node.")
	cancel()
	httpShutdownCtx, httpShutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpShutdownCancel()
	if err := httpServer.Shutdown(httpShutdownCtx); err != nil {
		logging.Warn("HTTP server shutdown reported an error with error=%v.", err)
	}
	if err := stunServer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		logging.Warn("STUN server shutdown reported an error with error=%v.", err)
	}
	grpcServer.GracefulStop()
	logging.Info("Node shutdown complete.")
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
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			logging.Error("gRPC server stopped unexpectedly with error=%v.", serveErr)
		}
	}()

	return grpcServer, nil
}

func startHTTPServer(listenAddr string, handler http.Handler) *http.Server {
	httpServer := &http.Server{
		Addr:              listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.Error("HTTP server stopped unexpectedly with error=%v.", err)
		}
	}()

	return httpServer
}

func startSTUNServer(listenAddr string) (*stun.Server, error) {
	server, err := stun.Listen(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("stun listen %s: %w", listenAddr, err)
	}
	go func() {
		if err := server.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
			logging.Error("STUN server stopped unexpectedly with error=%v.", err)
		}
	}()
	return server, nil
}

func initializeMembership(ctx context.Context, cfg config.NodeConfig, mgr *membership.Manager, store *storage.Store) error {
	if len(cfg.BootstrapPeers) > 0 {
		if err := mgr.Join(ctx, cfg.BootstrapPeers); err != nil {
			return fmt.Errorf("join mesh: %w", err)
		}

		// If no active ranges exist yet, this node is effectively the first live node.
		// Activate local vnodes so the ring can begin serving traffic.
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
	httpListenAddr := flag.String("http-listen", ":8081", "HTTP API listen address")
	httpAdvertiseAddr := flag.String("http-advertise", "", "public HTTP address/base URL for browsers and inter-node signaling")
	stunListenAddr := flag.String("stun-listen", ":3478", "UDP STUN listen address")
	stunAdvertiseAddr := flag.String("stun-advertise", "", "public STUN URL for browsers, e.g. stun:node1.example.com:3478")
	advertiseAddr := flag.String("advertise", "", "address peers use to dial this node (required)")
	bootstrapPeers := flag.String("peers", "", "comma-separated bootstrap peer addresses")
	dbPath := flag.String("db", "node.db", "SQLite DB path (use :memory: for ephemeral)")
	replicaCount := flag.Int("replicas", 2, "number of replicas per key (min 1)")
	verbose := flag.Bool("verbose", false, "enable verbose logging")
	logLevel := flag.String("log-level", "", "deprecated: use -verbose; 'verbose' or 'debug' still enables verbose logs")
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
	if *stunAdvertiseAddr == "" {
		*stunAdvertiseAddr = deriveSTUNAdvertiseAddr(*httpAdvertiseAddr)
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
		STUNListenAddr: *stunListenAddr,
		STUNAdvertise:  *stunAdvertiseAddr,
		AdvertiseAddr:  *advertiseAddr,
		BootstrapPeers: peers,
		DBPath:         *dbPath,
		ReplicaCount:   *replicaCount,
		Verbose:        *verbose || logging.ParseLevel(*logLevel),
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

func deriveSTUNAdvertiseAddr(httpAdvertise string) string {
	httpAdvertise = strings.TrimSpace(httpAdvertise)
	if httpAdvertise == "" {
		return ""
	}
	if !strings.Contains(httpAdvertise, "://") {
		httpAdvertise = "http://" + httpAdvertise
	}
	parsed, err := neturl.Parse(httpAdvertise)
	if err != nil || parsed.Host == "" {
		return ""
	}
	host := parsed.Hostname()
	if host == "" {
		return ""
	}
	return "stun:" + net.JoinHostPort(host, "3478")
}

func localICEServerURLs(stunAdvertise string) []string {
	stunAdvertise = strings.TrimSpace(stunAdvertise)
	if stunAdvertise == "" {
		return nil
	}
	return []string{stunAdvertise}
}
