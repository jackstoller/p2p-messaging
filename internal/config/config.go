package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Runtime configuration for a node.
type NodeConfig struct {
	NodeID         string
	ListenAddr     string   // e.g. ":9000" - the gRPC address this node binds to
	AdvertiseAddr  string   // e.g. "node1:9000" - what peers use to dial this node
	BootstrapPeers []string // addresses of seed nodes, empty if first in network
	DBPath         string   // SQLite file path, ":memory:" for in-memory
	ReplicaCount   int

	CACertPath   string
	NodeCertPath string
	NodeKeyPath  string
}


// TLSConfig builds a mutual-TLS configuration for inter-node communication.
//
// All three paths are PEM files:
//   - caCert:   the shared CA certificate (all nodes must share this CA)
//   - nodeCert: this node's certificate (signed by the CA)
//   - nodeKey:  this node's private key
//
// Any node presenting a certificate not signed by the shared CA will be
// rejected at the TLS handshake.
func TLSConfig(caCertPath, nodeCertPath, nodeKeyPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(nodeCertPath, nodeKeyPath)
	if err != nil {
		return nil, fmt.Errorf("tls: load node cert/key: %w", err)
	}

	caPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("tls: read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("tls: failed to parse CA cert")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}, nil
}
