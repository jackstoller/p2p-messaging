#!/bin/bash
# Generates a private CA and per-node certificates for mTLS.
# Run once per environment. Distribute ca.crt to all nodes.
# Each node gets its own node.crt + node.key pair.
#
# Usage:
#   ./gen.sh                    # generate CA + single node cert
#   ./gen.sh node1 node2 node3  # generate CA + one cert per named node

set -euo pipefail

DAYS=3650  # 10 years- fine for a dev/demo environment
DIR="$(cd "$(dirname "$0")" && pwd)"

# CA generation
if [ ! -f "$DIR/ca.key" ]; then
    echo "Generating CA key and certificate..."
    openssl genrsa -out "$DIR/ca.key" 4096

    openssl req -new -x509 \
        -key "$DIR/ca.key" \
        -out "$DIR/ca.crt" \
        -days $DAYS \
        -subj "/CN=mesh-ca/O=mesh"

    echo "  CA cert: $DIR/ca.crt"
else
    echo "CA already exists, skipping CA generation."
fi

# Node certs generation
NODES=("${@:-node}")

for NODE in "${NODES[@]}"; do
    echo "Generating certificate for node: $NODE"

    KEY="$DIR/${NODE}.key"
    CSR="$DIR/${NODE}.csr"
    CRT="$DIR/${NODE}.crt"

    openssl genrsa -out "$KEY" 2048

    openssl req -new \
        -key "$KEY" \
        -out "$CSR" \
        -subj "/CN=${NODE}/O=mesh"

    # Sign with the CA, adding SAN so Go's TLS verifier is happy.
    openssl x509 -req \
        -in "$CSR" \
        -CA "$DIR/ca.crt" \
        -CAkey "$DIR/ca.key" \
        -CAcreateserial \
        -out "$CRT" \
        -days $DAYS \
        -extfile <(printf "subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1" "$NODE")

    rm "$CSR"
    echo "  Key:  $KEY"
    echo "  Cert: $CRT"
done

echo ""
echo "✔ Key Generation Completed"
echo ""
