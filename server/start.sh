#!/bin/sh
# Container start: TLS certificate, nginx in front of Quack, then the DuckDB server.
set -eu

mkdir -p /tmp/tls
if [ -f /config/tls/cert.pem ] && [ -f /config/tls/key.pem ]; then
    cp /config/tls/cert.pem /config/tls/key.pem /tmp/tls/
else
    # No certificate mounted: make a self-signed one for this container's lifetime.
    openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=${TLS_HOSTNAME:-localhost}" \
        -addext "subjectAltName=DNS:${TLS_HOSTNAME:-localhost},DNS:localhost,IP:127.0.0.1" \
        -keyout /tmp/tls/key.pem -out /tmp/tls/cert.pem 2>/dev/null
    echo "[rdf-server] self-signed TLS certificate; mount /config/tls/{cert,key}.pem for a real one"
    printf '[rdf-server] certificate SHA-256 fingerprint: '
    openssl x509 -in /tmp/tls/cert.pem -noout -fingerprint -sha256 | cut -d= -f2
fi

nginx -c /opt/rdf-server/nginx.conf
exec python3 -u /opt/rdf-server/entrypoint.py
