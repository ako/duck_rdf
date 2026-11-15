#!/usr/bin/env bash
# Build the image, start it on a fixture, and query it from a plain DuckDB client.
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE=${IMAGE:-duck-rdf-server:dev}
TOKEN=smoke-test-token
PORT=${PORT:-19443}

python3 test/make_data.py test/fixture
docker build ${DOCKER_BUILD_ARGS:-} -t "$IMAGE" .
docker rm -f rdf-smoke >/dev/null 2>&1 || true
docker run -d --name rdf-smoke -e QUACK_TOKEN=$TOKEN -p 127.0.0.1:$PORT:9443 \
    -v "$PWD/test/fixture/data:/data:ro" -v "$PWD/test/fixture/config:/config:ro" "$IMAGE" >/dev/null
trap 'docker logs rdf-smoke 2>&1 | tail -40; docker rm -f rdf-smoke >/dev/null' EXIT
for _ in $(seq 1 60); do docker logs rdf-smoke 2>&1 | grep -q "serving Quack" && break; sleep 1; done
FP=$(docker logs rdf-smoke 2>&1 | sed -n 's/.*fingerprint: //p' | head -1)
python3 test/client.py "127.0.0.1:$PORT" "$TOKEN" "$FP"
