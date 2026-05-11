#!/usr/bin/env bash
set -euo pipefail

RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
TIMEOUT=${TIMEOUT:-600}
COORD_DEPLOY="${RELEASE_NAME}-styx-coordinator"
COORD_PORT="${COORDINATOR_PORT:-8888}"
# Extra wait after pod Ready until the process binds discovery
COORD_TCP_TIMEOUT=${COORD_TCP_TIMEOUT:-300}

echo "Waiting for all pods to be Ready (release=$RELEASE_NAME, ns=$NAMESPACE, timeout=${TIMEOUT}s)..."
kubectl wait --for=condition=Ready pod --all \
  -n "${NAMESPACE}" --timeout="${TIMEOUT}s"

echo "Waiting for coordinator (${COORD_DEPLOY}) to accept TCP on port ${COORD_PORT} (timeout ${COORD_TCP_TIMEOUT}s)..."
coord_deadline=$(( $(date +%s) + COORD_TCP_TIMEOUT ))
while true; do
  if kubectl exec -n "${NAMESPACE}" "deploy/${COORD_DEPLOY}" -c coordinator -- \
    python -c "import socket;s=socket.socket(socket.AF_INET,socket.SOCK_STREAM);s.settimeout(2);s.connect(('127.0.0.1',${COORD_PORT}));s.close()"; then
    echo "Coordinator is listening on port ${COORD_PORT}."
    break
  fi
  if [ "$(date +%s)" -ge "$coord_deadline" ]; then
    echo "ERROR: timed out after ${COORD_TCP_TIMEOUT}s waiting for coordinator TCP port ${COORD_PORT}" >&2
    exit 1
  fi
  sleep 2
done

echo "Styx cluster is Ready."
