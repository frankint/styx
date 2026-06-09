#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
DELETE_NAMESPACE=${DELETE_NAMESPACE:-true}
COORDINATOR_IMAGE=${COORDINATOR_IMAGE:-styx-coordinator}
WORKER_IMAGE=${WORKER_IMAGE:-styx-worker}
TAG=${TAG:-dev}
DEPLOY_MODE=${DEPLOY_MODE:-k8s-minikube}   # k8s-minikube | k8s-cluster

echo "Uninstalling Helm release '$RELEASE_NAME' from namespace '$NAMESPACE'..."

# EXPORT LOGS 
EXPORT_LOGS=${EXPORT_LOGS:-true}
if [[ "${EXPORT_LOGS}" == "true" || "${EXPORT_LOGS}" == "1" ]]; then
  echo "Exporting coordinator/worker logs (EXPORT_LOGS=${EXPORT_LOGS}; set EXPORT_LOGS=false to skip)..."
  "$ROOT_DIR/scripts/export_k8s_styx_logs.sh" || echo "WARN: log export failed (pods may already be gone)." >&2
fi

helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || true

if [[ "$DELETE_NAMESPACE" == "true" ]]; then
  echo "Deleting namespace '$NAMESPACE'..."
  kubectl delete namespace "$NAMESPACE" || true
fi

if [[ "$DEPLOY_MODE" == "k8s-minikube" ]]; then
  echo "Removing images from minikube..."
  #minikube image rm "${COORDINATOR_IMAGE}:${TAG}"
  #minikube image rm "${WORKER_IMAGE}:${TAG}"
fi

echo "Done."
