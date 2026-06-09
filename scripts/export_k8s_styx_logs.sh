#!/usr/bin/env bash
# Dump Styx coordinator and worker pod logs to ./logs (same layout idea as stop_styx_cluster.sh).
# Run while pods still exist (before helm uninstall / namespace delete).
#
# Usage:
#   ./scripts/export_k8s_styx_logs.sh
#   LOG_DIR=/tmp/styx-logs NAMESPACE=styx RELEASE_NAME=styx-cluster ./scripts/export_k8s_styx_logs.sh
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
LOG_DIR=${LOG_DIR:-"$ROOT_DIR/logs"}
TS=$(date +"%Y%m%d-%H%M%S")

mkdir -p "$LOG_DIR"

coordinator_pods=$(
  kubectl get pods -n "$NAMESPACE" \
    -l "app.kubernetes.io/instance=${RELEASE_NAME},app.kubernetes.io/component=coordinator" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
)

worker_pods=$(
  kubectl get pods -n "$NAMESPACE" \
    -l "app.kubernetes.io/instance=${RELEASE_NAME},app.kubernetes.io/component=worker" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
)

if [[ -z "${coordinator_pods//[$'\t\r\n ']/}" && -z "${worker_pods//[$'\t\r\n ']/}" ]]; then
  echo "WARN: No coordinator/worker pods found in ns=$NAMESPACE (release=$RELEASE_NAME). Nothing exported." >&2
  exit 0
fi

if [[ -n "${coordinator_pods//[$'\t\r\n ']/}" ]]; then
  out_coord="${LOG_DIR}/coordinator-logs-${TS}.log"
  : >"$out_coord"
  while read -r pod; do
    [[ -z "$pod" ]] && continue
    {
      echo "======== pod: $pod ========"
      kubectl logs -n "$NAMESPACE" "$pod" -c coordinator 2>&1 || echo "(kubectl logs failed for $pod)"
      echo
    } >>"$out_coord"
  done <<<"$coordinator_pods"
  echo "Wrote $out_coord"
fi

while read -r pod; do
  [[ -z "$pod" ]] && continue
  out="${LOG_DIR}/${pod}-logs-${TS}.log"
  kubectl logs -n "$NAMESPACE" "$pod" -c worker 2>&1 | grep -v '|[[:space:]]*$' >"$out" || true
  echo "Wrote $out"
done <<<"$worker_pods"
