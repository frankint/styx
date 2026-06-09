#!/bin/bash
set -euo pipefail

scale_factor=$1
epoch_size=$2
threads_per_worker=$3
enable_compression=$4
use_composite_keys=$5
enable_autoscale=${6:-true}
minimum_amount_of_workers=1

echo "============== Starting Styx Cluster ================"
echo "scale_factor: $scale_factor"
echo "epoch_size: $epoch_size"
echo "threads_per_worker: $threads_per_worker"
echo "minimum_amount_of_workers: $minimum_amount_of_workers"
echo "enable_compression: $enable_compression"
echo "use_composite_keys: $use_composite_keys"
echo "enable_autoscale: $enable_autoscale"

threaded_scale_factor=$(( (scale_factor + threads_per_worker - 1) / threads_per_worker ))
# Override worker count if INITIAL_WORKERS is set (used for autoscaling experiments)
if [[ -n "${INITIAL_WORKERS:-}" && "${INITIAL_WORKERS}" -gt 0 ]]; then
    echo "OVERWRITE: using INITIAL_WORKERS=${INITIAL_WORKERS} instead of computed ${threaded_scale_factor}"
    threaded_scale_factor=${INITIAL_WORKERS}
fi
# Enforce minimum number of active workers
(( threaded_scale_factor < minimum_amount_of_workers )) && threaded_scale_factor=$minimum_amount_of_workers
echo "threaded_scale_factor: $threaded_scale_factor"
echo "====================================================="

#docker system prune -f --volumes >/dev/null 

# START NEW DEPLOYMENT
docker compose -f docker-compose-kafka.yml up -d >/dev/null
sleep 10
docker compose -f docker-compose-s3.yml up -d >/dev/null
sleep 10
export STYX_WORKER_THREADS="$threads_per_worker"
export ENABLE_AUTOSCALE="$enable_autoscale"
# Enable BuildKit for cache mount support
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
docker compose build \
    --build-arg epoch_size="$epoch_size" \
    --build-arg worker_threads="$threads_per_worker" \
    --build-arg enable_compression="$enable_compression" \
    --build-arg use_composite_keys="$use_composite_keys"
if [[ "$enable_autoscale" == "true" ]]; then
    docker compose build worker-standby \
    --build-arg epoch_size="$epoch_size" \
    --build-arg worker_threads="$threads_per_worker" \
    --build-arg enable_compression="$enable_compression" \
    --build-arg use_composite_keys="$use_composite_keys"
fi
docker compose up --scale worker="$threaded_scale_factor" -d >/dev/null
sleep 5