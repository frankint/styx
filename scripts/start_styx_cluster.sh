#!/bin/bash
set -euo pipefail

scale_factor=$1
epoch_size=$2
# Infer the number of threads from the docker-compose.yml (WORKER_THREADS=...),
#threads_per_worker="$(
#  sed -nE 's/.*WORKER_THREADS=([0-9]+).*/\1/p' docker-compose.yml | head -n 1
#)"
max_operator_parallelism=$3
threads_per_worker=$4
enable_compression=$5
use_composite_keys=$6
use_fallback_cache=$7
enable_autoscale=${8:-true}
minimum_amount_of_workers=1

echo "============== Starting Styx Cluster ================"
echo "scale_factor: $scale_factor"
echo "epoch_size: $epoch_size"
echo "max_operator_parallelism: $max_operator_parallelism"
echo "threads_per_worker: $threads_per_worker"
echo "minimum_amount_of_workers: $minimum_amount_of_workers"
echo "enable_compression: $enable_compression"
echo "use_composite_keys: $use_composite_keys"
echo "use_fallback_cache: $use_fallback_cache"
echo "enable_autoscale: $enable_autoscale"
# Ceiling division
threaded_scale_factor=$(( (scale_factor + threads_per_worker - 1) / threads_per_worker ))
# Enforce minimum
(( threaded_scale_factor < minimum_amount_of_workers )) && threaded_scale_factor=$minimum_amount_of_workers
echo "threaded_scale_factor: $threaded_scale_factor"
echo "====================================================="

docker system prune -f --volumes >/dev/null
# START NEW DEPLOYMENT
docker compose -f docker-compose-kafka.yml up -d >/dev/null
sleep 5
docker compose -f docker-compose-minio.yml up -d >/dev/null
sleep 10
export STYX_WORKER_THREADS="$threads_per_worker"
export ENABLE_AUTOSCALE="$enable_autoscale"
# Enable BuildKit for cache mount support
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
docker compose build \
    --build-arg epoch_size="$epoch_size" \
    --build-arg max_operator_parallelism="$max_operator_parallelism" \
    --build-arg worker_threads="$threads_per_worker" \
    --build-arg enable_compression="$enable_compression" \
    --build-arg use_composite_keys="$use_composite_keys" \
    --build-arg use_fallback_cache="$use_fallback_cache"
docker compose up --scale worker="$threaded_scale_factor" -d >/dev/null
sleep 5