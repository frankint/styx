#!/bin/bash
set -euo pipefail

# Finds one container in the worker-standby service and triggers it to start
# `worker/boot_worker.py` by creating the start file inside the container.

container_id="$(
  docker compose --profile standby ps -q worker-standby | head -n 1
)"

if [[ -z "${container_id}" ]]; then
  echo "No worker-standby containers found. Start some first:" >&2
  echo "  scripts/scale_standby_workers.sh 1" >&2
  exit 1
fi

docker exec "${container_id}" sh -lc 'touch "${STYX_START_FILE:-/tmp/styx_start}"'
echo "Activated standby worker container: ${container_id}"

