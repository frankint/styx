#!/bin/sh
set -eu

STYX_START_FILE="${STYX_START_FILE:-/tmp/styx_start}"
STYX_START_POLL_MS="${STYX_START_POLL_MS:-200}"

echo "Waiting for start file: ${STYX_START_FILE}"
while [ ! -f "$STYX_START_FILE" ]; do
  sleep "$(awk "BEGIN {print ${STYX_START_POLL_MS}/1000}")"
done
echo "Start file found; starting worker."

python worker/boot_worker.py
