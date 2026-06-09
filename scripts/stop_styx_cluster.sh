#!/bin/bash

threads_per_worker=$1

TS=$(date +"%Y%m%d-%H%M%S")

export STYX_WORKER_THREADS="$threads_per_worker"
dir_location="logs"
mkdir -p "$dir_location"

# One file per scaled worker replica (compose logs worker merges all replicas).
for cid in $(docker compose ps -a -q worker 2>/dev/null); do
  cname=$(docker inspect --format '{{.Name}}' "$cid" | sed 's|^/||')
  docker logs "$cid" 2>&1 | grep -v '|[[:space:]]*$' > "${dir_location}/${cname}-logs-${TS}.log"
done
docker compose logs coordinator > "${dir_location}/coordinator-logs-${TS}.log"
for cid in $(docker compose ps -a -q worker-standby 2>/dev/null); do
  cname=$(docker inspect --format '{{.Name}}' "$cid" | sed 's|^/||')
  docker logs "$cid" 2>&1 | grep -v '|[[:space:]]*$' > "${dir_location}/${cname}-logs-${TS}.log"
done

# DELETE PREVIOUS DEPLOYMENT
docker compose down --volumes --remove-orphans
docker compose -f docker-compose-kafka.yml down --volumes --remove-orphans
docker compose -f docker-compose-s3.yml down --volumes --remove-orphans
