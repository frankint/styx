#!/bin/bash
set -euo pipefail

workload_name=$1
input_rate=$2
n_keys=$3
n_part=$4
zipf_const=$5
client_threads=$6
total_time=$7
saving_dir=$8
warmup_seconds=$9
epoch_size=${10}
styx_threads_per_worker=${11}
enable_compression=${12}
use_composite_keys=${13}
use_fallback_cache=${14}
regenerate_tpcc_data=${15:-false}
manual_scale_sec=${16:-30}

echo "============= Running Experiment ================="
echo "workload_name: $workload_name"
echo "input_rate: $input_rate"
echo "n_keys: $n_keys"
echo "n_part: $n_part"
echo "zipf_const: $zipf_const"
echo "client_threads: $client_threads"
echo "total_time: $total_time"
echo "saving_dir: $saving_dir"
echo "warmup_seconds: $warmup_seconds"
echo "epoch_size: $epoch_size"
echo "styx_threads_per_worker: $styx_threads_per_worker"
echo "enable_compression: $enable_compression"
echo "use_composite_keys: $use_composite_keys"
echo "use_fallback_cache: $use_fallback_cache"
echo "regenerate_tpcc_data: $regenerate_tpcc_data"
echo "manual_scale_sec: $manual_scale_sec"
echo "=================================================="

bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size" "$(($n_part + 1))" "$styx_threads_per_worker" "$enable_compression" "$use_composite_keys" "$use_fallback_cache"

sleep 10

if [[ $workload_name == "scale_test" ]]; then
    # YCSB-T
    run_with_validation=false
    docker compose up --scale worker-standby=1 -d worker-standby >/dev/null
    (sleep $((manual_scale_sec - 3)) && echo "Activating standby worker" && exec scripts/activate_standby_worker.sh) & 
    python demo/demo-ycsb/scale_client.py "$client_threads" "$n_keys" "$n_part" "$zipf_const" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$run_with_validation" "$epoch_size" "$manual_scale_sec"
else
    echo "Benchmark not supported!"
fi



#bash scripts/stop_styx_cluster.sh "$styx_threads_per_worker"
#docker compose stop coordinator worker worker-standby
docker compose stop coordinator