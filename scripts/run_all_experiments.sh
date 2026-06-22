#!/bin/bash

# Make sure the script stops if any command fails
# set -e

# Define the two experiment commands
CMD_AUTOSCALE="./scripts/run_autoscale_experiment.sh dhr 5000 1000000 1 0.0 1 740 results 10 400 alibaba 6"
CMD_NO_AUTOSCALE="./scripts/run_autoscale_experiment.sh dhr 5000 1000000 7 0.0 1 740 results 10 400 alibaba 0"

# Helper function to stop the cluster and wait a few seconds before the next run
cleanup() {
    echo "Stopping Styx cluster..."
    ./scripts/stop_styx_cluster.sh
    sleep 5
}

echo "======================================================"
echo " PHASE 1: AUTOSCALING ENABLED (Different Models)"
echo "======================================================"

export ENABLE_AUTOSCALE=true
export INITIAL_WORKERS=1

echo ">>> 1. Running CHRONOS"
export FORECASTER_TYPE=custom_chronos
export FORECASTER_MAX_CONTEXT_LENGTH=1000
$CMD_AUTOSCALE
cleanup

echo ">>> 2. Running LSTM"
export FORECASTER_TYPE=lstm
export FORECASTER_MAX_CONTEXT_LENGTH=300
export RNN_HIDDEN_SIZE=32
export RNN_NUM_LAYERS=1
export RNN_LEARNING_RATE=0.01
export DOWNSAMPLE_RATE=2
$CMD_AUTOSCALE
cleanup

echo ">>> 3. Running GRU"
export FORECASTER_TYPE=gru
export FORECASTER_MAX_CONTEXT_LENGTH=600
export RNN_HIDDEN_SIZE=16
export RNN_NUM_LAYERS=2
export RNN_LEARNING_RATE=0.01
export DOWNSAMPLE_RATE=5
$CMD_AUTOSCALE
cleanup

echo ">>> 4. Running RIVER"
export FORECASTER_TYPE=river
unset FORECASTER_MAX_CONTEXT_LENGTH  # River doesn't use context length
unset RNN_HIDDEN_SIZE RNN_NUM_LAYERS RNN_LEARNING_RATE DOWNSAMPLE_RATE
export RIVER_P=5
export RIVER_D=1
export RIVER_Q=2
export RIVER_M=30
$CMD_AUTOSCALE
cleanup
unset RIVER_P RIVER_D RIVER_Q RIVER_M


echo "======================================================"
echo " PHASE 2: NO AUTOSCALING "
echo "======================================================"

export ENABLE_AUTOSCALE=false
unset FORECASTER_TYPE

# Loop through the different static worker counts (7, 5, 4)
for workers in 7 5 4; do
    echo ">>> Running NO AUTOSCALING with INITIAL_WORKERS=$workers"
    export INITIAL_WORKERS=$workers
    $CMD_NO_AUTOSCALE
    cleanup
done

echo "All experiments have been successfully completed!"