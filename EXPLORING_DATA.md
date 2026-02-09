## Usage
To explore all recorded experiments, its only necessary to start the monitoring containers:
```bash
docker compose up grafana prometheus

To quickly inspect the data and automatically open the Grafana dashboard with the correct time range, use the `open_grafana_range.py` script:
```bash
python scripts/open_grafana_range.py dhr
```
The script also supports more fine-grained filtering. For example: 
- filtering by both workload AND TPS, e.g. to get all 'dhr' runs with 10k TPS: `python scripts/open_grafana_range.py dhr 10000tps` 
- filtering for experiements ran with a certain number of partitions: `python scripts/open_grafana_range.py 8part` 

After running the script, just select the index of the experiment and a browser window will open with the Grafana dashboard preloaded to the experiment’s time range. All the time-series data is stored in the `prometheus-data` directory. 

*Sidenote*: in some experiments with a lot of backpressure, the script sometimes records the end timestamp slightly too early. This can cause the Grafana view to stop before backpressure fully returns to zero. If the dashboard seems truncated, simply extend the “to” timestamp in Grafana by 10–20 seconds to show the full experiment duration.

```
From here, it is also possible to just copy the `start` and `end` timestamps directly into Grafana to display the correct time window.

## Running extra experiments
Running command experiments was not changed:
`./scripts/run_experiment.sh [WORKLOAD_NAME] [INPUT_RATE] [N_KEYS] [N_PART] [ZIPF_CONST] [CLIENT_THREADS] [TOTAL_TIME] [SAVING_DIR] [WARMUP_SECONDS] [EPOCH_SIZE]`
example: (`./scripts/run_experiment.sh ycsbt 5000 100000 4 0.0 1 180 results 10 4000`)
The only difference is after the workload finished, only the workers and coordinators containers are stopped, *the prometheus and grafana container will keep running in order to easily observe the recently captured metrics*. Thus, to fully shut doen the cluster it is needed to manually run: `./scripts/stop_styx_cluster.sh`.