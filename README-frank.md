## Preliminaries

This project requires an environment with *Python 3.14* installed. 
Please install the styx-package and all the requirements of the coordinator
and the worker modules as well as pandas, numpy and matplotlib. 

You can use the following commands:

```
pip install styx-package/.  
pip install -r requirements.txt
pip install river==0.24.2 --no-deps
```

Downloading the Chronos model:
```
mkdir -p models/chronos-2
pip install -U "huggingface_hub[cli]"
hf download amazon/chronos-2 --local-dir models/chronos-2
```

## Offline Analysis

The first step is to run all the simulations. `./scripts/eval-models.py` contains a param grid where all the parameters to be explored can be filled in. While this configuration matches the paper's original results, running it can be time-consuming. To speed things up, you can prune the `PARAM_GRID` by removing certain context lengths (e.g., 10, 50, or 100). Run this script using the following command:
```
python scripts/eval-models.py
``` 
To analyse the results and get the actual plots, run the following command:
```
python scripts/offline-analysis.py
```
The plots will appear in the directory `analysis_plots/` (the plots shown in the paper are named `00_global_best_comparison.png`, `02_context_time_evolution.png`, and `03_chronos_rank_01_overlay.png`)

## Styx experiments

To run all the experiments run the following command (this takes approximately 90 minutes):
```
./scripts/run_all_experiments.sh
```
It is also possible to manually run a single experiment by copying all the exports from `run_all_experiments.sh`, then running `./scripts/run_autoscale_experiment.sh dhr 5000 1000000 1 0.0 1 740 results 10 400 alibaba 6`(autoscaling) or `./scripts/run_autoscale_experiment.sh dhr 5000 1000000 7 0.0 1 740 results 10 400 alibaba 0`(no autoscaling) and after it finishes, running `./scripts/stop_styx_cluster.sh`. For just the Chronos autoscaling experiment that would look like the following:
```
export ENABLE_AUTOSCALE=true
export INITIAL_WORKERS=1
export FORECASTER_TYPE=custom_chronos
export FORECASTER_MAX_CONTEXT_LENGTH=1000
./scripts/run_autoscale_experiment.sh dhr 5000 1000000 1 0.0 1 740 results 10 400 alibaba 6
./scripts/stop_styx_cluster.sh
```
At this point all the csv's of the run are stored in `results/`. To see the graphs used in the paper run the following command:
```
python scripts/plot-experiment-data.py
```
The resulting plots will be stored in the current directory (If you ran `run_all_experiments.sh` and no other experiments were stored there the first will be Chronos, the second LSTM, the third will be GRU, the fourth River and the last three will be 7, 5, and 4 workers).


Note: the title will most likely say unknow model, this is because I manually added the names `_custom_chronos`, `_lstm`, `_gru`, `_river` and `_xworkers`(where x is the number of workers) respectively to the end of the directory name for each run in `./results/` which the `plot-experiment-data.py` would recognize. 
