from __future__ import annotations

import logging
import multiprocessing
import time
from typing import Any

from chronos import BaseChronosPipeline
import pandas as pd
import torch

MIN_CONTEXT_LENGTH: int = 40
CHRONOS_MODEL_PATH: str = "models/chronos-bolt"
CHRONOS_NUM_THREADS: int = 2

log = logging.getLogger("chronos_forecaster")


def _forecaster_loop(
    request_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
) -> None:
    """Entry point for the forecaster child process."""
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [CHRONOS] %(message)s")

    # Configure PyTorch threading for optimal CPU inference
    torch.set_num_threads(CHRONOS_NUM_THREADS)
    torch.set_num_interop_threads(2)
    log.warning("PyTorch configured with %d threads", CHRONOS_NUM_THREADS)

    log.warning("Loading Chronos model")
    load_start = time.time()
    try:
        pipeline = BaseChronosPipeline.from_pretrained(
            CHRONOS_MODEL_PATH,
            device_map="cpu",
        )
    except Exception:
        log.exception("Failed to load Chronos model")
        return
    log.warning("Chronos model loaded in %.2fs", time.time() - load_start)

    while True:
        req: dict[str, Any] | None = request_queue.get()
        if req is None:
            log.warning("Shutdown received, exiting forecaster process")
            return

        try:
            start_time = time.time()
            context_map: dict[str, list[float]] = req["context"]
            prediction_length: int = req.get("prediction_length", 10)
            series = context_map["input_rate"]
            if len(series) < MIN_CONTEXT_LENGTH:
                log.warning("CHRONOS | Not enough data to forecast")
                result_queue.put(
                    {
                        "predictions": {
                            "0.75": [0.0] * prediction_length,
                        },
                    }
                )
                continue
            regular_ts = pd.date_range("2024-01-01", periods=len(series), freq="1s")

            context_df = pd.DataFrame(
                {
                    "item_id": ["styx"] * len(series),
                    "timestamp": regular_ts,
                    "target": series,
                }
            )

            predictions_df = pipeline.predict_df(
                context_df,
                prediction_length=prediction_length,
                quantile_levels=[0.75],
            )

            result_queue.put(
                {
                    "predictions": {
                        "0.75": predictions_df["0.75"].tolist(),
                    },
                }
            )
            log.warning("CHRONOS | forecast completed in %.2fs", time.time() - start_time)
        except Exception as e:
            log.exception("Forecast error")
            result_queue.put({"error": str(e)})


# Coordinator-side handle for the background forecaster process.
class ChronosForecaster:
    """Runs in a separate process so that PyTorch inference does not block the
    coordinator's asyncio event loop.  Communicates via two ``multiprocessing.Queue`` objects
    """

    def __init__(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        self._request_queue: multiprocessing.Queue = ctx.Queue(maxsize=2)
        self._result_queue: multiprocessing.Queue = ctx.Queue(maxsize=2)
        self._process: multiprocessing.Process | None = None
        self.latest_predictions: dict[str, dict[str, list[float]]] | None = None

    def start(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        self._process = ctx.Process(
            target=_forecaster_loop,
            args=(self._request_queue, self._result_queue),
            daemon=True,
        )
        self._process.start()
        log.warning("Chronos forecaster process started (pid=%s)", self._process.pid)

    def submit(
        self,
        context: dict[str, list[float]],
        prediction_length: int = 10,
    ) -> bool:
        """Enqueue a forecast request.  Returns False if the queue is full
        (a previous request is still being processed)."""
        try:
            self._request_queue.put_nowait(
                {
                    "context": context,
                    "prediction_length": prediction_length,
                }
            )
        except Exception:
            return False
        else:
            return True

    def poll(self) -> dict[str, dict[str, list[float]]] | None:
        """Blocking poll for completed forecasts.

        Returns the predictions dict or None.  Internally caches the
        latest successful result in ``self.latest_predictions``.
        """
        result = None
        # Drain the queue to get the most recent result
        while not self._result_queue.empty():
            try:
                result = self._result_queue.get()
            except Exception:
                break

        if result is None:
            return self.latest_predictions

        if "error" in result:
            log.warning("Chronos forecast error: %s", result["error"])
            return self.latest_predictions

        self.latest_predictions = result.get("predictions")
        return self.latest_predictions

    def stop(self) -> None:
        if self._process and self._process.is_alive():
            self._request_queue.put(None)
            self._process.join(timeout=5)
            if self._process.is_alive():
                self._process.kill()

    @property
    def is_alive(self) -> bool:
        return self._process is not None and self._process.is_alive()
