from __future__ import annotations

import logging
import multiprocessing
import time
from typing import Any

import numpy as np
import pandas as pd
from chronos import Chronos2Pipeline

MIN_CONTEXT_LENGTH: int = 40
log = logging.getLogger("custom_chronos_forecaster")


def _forecaster_loop(
    request_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    model_path: str,
    downsample_rate: int,
    max_context_length: int | None,
) -> None:
    """Entry point for the custom chronos forecaster child process."""
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [CUSTOM_CHRONOS] %(message)s")

    log.warning("Loading custom Chronos model: %s", model_path)
    load_start = time.time()
    try:
        pipeline = Chronos2Pipeline.from_pretrained(
            model_path,
            device_map="cpu",
        )
    except Exception:
        log.exception("Failed to load custom Chronos model")
        return
    log.warning("Custom Chronos model loaded in %.2fs", time.time() - load_start)

    while True:
        req: dict[str, Any] | None = request_queue.get()
        if req is None:
            log.warning("Shutdown received, exiting custom chronos process")
            return

        try:
            start_time = time.time()
            context_map: dict[str, list[float]] = req["context"]
            prediction_length: int = req.get("prediction_length", 10)
            series = context_map["input_rate"]

            # Apply custom max context length truncation
            if max_context_length is not None and len(series) > max_context_length:
                series = series[-max_context_length:]

            if len(series) < max(MIN_CONTEXT_LENGTH, 5 * downsample_rate):
                log.warning("CUSTOM_CHRONOS | Not enough data to forecast")
                fallback = [0.0] * prediction_length
                result_queue.put(
                    {
                        "predictions": {
                            "0.75": fallback,
                            "0.9": fallback,
                        },
                    }
                )
                continue

            # 1. Downsample history (matching eval-models.py)
            trim_offset = len(series) % downsample_rate
            trimmed_history = series[trim_offset:]
            downsampled_history = [
                float(np.mean(trimmed_history[i : i + downsample_rate]))
                for i in range(0, len(trimmed_history), downsample_rate)
            ]
            downsampled_horizon = max(1, (prediction_length + downsample_rate - 1) // downsample_rate)

            # 2. Build context and future DataFrames
            base_time = pd.Timestamp("2024-01-01 00:00:00")
            timestamps = [base_time + pd.Timedelta(seconds=i) for i in range(len(downsampled_history))]
            context_df = pd.DataFrame(
                {
                    "id": ["styx"] * len(downsampled_history),
                    "timestamp": timestamps,
                    "target": downsampled_history,
                }
            )

            future_timestamps = [
                timestamps[-1] + pd.Timedelta(seconds=i + 1) for i in range(downsampled_horizon)
            ]
            future_df = pd.DataFrame({"id": ["styx"] * downsampled_horizon, "timestamp": future_timestamps})

            # 3. Predict df with explicit timestamps and column mappings
            predictions_df = pipeline.predict_df(
                context_df,
                future_df=future_df,
                prediction_length=downsampled_horizon,
                quantile_levels=[0.75, 0.9],
                id_column="id",
                timestamp_column="timestamp",
                target="target",
            )

            # 4. Expand predictions back to match original horizon
            raw_75 = predictions_df["0.75"].tolist()
            raw_90 = predictions_df["0.9"].tolist()

            expanded_75 = []
            expanded_90 = []
            for p75, p90 in zip(raw_75, raw_90):
                expanded_75.extend([p75] * downsample_rate)
                expanded_90.extend([p90] * downsample_rate)

            result_queue.put(
                {
                    "predictions": {
                        "0.75": expanded_75[:prediction_length],
                        "0.9": expanded_90[:prediction_length],
                    },
                }
            )
            log.warning("CUSTOM_CHRONOS | forecast completed in %.2fs", time.time() - start_time)
        except Exception as e:
            log.exception("Forecast error")
            result_queue.put({"error": str(e)})


class CustomChronosForecaster:
    """Wrapper for Custom Chronos using downsampling and explicit future dataframes."""

    def __init__(
        self,
        model_path: str = "models/chronos-2",
        downsample_rate: int = 1,
        max_context_length: int | None = None,
    ) -> None:
        self.model_path = model_path
        self.downsample_rate = downsample_rate
        self.max_context_length = max_context_length

        ctx = multiprocessing.get_context("spawn")
        self._request_queue: multiprocessing.Queue = ctx.Queue(maxsize=2)
        self._result_queue: multiprocessing.Queue = ctx.Queue(maxsize=2)
        self._process: multiprocessing.Process | None = None
        self.latest_predictions: dict[str, list[float]] | None = None

    def start(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        self._process = ctx.Process(
            target=_forecaster_loop,
            args=(
                self._request_queue,
                self._result_queue,
                self.model_path,
                self.downsample_rate,
                self.max_context_length,
            ),
            daemon=True,
        )
        self._process.start()
        log.warning("Custom Chronos forecaster process started (pid=%s)", self._process.pid)

    def submit(
        self,
        context: dict[str, list[float]],
        prediction_length: int = 10,
    ) -> bool:
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

    def poll(self) -> dict[str, list[float]] | None:
        result = None
        while not self._result_queue.empty():
            try:
                result = self._result_queue.get()
            except Exception:
                break

        if result is None:
            return self.latest_predictions

        if "error" in result:
            log.warning("Custom Chronos forecast error: %s", result["error"])
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
