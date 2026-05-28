from __future__ import annotations

import logging
import multiprocessing
import time
from typing import Any

from river import time_series

log = logging.getLogger("river_forecaster")


def _forecaster_loop(
    request_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    p: int,
    d: int,
    q: int,
    m: int,
) -> None:
    """Entry point for the River forecaster child process."""
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [RIVER_FORECASTER] %(message)s")

    log.warning("Initializing River SNARIMAX model (p=%d, d=%d, q=%d, m=%d)", p, d, q, m)
    model = time_series.SNARIMAX(p=p, d=d, q=q, m=m)
    processed_count = 0

    while True:
        req: dict[str, Any] | None = request_queue.get()
        if req is None:
            log.warning("Shutdown received, exiting River forecaster process")
            return

        try:
            start_time = time.time()
            context_map: dict[str, list[float]] = req["context"]
            prediction_length: int = req.get("prediction_length", 10)
            series = context_map["input_rate"]

            # Update SNARIMAX incrementally with any new points
            if len(series) > processed_count:
                new_points = series[processed_count:]
                for val in new_points:
                    model.learn_one(val)
                processed_count = len(series)

            # Generate forecast
            raw_forecasts = model.forecast(horizon=prediction_length)
            preds_list = [float(f) for f in raw_forecasts]

            result_queue.put({"predictions": {"truth": preds_list}})
            log.warning("RIVER | forecast completed in %.2fs", time.time() - start_time)
        except Exception as e:
            log.exception("Forecast error")
            result_queue.put({"error": str(e)})


class RiverForecaster:
    """Wrapper for River SNARIMAX model running in a subprocess."""

    def __init__(
        self,
        p: int = 1,
        d: int = 0,
        q: int = 0,
        m: int = 30,
    ) -> None:
        self.p = p
        self.d = d
        self.q = q
        self.m = m

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
                self.p,
                self.d,
                self.q,
                self.m,
            ),
            daemon=True,
        )
        self._process.start()
        log.warning("River forecaster process started (pid=%s)", self._process.pid)

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
            log.warning("River forecast error: %s", result["error"])
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
