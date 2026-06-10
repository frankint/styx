from __future__ import annotations

import logging
import multiprocessing
import time
from typing import Any

import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler

log = logging.getLogger("rnn_forecaster")


class TimeSeriesRNN(nn.Module):
    def __init__(self, rnn_type: str = "LSTM", input_size: int = 1, hidden_size: int = 32, num_layers: int = 1):
        super().__init__()
        self.rnn = (nn.LSTM if rnn_type == "LSTM" else nn.GRU)(
            input_size, hidden_size, num_layers, batch_first=True
        )
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.rnn(x)
        return self.fc(out[:, -1, :])


def _forecaster_loop(
    request_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    rnn_type: str,
    hidden_size: int,
    num_layers: int,
    learning_rate: float,
    max_context_length: int | None,
    downsample_rate: int,
) -> None:
    """Entry point for the RNN forecaster child process."""
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [RNN_FORECASTER] %(message)s")

    # Configure PyTorch threading for optimal CPU inference
    torch.set_num_threads(2)
    torch.set_num_interop_threads(2)

    log.warning("Initializing RNN model (%s, hidden_size=%d, layers=%d)", rnn_type, hidden_size, num_layers)
    model = TimeSeriesRNN(rnn_type.upper(), hidden_size=hidden_size, num_layers=num_layers)
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
    criterion = nn.MSELoss()

    while True:
        req: dict[str, Any] | None = request_queue.get()
        if req is None:
            log.warning("Shutdown received, exiting RNN forecaster process")
            return

        try:
            start_time = time.time()
            context_map: dict[str, list[float]] = req["context"]
            prediction_length: int = req.get("prediction_length", 10)
            series = context_map["input_rate"]

            # Apply custom max context length truncation
            if max_context_length is not None and len(series) > max_context_length:
                series = series[-max_context_length:]

            # 1. Downsample history
            if downsample_rate > 1:
                trim_offset = len(series) % downsample_rate
                trimmed_series = series[trim_offset:]
                downsampled_series = [
                    float(np.mean(trimmed_series[i : i + downsample_rate]))
                    for i in range(0, len(trimmed_series), downsample_rate)
                ]
            else:
                downsampled_series = series

            if len(downsampled_series) < 10:
                log.warning("RNN | Not enough data to forecast")
                fallback = [series[-1]] * prediction_length if series else [0.0] * prediction_length
                result_queue.put({"predictions": {"truth": fallback}})
                continue

            scaler = MinMaxScaler()
            data_scaled = scaler.fit_transform(np.array(downsampled_series).reshape(-1, 1))

            seq_length = min(len(data_scaled) - 1, 10)
            X = torch.tensor(
                np.array([data_scaled[i : i + seq_length] for i in range(len(data_scaled) - seq_length)]),
                dtype=torch.float32,
            )
            y = torch.tensor(data_scaled[seq_length:], dtype=torch.float32)

            # Online fine-tuning (2 gradient descents)
            model.train()
            for _ in range(2):
                optimizer.zero_grad()
                loss = criterion(model(X), y)
                loss.backward()
                optimizer.step()

            # Autoregressive decoding / forecasting
            model.eval()
            preds_scaled = []
            current_seq = torch.tensor(
                data_scaled[-seq_length:].reshape(1, seq_length, 1), dtype=torch.float32
            )

            downsampled_horizon = max(1, (prediction_length + downsample_rate - 1) // downsample_rate)

            with torch.no_grad():
                for _ in range(downsampled_horizon):
                    pred = model(current_seq)
                    preds_scaled.append(pred.item())
                    current_seq = torch.cat((current_seq[:, 1:, :], pred.unsqueeze(1)), dim=1)

            preds = scaler.inverse_transform(np.array(preds_scaled).reshape(-1, 1)).flatten()
            raw_preds_list = np.clip(preds, 0, max(series) * 1.5).tolist()

            # 2. Upsample back to second-level resolution
            preds_list = []
            for p in raw_preds_list:
                preds_list.extend([p] * downsample_rate)
            preds_list = preds_list[:prediction_length]

            result_queue.put({"predictions": {"truth": preds_list}})
            log.warning("RNN | forecast completed in %.2fs", time.time() - start_time)
        except Exception as e:
            log.exception("Forecast error")
            result_queue.put({"error": str(e)})


class RNNForecaster:
    """Wrapper for RNN models (LSTM/GRU) running online fine-tuning in a subprocess."""

    def __init__(
        self,
        rnn_type: str = "lstm",
        hidden_size: int = 32,
        num_layers: int = 1,
        learning_rate: float = 0.01,
        max_context_length: int | None = None,
        downsample_rate: int = 1,
    ) -> None:
        self.rnn_type = rnn_type
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.learning_rate = learning_rate
        self.max_context_length = max_context_length
        self.downsample_rate = downsample_rate

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
                self.rnn_type,
                self.hidden_size,
                self.num_layers,
                self.learning_rate,
                self.max_context_length,
                self.downsample_rate,
            ),
            daemon=True,
        )
        self._process.start()
        log.warning("RNN forecaster process started (pid=%s)", self._process.pid)

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
            log.warning("RNN forecast error: %s", result["error"])
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
