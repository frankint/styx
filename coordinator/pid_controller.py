from dataclasses import dataclass

from styx.common.logging import logging


@dataclass
class BacklogPIDController:
    kp: float = 0.1
    ki: float = 0.001
    kd: float = 0.1
    d_smoothing: float = 0.6  # EMA alpha for derivative

    integral_max: float = 100.0
    scale_up_threshold: float = 1.0
    backlog_threshold: float = 10.0

    # Internal state
    integral: float = 0.0
    prev_error: float | None = None
    smoothed_derivative: float = 0.0

    def compute(self, total_backlog: float, smoothed_tps: float) -> float:
        if smoothed_tps <= 0 or total_backlog <= self.backlog_threshold:
            return 0.0

        error = total_backlog / smoothed_tps

        logging.warning(f"CONTROLLER | Total backlog: {total_backlog}; tps: {smoothed_tps}")
        if self.prev_error is None:
            self.prev_error = error
        # P: how bad is it right now?
        p_term = self.kp * error

        # I: has it been bad for a while? (per-epoch accumulation)
        delta = error - self.prev_error
        self.integral += delta
        self.integral = max(-self.integral_max, min(self.integral_max, self.integral))
        i_term = self.ki * self.integral

        # D: is the trend getting worse? (smoothed, per-epoch)
        raw_derivative = error - self.prev_error
        self.smoothed_derivative = self.d_smoothing * raw_derivative + (1 - self.d_smoothing) * self.smoothed_derivative
        d_term = self.kd * self.smoothed_derivative

        self.prev_error = error
        logging.warning(f"CONTROLLER | P term: {p_term}, I term: {i_term}, D term: {d_term}")

        return p_term + i_term + d_term
