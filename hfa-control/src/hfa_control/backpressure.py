
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class BackpressureDecision:
    throttled: bool
    reason: str | None
    saturation: float
    max_dispatches_allowed: int = 0

    @property
    def active(self) -> bool:
        return self.throttled

    @property
    def is_soft(self) -> bool:
        return self.throttled and self.reason in {"soft_high", "soft_hysteresis"}

    @property
    def in_soft_throttle(self) -> bool:
        return self.is_soft

    @property
    def hard_cap_reached(self) -> bool:
        return self.reason == "hard_limit"


class BackpressureGuard:
    """Persistent hysteresis guard.

    Supports both newer compact calls:
      evaluate(inflight=..., capacity=..., saturation=...)
    and older audit-style calls:
      evaluate(global_inflight=..., total_capacity=..., queue_depth=..., worker_load_factors=..., ...)
    """

    def __init__(self, config) -> None:
        self._config = config
        self._in_soft_throttle = False

    def reset(self) -> None:
        self._in_soft_throttle = False

    @property
    def is_soft_throttled(self) -> bool:
        return self._in_soft_throttle

    def _as_list(self, value) -> list[float]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return list(value)
        return [float(value)]

    def evaluate(
        self,
        *,
        queued: int | None = None,
        inflight: int | None = None,
        capacity: int | None = None,
        saturation: float | None = None,
        global_inflight: int | None = None,
        total_capacity: int | None = None,
        queue_depth: int | None = None,
        worker_load_factors: Iterable[float] | None = None,
        worker_capacities: Iterable[int] | None = None,
        max_dispatches_requested: int | None = None,
    ) -> BackpressureDecision:
        current_inflight = global_inflight if global_inflight is not None else inflight if inflight is not None else queued or 0
        total_cap = total_capacity if total_capacity is not None else capacity if capacity is not None else 0
        req = int(max_dispatches_requested or 0)

        if total_cap <= 0:
            self._in_soft_throttle = True
            return BackpressureDecision(True, "no_capacity", 1.0, 0)

        if saturation is None:
            saturation = current_inflight / float(max(total_cap, 1))

        # capacity-weighted worker saturation check if provided
        loads = self._as_list(worker_load_factors)
        peak_load = max(loads) if loads else 0.0
        worker_sat_limit = float(getattr(self._config, "backpressure_worker_saturation", 1.0))
        if peak_load >= worker_sat_limit:
            self._in_soft_throttle = True
            return BackpressureDecision(True, "worker_saturation", max(saturation, peak_load), 0)

        # hard gate: explicit inflight cap or saturation/hard-cap
        hard_inflight_cap = int(
            getattr(self._config, "backpressure_hard_inflight_cap",
            getattr(self._config, "backpressure_hard_cap", 0))
        )
        if hard_inflight_cap > 0 and current_inflight >= hard_inflight_cap:
            self._in_soft_throttle = True
            return BackpressureDecision(True, "hard_limit", saturation, 0)

        hard = float(getattr(self._config, "backpressure_hard", 1.0))
        if saturation >= hard:
            self._in_soft_throttle = True
            return BackpressureDecision(True, "hard_limit", saturation, 0)

        # soft gate: prefer explicit inflight threshold if configured
        soft_inflight_cap = int(getattr(self._config, "backpressure_soft_inflight_cap", 0))
        ratio = float(getattr(self._config, "backpressure_hysteresis_ratio", 0.80))
        soft_high = float(getattr(self._config, "backpressure_soft_high", 0.90))
        soft_low = float(getattr(self._config, "backpressure_soft_low", soft_high * ratio))

        if soft_inflight_cap > 0:
            enter_soft = current_inflight >= soft_inflight_cap
            exit_soft = current_inflight < max(int(soft_inflight_cap * ratio), 0)
        else:
            enter_soft = saturation >= soft_high
            exit_soft = saturation < soft_low

        if self._in_soft_throttle:
            if exit_soft:
                self._in_soft_throttle = False
            else:
                allowed = max(req // 2, 1) if req > 0 else 0
                return BackpressureDecision(True, "soft_hysteresis", saturation, allowed)

        if enter_soft:
            self._in_soft_throttle = True
            allowed = max(req // 2, 1) if req > 0 else 0
            return BackpressureDecision(True, "soft_high", saturation, allowed)

        return BackpressureDecision(False, None, saturation, req)
