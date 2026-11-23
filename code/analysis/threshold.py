from collections import deque
from dataclasses import dataclass, field
import datetime
from enum import Enum
import logging
from typing import Any, Deque, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class ThresholdMode(str, Enum):
    EMA_DEBOUNCE = "ema_debounce"
    LEAKY_INTEGRATOR = "leaky_integrator"
    PURE_INTEGRATOR = "pure_integrator"


class ThresholdEngine:
    __instance = None

    @dataclass
    class Machine:
        threshold_value: float
        threshold_parameter_name: str
        timestamp_parameter_name: str

        # Mode selection (defaults to original EMA + debounce behavior)
        mode: ThresholdMode = ThresholdMode.EMA_DEBOUNCE

        # --- EMA + Debounce parameters ---
        # Alpha factor for EMA (0.0 to 1.0); 1.0 = no smoothing
        smoothing_factor: float = 1.0
        # 0.0 = no debouncing
        debounce_seconds: float = 0.0

        # --- Leaky Integrator parameters ---
        # Cumulative (excess * seconds) required to trigger True
        integral_threshold: Optional[float] = None
        # Multiplier on recovery rate when below threshold_value (1.0 = symmetric, 0.0 = no decay)
        decay_rate: float = 1.0
        # Optional anti-windup ceiling for accumulated integral
        integral_max: Optional[float] = None

        # --- Pure Integrator parameters ---
        # Rolling window in seconds (None = cumulative totalizer until reset_machine() is called)
        integration_window_seconds: Optional[float] = None
        # Divisor to convert seconds into the rate's time unit (1.0 = /sec, 60.0 = /min, 3600.0 = /hr)
        integration_time_unit_seconds: float = 1.0

        # --- Common time-guard & retransmit parameters ---
        # Optional cap on delta-t between samples to prevent false trips after outages
        max_gap_seconds: Optional[float] = None
        # Retransmit state if unchanged (default 10 minutes)
        retransmit_interval_seconds: float = 600.0

        # --- Runtime state tracking ---
        last_outcome: Optional[bool] = None
        last_change_ts: Optional[datetime.datetime] = None
        last_output_ts: Optional[datetime.datetime] = None
        last_sample_ts: Optional[datetime.datetime] = None
        last_value: Optional[float] = None
        integral_value: float = 0.0
        _integration_segments: Deque[
            Tuple[datetime.datetime, datetime.datetime, float]
        ] = field(default_factory=deque, repr=False)

        def __post_init__(self):
            if isinstance(self.mode, str):
                self.mode = ThresholdMode(self.mode)

            if not (0.0 < self.smoothing_factor <= 1.0):
                raise ValueError("smoothing_factor must be in (0.0, 1.0].")
            if self.debounce_seconds < 0.0:
                raise ValueError("debounce_seconds must be >= 0.0.")
            if self.integration_time_unit_seconds <= 0.0:
                raise ValueError("integration_time_unit_seconds must be > 0.0.")
            if (
                self.integration_window_seconds is not None
                and self.integration_window_seconds <= 0.0
            ):
                raise ValueError("integration_window_seconds must be > 0.0.")
            if self.mode == ThresholdMode.LEAKY_INTEGRATOR:
                if self.integral_threshold is None or self.integral_threshold <= 0.0:
                    raise ValueError(
                        "integral_threshold (> 0.0) is required when mode is 'leaky_integrator'."
                    )
                if self.decay_rate < 0.0:
                    raise ValueError("decay_rate must be >= 0.0.")

        def reset(self):
            self.last_outcome = None
            self.last_change_ts = None
            self.last_output_ts = None
            self.last_sample_ts = None
            self.last_value = None
            self.integral_value = 0.0
            self._integration_segments.clear()

    def __init__(self, config: Dict[str, Any]):
        self.__machines: Dict[str, ThresholdEngine.Machine] = {}
        for machine_id, machine_cfg in config.items():
            self.__machines[machine_id] = ThresholdEngine.Machine(**machine_cfg)

    @classmethod
    def get_engine(cls, config: Optional[Dict[str, Any]] = None):
        if cls.__instance is None:
            if config is None:
                raise ValueError("Config required for initial engine creation.")
            cls.__instance = cls(config)
        return cls.__instance

    def reset_machine(self, machine_id: str) -> None:
        """Reset runtime state and accumulated integrals for a specific machine."""
        machine_entry = self.__machines.get(machine_id)
        if machine_entry is None:
            logger.warning(f"No machine threshold entry found for {machine_id}")
            return
        machine_entry.reset()

    def get_integral_value(self, machine_id: str) -> Optional[float]:
        """Inspect the current accumulated integral for a machine."""
        machine_entry = self.__machines.get(machine_id)
        return machine_entry.integral_value if machine_entry else None

    def check_threshold(
        self, machine_id: str, payload: dict
    ) -> Optional[Tuple[bool, datetime.datetime]]:
        machine_entry = self.__machines.get(machine_id)
        if machine_entry is None:
            logger.warning(f"No machine threshold entry found for {machine_id}")
            return None

        # Extract and parse parameter value
        raw_parameter_value = payload.get(machine_entry.threshold_parameter_name)
        if raw_parameter_value is None:
            logger.warning(
                f"No parameter found at '{machine_entry.threshold_parameter_name}' for {machine_id}"
            )
            return None

        try:
            parameter_value = float(raw_parameter_value)
        except (ValueError, TypeError) as e:
            logger.error(
                f"Invalid parameter value '{raw_parameter_value}' for {machine_id}: {e}"
            )
            return None

        # Extract and parse timestamp
        timestamp_raw = payload.get(machine_entry.timestamp_parameter_name)
        if timestamp_raw is None:
            logger.warning(
                f"No timestamp parameter found at '{machine_entry.timestamp_parameter_name}' for {machine_id}"
            )
            return None

        try:
            if isinstance(timestamp_raw, datetime.datetime):
                timestamp = timestamp_raw
            else:
                timestamp = datetime.datetime.fromisoformat(timestamp_raw)
        except (ValueError, TypeError) as e:
            logger.error(
                f"Invalid timestamp value '{timestamp_raw}' for {machine_id}: {e}"
            )
            return None

        # Guard against out-of-order or duplicate timestamps
        dt: Optional[float] = None
        if machine_entry.last_sample_ts is not None:
            raw_dt = (timestamp - machine_entry.last_sample_ts).total_seconds()
            if raw_dt <= 0.0:
                logger.debug(
                    f"Ignoring out-of-order/duplicate timestamp {timestamp} for {machine_id} "
                    f"(last sample: {machine_entry.last_sample_ts})"
                )
                return None
            dt = (
                min(raw_dt, machine_entry.max_gap_seconds)
                if machine_entry.max_gap_seconds is not None
                else raw_dt
            )

        # Dispatch evaluation based on configured mode
        if machine_entry.mode == ThresholdMode.EMA_DEBOUNCE:
            result = self._evaluate_ema_debounce(
                machine_id, machine_entry, parameter_value, timestamp
            )
        elif machine_entry.mode == ThresholdMode.LEAKY_INTEGRATOR:
            result = self._evaluate_leaky_integrator(
                machine_id, machine_entry, parameter_value, timestamp, dt
            )
        elif machine_entry.mode == ThresholdMode.PURE_INTEGRATOR:
            result = self._evaluate_pure_integrator(
                machine_id, machine_entry, parameter_value, timestamp, dt
            )
        else:
            raise ValueError(f"Unsupported threshold mode: {machine_entry.mode}")

        machine_entry.last_sample_ts = timestamp
        return result

    def _evaluate_ema_debounce(
        self,
        machine_id: str,
        machine_entry: Machine,
        parameter_value: float,
        timestamp: datetime.datetime,
    ) -> Optional[Tuple[bool, datetime.datetime]]:
        # Smooth value (EMA) - bypassed if alpha >= 1.0
        if machine_entry.smoothing_factor < 1.0:
            if machine_entry.last_value is not None:
                parameter_value = (
                    machine_entry.smoothing_factor * parameter_value
                    + (1 - machine_entry.smoothing_factor) * machine_entry.last_value
                )
            machine_entry.last_value = parameter_value

        current_outcome = parameter_value >= machine_entry.threshold_value

        if machine_entry.last_outcome is None:
            machine_entry.last_outcome = current_outcome
            machine_entry.last_output_ts = timestamp
            return machine_entry.last_outcome, timestamp

        if machine_entry.last_outcome != current_outcome:
            if machine_entry.debounce_seconds > 0.0:
                if machine_entry.last_change_ts is None:
                    machine_entry.last_change_ts = timestamp
                    return None
                elif (
                    timestamp - machine_entry.last_change_ts
                ).total_seconds() >= machine_entry.debounce_seconds:
                    machine_entry.last_outcome = current_outcome
                    machine_entry.last_output_ts = machine_entry.last_change_ts
                    machine_entry.last_change_ts = None
                    logger.info(
                        f"Machine {machine_id} outcome change to {current_outcome} (debounced) as "
                        f"{machine_entry.threshold_parameter_name} passed threshold "
                        f"{machine_entry.threshold_value} at {machine_entry.last_output_ts}"
                    )
                    return machine_entry.last_outcome, machine_entry.last_output_ts
                else:
                    return None
            else:
                logger.info(
                    f"Machine {machine_id} outcome change to {current_outcome} as "
                    f"{machine_entry.threshold_parameter_name} passed threshold "
                    f"{machine_entry.threshold_value} at {timestamp}"
                )
                machine_entry.last_outcome = current_outcome
                machine_entry.last_output_ts = timestamp
                return current_outcome, timestamp
        else:
            machine_entry.last_change_ts = None
            return self._check_retransmit(machine_id, machine_entry, timestamp)

    def _evaluate_leaky_integrator(
        self,
        machine_id: str,
        machine_entry: Machine,
        parameter_value: float,
        timestamp: datetime.datetime,
        dt: Optional[float],
    ) -> Optional[Tuple[bool, datetime.datetime]]:
        if dt is None:
            # First sample: initialize baseline without integrating
            machine_entry.last_value = parameter_value
            machine_entry.integral_value = 0.0
            machine_entry.last_outcome = False
            machine_entry.last_output_ts = timestamp
            return machine_entry.last_outcome, timestamp

        excess = parameter_value - machine_entry.threshold_value
        if excess >= 0.0:
            machine_entry.integral_value += excess * dt
            if machine_entry.integral_max is not None:
                machine_entry.integral_value = min(
                    machine_entry.integral_value, machine_entry.integral_max
                )
        else:
            machine_entry.integral_value = max(
                0.0,
                machine_entry.integral_value + (excess * machine_entry.decay_rate * dt),
            )

        machine_entry.last_value = parameter_value
        current_outcome = (
            machine_entry.integral_value >= machine_entry.integral_threshold
        )

        return self._emit_integrator_outcome(
            machine_id, machine_entry, current_outcome, timestamp
        )

    def _evaluate_pure_integrator(
        self,
        machine_id: str,
        machine_entry: Machine,
        parameter_value: float,
        timestamp: datetime.datetime,
        dt: Optional[float],
    ) -> Optional[Tuple[bool, datetime.datetime]]:
        if dt is None or machine_entry.last_value is None:
            # First sample: establish starting point for trapezoidal integration
            machine_entry.last_value = parameter_value
            machine_entry.integral_value = 0.0
            machine_entry.last_outcome = (
                machine_entry.integral_value >= machine_entry.threshold_value
            )
            machine_entry.last_output_ts = timestamp
            return machine_entry.last_outcome, timestamp

        avg_rate = 0.5 * (parameter_value + machine_entry.last_value)

        if machine_entry.integration_window_seconds is None:
            # Cumulative integration since start / last reset
            dt_scaled = dt / machine_entry.integration_time_unit_seconds
            machine_entry.integral_value += avg_rate * dt_scaled
        else:
            # Rolling-window trapezoidal integration
            effective_start = timestamp - datetime.timedelta(seconds=dt)
            machine_entry._integration_segments.append(
                (effective_start, timestamp, avg_rate)
            )
            window_start = timestamp - datetime.timedelta(
                seconds=machine_entry.integration_window_seconds
            )

            # Drop segments completely outside the rolling window
            while (
                machine_entry._integration_segments
                and machine_entry._integration_segments[0][1] <= window_start
            ):
                machine_entry._integration_segments.popleft()

            # Sum segments inside window (proportionally trimming the oldest partial segment)
            total = 0.0
            for seg_start, seg_end, seg_rate in machine_entry._integration_segments:
                clipped_start = max(seg_start, window_start)
                seg_dt = (seg_end - clipped_start).total_seconds()
                if seg_dt > 0.0:
                    total += seg_rate * (
                        seg_dt / machine_entry.integration_time_unit_seconds
                    )
            machine_entry.integral_value = total

        machine_entry.last_value = parameter_value
        current_outcome = machine_entry.integral_value >= machine_entry.threshold_value

        return self._emit_integrator_outcome(
            machine_id, machine_entry, current_outcome, timestamp
        )

    def _emit_integrator_outcome(
        self,
        machine_id: str,
        machine_entry: Machine,
        current_outcome: bool,
        timestamp: datetime.datetime,
    ) -> Optional[Tuple[bool, datetime.datetime]]:
        if machine_entry.last_outcome != current_outcome:
            logger.info(
                f"Machine {machine_id} ({machine_entry.mode.value}) outcome change to {current_outcome} "
                f"(integral={machine_entry.integral_value:.3f}) at {timestamp}"
            )
            machine_entry.last_outcome = current_outcome
            machine_entry.last_output_ts = timestamp
            return current_outcome, timestamp

        return self._check_retransmit(machine_id, machine_entry, timestamp)

    def _check_retransmit(
        self,
        machine_id: str,
        machine_entry: Machine,
        timestamp: datetime.datetime,
    ) -> Optional[Tuple[bool, datetime.datetime]]:
        if (
            machine_entry.retransmit_interval_seconds > 0.0
            and machine_entry.last_output_ts is not None
        ):
            elapsed_since_output = (
                timestamp - machine_entry.last_output_ts
            ).total_seconds()
            if elapsed_since_output >= machine_entry.retransmit_interval_seconds:
                logger.info(
                    f"Retransmitting {machine_entry.last_outcome} outcome for machine {machine_id} "
                    f"as retransmit interval has elapsed"
                )
                machine_entry.last_output_ts = timestamp
                return machine_entry.last_outcome, timestamp  # type: ignore[return-value]
        return None


def apply_threshold(config: dict, machine: str):
    engine = ThresholdEngine.get_engine(config)

    async def wrapped(payload: dict):
        return engine.check_threshold(machine, payload)

    return wrapped
