import logging
from pandas import DataFrame
from dataclasses import dataclass
import datetime
from enum import Enum
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ThresholdEngine:
    __instance = None

    @dataclass
    class Machine:
        threshold_value: float
        threshold_parameter_name: str
        timestamp_parameter_name: str

        # Alpha factor for EMA (0.0 to 1.0)  1.0 = no smoothing
        smoothing_factor: float = 1.0
        # 0.0 = no debouncing
        debounce_seconds: float = 0.0
        # Retransmit state if unchanged (default 10 minutes)
        retransmit_interval_seconds: float = 600.0

        # Runtime state tracking
        last_outcome: Optional[bool] = None
        last_change_ts: Optional[datetime.datetime] = None
        last_output_ts: Optional[datetime.datetime] = None
        last_value: Optional[float] = None

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

    def check_threshold(self, machine_id: str, payload: dict) -> Optional[bool]:
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
            timestamp = datetime.fromisoformat(timestamp_raw)
        except (ValueError, TypeError) as e:
            logger.error(
                f"Invalid timestamp value '{timestamp_raw}' for {machine_id}: {e}"
            )
            return None

        # Step 1: Smooth value (EMA) - bypassed if alpha >= 1.0
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
            # Step 2: Debounce outcome (bypassed functionally if debounce_seconds == 0.0)
            if machine_entry.debounce_seconds > 0.0:
                if machine_entry.last_change_ts is None:
                    # Candidate transition detected; start timer without changing state
                    machine_entry.last_change_ts = timestamp
                    return None
                elif (
                    timestamp - machine_entry.last_change_ts
                ).total_seconds() >= machine_entry.debounce_seconds:
                    # Debounce duration met; commit new state
                    machine_entry.last_outcome = current_outcome
                    machine_entry.last_output_ts = machine_entry.last_change_ts
                    machine_entry.last_change_ts = None
                    logger.info(
                        f"Machine {machine_id} outcome change to {current_outcome} (debounced) as {machine_entry.threshold_parameter_name} passing threshold {machine_entry.threshold_value} at {machine_entry.last_output_ts}"
                    )
                    return machine_entry.last_outcome, machine_entry.last_output_ts
                else:
                    # Debounce duration not complete
                    return None
            else:  # No debouncing
                logger.info(
                    f"Machine {machine_id} outcome change to {current_outcome} as {machine_entry.threshold_parameter_name} passing threshold {machine_entry.threshold_value} at {timestamp}"
                )
                machine_entry.last_outcome = current_outcome
                machine_entry.last_output_ts = timestamp
                return current_outcome, timestamp
        else:
            machine_entry.last_change_ts = (
                None  # at last output state - reset transition timer
            )

            # Retransmit check for static state
            if (
                machine_entry.retransmit_interval_seconds > 0.0
                and machine_entry.last_output_ts is not None
            ):
                elapsed_since_output = (
                    timestamp - machine_entry.last_output_ts
                ).total_seconds()
                if elapsed_since_output >= machine_entry.retransmit_interval_seconds:
                    logger.info(
                        f"Retransmitting {machine_entry.last_outcome} outcome for machine {machine_id} as retransmit interval has ellapsed"
                    )
                    machine_entry.last_output_ts = timestamp
                    return machine_entry.last_outcome, timestamp

            return None


def apply_threshold(config: dict, machine: str):
    # Retrieve or initialize the singleton ThresholdEngine instance
    engine = ThresholdEngine.get_engine(config)

    async def wrapped(payload: dict):
        # Evaluate threshold for the specified machine_id and payload
        return engine.check_threshold(machine, payload)

    return wrapped
