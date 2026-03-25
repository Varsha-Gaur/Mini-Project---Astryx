"""
simulator/smart_meter_simulator.py
====================================
Smart Meter Simulator
----------------------
Transforms the UCI Individual Household Electric Power Consumption dataset
into a fleet of N virtual smart meters with realistic per-meter variation,
regional clustering, peak-hour behaviour, and appliance spikes.

Exposes a Python **generator** interface so readings are produced on-demand
(O(1) memory) — critical for large fleets and long simulations.

Pipeline position
-----------------
    SmartGridSimulator.simulate_stream()
        │
        ▼  [DifferentialPrivacyModule.apply()]
    MeterReading (noised)
        │
        ▼  [HomomorphicEncryptionModule.encrypt()]
    EncryptedReading  →  aggregation server
"""

from __future__ import annotations

import csv
import json
import logging
import math
import random
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

import numpy as np

if TYPE_CHECKING:
    # Lazy imports to avoid circular dependencies
    from privacy.dp_module import DifferentialPrivacyModule
    from encryption.he_module import HomomorphicEncryptionModule, EncryptedReading

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hour-of-day consumption multipliers (index = hour 0–23)
# Based on typical UK household demand profiles.
# ---------------------------------------------------------------------------
_DIURNAL: List[float] = [
    0.30,
    0.28,
    0.26,
    0.25,
    0.25,
    0.28,  # 00–05  night
    0.52,
    0.76,
    0.88,
    0.74,
    0.66,
    0.65,  # 06–11  morning ramp
    0.70,
    0.68,
    0.65,
    0.68,
    0.76,
    0.96,  # 12–17  afternoon
    1.00,
    0.98,
    0.91,
    0.82,
    0.67,
    0.46,  # 18–23  evening peak
]


# ---------------------------------------------------------------------------
# Configuration dataclass (kept for backward compatibility; config.py is
# the authoritative source for new code)
# ---------------------------------------------------------------------------
@dataclass
class SimulatorConfig:
    """Per-simulation configuration. Mirrors SimulatorSettings in config.py."""

    num_meters: int = 20
    sample_size: Optional[int] = None
    noise_std: float = 0.05
    peak_hours: Tuple[int, int] = (17, 21)
    peak_multiplier: float = 1.35
    weekend_multiplier: float = 0.85
    spike_probability: float = 0.02
    spike_max_kw: float = 1.5
    num_regions: int = 5
    stream_delay_seconds: float = 0.0
    random_seed: Optional[int] = 42
    # Security-pipeline flags
    enable_dp: bool = False
    enable_he: bool = False


# ---------------------------------------------------------------------------
# Data-transfer objects
# ---------------------------------------------------------------------------
@dataclass
class RawReading:
    """One parsed row from the UCI household power dataset."""

    timestamp: datetime
    global_active_power: Optional[float]
    global_reactive_power: Optional[float]
    voltage: Optional[float]
    global_intensity: Optional[float]
    sub_metering_1: Optional[float]
    sub_metering_2: Optional[float]
    sub_metering_3: Optional[float]


@dataclass
class MeterReading:
    """
    A single timestamped reading from one virtual smart meter.

    Security-pipeline fields
    ------------------------
    privacy_noise_applied : ε value set by DifferentialPrivacyModule.
    encrypted_payload     : First-16-byte preview set by HEModule.
    """

    meter_id: str
    timestamp: str  # ISO-8601 string
    energy_usage: float  # kW (active power)
    voltage: float  # V
    current: float  # A
    sub_metering: Dict[str, float]  # kitchen / laundry / hvac  (Wh)
    region_id: int
    is_peak_hour: bool
    is_weekend: bool

    # Security hooks — populated downstream
    privacy_noise_applied: Optional[float] = None
    encrypted_payload: Optional[bytes] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dictionary."""
        d = asdict(self)
        if d["encrypted_payload"] is not None:
            d["encrypted_payload"] = d["encrypted_payload"].hex()
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Synthetic data generator (no UCI file required)
# ---------------------------------------------------------------------------
def generate_synthetic_rows(n: int = 120, seed: int = 42) -> List[RawReading]:
    """
    Generate ``n`` synthetic RawReading objects with a realistic diurnal pattern.

    Used when the UCI dataset file is unavailable.

    Parameters
    ----------
    n    : Number of 1-minute intervals to generate.
    seed : Random seed for reproducibility.
    """
    rng = np.random.default_rng(seed)
    base = datetime(2026, 1, 15, 8, 0, 0)
    rows: List[RawReading] = []

    for i in range(n):
        ts = base + timedelta(minutes=i)
        d = _DIURNAL[ts.hour]
        rows.append(
            RawReading(
                timestamp=ts,
                global_active_power=round(
                    max(0.1, d * 2.0 + float(rng.normal(0, 0.15))), 4
                ),
                global_reactive_power=round(
                    max(0.0, 0.12 + float(rng.normal(0, 0.02))), 4
                ),
                voltage=round(230.0 + float(rng.normal(0, 1.2)), 2),
                global_intensity=round(max(0.5, d * 8 + float(rng.normal(0, 0.4))), 3),
                sub_metering_1=round(max(0.0, d * 5 + float(rng.normal(0, 0.5))), 3),
                sub_metering_2=round(max(0.0, float(rng.uniform(0, 2))), 3),
                sub_metering_3=round(max(0.0, d * 10 + float(rng.normal(0, 1))), 3),
            )
        )

    logger.info("Generated %d synthetic rows (seed=%d).", n, seed)
    return rows


# ---------------------------------------------------------------------------
# Per-meter model
# ---------------------------------------------------------------------------
class SmartMeter:
    """
    A virtual smart meter derived from base dataset rows.

    Each instance has a unique scaling factor, voltage offset, current offset,
    and appliance-mix weights — simulating a heterogeneous neighbourhood.

    Parameters
    ----------
    meter_id  : Unique string identifier, e.g. "meter_007".
    config    : Shared SimulatorConfig.
    region_id : Geographic cluster label (0 … num_regions-1).
    rng       : Per-meter seeded ``random.Random`` for reproducibility.
    """

    def __init__(
        self,
        meter_id: str,
        config: SimulatorConfig,
        region_id: int,
        rng: random.Random,
    ) -> None:
        self.meter_id = meter_id
        self.config = config
        self.region_id = region_id
        self._rng = rng
        # Independent NumPy RNG for vectorised noise
        self._np_rng = np.random.default_rng(abs(hash(meter_id)) % (2**31))

        # Invariant household characteristics (sampled once at construction)
        self._power_scale: float = rng.uniform(0.6, 1.6)
        self._voltage_offset: float = rng.uniform(-5.0, 5.0)
        self._current_offset: float = rng.uniform(-0.5, 0.5)
        self._sub_weights: Tuple[float, float, float] = self._sample_weights()

        logger.debug(
            "SmartMeter %s | region=%d | scale=%.3f",
            meter_id,
            region_id,
            self._power_scale,
        )

    # ------------------------------------------------------------------
    def _sample_weights(self) -> Tuple[float, float, float]:
        """Normalised (kitchen, laundry, hvac) appliance weight vector."""
        w = [self._rng.uniform(0.1, 1.0) for _ in range(3)]
        s = sum(w)
        return (w[0] / s, w[1] / s, w[2] / s)

    def _is_peak(self, ts: datetime) -> bool:
        s, e = self.config.peak_hours
        return ts.weekday() < 5 and s <= ts.hour < e

    def _is_weekend(self, ts: datetime) -> bool:
        return ts.weekday() >= 5

    # ------------------------------------------------------------------
    def generate_reading(self, raw: RawReading) -> MeterReading:
        """
        Produce one MeterReading from a RawReading.

        Applies:
          1. Per-meter power scaling
          2. Diurnal consumption factor
          3. Peak-hour / weekend multiplier
          4. Gaussian measurement noise
          5. Random appliance spike
        """
        ts = raw.timestamp
        base_p = raw.global_active_power or 1.0

        # Apply scaling and contextual multipliers
        power = base_p * self._power_scale * _DIURNAL[ts.hour]
        if self._is_peak(ts):
            power *= self.config.peak_multiplier
        if self._is_weekend(ts):
            power *= self.config.weekend_multiplier

        # Gaussian noise proportional to reading magnitude
        power += float(self._np_rng.normal(0.0, self.config.noise_std * power))

        # Occasional random appliance spike
        if self._rng.random() < self.config.spike_probability:
            spike = self._rng.uniform(0.1, self.config.spike_max_kw)
            power += spike
            logger.debug("%s spike +%.3f kW @ %s", self.meter_id, spike, ts)

        power = max(0.05, round(power, 4))

        # Voltage and current
        voltage = round(
            (raw.voltage or 230.0)
            + self._voltage_offset
            + float(self._np_rng.normal(0, 0.6)),
            2,
        )
        current = round(
            max(
                0.1,
                power * 1000.0 / max(voltage, 1.0)
                + self._current_offset
                + float(self._np_rng.normal(0, 0.04)),
            ),
            3,
        )

        # Sub-metering breakdown
        total_sm = (
            (raw.sub_metering_1 or 0.0)
            + (raw.sub_metering_2 or 0.0)
            + (raw.sub_metering_3 or 0.0)
        )
        wk, wl, wh = self._sub_weights
        sub = {
            "kitchen": round(total_sm * wk, 3),
            "laundry": round(total_sm * wl, 3),
            "hvac": round(total_sm * wh, 3),
        }

        return MeterReading(
            meter_id=self.meter_id,
            timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
            energy_usage=power,
            voltage=voltage,
            current=current,
            sub_metering=sub,
            region_id=self.region_id,
            is_peak_hour=self._is_peak(ts),
            is_weekend=self._is_weekend(ts),
        )


# ---------------------------------------------------------------------------
# SmartGridSimulator — fleet orchestrator
# ---------------------------------------------------------------------------
class SmartGridSimulator:
    """
    Orchestrates a fleet of virtual smart meters over a shared dataset.

    Usage
    -----
    >>> sim = SmartGridSimulator()          # synthetic data, default 20 meters
    >>> sim.setup()
    >>> for reading in sim.simulate_stream():
    ...     # pipe through DP → HE → server
    ...     pass

    Memory
    ------
    ``simulate_stream()`` is a generator — it never holds all readings in
    RAM simultaneously.  100 meters × 1 440 rows/day = 144 000 readings
    produced one at a time.

    Parameters
    ----------
    dataset_path   : Path to the UCI dataset file (optional; synthetic used if absent).
    num_meters     : Fleet size.
    config         : SimulatorConfig (defaults applied if None).
    dp_module      : Optional DifferentialPrivacyModule.
    he_module      : Optional HomomorphicEncryptionModule.
    """

    def __init__(
        self,
        dataset_path: str = "household_power_consumption.txt",
        num_meters: int = 20,
        config: Optional[SimulatorConfig] = None,
        dp_module: Optional["DifferentialPrivacyModule"] = None,
        he_module: Optional["HomomorphicEncryptionModule"] = None,
    ) -> None:
        self.dataset_path = Path(dataset_path)
        self.config = config or SimulatorConfig(num_meters=num_meters)
        self.config.num_meters = num_meters

        if self.config.random_seed is not None:
            random.seed(self.config.random_seed)
            np.random.seed(self.config.random_seed)

        self._raw_rows: List[RawReading] = []
        self.meters: List[SmartMeter] = []

        # Security modules (set at construction or via setup())
        self.dp_module = dp_module
        self.he_module = he_module

        logger.info(
            "SmartGridSimulator | meters=%d | dp=%s | he=%s",
            num_meters,
            "✓" if dp_module else "✗",
            "✓" if he_module else "✗",
        )

    # ------------------------------------------------------------------
    # Public convenience setup
    # ------------------------------------------------------------------

    def setup(
        self,
        synthetic_minutes: int = 120,
    ) -> "SmartGridSimulator":
        """
        Load (or generate) data rows and create meter instances.

        Returns self for chaining.
        """
        self._load_data(synthetic_minutes)
        self.create_meters()
        return self

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_data(self, synthetic_minutes: int = 120) -> None:
        """Load UCI dataset or fall back to synthetic data."""
        if self.dataset_path.exists():
            self.load_dataset()
        else:
            logger.warning(
                "Dataset not found at '%s'. Using synthetic data.", self.dataset_path
            )
            self._raw_rows = generate_synthetic_rows(
                n=synthetic_minutes,
                seed=self.config.random_seed or 42,
            )

    def load_dataset(self) -> None:
        """
        Load and preprocess the UCI dataset from disk.

        Raises
        ------
        FileNotFoundError : If ``dataset_path`` does not exist.
        """
        if not self.dataset_path.exists():
            raise FileNotFoundError(
                f"Dataset not found: {self.dataset_path}\n"
                "Download from https://archive.ics.uci.edu/dataset/235/"
                "individual+household+electric+power+consumption"
            )

        logger.info("Loading dataset: %s", self.dataset_path)
        rows: List[RawReading] = []
        limit = self.config.sample_size

        with self.dataset_path.open(
            newline="", encoding="utf-8", errors="replace"
        ) as fh:
            reader = csv.DictReader(fh, delimiter=";")
            for i, csv_row in enumerate(reader):
                if limit is not None and i >= limit:
                    break
                raw = self._parse_row(csv_row)
                if raw:
                    rows.append(raw)

        rows.sort(key=lambda r: r.timestamp)
        self._raw_rows = rows
        logger.info("Dataset loaded: %d rows.", len(self._raw_rows))

    @staticmethod
    def _parse_float(value: str) -> Optional[float]:
        v = value.strip()
        if v in ("", "?"):
            return None
        try:
            return float(v)
        except ValueError:
            return None

    def _parse_row(self, row: Dict[str, str]) -> Optional[RawReading]:
        try:
            ts = datetime.strptime(
                f"{row['Date'].strip()} {row['Time'].strip()}", "%d/%m/%Y %H:%M:%S"
            )
        except (ValueError, KeyError):
            return None
        pf = self._parse_float
        return RawReading(
            timestamp=ts,
            global_active_power=pf(row.get("Global_active_power", "")),
            global_reactive_power=pf(row.get("Global_reactive_power", "")),
            voltage=pf(row.get("Voltage", "")),
            global_intensity=pf(row.get("Global_intensity", "")),
            sub_metering_1=pf(row.get("Sub_metering_1", "")),
            sub_metering_2=pf(row.get("Sub_metering_2", "")),
            sub_metering_3=pf(row.get("Sub_metering_3", "")),
        )

    # ------------------------------------------------------------------
    # Meter creation
    # ------------------------------------------------------------------

    def create_meters(self) -> None:
        """Instantiate ``num_meters`` SmartMeter objects with distinct profiles."""
        self.meters = []
        for i in range(self.config.num_meters):
            meter_rng = random.Random((self.config.random_seed or 0) + i * 31_337)
            self.meters.append(
                SmartMeter(
                    meter_id=f"meter_{i:03d}",
                    config=self.config,
                    region_id=i % self.config.num_regions,
                    rng=meter_rng,
                )
            )
        logger.info(
            "Created %d meters across %d regions.",
            len(self.meters),
            self.config.num_regions,
        )

    # ------------------------------------------------------------------
    # Sync streaming  (generator — O(1) memory)
    # ------------------------------------------------------------------

    def simulate_stream(
        self,
        privacy_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        encryption_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
    ) -> Generator[MeterReading, None, None]:
        """
        Yield MeterReading objects for every (timestamp × meter) pair.

        Auto-applies the attached ``dp_module`` and ``he_module`` when present.
        Per-call hooks can supplement or override the attached modules.

        Parameters
        ----------
        privacy_hook    : Optional per-call DP hook.
        encryption_hook : Optional per-call HE hook.

        Yields
        ------
        MeterReading objects, ordered by (timestamp, meter_id).
        """
        if not self.meters:
            raise RuntimeError(
                "Call create_meters() (or setup()) before simulate_stream()."
            )
        if not self._raw_rows:
            raise RuntimeError(
                "No data rows available. Call load_dataset() or setup()."
            )

        logger.info(
            "Stream started: %d meters × %d rows = %d readings",
            len(self.meters),
            len(self._raw_rows),
            len(self._raw_rows) * len(self.meters),
        )

        for raw in self._raw_rows:
            for meter in self.meters:
                reading = meter.generate_reading(raw)

                # --- Differential Privacy ---
                if privacy_hook:
                    reading = privacy_hook(reading)
                if self.config.enable_dp and self.dp_module:
                    reading = self.dp_module.apply(reading)

                # --- Homomorphic Encryption ---
                # NOTE: the EncryptedReading is attached back to the MeterReading
                # via reading.encrypted_payload (side-effect in he_module.encrypt).
                # The full EncryptedReading is also available to callers that need
                # it for aggregation — use collect_encrypted_batch() for that.
                if encryption_hook:
                    reading = encryption_hook(reading)
                if self.config.enable_he and self.he_module:
                    self.he_module.encrypt(
                        reading
                    )  # side-effect sets encrypted_payload

                if self.config.stream_delay_seconds > 0:
                    time.sleep(self.config.stream_delay_seconds / len(self.meters))

                yield reading

    # ------------------------------------------------------------------
    # Async streaming (for FastAPI SSE / WebSocket)
    # ------------------------------------------------------------------

    async def async_stream(
        self,
        privacy_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        encryption_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        yield_delay: float = 0.0,
    ) -> AsyncGenerator[MeterReading, None]:
        """
        Async generator version of simulate_stream() for FastAPI endpoints.

        Parameters
        ----------
        privacy_hook    : Optional DP hook.
        encryption_hook : Optional HE hook.
        yield_delay     : asyncio.sleep() between yields (simulates real-time).

        Yields
        ------
        MeterReading objects.
        """
        import asyncio

        if not self.meters:
            raise RuntimeError("Call setup() before async_stream().")
        if not self._raw_rows:
            raise RuntimeError("No data rows available.")

        for raw in self._raw_rows:
            for meter in self.meters:
                reading = meter.generate_reading(raw)

                if privacy_hook:
                    reading = privacy_hook(reading)
                if self.config.enable_dp and self.dp_module:
                    reading = self.dp_module.apply(reading)

                if encryption_hook:
                    reading = encryption_hook(reading)
                if self.config.enable_he and self.he_module:
                    self.he_module.encrypt(reading)

                if yield_delay > 0:
                    await asyncio.sleep(yield_delay)

                yield reading

    # ------------------------------------------------------------------
    # Batch helpers
    # ------------------------------------------------------------------

    def simulate_batch(self, max_records: int = 500) -> List[MeterReading]:
        """Return up to ``max_records`` readings as a plain list."""
        return [r for i, r in enumerate(self.simulate_stream()) if i < max_records]

    def export_batch(
        self,
        max_records: int = 1000,
        output_path: Optional[str] = None,
        privacy_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        encryption_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Materialise up to max_records readings into a list of dicts.
        Optionally writes JSON Lines to output_path.
        """
        batch: List[Dict[str, Any]] = []
        for i, reading in enumerate(
            self.simulate_stream(privacy_hook, encryption_hook)
        ):
            if i >= max_records:
                break
            batch.append(reading.to_dict())

        logger.info("Exported batch: %d records.", len(batch))

        if output_path:
            out = Path(output_path)
            with out.open("w", encoding="utf-8") as fh:
                for rec in batch:
                    fh.write(json.dumps(rec) + "\n")
            logger.info("Batch written → %s", out)

        return batch

    def collect_encrypted_batch(
        self,
        max_records: int = 100,
    ) -> List["EncryptedReading"]:
        """
        Run the full DP + HE pipeline and collect EncryptedReading objects.

        Used by the aggregation server to perform homomorphic summation.
        Individual ciphertexts are summed *without decryption* — only the
        aggregate is later decrypted.

        BUG FIX: original code called ``self.he_module.encrypt(reading)``
        but discarded the return value, so no EncryptedReading was ever
        stored.  Fixed here by capturing the return.

        Requires he_module to be attached.
        """
        if not self.he_module:
            raise RuntimeError(
                "he_module must be attached to collect encrypted batches."
            )

        enc_readings: List[Any] = []
        for i, reading in enumerate(self.simulate_stream()):
            if i >= max_records:
                break
            enc = self.he_module.encrypt(reading)  # ← capture the EncryptedReading
            enc_readings.append(enc)

        return enc_readings

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def security_status(self) -> Dict[str, Any]:
        """Return a summary of attached security modules and their status."""
        status: Dict[str, Any] = {
            "dp_module_attached": self.dp_module is not None,
            "he_module_attached": self.he_module is not None,
            "dp_enabled": self.config.enable_dp,
            "he_enabled": self.config.enable_he,
            "num_meters": len(self.meters),
            "num_rows": len(self._raw_rows),
        }
        if self.dp_module:
            status["dp_budget_report"] = self.dp_module.budget_report()
        if self.he_module:
            status["he_context"] = self.he_module.context_summary()
        return status
