"""
smart_meter_simulator.py
========================
Hybrid Secure Data Aggregation for Smart Grids
-----------------------------------------------
Simulates multiple smart meters using the UCI Individual Household Electric
Power Consumption dataset.  Integrates with:

  * dp_module.py                 – Differential Privacy (Laplace / Gaussian)
  * he_module.py                 – Homomorphic Encryption (CKKS via TenSEAL)
  * kafka_producer.py            – Kafka-style streaming output
  * api_server.py (FastAPI)      – Real-time SSE / REST streaming endpoint

Full pipeline
-------------
    RawReading
        │
        ▼
    SmartMeter.generate_reading()
        │
        ▼  [DifferentialPrivacyModule.apply()]
    MeterReading  (DP-noised, privacy_noise_applied set)
        │
        ▼  [HomomorphicEncryptionModule.encrypt()]
    EncryptedReading  (CKKS ciphertext, encrypted_payload set)
        │
        ├──► KafkaStreamProducer.publish_reading()
        │         → topic: smart_grid.encrypted
        │
        └──► FastAPI /stream/readings SSE endpoint
                  → aggregation_server

Dataset source:
    https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import random
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from dp_module import DifferentialPrivacyModule
    from he_module import HomomorphicEncryptionModule, EncryptedReading
    from kafka_producer import KafkaStreamProducer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("smart_grid_simulator")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class SimulatorConfig:
    """
    Centralised configuration for the entire simulation.

    Attributes
    ----------
    num_meters            : Number of virtual smart meters.
    sample_size           : Max dataset rows to load (None = all).
    noise_std             : Gaussian noise std-dev added to readings.
    peak_hours            : (start_h, end_h) for weekday peak window.
    peak_multiplier       : Scaling factor during peak hours.
    weekend_multiplier    : Scaling factor on weekends.
    spike_probability     : Per-reading probability of an appliance spike.
    spike_max_kw          : Max extra kW during a spike event.
    num_regions           : Geographic cluster count.
    stream_delay_seconds  : Wall-clock delay between readings (0 = max speed).
    random_seed           : Global RNG seed (None = non-deterministic).
    enable_dp             : Auto-apply DP module in simulate_stream().
    enable_he             : Auto-apply HE module in simulate_stream().
    enable_kafka          : Auto-publish to Kafka in simulate_stream().
    kafka_topic           : Target Kafka topic for auto-publish.
    """

    num_meters: int = 100
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
    # Security pipeline flags (set True to auto-apply in simulate_stream)
    enable_dp: bool = False
    enable_he: bool = False
    enable_kafka: bool = False
    kafka_topic: str = "smart_grid.dp_protected"


# ---------------------------------------------------------------------------
# Raw dataset row
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


# ---------------------------------------------------------------------------
# MeterReading – the central data transfer object
# ---------------------------------------------------------------------------
@dataclass
class MeterReading:
    """
    A single reading from one virtual smart meter.

    Security pipeline fields
    ------------------------
    privacy_noise_applied : ε value set by DifferentialPrivacyModule.
    encrypted_payload     : Serialised CKKS ciphertext set by HEModule.
    """

    meter_id: str
    timestamp: str
    energy_usage: float
    voltage: float
    current: float
    sub_metering: Dict[str, float]
    region_id: int
    is_peak_hour: bool
    is_weekend: bool
    privacy_noise_applied: Optional[float] = None
    encrypted_payload: Optional[bytes] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if d["encrypted_payload"] is not None:
            d["encrypted_payload"] = d["encrypted_payload"].hex()
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# SmartMeter
# ---------------------------------------------------------------------------
class SmartMeter:
    """
    Virtual smart meter.  Transforms raw dataset rows into per-meter readings
    with realistic noise, peak-hour effects, and appliance spikes.

    Parameters
    ----------
    meter_id  : Unique string identifier.
    config    : Shared SimulatorConfig.
    region_id : Geographic cluster label.
    rng       : Per-meter seeded Random instance.
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

        self._power_scale: float = rng.uniform(0.6, 1.6)
        self._voltage_offset: float = rng.uniform(-5.0, 5.0)
        self._current_offset: float = rng.uniform(-0.5, 0.5)
        self._sub_weights: Tuple[float, float, float] = self._sample_sub_weights()

        logger.debug(
            "SmartMeter %s | region=%d | scale=%.3f",
            meter_id,
            region_id,
            self._power_scale,
        )

    # ------------------------------------------------------------------
    def _sample_sub_weights(self) -> Tuple[float, float, float]:
        w = [self._rng.uniform(0.1, 1.0) for _ in range(3)]
        total = sum(w)
        return tuple(x / total for x in w)  # type: ignore[return-value]

    def _is_peak(self, ts: datetime) -> bool:
        s, e = self.config.peak_hours
        return s <= ts.hour < e

    def _is_weekend(self, ts: datetime) -> bool:
        return ts.weekday() >= 5

    # ------------------------------------------------------------------
    def apply_random_variation(self, base_value: float, ts: datetime) -> float:
        """Scale, add context multipliers, noise, and spike events."""
        value = base_value * self._power_scale
        if self._is_peak(ts):
            value *= self.config.peak_multiplier
        if self._is_weekend(ts):
            value *= self.config.weekend_multiplier
        value += self._rng.gauss(0.0, self.config.noise_std)
        if self._rng.random() < self.config.spike_probability:
            spike = self._rng.uniform(0.1, self.config.spike_max_kw)
            value += spike
            logger.debug("%s spike +%.3f kW at %s", self.meter_id, spike, ts)
        return max(0.0, round(value, 4))

    def generate_reading(self, raw: RawReading) -> MeterReading:
        """Convert one RawReading into a MeterReading for this meter."""
        ts = raw.timestamp
        base_power = raw.global_active_power or 1.0
        base_voltage = raw.voltage or 230.0
        base_current = raw.global_intensity or 5.0
        base_sm1 = raw.sub_metering_1 or 0.0
        base_sm2 = raw.sub_metering_2 or 0.0
        base_sm3 = raw.sub_metering_3 or 0.0

        energy = self.apply_random_variation(base_power, ts)
        voltage = round(
            base_voltage + self._voltage_offset + self._rng.gauss(0, 0.5), 2
        )
        current = round(
            max(0.0, base_current + self._current_offset + self._rng.gauss(0, 0.1)), 3
        )

        total_sub = base_sm1 + base_sm2 + base_sm3
        wk, wl, wh = self._sub_weights
        sub_metering = {
            "kitchen": round(total_sub * wk, 3),
            "laundry": round(total_sub * wl, 3),
            "hvac": round(total_sub * wh, 3),
        }

        return MeterReading(
            meter_id=self.meter_id,
            timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
            energy_usage=energy,
            voltage=voltage,
            current=current,
            sub_metering=sub_metering,
            region_id=self.region_id,
            is_peak_hour=self._is_peak(ts),
            is_weekend=self._is_weekend(ts),
        )

    def stream_data(
        self,
        raw_stream: Iterable[RawReading],
        privacy_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        encryption_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
    ) -> Generator[MeterReading, None, None]:
        """
        Yield MeterReadings from a raw-row iterable, passing each through the
        optional DP and HE hooks.
        """
        for raw in raw_stream:
            reading = self.generate_reading(raw)
            if privacy_hook:
                reading = privacy_hook(reading)
            if encryption_hook:
                reading = encryption_hook(reading)
            if self.config.stream_delay_seconds > 0:
                time.sleep(self.config.stream_delay_seconds)
            yield reading


# ---------------------------------------------------------------------------
# SmartGridSimulator
# ---------------------------------------------------------------------------
class SmartGridSimulator:
    """
    Orchestrates N virtual smart meters over a shared dataset.

    Security module integration
    ---------------------------
    Pass module instances at construction time; they are applied automatically
    when the corresponding config flags are set, or explicitly via the hook
    parameters on simulate_stream() / async_stream().

    Kafka integration
    -----------------
    Pass a KafkaStreamProducer at construction time.  When config.enable_kafka
    is True every reading is published automatically in simulate_stream().

    Parameters
    ----------
    dataset_path   : Path to ``household_power_consumption.txt``.
    num_meters     : Number of virtual meters.
    config         : Optional SimulatorConfig.
    dp_module      : Optional DifferentialPrivacyModule.
    he_module      : Optional HomomorphicEncryptionModule.
    kafka_producer : Optional KafkaStreamProducer.
    """

    _COLUMNS = [
        "Date",
        "Time",
        "Global_active_power",
        "Global_reactive_power",
        "Voltage",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3",
    ]

    def __init__(
        self,
        dataset_path: str,
        num_meters: int = 100,
        config: Optional[SimulatorConfig] = None,
        dp_module: Optional["DifferentialPrivacyModule"] = None,
        he_module: Optional["HomomorphicEncryptionModule"] = None,
        kafka_producer: Optional["KafkaStreamProducer"] = None,
    ) -> None:
        self.dataset_path = Path(dataset_path)
        self.config = config or SimulatorConfig(num_meters=num_meters)
        self.config.num_meters = num_meters

        if self.config.random_seed is not None:
            random.seed(self.config.random_seed)

        self._raw_rows: List[RawReading] = []
        self.meters: List[SmartMeter] = []

        # Security modules (optional; can be injected or set later)
        self.dp_module = dp_module
        self.he_module = he_module
        self.kafka_producer = kafka_producer

        logger.info(
            "SmartGridSimulator | meters=%d | dp=%s | he=%s | kafka=%s",
            num_meters,
            "✓" if dp_module else "✗",
            "✓" if he_module else "✗",
            "✓" if kafka_producer else "✗",
        )

    # ------------------------------------------------------------------
    # 1. Dataset loading
    # ------------------------------------------------------------------

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
        return RawReading(
            timestamp=ts,
            global_active_power=self._parse_float(row.get("Global_active_power", "")),
            global_reactive_power=self._parse_float(
                row.get("Global_reactive_power", "")
            ),
            voltage=self._parse_float(row.get("Voltage", "")),
            global_intensity=self._parse_float(row.get("Global_intensity", "")),
            sub_metering_1=self._parse_float(row.get("Sub_metering_1", "")),
            sub_metering_2=self._parse_float(row.get("Sub_metering_2", "")),
            sub_metering_3=self._parse_float(row.get("Sub_metering_3", "")),
        )

    def load_dataset(self) -> None:
        """
        Load and preprocess the UCI dataset.

        Falls back to synthetic rows if the file is absent.
        """
        if not self.dataset_path.exists():
            raise FileNotFoundError(
                f"Dataset not found: {self.dataset_path}\n"
                "Download: https://archive.ics.uci.edu/dataset/235/"
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

    # ------------------------------------------------------------------
    # 2. Meter creation
    # ------------------------------------------------------------------

    def create_meters(self) -> None:
        """Instantiate num_meters SmartMeter objects with distinct profiles."""
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
    # 3a. Sync streaming
    # ------------------------------------------------------------------

    def simulate_stream(
        self,
        privacy_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        encryption_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        kafka_topic: Optional[str] = None,
    ) -> Generator[MeterReading, None, None]:
        """
        Yield MeterReading objects for every (meter × dataset-row) pair.

        Auto-applies DP / HE modules and Kafka publishing when enabled in
        config or when the corresponding module is attached.

        Parameters
        ----------
        privacy_hook    : Override / supplement the attached dp_module.
        encryption_hook : Override / supplement the attached he_module.
        kafka_topic     : Override config.kafka_topic for auto-publish.

        Yields
        ------
        MeterReading (after DP and/or HE if configured).
        """
        if not self.meters:
            raise RuntimeError("Call create_meters() before simulate_stream().")
        if not self._raw_rows:
            raise RuntimeError("Call load_dataset() before simulate_stream().")

        total = len(self._raw_rows) * len(self.meters)
        logger.info(
            "Stream start: %d meters × %d rows = %d readings",
            len(self.meters),
            len(self._raw_rows),
            total,
        )

        # Build the effective hooks chain
        def _dp_hook(r: MeterReading) -> MeterReading:
            if privacy_hook:
                r = privacy_hook(r)
            if self.config.enable_dp and self.dp_module:
                r = self.dp_module.apply(r)
            return r

        def _he_hook(r: MeterReading) -> MeterReading:
            if encryption_hook:
                r = encryption_hook(r)
            if self.config.enable_he and self.he_module:
                self.he_module.encrypt(r)
            return r

        topic = kafka_topic or self.config.kafka_topic

        for raw in self._raw_rows:
            for meter in self.meters:
                reading = meter.generate_reading(raw)
                reading = _dp_hook(reading)
                reading = _he_hook(reading)

                # Kafka: sync publish via mock broker (no event loop needed)
                if self.config.enable_kafka and self.kafka_producer:
                    try:
                        self.kafka_producer.publish_sync(reading, topic=topic)
                    except Exception as exc:
                        logger.warning("Kafka publish failed: %s", exc)

                if self.config.stream_delay_seconds > 0:
                    time.sleep(self.config.stream_delay_seconds / len(self.meters))

                yield reading

    # ------------------------------------------------------------------
    # 3b. Async streaming (for FastAPI)
    # ------------------------------------------------------------------

    async def async_stream(
        self,
        privacy_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        encryption_hook: Optional[Callable[[MeterReading], MeterReading]] = None,
        kafka_topic: Optional[str] = None,
        yield_delay: float = 0.0,
    ) -> AsyncGenerator[MeterReading, None]:
        """
        Async generator version of simulate_stream() for use with FastAPI
        Server-Sent Events (SSE) and WebSocket endpoints.

        Parameters
        ----------
        privacy_hook    : Optional DP hook.
        encryption_hook : Optional HE hook.
        kafka_topic     : Override for Kafka topic.
        yield_delay     : asyncio.sleep() between yields (simulates real-time).

        Yields
        ------
        MeterReading objects.
        """
        if not self.meters:
            raise RuntimeError("Call create_meters() before async_stream().")
        if not self._raw_rows:
            raise RuntimeError("Call load_dataset() before async_stream().")

        topic = kafka_topic or self.config.kafka_topic

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

                # Kafka: async publish
                if self.config.enable_kafka and self.kafka_producer:
                    try:
                        await self.kafka_producer.publish_reading(reading, topic=topic)
                    except Exception as exc:
                        logger.warning("Async Kafka publish failed: %s", exc)

                if yield_delay > 0:
                    await asyncio.sleep(yield_delay)

                yield reading

    # ------------------------------------------------------------------
    # 4. Batch export
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # 5. Aggregation helpers (for HE use-case)
    # ------------------------------------------------------------------

    def collect_encrypted_batch(
        self,
        max_records: int = 100,
    ) -> List["EncryptedReading"]:
        """
        Run the full pipeline (DP + HE) and collect EncryptedReading objects.
        Used by the aggregation server to perform homomorphic summation.

        Requires he_module to be attached and config.enable_he = True.
        """
        if not self.he_module:
            raise RuntimeError(
                "he_module must be attached to collect encrypted batches."
            )

        enc_readings: List[Any] = []
        for i, reading in enumerate(self.simulate_stream()):
            if i >= max_records:
                break
            enc = self.he_module.encrypt(reading)
            enc_readings.append(enc)

        return enc_readings

    # ------------------------------------------------------------------
    # 6. Security module status
    # ------------------------------------------------------------------

    def security_status(self) -> Dict[str, Any]:
        """Return a summary of attached security modules and their status."""
        status: Dict[str, Any] = {
            "dp_module_attached": self.dp_module is not None,
            "he_module_attached": self.he_module is not None,
            "kafka_producer_attached": self.kafka_producer is not None,
            "dp_enabled": self.config.enable_dp,
            "he_enabled": self.config.enable_he,
            "kafka_enabled": self.config.enable_kafka,
        }
        if self.dp_module:
            status["dp_budget_report"] = self.dp_module.budget_report()
        if self.he_module:
            status["he_context"] = self.he_module.context_summary()
        if self.kafka_producer:
            status["kafka_stats"] = self.kafka_producer.stats()
        return status


# ---------------------------------------------------------------------------
# Synthetic data generator (no dataset file required)
# ---------------------------------------------------------------------------


def generate_synthetic_rows(n: int = 1440, seed: int = 0) -> List[RawReading]:
    """
    Generate n synthetic RawReading objects for testing / CI.

    Uses a realistic diurnal consumption pattern.
    """
    rng = random.Random(seed)
    base_ts = datetime(2007, 2, 1, 0, 0, 0)
    rows: List[RawReading] = []
    for i in range(n):
        ts = base_ts + timedelta(minutes=i)
        hour = ts.hour
        diurnal = (
            0.5 + 1.0 * math.sin(math.pi * (hour - 6) / 14) if 6 <= hour < 20 else 0.3
        )
        rows.append(
            RawReading(
                timestamp=ts,
                global_active_power=round(max(0.1, diurnal + rng.gauss(0, 0.1)), 4),
                global_reactive_power=round(max(0.0, 0.1 + rng.gauss(0, 0.02)), 4),
                voltage=round(230.0 + rng.gauss(0, 1.5), 2),
                global_intensity=round(max(0.5, diurnal * 4 + rng.gauss(0, 0.2)), 3),
                sub_metering_1=round(max(0.0, diurnal * 5 + rng.gauss(0, 0.5)), 3),
                sub_metering_2=round(max(0.0, rng.uniform(0, 2)), 3),
                sub_metering_3=round(max(0.0, diurnal * 8 + rng.gauss(0, 1)), 3),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Standalone example security stubs (kept for backward compatibility)
# ---------------------------------------------------------------------------


def example_differential_privacy_hook(
    reading: MeterReading,
    epsilon: float = 1.0,
) -> MeterReading:
    """Laplace mechanism stub (use dp_module.DifferentialPrivacyModule in production)."""
    sensitivity = 1.0
    scale = sensitivity / epsilon
    noise = random.gauss(0, scale * math.sqrt(2) / 2)
    reading.energy_usage = max(0.0, round(reading.energy_usage + noise, 4))
    reading.privacy_noise_applied = epsilon
    return reading


def example_homomorphic_encryption_hook(reading: MeterReading) -> MeterReading:
    """Trivial bytes stub (use he_module.HomomorphicEncryptionModule in production)."""
    reading.encrypted_payload = str(reading.energy_usage).encode("utf-8")
    return reading


# ---------------------------------------------------------------------------
# Entry point – runnable example with all modules integrated
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # -----------------------------------------------------------------------
    # Attempt to import the full security modules; fall back to stubs
    # -----------------------------------------------------------------------
    try:
        from dp_module import DifferentialPrivacyModule, DPConfig

        dp_available = True
    except ImportError:
        dp_available = False
        logger.warning("dp_module.py not found – using stub DP hook.")

    try:
        from he_module import HomomorphicEncryptionModule, HEConfig

        he_available = True
    except ImportError:
        he_available = False
        logger.warning("he_module.py not found – using stub HE hook.")

    try:
        from kafka_producer import KafkaStreamProducer, KafkaConfig

        kafka_available = True
    except ImportError:
        kafka_available = False
        logger.warning("kafka_producer.py not found – Kafka disabled.")

    # -----------------------------------------------------------------------
    # Build security modules
    # -----------------------------------------------------------------------
    dp_mod = DifferentialPrivacyModule(DPConfig(epsilon=0.5)) if dp_available else None
    he_mod = HomomorphicEncryptionModule(HEConfig()) if he_available else None
    kp = (
        KafkaStreamProducer(
            KafkaConfig(use_mock=True)
        )  # mock broker, no real Kafka needed
        if kafka_available
        else None
    )

    # -----------------------------------------------------------------------
    # Configure simulator
    # -----------------------------------------------------------------------
    DATASET_FILE = "household_power_consumption.txt"
    NUM_METERS = 100
    MAX_DISPLAY = 5

    sim_config = SimulatorConfig(
        num_meters=NUM_METERS,
        sample_size=2880,  # two simulated days
        noise_std=0.05,
        peak_hours=(17, 21),
        peak_multiplier=1.35,
        weekend_multiplier=0.85,
        spike_probability=0.02,
        spike_max_kw=1.5,
        num_regions=5,
        stream_delay_seconds=0.0,
        random_seed=42,
        enable_dp=dp_available,
        enable_he=he_available,
        enable_kafka=kafka_available,
    )

    simulator = SmartGridSimulator(
        dataset_path=DATASET_FILE,
        num_meters=NUM_METERS,
        config=sim_config,
        dp_module=dp_mod,
        he_module=he_mod,
        kafka_producer=kp,
    )

    # -----------------------------------------------------------------------
    # Load dataset (fall back to synthetic data if file absent)
    # -----------------------------------------------------------------------
    try:
        simulator.load_dataset()
    except FileNotFoundError as exc:
        logger.warning("%s", exc)
        logger.warning("Falling back to synthetic data.")
        simulator._raw_rows = generate_synthetic_rows(n=1440, seed=42)
        logger.info("Synthetic data: %d rows.", len(simulator._raw_rows))

    simulator.create_meters()

    # -----------------------------------------------------------------------
    # Print security module status
    # -----------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("  Security Module Status")
    print("=" * 72)
    print(json.dumps(simulator.security_status(), indent=2, default=str))

    # -----------------------------------------------------------------------
    # Stream and display first MAX_DISPLAY readings
    # -----------------------------------------------------------------------
    print("\n" + "=" * 72)
    print(f"  Smart Grid Stream  —  first {MAX_DISPLAY} readings")
    print("=" * 72 + "\n")

    for idx, record in enumerate(simulator.simulate_stream()):
        if idx >= MAX_DISPLAY:
            break
        print(record.to_json())
        print("-" * 72)

    # -----------------------------------------------------------------------
    # Export batch to JSONL
    # -----------------------------------------------------------------------
    simulator.export_batch(max_records=200, output_path="output_batch.jsonl")

    # -----------------------------------------------------------------------
    # DP budget report
    # -----------------------------------------------------------------------
    if dp_mod:
        print("\n" + "=" * 72)
        print("  Differential Privacy Budget Report")
        print("=" * 72)
        print(json.dumps(dp_mod.budget_report(), indent=2))

    # -----------------------------------------------------------------------
    # Kafka stats
    # -----------------------------------------------------------------------
    if kp:
        print("\n" + "=" * 72)
        print("  Kafka Producer Stats")
        print("=" * 72)
        print(json.dumps(kp.stats(), indent=2))

    # -----------------------------------------------------------------------
    # HE aggregation demo
    # -----------------------------------------------------------------------
    if he_mod:
        print("\n" + "=" * 72)
        print("  Homomorphic Encryption Aggregation Demo")
        print("  (Sum 10 encrypted readings without decrypting individually)")
        print("=" * 72)

        # Collect 10 encrypted readings
        enc_batch = simulator.collect_encrypted_batch(max_records=10)
        agg_ct = he_mod.aggregate_ciphertexts(enc_batch)
        agg_result = he_mod.decrypt_aggregate(agg_ct)
        print(f"\n  Aggregated sums over {len(enc_batch)} encrypted readings:")
        for field_name, total in agg_result.items():
            print(f"    {field_name:20s}: {total:.4f}")

    print("\n✓ Pipeline complete.")
    print(
        "  meter_data → dp_module → he_module → kafka_producer → aggregation_server\n"
    )
