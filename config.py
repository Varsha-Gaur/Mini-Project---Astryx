"""
config.py
=========
Central Configuration
----------------------
Single source of truth for every tunable parameter in the
Hybrid Secure Data Aggregation prototype.

Importing ``DEFAULT_CONFIG`` gives every module a consistent
baseline without hard-coding values in multiple places.

Usage
-----
    from config import DEFAULT_CONFIG, AppConfig
    cfg = AppConfig()          # override specific fields
    print(cfg.dp.epsilon)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple


# ---------------------------------------------------------------------------
# Simulator parameters
# ---------------------------------------------------------------------------
@dataclass
class SimulatorSettings:
    """Controls the smart-meter fleet and data generation."""

    num_meters: int = 20  # virtual meters in the fleet
    num_regions: int = 5  # geographic cluster count
    sample_size: int = 1_440  # UCI rows to load (1 day × 1 min)
    use_synthetic: bool = True  # True → skip UCI file lookup
    synthetic_minutes: int = 120  # rows for synthetic mode

    dataset_path: str = "household_power_consumption.txt"

    noise_std: float = 0.05  # Gaussian σ on raw power reading
    peak_hours: Tuple[int, int] = (17, 21)  # weekday peak window
    peak_multiplier: float = 1.35
    weekend_multiplier: float = 0.85
    spike_probability: float = 0.02
    spike_max_kw: float = 1.5

    stream_delay_seconds: float = 0.0  # wall-clock gap between readings
    random_seed: int = 42

    regions: List[str] = field(
        default_factory=lambda: ["north", "south", "east", "west", "central"]
    )


# ---------------------------------------------------------------------------
# Differential-privacy parameters
# ---------------------------------------------------------------------------
@dataclass
class DPSettings:
    """
    Controls the Laplace / Gaussian DP mechanism.

    Lower epsilon → stronger privacy guarantee → more noise added.
    The Laplace mechanism adds Lap(0, sensitivity / ε) noise.
    """

    epsilon: float = 1.0  # privacy budget ε
    delta: float = 1e-5  # δ for (ε,δ)-DP  (Gaussian only)
    mechanism: str = "laplace"  # "laplace" | "gaussian"
    sensitivity: float = 1.0  # L1 sensitivity (kW)
    clip_min: float = 0.0
    clip_max: float = 20.0  # realistic household upper bound (kW)

    protect_fields: List[str] = field(
        default_factory=lambda: ["energy_usage", "voltage", "current"]
    )


# ---------------------------------------------------------------------------
# Homomorphic-encryption parameters
# ---------------------------------------------------------------------------
@dataclass
class HESettings:
    """
    Controls the CKKS homomorphic-encryption context.

    TenSEAL is used when available; MockCKKS (research scaffold) otherwise.
    MockCKKS preserves the correct algebraic semantics but is NOT
    cryptographically secure.
    """

    poly_modulus_degree: int = 8_192
    coeff_mod_bit_sizes: List[int] = field(default_factory=lambda: [60, 40, 40, 60])
    scale_bits: int = 40
    use_tenseal: bool = True  # False → always use MockCKKS

    encrypt_fields: List[str] = field(
        default_factory=lambda: ["energy_usage", "voltage", "current"]
    )


# ---------------------------------------------------------------------------
# Server parameters
# ---------------------------------------------------------------------------
@dataclass
class ServerSettings:
    """FastAPI aggregation-server settings."""

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"


# ---------------------------------------------------------------------------
# Dashboard parameters
# ---------------------------------------------------------------------------
@dataclass
class DashboardSettings:
    """Streamlit dashboard settings."""

    page_title: str = "⚡ SecureGrid Research Dashboard"
    page_icon: str = "⚡"
    refresh_seconds: float = 1.5  # live-simulation tick interval
    max_table_rows: int = 150
    default_meters: int = 8  # pre-selected in sidebar


# ---------------------------------------------------------------------------
# Master configuration object
# ---------------------------------------------------------------------------
@dataclass
class AppConfig:
    """
    Aggregates all sub-configurations.

    Usage
    -----
    >>> from config import AppConfig
    >>> cfg = AppConfig()
    >>> cfg.dp.epsilon = 0.5          # tighten privacy for a demo
    >>> print(cfg.simulator.num_meters)
    20
    """

    simulator: SimulatorSettings = field(default_factory=SimulatorSettings)
    dp: DPSettings = field(default_factory=DPSettings)
    he: HESettings = field(default_factory=HESettings)
    server: ServerSettings = field(default_factory=ServerSettings)
    dashboard: DashboardSettings = field(default_factory=DashboardSettings)


# Default singleton used by all modules
DEFAULT_CONFIG = AppConfig()
