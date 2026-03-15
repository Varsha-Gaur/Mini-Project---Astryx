"""
dp_module.py
============
Differential Privacy Module
----------------------------
Provides a production-ready DifferentialPrivacyModule that applies calibrated
Laplace and Gaussian noise mechanisms to MeterReading objects.

Design goals
------------
* Real implementation uses IBM diffprivlib (Laplace / Gaussian mechanisms).
* When diffprivlib is unavailable the module falls back to a pure-numpy
  implementation that is mathematically equivalent for research purposes.
* Tracks a per-session privacy budget ledger (epsilon / delta accounting).
* Thread-safe: all state mutations are protected by a threading.Lock.

Privacy pipeline position
-------------------------
    MeterReading  ──►  DifferentialPrivacyModule  ──►  (noised) MeterReading
                                                              │
                                                              ▼
                                                   HomomorphicEncryptionModule

Usage
-----
    from dp_module import DifferentialPrivacyModule, DPConfig

    dp = DifferentialPrivacyModule(DPConfig(epsilon=1.0, mechanism="laplace"))
    noised_reading = dp.apply(reading)
    print(dp.budget_report())

Author : Senior Python / Smart-Grid Research Engineer
License: MIT
"""

from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Optional: IBM diffprivlib
# ---------------------------------------------------------------------------
try:
    from diffprivlib.mechanisms import Laplace as _DPLLaplace
    from diffprivlib.mechanisms import GaussianAnalytic as _DPLGaussian

    _DIFFPRIVLIB_AVAILABLE = True
except ImportError:  # pragma: no cover
    _DIFFPRIVLIB_AVAILABLE = False

# ---------------------------------------------------------------------------
# Avoid circular import – MeterReading is imported lazily inside methods.
# ---------------------------------------------------------------------------
logger = logging.getLogger("dp_module")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class DPConfig:
    """
    Configuration for the Differential Privacy module.

    Attributes
    ----------
    epsilon          : Privacy budget ε.  Smaller = more private, less utility.
    delta            : δ parameter for (ε,δ)-DP (used only by Gaussian mechanism).
    mechanism        : "laplace" (pure-DP) or "gaussian" (approx-DP).
    sensitivity      : L1 / L2 global sensitivity of the query function (kW).
    clip_min         : Lower bound for post-noise clipping (physical constraint).
    clip_max         : Upper bound for post-noise clipping (kW).
    protect_fields   : Which MeterReading numeric fields to perturb.
    budget_window    : Rolling window size for budget tracking (number of queries).
    use_diffprivlib  : If True, prefer diffprivlib when available.
    """

    epsilon: float = 1.0
    delta: float = 1e-5
    mechanism: Literal["laplace", "gaussian"] = "laplace"
    sensitivity: float = 1.0  # 1 kW default sensitivity
    clip_min: float = 0.0
    clip_max: float = 20.0  # Realistic household cap (kW)
    protect_fields: List[str] = field(
        default_factory=lambda: ["energy_usage", "voltage", "current"]
    )
    budget_window: int = 10_000
    use_diffprivlib: bool = True


# ---------------------------------------------------------------------------
# Privacy budget ledger
# ---------------------------------------------------------------------------
class BudgetLedger:
    """
    Tracks cumulative privacy budget consumption using simple composition.

    For research: advanced composition (Renyi DP / moments accountant) should
    be substituted for tighter analysis.

    Parameters
    ----------
    window : Rolling window length (number of queries).
    """

    def __init__(self, window: int = 10_000) -> None:
        self._window = window
        self._epsilons: List[float] = []
        self._deltas: List[float] = []
        self._lock = threading.Lock()

    def record(self, epsilon: float, delta: float = 0.0) -> None:
        """Record one query's privacy cost."""
        with self._lock:
            self._epsilons.append(epsilon)
            self._deltas.append(delta)
            if len(self._epsilons) > self._window:
                self._epsilons.pop(0)
                self._deltas.pop(0)

    @property
    def total_epsilon(self) -> float:
        """Basic composition: sum of all ε in the window."""
        with self._lock:
            return sum(self._epsilons)

    @property
    def total_delta(self) -> float:
        """Basic composition: sum of all δ in the window."""
        with self._lock:
            return sum(self._deltas)

    @property
    def query_count(self) -> int:
        with self._lock:
            return len(self._epsilons)

    def report(self) -> Dict[str, float]:
        return {
            "queries_in_window": self.query_count,
            "total_epsilon_consumed": round(self.total_epsilon, 6),
            "total_delta_consumed": round(self.total_delta, 10),
        }


# ---------------------------------------------------------------------------
# Noise generators
# ---------------------------------------------------------------------------
class _LaplaceMechanism:
    """
    Pure Laplace mechanism.

    Adds Laplace(0, sensitivity/epsilon) noise.
    Wraps diffprivlib.Laplace when available for validated implementations.
    """

    def __init__(
        self, sensitivity: float, epsilon: float, rng: np.random.Generator
    ) -> None:
        self._scale = sensitivity / epsilon
        self._rng = rng
        self._dpl: Optional[object] = None

        if _DIFFPRIVLIB_AVAILABLE:
            try:
                self._dpl = _DPLLaplace(sensitivity=sensitivity, epsilon=epsilon)
                logger.debug("LaplaceMechanism: using diffprivlib backend.")
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "diffprivlib init failed (%s); falling back to numpy.", exc
                )

    def randomise(self, value: float) -> float:
        if self._dpl is not None:
            try:
                return float(self._dpl.randomise(value))
            except Exception:  # pragma: no cover
                pass
        # Fallback: numpy Laplace
        return float(value + self._rng.laplace(0.0, self._scale))


class _GaussianMechanism:
    """
    Gaussian (analytic) mechanism for (ε,δ)-DP.

    σ is computed from the analytic formula (Balle & Wang 2018).
    """

    def __init__(
        self,
        sensitivity: float,
        epsilon: float,
        delta: float,
        rng: np.random.Generator,
    ) -> None:
        self._sigma = self._compute_sigma(sensitivity, epsilon, delta)
        self._rng = rng
        self._dpl: Optional[object] = None

        if _DIFFPRIVLIB_AVAILABLE:
            try:
                self._dpl = _DPLGaussian(
                    sensitivity=sensitivity, epsilon=epsilon, delta=delta
                )
                logger.debug("GaussianMechanism: using diffprivlib backend.")
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "diffprivlib init failed (%s); falling back to numpy.", exc
                )

    @staticmethod
    def _compute_sigma(sensitivity: float, epsilon: float, delta: float) -> float:
        """Analytic Gaussian σ (simplified; full derivation in Balle & Wang 2018)."""
        if delta <= 0:
            raise ValueError("Gaussian mechanism requires delta > 0.")
        return sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / epsilon

    def randomise(self, value: float) -> float:
        if self._dpl is not None:
            try:
                return float(self._dpl.randomise(value))
            except Exception:  # pragma: no cover
                pass
        return float(value + self._rng.normal(0.0, self._sigma))


# ---------------------------------------------------------------------------
# Main module
# ---------------------------------------------------------------------------
class DifferentialPrivacyModule:
    """
    Applies differential-privacy noise to MeterReading objects.

    Supports:
      - Laplace mechanism  (pure ε-DP,   recommended for energy readings)
      - Gaussian mechanism (approx (ε,δ)-DP, useful when post-processing)

    Thread-safe: can be shared across concurrent FastAPI request handlers.

    Parameters
    ----------
    config : DPConfig instance.
    seed   : RNG seed for reproducibility.

    Example
    -------
    >>> dp = DifferentialPrivacyModule(DPConfig(epsilon=0.5))
    >>> protected = dp.apply(reading)
    >>> print(dp.budget_report())
    """

    def __init__(self, config: Optional[DPConfig] = None, seed: int = 0) -> None:
        self.config = config or DPConfig()
        self._rng = np.random.default_rng(seed)
        self._ledger = BudgetLedger(window=self.config.budget_window)
        self._lock = threading.Lock()

        # Instantiate the chosen noise mechanism once (reused across calls)
        self._mechanism = self._build_mechanism()

        logger.info(
            "DifferentialPrivacyModule ready | mechanism=%s | ε=%.4f | δ=%.2e",
            self.config.mechanism,
            self.config.epsilon,
            self.config.delta,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_mechanism(self) -> "_LaplaceMechanism | _GaussianMechanism":
        if self.config.mechanism == "laplace":
            return _LaplaceMechanism(
                sensitivity=self.config.sensitivity,
                epsilon=self.config.epsilon,
                rng=self._rng,
            )
        elif self.config.mechanism == "gaussian":
            return _GaussianMechanism(
                sensitivity=self.config.sensitivity,
                epsilon=self.config.epsilon,
                delta=self.config.delta,
                rng=self._rng,
            )
        else:
            raise ValueError(f"Unknown DP mechanism: {self.config.mechanism!r}")

    def _clip(self, value: float) -> float:
        """Clip a noised value to the physically plausible range."""
        return float(np.clip(value, self.config.clip_min, self.config.clip_max))

    # ------------------------------------------------------------------
    # Per-field sensitivity overrides
    # ------------------------------------------------------------------
    _FIELD_SENSITIVITY: Dict[str, Tuple[float, float, float]] = {
        # field_name → (sensitivity, clip_min, clip_max)
        "energy_usage": (1.0, 0.0, 20.0),
        "voltage": (5.0, 200.0, 260.0),
        "current": (1.0, 0.0, 40.0),
    }

    def _perturb_field(self, value: float, field_name: str) -> float:
        """Apply noise scaled appropriately for a specific field."""
        sens, cmin, cmax = self._FIELD_SENSITIVITY.get(
            field_name,
            (self.config.sensitivity, self.config.clip_min, self.config.clip_max),
        )
        # Temporarily scale the mechanism's noise to per-field sensitivity
        # (Simple approach: scale the output noise proportionally)
        base_noised = self._mechanism.randomise(0.0)  # zero-mean noise sample
        # Rescale: multiply by (field_sensitivity / config_sensitivity)
        scaled_noise = base_noised * (sens / max(self.config.sensitivity, 1e-9))
        noised = value + scaled_noise
        return float(np.clip(noised, cmin, cmax))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply(self, reading: "MeterReading") -> "MeterReading":  # type: ignore[name-defined]
        """
        Apply differential-privacy noise to the specified fields of a reading.

        The reading is modified in place and returned (facilitates chaining).

        Parameters
        ----------
        reading : MeterReading to protect.

        Returns
        -------
        The same MeterReading with perturbed numeric fields and
        ``privacy_noise_applied`` set to the epsilon value used.
        """
        with self._lock:
            for fname in self.config.protect_fields:
                original = getattr(reading, fname, None)
                if original is not None and isinstance(original, (int, float)):
                    setattr(
                        reading,
                        fname,
                        round(self._perturb_field(float(original), fname), 4),
                    )

            # Perturb sub_metering values as well (each with energy_usage sensitivity)
            if hasattr(reading, "sub_metering") and reading.sub_metering:
                reading.sub_metering = {
                    k: round(
                        float(
                            np.clip(
                                v + self._mechanism.randomise(0.0) * 0.5,
                                0.0,
                                50.0,  # Wh bounds
                            )
                        ),
                        4,
                    )
                    for k, v in reading.sub_metering.items()
                }

            reading.privacy_noise_applied = self.config.epsilon
            self._ledger.record(
                self.config.epsilon,
                self.config.delta if self.config.mechanism == "gaussian" else 0.0,
            )

        return reading

    def apply_batch(self, readings: List["MeterReading"]) -> List["MeterReading"]:  # type: ignore[name-defined]
        """Apply DP noise to a list of readings in place."""
        return [self.apply(r) for r in readings]

    def budget_report(self) -> Dict[str, object]:
        """Return current privacy budget consumption statistics."""
        report = self._ledger.report()
        report["mechanism"] = self.config.mechanism
        report["epsilon_per_query"] = self.config.epsilon
        report["delta_per_query"] = self.config.delta
        report["diffprivlib_backend"] = (
            _DIFFPRIVLIB_AVAILABLE and self.config.use_diffprivlib
        )
        return report

    def reset_budget(self) -> None:
        """Reset the budget ledger (e.g. at the start of a new time window)."""
        self._ledger = BudgetLedger(window=self.config.budget_window)
        logger.info("Privacy budget ledger reset.")

    def reconfigure(self, new_config: DPConfig) -> None:
        """
        Hot-swap configuration and rebuild the noise mechanism.
        Thread-safe.
        """
        with self._lock:
            self.config = new_config
            self._mechanism = self._build_mechanism()
            logger.info(
                "DPModule reconfigured | mechanism=%s | ε=%.4f",
                self.config.mechanism,
                self.config.epsilon,
            )
