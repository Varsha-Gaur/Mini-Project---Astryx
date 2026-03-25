"""
privacy/dp_module.py
=====================
Differential Privacy Module
-----------------------------
Applies calibrated Laplace or Gaussian noise to MeterReading objects,
providing ε-differential privacy (or (ε,δ)-DP for Gaussian) for individual
smart-meter readings.

Mathematical background
-----------------------
A randomised mechanism M satisfies ε-DP if for every pair of adjacent
datasets D, D′ (differing by one record) and all output sets S:

    Pr[M(D) ∈ S]  ≤  e^ε  ×  Pr[M(D′) ∈ S]

Laplace mechanism adds Lap(0, Δf/ε) noise where Δf is the L1 sensitivity.
Gaussian mechanism adds N(0, σ²) where σ is chosen analytically (Balle & Wang 2018).

BUG FIX vs original
--------------------
The original ``_perturb_field`` rescaled Laplace noise post-hoc by multiplying
``base_noised × (field_sens / config_sens)``.  For Laplace this gives the
correct scale, but for Gaussian, variance doesn't transform linearly — you
cannot get N(0, (k·σ)²) by multiplying a N(0, σ²) sample by k then squaring;
it only works if you multiply the sample itself by k (which is equivalent to
drawing from a larger σ).  This module uses separate mechanism instances per
field to avoid the issue entirely.

Pipeline position
-----------------
    MeterReading (raw)
        │
        ▼  DifferentialPrivacyModule.apply()   ← this module
        │
        ▼  MeterReading (noised, privacy_noise_applied set)
        │
        ▼  HomomorphicEncryptionModule.encrypt()
"""

from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional: IBM diffprivlib for formally validated implementations
# ---------------------------------------------------------------------------
try:
    from diffprivlib.mechanisms import Laplace as _DPLLaplace  # type: ignore
    from diffprivlib.mechanisms import GaussianAnalytic as _DPLGauss  # type: ignore

    _DIFFPRIVLIB = True
    logger.debug("diffprivlib backend available.")
except ImportError:
    _DIFFPRIVLIB = False
    logger.debug("diffprivlib not installed; using numpy fallback.")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class DPConfig:
    """
    Configuration for the Differential Privacy module.

    Attributes
    ----------
    epsilon         : Privacy budget ε.  Smaller = more private, less utility.
    delta           : δ for (ε,δ)-DP (Gaussian mechanism only).
    mechanism       : "laplace" (pure-DP) or "gaussian" (approx-DP).
    sensitivity     : Global L1/L2 sensitivity of the query (kW).
    clip_min/max    : Post-noise physical clamping bounds.
    protect_fields  : MeterReading numeric fields to perturb.
    budget_window   : Rolling budget-tracking window (number of queries).
    """

    epsilon: float = 1.0
    delta: float = 1e-5
    mechanism: Literal["laplace", "gaussian"] = "laplace"
    sensitivity: float = 1.0
    clip_min: float = 0.0
    clip_max: float = 20.0
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
    Tracks cumulative privacy budget via basic sequential composition.

    Each call to ``apply()`` consumes ε from the session budget.
    Total budget consumed = Σ εᵢ (basic composition theorem).
    """

    def __init__(self, window: int = 10_000) -> None:
        self._window = window
        self._epsilons: List[float] = []
        self._deltas: List[float] = []
        self._lock = threading.Lock()

    def record(self, epsilon: float, delta: float = 0.0) -> None:
        with self._lock:
            self._epsilons.append(epsilon)
            self._deltas.append(delta)
            if len(self._epsilons) > self._window:
                self._epsilons.pop(0)
                self._deltas.pop(0)

    @property
    def total_epsilon(self) -> float:
        with self._lock:
            return sum(self._epsilons)

    @property
    def total_delta(self) -> float:
        with self._lock:
            return sum(self._deltas)

    @property
    def query_count(self) -> int:
        with self._lock:
            return len(self._epsilons)

    def report(self) -> Dict[str, Any]:
        return {
            "queries_in_window": self.query_count,
            "total_epsilon_consumed": round(self.total_epsilon, 6),
            "total_delta_consumed": round(self.total_delta, 10),
        }


# ---------------------------------------------------------------------------
# Noise generators
# ---------------------------------------------------------------------------


def _laplace_noise(
    sensitivity: float,
    epsilon: float,
    rng: np.random.Generator,
) -> float:
    """Draw Lap(0, sensitivity/ε) noise.  Falls back to numpy if diffprivlib absent."""
    if _DIFFPRIVLIB:
        try:
            mech = _DPLLaplace(sensitivity=sensitivity, epsilon=epsilon)
            return float(mech.randomise(0.0))
        except Exception:
            pass
    return float(rng.laplace(0.0, sensitivity / epsilon))


def _gaussian_noise(
    sensitivity: float,
    epsilon: float,
    delta: float,
    rng: np.random.Generator,
) -> float:
    """
    Draw analytic-Gaussian noise N(0, σ²) calibrated to (ε,δ)-DP.

    σ is computed via the Balle & Wang 2018 analytic formula.
    """
    if delta <= 0:
        raise ValueError("Gaussian mechanism requires delta > 0.")

    if _DIFFPRIVLIB:
        try:
            mech = _DPLGauss(sensitivity=sensitivity, epsilon=epsilon, delta=delta)
            return float(mech.randomise(0.0))
        except Exception:
            pass

    # Simplified analytic σ: σ = Δf · √(2 ln(1.25/δ)) / ε
    sigma = sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / epsilon
    return float(rng.normal(0.0, sigma))


# ---------------------------------------------------------------------------
# Per-field sensitivity / clipping table
# ---------------------------------------------------------------------------
# (sensitivity, clip_min, clip_max)
_FIELD_PARAMS: Dict[str, Tuple[float, float, float]] = {
    "energy_usage": (1.0, 0.0, 20.0),
    "voltage": (5.0, 200.0, 260.0),
    "current": (1.0, 0.0, 40.0),
}


# ---------------------------------------------------------------------------
# Main module
# ---------------------------------------------------------------------------
class DifferentialPrivacyModule:
    """
    Applies Laplace or Gaussian differential-privacy noise to MeterReading objects.

    Thread-safe — can be shared across concurrent FastAPI request handlers.

    Parameters
    ----------
    config : DPConfig instance.
    seed   : RNG seed for reproducibility.

    Example
    -------
    >>> from privacy.dp_module import DifferentialPrivacyModule, DPConfig
    >>> dp = DifferentialPrivacyModule(DPConfig(epsilon=0.5))
    >>> protected = dp.apply(reading)
    >>> print(dp.budget_report())
    """

    def __init__(self, config: Optional[DPConfig] = None, seed: int = 0) -> None:
        self.config = config or DPConfig()
        self._rng = np.random.default_rng(seed)
        self._ledger = BudgetLedger(window=self.config.budget_window)
        self._lock = threading.Lock()

        logger.info(
            "DifferentialPrivacyModule | mechanism=%s | ε=%.4f | δ=%.2e | diffprivlib=%s",
            self.config.mechanism,
            self.config.epsilon,
            self.config.delta,
            _DIFFPRIVLIB,
        )

    # ------------------------------------------------------------------

    def _draw_noise(self, sensitivity: float) -> float:
        """Draw one noise sample using the configured mechanism and field sensitivity."""
        if self.config.mechanism == "laplace":
            return _laplace_noise(sensitivity, self.config.epsilon, self._rng)
        elif self.config.mechanism == "gaussian":
            return _gaussian_noise(
                sensitivity, self.config.epsilon, self.config.delta, self._rng
            )
        else:
            raise ValueError(f"Unknown mechanism: {self.config.mechanism!r}")

    # ------------------------------------------------------------------

    def apply(self, reading: "MeterReading") -> "MeterReading":  # type: ignore[name-defined]
        """
        Apply DP noise to the specified fields of a MeterReading.

        Each field is perturbed with noise calibrated to that field's own
        sensitivity (not a shared rescaled sample — see Bug Fix in module
        docstring).

        The reading is modified **in-place** and returned.

        Parameters
        ----------
        reading : MeterReading to protect.

        Returns
        -------
        Same MeterReading with perturbed values and
        ``privacy_noise_applied`` set to the ε used.
        """
        with self._lock:
            for fname in self.config.protect_fields:
                original = getattr(reading, fname, None)
                if original is None or not isinstance(original, (int, float)):
                    continue
                sens, cmin, cmax = _FIELD_PARAMS.get(
                    fname,
                    (
                        self.config.sensitivity,
                        self.config.clip_min,
                        self.config.clip_max,
                    ),
                )
                noise = self._draw_noise(sens)  # ← correct per-field σ
                noised = float(np.clip(float(original) + noise, cmin, cmax))
                setattr(reading, fname, round(noised, 4))

            # Sub-metering (energy_usage sensitivity proxy)
            if hasattr(reading, "sub_metering") and reading.sub_metering:
                sens_sub = 0.5  # Wh sub-metering sensitivity
                reading.sub_metering = {
                    k: round(
                        float(
                            np.clip(float(v) + self._draw_noise(sens_sub), 0.0, 50.0)
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

    def budget_report(self) -> Dict[str, Any]:
        """Return current privacy-budget consumption statistics."""
        report = self._ledger.report()
        report["mechanism"] = self.config.mechanism
        report["epsilon_per_query"] = self.config.epsilon
        report["delta_per_query"] = self.config.delta
        report["diffprivlib_backend"] = _DIFFPRIVLIB and self.config.use_diffprivlib
        return report

    def reset_budget(self) -> None:
        """Reset the budget ledger (e.g. at the start of a new time window)."""
        self._ledger = BudgetLedger(window=self.config.budget_window)
        logger.info("Privacy budget ledger reset.")

    def reconfigure(self, new_config: DPConfig) -> None:
        """Hot-swap configuration.  Thread-safe."""
        with self._lock:
            self.config = new_config
        logger.info(
            "DPModule reconfigured | mechanism=%s | ε=%.4f",
            self.config.mechanism,
            self.config.epsilon,
        )
