"""
attack_simulator.py
====================
Cyber Attack Simulation Engine
--------------------------------
Implements five realistic attack scenarios against the smart-grid
aggregation pipeline.  Each attack:

  1. Attempts to succeed against an UNPROTECTED pipeline.
  2. Is then run against the SECURED pipeline to show detection.
  3. Returns a detailed AttackResult for the security dashboard.

Attacks
-------
  A1 – Data Inference      Reconstruct individual usage from aggregate
  A2 – Replay Attack       Re-submit an old valid packet
  A3 – Data Tampering      Forge inflated energy readings
  A4 – Man-in-the-Middle   Intercept and alter a packet in transit
  A5 – Traffic Analysis    Infer usage patterns from metadata alone

Design note
-----------
The attacks are intentionally didactic — they show what a student-level
attacker would try and how each security layer blocks them.  They do NOT
attempt to break the underlying crypto.
"""

from __future__ import annotations

import hashlib
import random
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


# ─────────────────────────────────────────────────────────────────────────────
# Result container
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class AttackResult:
    """Outcome of one simulated attack."""

    attack_name: str
    attack_type: str
    description: str
    succeeded_unprotected: bool
    detected_protected: bool
    detection_mechanism: str
    attacker_goal: str
    attacker_result: str
    defender_action: str
    severity: str  # LOW | MEDIUM | HIGH | CRITICAL
    timestamp: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attack_name": self.attack_name,
            "attack_type": self.attack_type,
            "description": self.description,
            "succeeded_unprotected": self.succeeded_unprotected,
            "detected_protected": self.detected_protected,
            "detection_mechanism": self.detection_mechanism,
            "attacker_goal": self.attacker_goal,
            "attacker_result": self.attacker_result,
            "defender_action": self.defender_action,
            "severity": self.severity,
            "timestamp": self.timestamp,
            "time_str": time.strftime("%H:%M:%S", time.localtime(self.timestamp)),
            **self.details,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Helper: make a plausible meter reading dict
# ─────────────────────────────────────────────────────────────────────────────


def _make_reading(
    meter_id: str = "meter_007",
    energy: float = 1.42,
    voltage: float = 231.5,
    current: float = 6.1,
    ts_offset: float = 0.0,  # seconds from now
    nonce: Optional[str] = None,
) -> Dict[str, Any]:
    now = time.time() + ts_offset
    return {
        "meter_id": meter_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
        "timestamp_epoch": now,
        "energy_usage": round(energy, 6),
        "voltage": round(voltage, 2),
        "current": round(current, 3),
        "nonce": nonce or secrets.token_hex(16),
    }


# ─────────────────────────────────────────────────────────────────────────────
# A1 — DATA INFERENCE ATTACK
# ─────────────────────────────────────────────────────────────────────────────


def attack_data_inference(
    n_meters: int = 20,
    n_readings: int = 60,
) -> AttackResult:
    """
    Reconstruct individual meter consumption from aggregate totals.

    Attack method (differencing attack)
    ------------------------------------
    If the server publishes aggregate totals for ALL meters and also
    for ALL-EXCEPT-ONE, the difference reveals that single meter's exact
    usage — even though only aggregates are released.

    Against unprotected pipeline
    ----------------------------
    With no DP noise and group sizes as small as 2, the difference gives
    near-exact individual values.

    Against protected pipeline
    --------------------------
    Differential Privacy adds Laplace noise: published = true + Lap(0, Δf/ε).
    The reconstructed value deviates by the noise magnitude, making
    individual attribution unreliable.  The larger the group, the better
    the protection (composition theorem).
    """
    rng = np.random.default_rng(42)

    # True individual usage values
    true_values = rng.uniform(0.5, 3.5, n_meters)

    # ----- UNPROTECTED: exact aggregates -----
    total_all = float(np.sum(true_values))
    total_without = float(np.sum(true_values[1:]))  # exclude meter_000
    inferred_raw = total_all - total_without  # exact reconstruction
    true_val = float(true_values[0])
    error_raw = abs(inferred_raw - true_val)

    # ----- PROTECTED: DP-noised aggregates -----
    epsilon = 1.0
    sensitivity = 1.0
    noise_scale = sensitivity / epsilon
    dp_total_all = total_all + rng.laplace(0, noise_scale)
    dp_total_without = total_without + rng.laplace(0, noise_scale)
    inferred_dp = dp_total_all - dp_total_without
    error_dp = abs(inferred_dp - true_val)

    # Attack succeeds if error < 0.05 kW (within normal meter resolution)
    succeeded = error_raw < 0.05
    dp_thwarted = error_dp > 0.3  # DP error larger than meter resolution

    return AttackResult(
        attack_name="Data Inference (Differencing)",
        attack_type="INFERENCE",
        description=(
            "Reconstruct individual meter usage by computing the difference "
            "between two overlapping aggregate queries."
        ),
        succeeded_unprotected=succeeded,
        detected_protected=dp_thwarted,
        detection_mechanism="Differential Privacy (Laplace noise, ε=1.0)",
        attacker_goal=f"Infer meter_000 usage (true={true_val:.4f} kW)",
        attacker_result=(
            f"Inferred {inferred_raw:.4f} kW (error={error_raw:.5f} kW) → "
            f"{'SUCCEEDED (unprotected)' if succeeded else 'FAILED'}"
        ),
        defender_action=(
            f"DP noise injected. Inferred {inferred_dp:.4f} kW "
            f"(error={error_dp:.4f} kW) → INFERENCE DISRUPTED"
        ),
        severity="HIGH",
        details={
            "true_value": round(true_val, 4),
            "inferred_raw": round(inferred_raw, 4),
            "inferred_dp": round(inferred_dp, 4),
            "error_raw": round(error_raw, 5),
            "error_dp": round(error_dp, 4),
            "epsilon": epsilon,
            "n_meters": n_meters,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# A2 — REPLAY ATTACK
# ─────────────────────────────────────────────────────────────────────────────


def attack_replay(gateway=None) -> AttackResult:
    """
    Re-submit a previously valid packet to inflate the aggregate.

    Attack method
    -------------
    1. Attacker captures a legitimately signed packet from meter_003.
    2. Waits 5 seconds (or immediately).
    3. Re-submits the identical packet.
    4. In an unprotected system, the aggregate is inflated by one extra reading.

    Against protected pipeline
    --------------------------
    The nonce is checked against the seen-nonce store.  The second
    submission is rejected because the nonce was already consumed.
    Timestamp window validation also catches delayed replays.
    """
    captured_nonce = secrets.token_hex(16)  # the "captured" nonce
    mid = "meter_003"

    # Build the original packet (timestamped NOW — within window)
    original = _make_reading(meter_id="meter_003", energy=2.15, nonce=captured_nonce)

    # ── Unprotected: second submission accepted ──
    unprotected_result = "Second packet ACCEPTED → aggregate inflated by 2.15 kW"

    # ── Protected: nonce already consumed ──
    if gateway is not None:
        import sys, os

        # Sign and submit the original (legitimate)
        sig = gateway.sign_reading(original)
        key = gateway.demo_api_key(mid)
        ok1, _ = gateway.process(original, api_key=key, signature=sig)

        # Re-submit the SAME packet (replay)
        replay = dict(original)  # identical copy — same nonce
        ok2, ev2 = gateway.process(replay, api_key=key, signature=sig)
        protected_result = (
            f"Second packet {'ACCEPTED' if ok2 else 'REJECTED'}: {ev2.description}"
        )
        detected = not ok2
    else:
        # Simulate without live gateway
        protected_result = (
            "Second packet REJECTED: Nonce already consumed — REPLAY BLOCKED"
        )
        detected = True

    return AttackResult(
        attack_name="Replay Attack",
        attack_type="REPLAY",
        description=(
            "Capture a valid signed packet and re-submit it to "
            "inflate the aggregated energy total."
        ),
        succeeded_unprotected=True,
        detected_protected=detected,
        detection_mechanism="Nonce uniqueness check + timestamp window",
        attacker_goal=f"Double-count meter_{mid} reading (2.15 kW × 2)",
        attacker_result=unprotected_result,
        defender_action=protected_result,
        severity="CRITICAL",
        details={
            "captured_nonce": captured_nonce[:16] + "…",
            "meter_id": mid,
            "energy_kw": 2.15,
            "window_seconds": 30,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# A3 — DATA TAMPERING ATTACK
# ─────────────────────────────────────────────────────────────────────────────


def attack_tampering(gateway=None) -> AttackResult:
    """
    Modify a meter reading in transit to manipulate the aggregate.

    Attack method
    -------------
    An attacker with access to the network (or a compromised proxy)
    intercepts a reading and inflates the energy value × 10 before
    forwarding it.  Without integrity verification the server accepts it.

    Against protected pipeline
    --------------------------
    The HMAC-SHA-256 signature covers the energy field.  Changing the
    energy value invalidates the tag.  hmac.compare_digest() returns
    False and the packet is rejected with event_type=TAMPER.
    """
    mid = "meter_011"
    real_e = 1.85

    # Legitimate reading + signature
    reading = _make_reading(meter_id=mid, energy=real_e)

    # Attacker inflates energy × 10
    tampered = dict(reading)
    tampered["energy_usage"] = real_e * 10  # 18.5 kW (fraudulent)

    # ── Unprotected: server accepts inflated value ──
    inflation_kw = tampered["energy_usage"] - real_e

    # ── Protected: HMAC check fails ──
    if gateway is not None:
        orig_sig = gateway.sign_reading(reading)
        key = gateway.demo_api_key(mid)
        ok, ev = gateway.process(tampered, api_key=key, signature=orig_sig)
        protected_result = (
            f"Tampered packet {'ACCEPTED' if ok else 'REJECTED'}: {ev.description}"
        )
        detected = not ok
    else:
        protected_result = (
            f"HMAC verification failed for {mid} — "
            f"energy changed from {real_e} to {real_e * 10:.2f} kW → TAMPER DETECTED"
        )
        detected = True

    return AttackResult(
        attack_name="Data Tampering",
        attack_type="TAMPER",
        description=(
            "Intercept a meter reading and inflate the energy value "
            "to manipulate billing or grid management decisions."
        ),
        succeeded_unprotected=True,
        detected_protected=detected,
        detection_mechanism="HMAC-SHA-256 digital signature verification",
        attacker_goal=f"Inflate {mid} reading from {real_e} → {real_e * 10:.2f} kW",
        attacker_result=(
            f"Inflated energy accepted: +{inflation_kw:.2f} kW injected into aggregate "
            f"(unprotected)"
        ),
        defender_action=protected_result,
        severity="CRITICAL",
        details={
            "original_energy": real_e,
            "tampered_energy": real_e * 10,
            "inflation_kw": round(inflation_kw, 2),
            "meter_id": mid,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# A4 — MAN-IN-THE-MIDDLE (MITM)
# ─────────────────────────────────────────────────────────────────────────────


def attack_mitm(gateway=None) -> AttackResult:
    """
    Intercept the meter→server channel, alter the payload, re-sign with
    attacker's own key, and forward.

    Attack method
    -------------
    If the server does not verify WHICH key was used, an attacker can:
    1. Intercept a reading.
    2. Modify the energy value.
    3. Re-sign with their own compromised key.
    4. Forward to server.

    Against protected pipeline
    --------------------------
    The server derives the expected key from its own KeyManager using
    meter_id + master_secret.  The attacker does not have the master
    secret, so their re-computed HMAC uses a different key → mismatch
    → TAMPER event raised.  This simulates the server acting as the
    verifying party, not trusting a client-provided key.
    """
    mid = "meter_015"
    real_e = 2.30

    reading = _make_reading(meter_id=mid, energy=real_e)

    # Attacker generates their own fake key and re-signs
    attacker_key = secrets.token_bytes(32)
    import hmac as _hmac

    fake_payload = (
        f"{mid}|{reading['timestamp']}|"
        f"{real_e * 5:.6f}|{reading['voltage']:.4f}|"
        f"{reading['current']:.4f}|{reading['nonce']}"
    ).encode()
    fake_sig = _hmac.new(attacker_key, fake_payload, "sha256").hexdigest()

    tampered = dict(reading)
    tampered["energy_usage"] = real_e * 5

    if gateway is not None:
        key = gateway.demo_api_key(mid)
        ok, ev = gateway.process(tampered, api_key=key, signature=fake_sig)
        protected_result = (
            f"MITM packet {'ACCEPTED' if ok else 'REJECTED'}: {ev.description}"
        )
        detected = not ok
    else:
        protected_result = (
            f"Attacker's re-signed packet rejected for {mid}: "
            f"server key ≠ attacker key → SIGNATURE MISMATCH → MITM DETECTED"
        )
        detected = True

    return AttackResult(
        attack_name="Man-in-the-Middle (MITM)",
        attack_type="MITM",
        description=(
            "Intercept meter→server channel, modify payload, "
            "re-sign with attacker's own key and forward."
        ),
        succeeded_unprotected=True,
        detected_protected=detected,
        detection_mechanism="Server-side HMAC key derivation (attacker has no master secret)",
        attacker_goal=f"Replace {mid} reading {real_e} kW → {real_e * 5:.2f} kW",
        attacker_result="Modified packet forwarded successfully (unprotected channel)",
        defender_action=protected_result,
        severity="CRITICAL",
        details={
            "original_energy": real_e,
            "forged_energy": real_e * 5,
            "attacker_key_prefix": attacker_key.hex()[:16] + "…",
            "meter_id": mid,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# A5 — TRAFFIC ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────


def attack_traffic_analysis(n_samples: int = 48) -> AttackResult:
    """
    Infer household occupancy and appliance usage from transmission timing
    and packet sizes alone — without decrypting the content.

    Attack method
    -------------
    Smart meters transmit at predictable intervals.  Even with encrypted
    payloads, an attacker can:
    • Count packets per hour → infer peak-usage times.
    • Measure inter-arrival jitter → distinguishes appliance duty cycles.
    • Correlate traffic bursts with known appliance signatures (NALM).

    Against protected pipeline
    --------------------------
    Differential privacy at the reading level does NOT hide transmission
    metadata.  Mitigation requires:
    • Fixed-rate transmission (constant bit-rate / dummy packets).
    • Random delays on packet emission.
    • Traffic shaping.
    This attack demonstrates a RESIDUAL RISK even with DP + HE.
    """
    rng = np.random.default_rng(7)

    # Simulate 48 half-hour intervals of packet inter-arrival times (ms)
    # Real pattern: correlated with diurnal cycle
    hours = np.arange(n_samples) / 2.0
    diurnal = np.sin(np.pi * (hours - 7) / 14) * 0.5 + 0.5
    diurnal = np.clip(diurnal, 0, 1)

    # Attacker observes inter-arrival jitter (ms)
    base_interval_ms = 60_000
    jitter = (1 - diurnal) * 500 + rng.normal(0, 50, n_samples)
    observed = base_interval_ms + jitter

    # Reconstruct activity score (high jitter → high activity)
    inferred_activity = 1 - (observed - observed.min()) / (
        observed.max() - observed.min() + 1
    )
    correlation = float(np.corrcoef(diurnal, inferred_activity)[0, 1])

    # Attack "succeeds" if Pearson r > 0.7 (strong correlation)
    succeeded = correlation > 0.70

    return AttackResult(
        attack_name="Traffic Analysis",
        attack_type="TRAFFIC_ANALYSIS",
        description=(
            "Infer household occupancy patterns by analysing "
            "packet timing metadata without decrypting content."
        ),
        succeeded_unprotected=succeeded,
        detected_protected=False,  # DP/HE do not stop this; detection is IDS-based
        detection_mechanism="⚠ Residual risk — requires traffic shaping / dummy packets",
        attacker_goal="Infer occupancy schedule from packet inter-arrival jitter",
        attacker_result=(
            f"Activity correlation r={correlation:.3f} "
            f"({'STRONG infer' if succeeded else 'WEAK'}) — pattern extracted from timing alone"
        ),
        defender_action=(
            "DP + HE protect VALUES but not metadata. "
            "Recommend: constant-rate transmission, random packet delays."
        ),
        severity="MEDIUM",
        details={
            "pearson_r": round(correlation, 4),
            "n_intervals": n_samples,
            "mean_jitter_ms": round(float(np.mean(np.abs(jitter))), 2),
            "activity_profile": inferred_activity.round(3).tolist(),
            "hours": hours.tolist(),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Master runner
# ─────────────────────────────────────────────────────────────────────────────


def run_all_attacks(gateway=None) -> List[AttackResult]:
    """
    Execute all five attack simulations and return results.

    Parameters
    ----------
    gateway : SecurityGateway instance (None → simulate without live gateway).
    """
    results = [
        attack_data_inference(),
        attack_replay(gateway),
        attack_tampering(gateway),
        attack_mitm(gateway),
        attack_traffic_analysis(),
    ]
    return results


def run_random_attack(gateway=None) -> AttackResult:
    """Run one randomly selected attack (useful for live demo simulation)."""
    funcs = [
        lambda: attack_data_inference(),
        lambda: attack_replay(gateway),
        lambda: attack_tampering(gateway),
        lambda: attack_mitm(gateway),
        lambda: attack_traffic_analysis(),
    ]
    return random.choice(funcs)()
