"""
security_core.py
=================
Security Core Module
---------------------
Implements every protection layer required for a production-grade
smart-grid data aggregation system:

  1. Key Management        — per-meter HMAC keys, derivation, rotation
  2. Digital Signatures    — HMAC-SHA-256 per reading (integrity + authenticity)
  3. Replay Prevention     — nonce + timestamp window validation
  4. Authentication        — API-key / bearer-token gate
  5. Rate Limiting         — sliding-window per meter/IP
  6. Input Validation      — schema + range checks on all incoming fields
  7. Security Event Log    — tamper-evident append-only event store

Security design comments are embedded throughout. Every function explains
WHAT it prevents and HOW an attacker would otherwise exploit it.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Maximum age (seconds) of a reading before it is rejected as a replay.
# A legitimate meter sends data within this window of wall-clock time.
NONCE_WINDOW_SECONDS: int = 30

# Maximum readings accepted from one meter per minute.
# Prevents a compromised/spoofed meter from flooding the aggregator.
RATE_LIMIT_RPM: int = 60

# Minimum / maximum acceptable energy reading (kW).
ENERGY_MIN_KW: float = 0.0
ENERGY_MAX_KW: float = 25.0  # 25 kW hard upper bound for a household

# Acceptable voltage band (IEC 60038 nominal ±10 %).
VOLTAGE_MIN_V: float = 200.0
VOLTAGE_MAX_V: float = 260.0

CURRENT_MIN_A: float = 0.0
CURRENT_MAX_A: float = 50.0

# HMAC algorithm used for all signatures.
HMAC_ALGO: str = "sha256"

# Security event severity levels.
SEV_INFO: str = "INFO"
SEV_WARNING: str = "WARNING"
SEV_CRITICAL: str = "CRITICAL"


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class SecurityEvent:
    """A single tamper-evident security log entry."""

    timestamp: float  # UNIX epoch
    severity: str  # INFO | WARNING | CRITICAL
    event_type: str  # REPLAY | TAMPER | AUTH_FAIL | RATE_LIMIT | ANOMALY | OK
    meter_id: str
    description: str
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "time_str": time.strftime("%H:%M:%S", time.localtime(self.timestamp)),
            "severity": self.severity,
            "event_type": self.event_type,
            "meter_id": self.meter_id,
            "description": self.description,
            **self.extra,
        }


@dataclass
class ValidationResult:
    """Outcome of a full security validation pass."""

    ok: bool
    event_type: str
    description: str
    severity: str = SEV_INFO


# ─────────────────────────────────────────────────────────────────────────────
# 1. KEY MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
class KeyManager:
    """
    Per-meter HMAC key store.

    Security rationale
    ------------------
    Each smart meter is provisioned with a unique 256-bit secret key at
    enrollment time.  Using per-meter keys means that compromise of one
    meter does not affect any other meter — it limits the blast radius
    of a physical tamper attack.

    Keys are derived deterministically from a master secret + meter ID
    using HKDF-like expansion (HMAC(master, "meter_id:N")) so the
    master can be rotated without re-provisioning every device.

    In a real deployment these keys would live in an HSM (Hardware
    Security Module).  Here they are kept in memory for demonstration.
    """

    def __init__(self, master_secret: Optional[bytes] = None) -> None:
        # Master secret: 32 cryptographically-random bytes.
        # os.urandom uses the OS CSPRNG — stronger than Python's random module.
        self._master: bytes = master_secret or os.urandom(32)
        self._keys: Dict[str, bytes] = {}
        self._lock = threading.RLock()
        logger.info("KeyManager initialised with fresh master secret.")

    def get_key(self, meter_id: str) -> bytes:
        """
        Return (and lazily derive) the 32-byte HMAC key for a meter.

        Derivation: HMAC-SHA256(master_secret, "sgrid:v1:" + meter_id)
        This is a lightweight HKDF-info-based derivation that binds
        the sub-key to the meter's identity.
        """
        with self._lock:
            if meter_id not in self._keys:
                info = f"sgrid:v1:{meter_id}".encode()
                self._keys[meter_id] = hmac.new(self._master, info, HMAC_ALGO).digest()
            return self._keys[meter_id]

    def rotate_master(self) -> None:
        """
        Re-derive all keys from a new master secret.
        Call periodically (e.g. daily) to provide forward secrecy.
        """
        with self._lock:
            self._master = os.urandom(32)
            self._keys.clear()
        logger.info("Master key rotated; all meter keys invalidated.")

    def register_meter(
        self, meter_id: str, provided_key: Optional[bytes] = None
    ) -> bytes:
        """
        Explicitly register a meter with an optional pre-shared key.
        If no key is provided, one is derived from the master.
        """
        with self._lock:
            if provided_key:
                if len(provided_key) < 16:
                    raise ValueError("Meter key must be at least 128 bits (16 bytes).")
                self._keys[meter_id] = provided_key
            else:
                self._keys[meter_id] = self.get_key(meter_id)
        return self._keys[meter_id]


# ─────────────────────────────────────────────────────────────────────────────
# 2. DIGITAL SIGNATURES (HMAC-SHA-256)
# ─────────────────────────────────────────────────────────────────────────────
class SignatureEngine:
    """
    HMAC-SHA-256 signature generation and verification.

    Security rationale
    ------------------
    A digital signature on each meter reading provides TWO guarantees:
      • Integrity   — any bit-flip in the payload invalidates the tag.
      • Authenticity — only a party with the meter's key can produce a
                       valid tag, so a network attacker cannot forge data.

    HMAC is preferred over a raw hash because a hash is not keyed;
    an attacker who knows the data can trivially recompute the hash.
    HMAC-SHA-256 with a 256-bit key is unbreakable without the key.

    The message to sign includes:
        meter_id | timestamp | energy | voltage | current | nonce
    Binding the nonce means even an identical reading at a different
    time produces a different tag — this prevents cut-and-paste attacks.
    """

    def __init__(self, key_manager: KeyManager) -> None:
        self._km = key_manager

    def _canonical(self, reading: Dict[str, Any]) -> bytes:
        """
        Produce a deterministic byte string from a reading dict.

        All fields that an attacker might modify are included in the
        signed message.  The order is fixed to prevent reordering attacks.
        """
        return (
            f"{reading['meter_id']}|"
            f"{reading['timestamp']}|"
            f"{reading['energy_usage']:.6f}|"
            f"{reading['voltage']:.4f}|"
            f"{reading['current']:.4f}|"
            f"{reading.get('nonce', '')}"
        ).encode("utf-8")

    def sign(self, reading: Dict[str, Any]) -> str:
        """Return hex-encoded HMAC-SHA-256 tag for a reading."""
        key = self._km.get_key(reading["meter_id"])
        msg = self._canonical(reading)
        return hmac.new(key, msg, HMAC_ALGO).hexdigest()

    def verify(self, reading: Dict[str, Any], provided_tag: str) -> bool:
        """
        Constant-time comparison of expected vs provided tag.

        Uses hmac.compare_digest() to prevent timing side-channel attacks.
        A naive == comparison leaks information about how many bytes match.
        """
        key = self._km.get_key(reading["meter_id"])
        msg = self._canonical(reading)
        expected = hmac.new(key, msg, HMAC_ALGO).hexdigest()
        return hmac.compare_digest(expected, provided_tag)


# ─────────────────────────────────────────────────────────────────────────────
# 3. NONCE + TIMESTAMP  (Replay Attack Prevention)
# ─────────────────────────────────────────────────────────────────────────────
class ReplayGuard:
    """
    Prevents replay attacks by tracking used nonces inside a time window.

    Security rationale
    ------------------
    Without replay protection, an adversary who intercepts a valid
    signed packet can re-submit it later (or repeatedly) to:
      • Inflate/deflate aggregated energy totals.
      • Cause billing fraud.
      • Bypass anomaly detection by flooding with "normal" old data.

    Two complementary controls are applied:
      1. Timestamp window — reject packets older than NONCE_WINDOW_SECONDS.
         Limits the window of opportunity for a replayed packet.
      2. Nonce uniqueness — each packet carries a fresh random nonce;
         the server records seen nonces and rejects duplicates.
         Covers the case where an attacker replays within the time window.

    The nonce store is bounded: entries older than the window are pruned
    to prevent unbounded memory growth.
    """

    def __init__(self, window_seconds: int = NONCE_WINDOW_SECONDS) -> None:
        self._window = window_seconds
        # nonce → reception_timestamp
        self._seen: Dict[str, float] = {}
        self._lock = threading.Lock()

    def check_and_record(self, nonce: str, packet_timestamp: float) -> Tuple[bool, str]:
        """
        Returns (ok, reason).

        ok=True  → nonce is fresh and unique; packet is accepted.
        ok=False → packet is rejected; reason explains why.
        """
        now = time.time()

        # 1. Timestamp window check
        age = now - packet_timestamp
        if age < -5:  # allow 5 s clock skew
            return (
                False,
                f"Packet from the FUTURE (skew={age:.1f}s) — possible replay setup",
            )
        if age > self._window:
            return (
                False,
                f"Packet too old ({age:.0f}s > {self._window}s window) — REPLAY REJECTED",
            )

        with self._lock:
            self._prune(now)

            # 2. Nonce uniqueness check
            if nonce in self._seen:
                return False, f"Nonce '{nonce[:12]}…' already seen — REPLAY REJECTED"

            # Accept and record
            self._seen[nonce] = now

        return True, "OK"

    def _prune(self, now: float) -> None:
        """Remove expired nonces (called under lock)."""
        cutoff = now - self._window
        expired = [n for n, t in self._seen.items() if t < cutoff]
        for n in expired:
            del self._seen[n]


# ─────────────────────────────────────────────────────────────────────────────
# 4. AUTHENTICATION
# ─────────────────────────────────────────────────────────────────────────────
class AuthManager:
    """
    Simple API-key / bearer-token authentication.

    Security rationale
    ------------------
    Without authentication any process on the network can submit
    readings to the aggregation server.  An API key acts as a shared
    secret between a registered meter and the server.

    Keys are stored as SHA-256 hashes, never in plaintext — this means
    a database leak does not expose the actual credentials.

    In production, keys would be rotated via a certificate authority
    and transmitted over mutual TLS.
    """

    def __init__(self) -> None:
        # meter_id → hashed_api_key
        self._credentials: Dict[str, str] = {}
        self._failed_attempts: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def register(self, meter_id: str) -> str:
        """
        Register a meter and return a fresh random API key.
        The key itself is stored only as its SHA-256 hash.
        """
        raw_key = secrets.token_hex(32)  # 256-bit random key
        hashed = hashlib.sha256(raw_key.encode()).hexdigest()
        with self._lock:
            self._credentials[meter_id] = hashed
        logger.info("Registered meter %s with new API key.", meter_id)
        return raw_key  # returned ONCE to the meter

    def authenticate(self, meter_id: str, api_key: str) -> Tuple[bool, str]:
        """
        Verify a meter's API key.
        Returns (authenticated, reason).
        """
        with self._lock:
            if meter_id not in self._credentials:
                self._failed_attempts[meter_id] += 1
                return False, f"Unknown meter '{meter_id}'"
            expected = self._credentials[meter_id]
            provided = hashlib.sha256(api_key.encode()).hexdigest()
            if hmac.compare_digest(expected, provided):
                self._failed_attempts[meter_id] = 0
                return True, "OK"
            self._failed_attempts[meter_id] += 1
            return False, "Invalid API key"

    def failed_count(self, meter_id: str) -> int:
        return self._failed_attempts.get(meter_id, 0)

    def all_failed(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._failed_attempts)


# ─────────────────────────────────────────────────────────────────────────────
# 5. RATE LIMITER
# ─────────────────────────────────────────────────────────────────────────────
class RateLimiter:
    """
    Sliding-window rate limiter per meter.

    Security rationale
    ------------------
    A compromised or malicious meter could flood the aggregation server
    with thousands of fake readings per second, exhausting CPU/memory
    (DoS) or polluting aggregates.  The rate limiter enforces an upper
    bound of RATE_LIMIT_RPM readings per minute per meter.
    """

    def __init__(self, max_per_minute: int = RATE_LIMIT_RPM) -> None:
        self._max = max_per_minute
        # meter_id → deque of UNIX timestamps
        self._calls: Dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, meter_id: str) -> Tuple[bool, str]:
        """
        Returns (allowed, reason).
        Prunes timestamps older than 60 seconds before checking.
        """
        now = time.time()
        cutoff = now - 60.0
        with self._lock:
            q = self._calls[meter_id]
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= self._max:
                return False, (
                    f"Rate limit exceeded: {len(q)} requests in last 60s "
                    f"(max={self._max})"
                )
            q.append(now)
            return True, "OK"


# ─────────────────────────────────────────────────────────────────────────────
# 6. INPUT VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
class InputValidator:
    """
    Schema and range validation for all incoming meter readings.

    Security rationale
    ------------------
    Malformed or out-of-range inputs can cause:
      • Integer overflow in aggregation
      • Negative energy totals (billing fraud)
      • Injection into downstream analytics queries
      • Crash the server via unexpected types

    Validation is the first gate — cheapest to apply, keeps bad data
    out of the cryptographic pipeline entirely.
    """

    REQUIRED_FIELDS = {
        "meter_id",
        "timestamp",
        "energy_usage",
        "voltage",
        "current",
        "nonce",
    }

    @staticmethod
    def validate(reading: Dict[str, Any]) -> ValidationResult:
        """Full validation pass.  Returns ValidationResult."""
        # 1. Required fields
        missing = InputValidator.REQUIRED_FIELDS - set(reading.keys())
        if missing:
            return ValidationResult(
                ok=False,
                severity=SEV_WARNING,
                event_type="TAMPER",
                description=f"Missing required fields: {missing}",
            )

        # 2. Type checks
        try:
            e = float(reading["energy_usage"])
            v = float(reading["voltage"])
            c = float(reading["current"])
        except (TypeError, ValueError) as ex:
            return ValidationResult(
                ok=False,
                severity=SEV_WARNING,
                event_type="TAMPER",
                description=f"Non-numeric field value: {ex}",
            )

        # 3. Physical range checks
        if not (ENERGY_MIN_KW <= e <= ENERGY_MAX_KW):
            return ValidationResult(
                ok=False,
                severity=SEV_CRITICAL,
                event_type="TAMPER",
                description=(
                    f"Energy {e:.4f} kW out of range "
                    f"[{ENERGY_MIN_KW}, {ENERGY_MAX_KW}] — possible tampering"
                ),
            )
        if not (VOLTAGE_MIN_V <= v <= VOLTAGE_MAX_V):
            return ValidationResult(
                ok=False,
                severity=SEV_WARNING,
                event_type="TAMPER",
                description=f"Voltage {v:.1f} V out of range [{VOLTAGE_MIN_V}, {VOLTAGE_MAX_V}]",
            )
        if not (CURRENT_MIN_A <= c <= CURRENT_MAX_A):
            return ValidationResult(
                ok=False,
                severity=SEV_WARNING,
                event_type="TAMPER",
                description=f"Current {c:.3f} A out of range [{CURRENT_MIN_A}, {CURRENT_MAX_A}]",
            )

        # 4. Meter ID sanity (prevent injection)
        mid = str(reading["meter_id"])
        if len(mid) > 64 or not mid.replace("_", "").replace("-", "").isalnum():
            return ValidationResult(
                ok=False,
                severity=SEV_WARNING,
                event_type="TAMPER",
                description=f"Suspicious meter_id format: '{mid[:30]}'",
            )

        return ValidationResult(
            ok=True, event_type="OK", description="Validation passed"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 7. ANOMALY DETECTOR
# ─────────────────────────────────────────────────────────────────────────────
class AnomalyDetector:
    """
    Online Z-score anomaly detector per meter.

    Security rationale
    ------------------
    A data-tampering attacker who knows the signing key (or has physical
    access to a meter) might submit readings that pass signature
    verification but contain subtly inflated values.  Statistical anomaly
    detection catches these by comparing each new reading against the
    meter's historical distribution.

    Algorithm: Welford's online mean/variance update (O(1) per reading).
    Flag if |z| > Z_THRESHOLD standard deviations from the running mean.
    """

    Z_THRESHOLD = 3.5  # readings beyond 3.5σ are flagged

    def __init__(self) -> None:
        # meter_id → (n, mean, M2) — Welford accumulators
        self._stats: Dict[str, Tuple[int, float, float]] = {}
        self._lock = threading.Lock()

    def update_and_score(self, meter_id: str, value: float) -> Tuple[float, bool]:
        """
        Ingest a new reading, return (z_score, is_anomaly).
        z_score=0.0 for the first two readings (not enough history).
        """
        with self._lock:
            n, mean, M2 = self._stats.get(meter_id, (0, 0.0, 0.0))
            n += 1
            delta = value - mean
            mean += delta / n
            M2 += delta * (value - mean)
            self._stats[meter_id] = (n, mean, M2)

            if n < 3:
                return 0.0, False

            variance = M2 / (n - 1)
            std = variance**0.5
            z = (value - mean) / std if std > 1e-9 else 0.0
            return round(z, 4), abs(z) > self.Z_THRESHOLD

    def get_stats(self) -> Dict[str, Dict]:
        with self._lock:
            return {
                mid: {
                    "n": n,
                    "mean": round(mean, 4),
                    "std": round((M2 / (n - 1)) ** 0.5 if n > 1 else 0, 4),
                }
                for mid, (n, mean, M2) in self._stats.items()
            }


# ─────────────────────────────────────────────────────────────────────────────
# 8. SECURITY EVENT LOG
# ─────────────────────────────────────────────────────────────────────────────
class SecurityEventLog:
    """
    Thread-safe append-only security event journal.

    Each entry is hashed against the previous entry's hash (chain-of-hashes)
    making it computationally infeasible to silently delete or modify
    historical events without breaking the chain.
    """

    MAX_ENTRIES = 2_000

    def __init__(self) -> None:
        self._events: List[Dict[str, Any]] = []
        self._chain: str = "genesis"  # rolling hash
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)

    def record(self, event: SecurityEvent) -> None:
        with self._lock:
            d = event.to_dict()
            # Extend the chain: new_hash = SHA256(prev_hash + event_repr)
            self._chain = hashlib.sha256((self._chain + str(d)).encode()).hexdigest()
            d["chain_hash"] = self._chain[:16]  # first 16 hex chars as label
            self._events.append(d)
            self._counters[event.event_type] += 1
            if len(self._events) > self.MAX_ENTRIES:
                self._events.pop(0)

        # Only log unusual events to the Python logger (avoid noise)
        if event.severity != SEV_INFO:
            logger.warning(
                "[SECURITY] %s | %s | %s",
                event.severity,
                event.event_type,
                event.description,
            )

    def recent(self, n: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._events[-n:])

    def counts(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counters)

    def critical_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._events if e.get("severity") == SEV_CRITICAL)


# ─────────────────────────────────────────────────────────────────────────────
# 9. UNIFIED SECURITY GATEWAY
# ─────────────────────────────────────────────────────────────────────────────
class SecurityGateway:
    """
    Single entry-point that runs the full security stack for every reading.

    Pipeline per incoming packet
    ----------------------------
    1. Input validation      (schema + range)
    2. Authentication        (API key check)
    3. Rate limiting         (flood prevention)
    4. Replay guard          (nonce + timestamp)
    5. Signature verification (HMAC-SHA-256)
    6. Anomaly detection     (Z-score)
    → Emit SecurityEvent (OK / WARN / CRITICAL)

    Usage
    -----
    >>> gw = SecurityGateway()
    >>> gw.register_meter("meter_001")
    >>> result, event = gw.process(reading_dict, api_key="...", signature="...")
    """

    def __init__(self) -> None:
        self.key_manager = KeyManager()
        self.auth = AuthManager()
        self.rate_limiter = RateLimiter()
        self.replay_guard = ReplayGuard()
        self.signer = SignatureEngine(self.key_manager)
        self.validator = InputValidator()
        self.anomaly = AnomalyDetector()
        self.event_log = SecurityEventLog()

        # Pre-register 20 default meters for demo
        self._demo_keys: Dict[str, str] = {}
        for i in range(20):
            mid = f"meter_{i:03d}"
            k = self.auth.register(mid)
            self._demo_keys[mid] = k
            self.key_manager.register_meter(mid)
        logger.info("SecurityGateway initialised with 20 pre-registered meters.")

    def register_meter(self, meter_id: str) -> str:
        """Register a new meter and return its API key."""
        key = self.auth.register(meter_id)
        self._demo_keys[meter_id] = key
        self.key_manager.register_meter(meter_id)
        return key

    def process(
        self,
        reading: Dict[str, Any],
        api_key: str,
        signature: str,
    ) -> Tuple[bool, SecurityEvent]:
        """
        Run the full security pipeline.
        Returns (accepted, SecurityEvent).
        """
        mid = reading.get("meter_id", "unknown")
        ts = float(reading.get("timestamp_epoch", time.time()))

        def _reject(etype, desc, sev=SEV_WARNING) -> Tuple[bool, SecurityEvent]:
            ev = SecurityEvent(time.time(), sev, etype, mid, desc)
            self.event_log.record(ev)
            return False, ev

        # 1. Validate schema & ranges
        v = self.validator.validate(reading)
        if not v.ok:
            return _reject(v.event_type, v.description, v.severity)

        # 2. Authenticate
        auth_ok, auth_msg = self.auth.authenticate(mid, api_key)
        if not auth_ok:
            return _reject(
                "AUTH_FAIL", f"Auth failed for {mid}: {auth_msg}", SEV_CRITICAL
            )

        # 3. Rate limit
        rate_ok, rate_msg = self.rate_limiter.check(mid)
        if not rate_ok:
            return _reject("RATE_LIMIT", rate_msg, SEV_WARNING)

        # 4. Replay guard
        nonce = reading.get("nonce", "")
        ok, replay_msg = self.replay_guard.check_and_record(nonce, ts)
        if not ok:
            return _reject("REPLAY", replay_msg, SEV_CRITICAL)

        # 5. Signature verification
        if not self.signer.verify(reading, signature):
            return _reject(
                "TAMPER",
                f"Signature mismatch for {mid} — DATA TAMPERING DETECTED",
                SEV_CRITICAL,
            )

        # 6. Anomaly detection
        z, is_anomaly = self.anomaly.update_and_score(
            mid, float(reading["energy_usage"])
        )
        ev_type = "ANOMALY" if is_anomaly else "OK"
        sev = SEV_WARNING if is_anomaly else SEV_INFO
        desc = (
            f"Anomalous reading: energy={reading['energy_usage']:.4f} kW, z={z:.2f}σ"
            if is_anomaly
            else f"Reading accepted (z={z:.2f})"
        )
        ev = SecurityEvent(
            time.time(),
            sev,
            ev_type,
            mid,
            desc,
            extra={"z_score": z, "energy": reading["energy_usage"]},
        )
        self.event_log.record(ev)
        return True, ev

    def sign_reading(self, reading: Dict[str, Any]) -> str:
        """Produce a signature for a reading (called at meter side)."""
        return self.signer.sign(reading)

    def demo_api_key(self, meter_id: str) -> str:
        """Return the pre-registered demo API key for a meter."""
        return self._demo_keys.get(meter_id, "")


# ─────────────────────────────────────────────────────────────────────────────
# Module-level default gateway (import and use directly in demos)
# ─────────────────────────────────────────────────────────────────────────────
_DEFAULT_GATEWAY: Optional[SecurityGateway] = None


def get_gateway() -> SecurityGateway:
    """Return (lazily creating) the module-level SecurityGateway."""
    global _DEFAULT_GATEWAY
    if _DEFAULT_GATEWAY is None:
        _DEFAULT_GATEWAY = SecurityGateway()
    return _DEFAULT_GATEWAY
