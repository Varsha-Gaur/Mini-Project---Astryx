"""
he_module.py
============
Homomorphic Encryption Module
-------------------------------
Provides a HomomorphicEncryptionModule that encrypts MeterReading fields using
the CKKS scheme (approximate arithmetic over real numbers), enabling an
aggregation server to compute sums/averages over encrypted smart-meter readings
without ever seeing plaintext values.

Encryption scheme
-----------------
CKKS (Cheon-Kim-Kim-Song) via TenSEAL.  Falls back to a simulated
``MockCKKSVector`` when TenSEAL is not installed, so the rest of the pipeline
(FastAPI, Kafka, DP module) continues to work in environments without the
native TenSEAL wheel.

Pipeline position
-----------------
    (DP-noised) MeterReading
          │
          ▼
    HomomorphicEncryptionModule.encrypt()
          │
          ▼
    EncryptedReading   ──► aggregation server (can sum ciphertexts)
          │
          ▼  (at aggregation server)
    HomomorphicEncryptionModule.decrypt_aggregate()
          │
          ▼
    float  (sum or average of plaintext values)

Usage
-----
    from he_module import HomomorphicEncryptionModule, HEConfig

    he = HomomorphicEncryptionModule(HEConfig())
    enc = he.encrypt(reading)
    # ... send enc to aggregation server ...
    total = he.decrypt_scalar(enc.ciphertext_energy)
    print(total)
"""

"""
Key fix: _setup_mock() now uses an explicit dict instead of asdict(self.config)
which could fail when config contains mutable-default List fields.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import struct
import threading
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

logger = logging.getLogger(__name__)

try:
    import tenseal as ts  # type: ignore

    _TENSEAL = True
except ImportError:
    ts = None  # type: ignore
    _TENSEAL = False


@dataclass
class HEConfig:
    poly_modulus_degree: int = 8192
    coeff_mod_bit_sizes: List[int] = field(default_factory=lambda: [60, 40, 40, 60])
    scale_bits: int = 40
    encrypt_fields: List[str] = field(
        default_factory=lambda: ["energy_usage", "voltage", "current"]
    )
    use_tenseal: bool = True

    @property
    def global_scale(self) -> float:
        return 2.0**self.scale_bits


class MockCKKSVector:
    """Algebraically-correct CKKS simulation (NOT cryptographically secure)."""

    _MAGIC = b"MOCK_CKKS_v1:"

    def __init__(self, values: List[float], key_hash: bytes) -> None:
        self._v = list(values)
        self._key = key_hash

    def __add__(self, other: "MockCKKSVector") -> "MockCKKSVector":
        return MockCKKSVector([a + b for a, b in zip(self._v, other._v)], self._key)

    def __mul__(self, scalar: float) -> "MockCKKSVector":
        return MockCKKSVector([v * scalar for v in self._v], self._key)

    def decrypt(self) -> List[float]:
        return list(self._v)

    def serialise(self) -> bytes:
        payload = json.dumps({"v": self._v, "k": self._key.hex()}).encode()
        return self._MAGIC + base64.b64encode(payload)

    @classmethod
    def deserialise(cls, data: bytes) -> "MockCKKSVector":
        if not data.startswith(cls._MAGIC):
            raise ValueError("Invalid MockCKKSVector magic bytes.")
        raw = base64.b64decode(data[len(cls._MAGIC) :])
        payload = json.loads(raw)
        return cls(values=payload["v"], key_hash=bytes.fromhex(payload["k"]))


@dataclass
class EncryptedReading:
    meter_id: str
    timestamp: str
    ciphertext_vector: bytes
    encrypted_fields: List[str]
    scheme: str
    integrity_tag: str
    region_id: int
    is_peak_hour: bool
    is_weekend: bool
    privacy_noise_applied: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["ciphertext_vector"] = base64.b64encode(self.ciphertext_vector).decode()
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class HomomorphicEncryptionModule:
    """
    CKKS homomorphic encryption — TenSEAL or MockCKKS fallback.

    Core operation:  Enc(a) + Enc(b) = Enc(a+b)  (additive homomorphism)

    The aggregation server can sum N ciphertexts and return ONE
    ciphertext whose decryption equals the true sum — without ever
    seeing individual readings.
    """

    def __init__(self, config: Optional[HEConfig] = None) -> None:
        self.config = config or HEConfig()
        self._lock = threading.RLock()
        self._context: Any = None
        self._mock_key_hash = b""
        self._scheme = "uninitialised"
        self._setup_context()

    def _setup_context(self) -> None:
        if _TENSEAL and self.config.use_tenseal:
            try:
                ctx = ts.context(
                    ts.SCHEME_TYPE.CKKS,
                    poly_modulus_degree=self.config.poly_modulus_degree,
                    coeff_mod_bit_sizes=self.config.coeff_mod_bit_sizes,
                )
                ctx.global_scale = self.config.global_scale
                ctx.generate_galois_keys()
                self._context = ctx
                self._scheme = "CKKS_TenSEAL"
                logger.info(
                    "TenSEAL CKKS ready | n=%d | scale_bits=%d",
                    self.config.poly_modulus_degree,
                    self.config.scale_bits,
                )
            except Exception as exc:
                logger.warning("TenSEAL init failed (%s) — using MockCKKS.", exc)
                self._setup_mock()
        else:
            self._setup_mock()

    def _setup_mock(self) -> None:
        # BUG FIX: use explicit dict, not asdict(config), to avoid issues
        # with mutable-default List fields in the dataclass.
        key_src = json.dumps(
            {
                "n": self.config.poly_modulus_degree,
                "bits": self.config.scale_bits,
                "mock": True,
            },
            sort_keys=True,
        ).encode()
        self._mock_key_hash = hashlib.sha256(key_src).digest()
        self._scheme = "MockCKKS"
        logger.info(
            "MockCKKS backend active (research scaffold — not cryptographically secure)."
        )

    def _compute_tag(self, values: List[float]) -> str:
        packed = struct.pack(f">{len(values)}d", *values)
        return hashlib.sha256(self._mock_key_hash + packed).hexdigest()

    def _encrypt_vector(self, values: List[float]) -> bytes:
        if self._scheme == "CKKS_TenSEAL" and self._context:
            return ts.ckks_vector(self._context, values).serialize()
        return MockCKKSVector(values, self._mock_key_hash).serialise()

    def _decrypt_vector(self, ct: bytes) -> List[float]:
        if self._scheme == "CKKS_TenSEAL" and self._context:
            return list(ts.ckks_vector_from(self._context, ct).decrypt())
        return MockCKKSVector.deserialise(ct).decrypt()

    def _add_ciphertexts(self, b1: bytes, b2: bytes) -> bytes:
        if self._scheme == "CKKS_TenSEAL" and self._context:
            return (
                ts.ckks_vector_from(self._context, b1)
                + ts.ckks_vector_from(self._context, b2)
            ).serialize()
        return (
            MockCKKSVector.deserialise(b1) + MockCKKSVector.deserialise(b2)
        ).serialise()

    # ------------------------------------------------------------------

    def encrypt(self, reading: "MeterReading") -> EncryptedReading:  # type: ignore
        """Encrypt configured MeterReading fields → EncryptedReading."""
        values = [
            float(getattr(reading, f, 0.0) or 0.0) for f in self.config.encrypt_fields
        ]
        tag = self._compute_tag(values)
        with self._lock:
            ct = self._encrypt_vector(values)
        reading.encrypted_payload = ct[:16]  # 16-byte preview for audit
        return EncryptedReading(
            meter_id=reading.meter_id,
            timestamp=reading.timestamp,
            ciphertext_vector=ct,
            encrypted_fields=list(self.config.encrypt_fields),
            scheme=self._scheme,
            integrity_tag=tag,
            region_id=reading.region_id,
            is_peak_hour=reading.is_peak_hour,
            is_weekend=reading.is_weekend,
            privacy_noise_applied=reading.privacy_noise_applied,
        )

    def decrypt_vector(self, ct: bytes) -> List[float]:
        with self._lock:
            return self._decrypt_vector(ct)

    def decrypt_reading(self, enc: EncryptedReading) -> Dict[str, float]:
        vals = self.decrypt_vector(enc.ciphertext_vector)
        return {
            n: round(float(v), 6)
            for n, v in zip(enc.encrypted_fields, vals[: len(enc.encrypted_fields)])
        }

    def aggregate_ciphertexts(
        self, encrypted_readings: Sequence[EncryptedReading]
    ) -> bytes:
        """
        Homomorphically sum ciphertexts without decrypting any individual reading.

        CT_sum = Enc(e₁) ⊕ Enc(e₂) ⊕ … ⊕ Enc(eₙ)  =  Enc(Σ eᵢ)
        """
        if not encrypted_readings:
            raise ValueError("Cannot aggregate an empty sequence.")
        with self._lock:
            acc = encrypted_readings[0].ciphertext_vector
            for enc in encrypted_readings[1:]:
                acc = self._add_ciphertexts(acc, enc.ciphertext_vector)
        logger.debug("HE aggregation: %d ciphertexts → 1.", len(encrypted_readings))
        return acc

    def decrypt_aggregate(
        self, agg_ct: bytes, field_names: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """Decrypt aggregate ciphertext — only the authorised analyst calls this."""
        names = field_names or self.config.encrypt_fields
        vals = self.decrypt_vector(agg_ct)
        return {n: round(float(v), 6) for n, v in zip(names, vals[: len(names)])}

    @property
    def scheme_name(self) -> str:
        return self._scheme

    @property
    def is_real_he(self) -> bool:
        return self._scheme == "CKKS_TenSEAL"

    def context_summary(self) -> Dict[str, Any]:
        return {
            "scheme": self._scheme,
            "tenseal_available": _TENSEAL,
            "poly_modulus_degree": self.config.poly_modulus_degree,
            "scale_bits": self.config.scale_bits,
            "encrypt_fields": self.config.encrypt_fields,
            "is_real_crypto": self.is_real_he,
        }
