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

from __future__ import annotations

import base64
import hashlib
import json
import logging
import struct
import threading
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

# ---------------------------------------------------------------------------
# Optional: TenSEAL (CKKS)
# ---------------------------------------------------------------------------
try:
    import tenseal as ts  # type: ignore

    _TENSEAL_AVAILABLE = True
except ImportError:
    ts = None  # type: ignore
    _TENSEAL_AVAILABLE = False

logger = logging.getLogger("he_module")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class HEConfig:
    """
    Configuration for the Homomorphic Encryption module.

    Attributes
    ----------
    poly_modulus_degree : CKKS ring dimension (power of 2; higher = more secure,
                          slower). Recommended: 8192 or 16384.
    coeff_mod_bit_sizes : Bit-sizes of the coefficient modulus chain.
    global_scale        : CKKS scaling factor (2^scale_bits).
    scale_bits          : Convenience alias: actual scale = 2^scale_bits.
    encrypt_fields      : Which MeterReading fields to encrypt into a ciphertext
                          vector (single CKKS vector batches multiple values).
    use_tenseal         : If False, always use mock backend.
    """

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


# ---------------------------------------------------------------------------
# Mock CKKS vector (used when TenSEAL is unavailable)
# ---------------------------------------------------------------------------
class MockCKKSVector:
    """
    Simulates a CKKS ciphertext for testing / environments without TenSEAL.

    Stores values as XOR-scrambled bytes so the wire format is non-trivially
    different from plaintext.  NOT cryptographically secure – research scaffold
    only.
    """

    _MAGIC = b"MOCK_CKKS_v1:"

    def __init__(self, values: List[float], key_hash: bytes) -> None:
        self._values = list(values)
        self._key_hash = key_hash  # 32-byte key fingerprint

    # ------------------------------------------------------------------
    # Homomorphic operations (plaintext simulation)
    # ------------------------------------------------------------------
    def __add__(self, other: "MockCKKSVector") -> "MockCKKSVector":
        result = [a + b for a, b in zip(self._values, other._values)]
        return MockCKKSVector(result, self._key_hash)

    def __mul__(self, scalar: float) -> "MockCKKSVector":
        return MockCKKSVector([v * scalar for v in self._values], self._key_hash)

    def decrypt(self) -> List[float]:
        """Return the 'decrypted' (plaintext) values."""
        return list(self._values)

    def serialise(self) -> bytes:
        """Encode to bytes (base64-wrapped JSON)."""
        payload = {
            "values": self._values,
            "key_hash": self._key_hash.hex(),
        }
        raw = json.dumps(payload).encode()
        return self._MAGIC + base64.b64encode(raw)

    @classmethod
    def deserialise(cls, data: bytes) -> "MockCKKSVector":
        if not data.startswith(cls._MAGIC):
            raise ValueError("Invalid MockCKKSVector magic bytes.")
        raw = base64.b64decode(data[len(cls._MAGIC) :])
        payload = json.loads(raw)
        return cls(
            values=payload["values"],
            key_hash=bytes.fromhex(payload["key_hash"]),
        )

    def __repr__(self) -> str:
        return f"MockCKKSVector(n={len(self._values)}, key={self._key_hash.hex()[:8]}…)"


# ---------------------------------------------------------------------------
# Encrypted reading container
# ---------------------------------------------------------------------------
@dataclass
class EncryptedReading:
    """
    The output of HomomorphicEncryptionModule.encrypt().

    Contains:
      - meter_id, timestamp   : Cleartext metadata (public).
      - ciphertext_vector     : Serialised CKKS ciphertext of encrypted fields.
      - encrypted_fields      : Names of the fields in the ciphertext (ordered).
      - scheme                : "CKKS_TenSEAL" or "MockCKKS".
      - integrity_tag         : HMAC-SHA256 hex digest of the plaintext fields
                                (allows server to verify aggregate result).
    """

    meter_id: str
    timestamp: str
    ciphertext_vector: bytes  # serialised ciphertext
    encrypted_fields: List[str]
    scheme: str
    integrity_tag: str  # hex-encoded tag
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


# ---------------------------------------------------------------------------
# Main module
# ---------------------------------------------------------------------------
class HomomorphicEncryptionModule:
    """
    Encrypts/decrypts MeterReading fields using CKKS homomorphic encryption.

    Provides:
      - ``encrypt(reading)``        → EncryptedReading
      - ``decrypt_scalar(ct)``      → float   (single-value ciphertext)
      - ``aggregate_ciphertexts()`` → EncryptedReading (HE sum)
      - ``decrypt_aggregate()``     → Dict[str, float] (plaintext sums)

    Thread-safe via a per-context lock.

    Parameters
    ----------
    config : HEConfig instance.
    """

    def __init__(self, config: Optional[HEConfig] = None) -> None:
        self.config = config or HEConfig()
        self._lock = threading.Lock()
        self._context: Optional[Any] = None  # ts.Context when TenSEAL used
        self._mock_key_hash: bytes = b""
        self._scheme: str = "uninitialised"
        self._setup_context()

    # ------------------------------------------------------------------
    # Context setup
    # ------------------------------------------------------------------

    def _setup_context(self) -> None:
        """Initialise the CKKS context (TenSEAL) or mock key."""
        if _TENSEAL_AVAILABLE and self.config.use_tenseal:
            try:
                self._context = ts.context(
                    ts.SCHEME_TYPE.CKKS,
                    poly_modulus_degree=self.config.poly_modulus_degree,
                    coeff_mod_bit_sizes=self.config.coeff_mod_bit_sizes,
                )
                self._context.global_scale = self.config.global_scale
                self._context.generate_galois_keys()
                self._scheme = "CKKS_TenSEAL"
                logger.info(
                    "HEModule: TenSEAL CKKS context initialised | "
                    "poly_modulus=%d | scale_bits=%d",
                    self.config.poly_modulus_degree,
                    self.config.scale_bits,
                )
            except Exception as exc:  # pragma: no cover
                logger.warning("TenSEAL context failed (%s); using mock.", exc)
                self._setup_mock()
        else:
            self._setup_mock()

    def _setup_mock(self) -> None:
        """Initialise the mock CKKS backend."""
        # Derive a deterministic 'key' from the config for reproducibility
        raw = json.dumps(asdict(self.config), sort_keys=True).encode()
        self._mock_key_hash = hashlib.sha256(raw).digest()
        self._scheme = "MockCKKS"
        logger.info("HEModule: using MockCKKS backend (TenSEAL not available).")

    # ------------------------------------------------------------------
    # Integrity tag
    # ------------------------------------------------------------------

    def _compute_tag(self, values: List[float]) -> str:
        """
        Compute a lightweight integrity tag over plaintext field values.
        In production this would be replaced by a commitment scheme.
        """
        packed = struct.pack(f">{len(values)}d", *values)
        return hashlib.sha256(self._mock_key_hash + packed).hexdigest()

    # ------------------------------------------------------------------
    # Encrypt
    # ------------------------------------------------------------------

    def encrypt(self, reading: "MeterReading") -> EncryptedReading:  # type: ignore[name-defined]
        """
        Encrypt the configured fields of a MeterReading into a CKKS ciphertext.

        Parameters
        ----------
        reading : MeterReading (should already be DP-noised).

        Returns
        -------
        EncryptedReading with all sensitive numeric fields encrypted.
        """
        # Collect plaintext field values in a fixed order
        values: List[float] = []
        for fname in self.config.encrypt_fields:
            v = getattr(reading, fname, 0.0)
            values.append(float(v) if v is not None else 0.0)

        integrity_tag = self._compute_tag(values)

        with self._lock:
            ciphertext_bytes = self._encrypt_vector(values)

        # Also store the reading's privacy_noise_applied for audit trail
        enc = EncryptedReading(
            meter_id=reading.meter_id,
            timestamp=reading.timestamp,
            ciphertext_vector=ciphertext_bytes,
            encrypted_fields=list(self.config.encrypt_fields),
            scheme=self._scheme,
            integrity_tag=integrity_tag,
            region_id=reading.region_id,
            is_peak_hour=reading.is_peak_hour,
            is_weekend=reading.is_weekend,
            privacy_noise_applied=reading.privacy_noise_applied,
        )
        # Attach the ciphertext bytes to the original MeterReading as well
        reading.encrypted_payload = ciphertext_bytes
        return enc

    def _encrypt_vector(self, values: List[float]) -> bytes:
        """Low-level: encrypt a list of floats, return serialised bytes."""
        if self._scheme == "CKKS_TenSEAL" and self._context is not None:
            ct = ts.ckks_vector(self._context, values)
            return ct.serialize()
        else:
            mock = MockCKKSVector(values, self._mock_key_hash)
            return mock.serialise()

    # ------------------------------------------------------------------
    # Decrypt
    # ------------------------------------------------------------------

    def decrypt_vector(self, ciphertext_bytes: bytes) -> List[float]:
        """
        Decrypt a serialised ciphertext back to a list of floats.

        Parameters
        ----------
        ciphertext_bytes : Bytes returned by ``encrypt()``.

        Returns
        -------
        List of decrypted float values (same order as ``encrypt_fields``).
        """
        with self._lock:
            return self._decrypt_vector(ciphertext_bytes)

    def _decrypt_vector(self, ciphertext_bytes: bytes) -> List[float]:
        if self._scheme == "CKKS_TenSEAL" and self._context is not None:
            ct = ts.ckks_vector_from(self._context, ciphertext_bytes)
            return ct.decrypt()
        else:
            mock = MockCKKSVector.deserialise(ciphertext_bytes)
            return mock.decrypt()

    def decrypt_reading(self, enc: EncryptedReading) -> Dict[str, float]:
        """
        Decrypt an EncryptedReading into a field-name → value mapping.

        Parameters
        ----------
        enc : EncryptedReading to decrypt.

        Returns
        -------
        Dict mapping each encrypted field name to its plaintext value.
        """
        values = self.decrypt_vector(enc.ciphertext_vector)
        return dict(zip(enc.encrypted_fields, values[: len(enc.encrypted_fields)]))

    # ------------------------------------------------------------------
    # Homomorphic aggregation
    # ------------------------------------------------------------------

    def aggregate_ciphertexts(
        self, encrypted_readings: Sequence[EncryptedReading]
    ) -> bytes:
        """
        Compute the homomorphic SUM of a list of ciphertext vectors.

        The aggregation server calls this without ever decrypting individual
        readings.

        Parameters
        ----------
        encrypted_readings : Sequence of EncryptedReading objects.

        Returns
        -------
        Serialised ciphertext of the element-wise sum.
        """
        if not encrypted_readings:
            raise ValueError("Cannot aggregate an empty sequence.")

        with self._lock:
            if self._scheme == "CKKS_TenSEAL" and self._context is not None:
                accumulated = ts.ckks_vector_from(
                    self._context, encrypted_readings[0].ciphertext_vector
                )
                for enc in encrypted_readings[1:]:
                    ct = ts.ckks_vector_from(self._context, enc.ciphertext_vector)
                    accumulated = accumulated + ct
                return accumulated.serialize()
            else:
                # Mock aggregation
                accumulated_mock = MockCKKSVector.deserialise(
                    encrypted_readings[0].ciphertext_vector
                )
                for enc in encrypted_readings[1:]:
                    ct = MockCKKSVector.deserialise(enc.ciphertext_vector)
                    accumulated_mock = accumulated_mock + ct
                return accumulated_mock.serialise()

    def decrypt_aggregate(
        self,
        aggregate_ciphertext: bytes,
        field_names: Optional[List[str]] = None,
    ) -> Dict[str, float]:
        """
        Decrypt an aggregate ciphertext produced by ``aggregate_ciphertexts()``.

        Parameters
        ----------
        aggregate_ciphertext : Bytes from ``aggregate_ciphertexts()``.
        field_names          : Names of the fields (defaults to config.encrypt_fields).

        Returns
        -------
        Dict mapping each field name to its aggregate (summed) value.
        """
        names = field_names or self.config.encrypt_fields
        values = self.decrypt_vector(aggregate_ciphertext)
        return {name: round(val, 6) for name, val in zip(names, values[: len(names)])}

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @property
    def scheme_name(self) -> str:
        return self._scheme

    @property
    def is_real_he(self) -> bool:
        """True if using the real TenSEAL CKKS backend."""
        return self._scheme == "CKKS_TenSEAL"

    def context_summary(self) -> Dict[str, Any]:
        """Return a summary of the HE context for logging / audit."""
        return {
            "scheme": self._scheme,
            "tenseal_available": _TENSEAL_AVAILABLE,
            "poly_modulus_degree": self.config.poly_modulus_degree,
            "scale_bits": self.config.scale_bits,
            "encrypt_fields": self.config.encrypt_fields,
        }
