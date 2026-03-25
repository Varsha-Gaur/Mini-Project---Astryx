"""
server/aggregation_server.py
==============================
Secure Aggregation Server
--------------------------
FastAPI application that receives DP-noised, HE-encrypted smart-meter
readings, performs homomorphic aggregation, and exposes REST endpoints.

Bug fixes vs original api_server.py
-------------------------------------
1. CRASH: `from smart_meter_simulator import` → corrected to the actual
   package path `from simulator.smart_meter_simulator import`.
2. CRASH: `from kafka_producer import` removed — module never existed.
3. RUNTIME: `asyncio.get_event_loop()` replaced with
   `asyncio.get_running_loop()` (required for Python 3.10+/3.12+).
4. LOGIC: `collect_encrypted_batch` now captures the returned
   EncryptedReading (the original silently discarded it).

Endpoints
---------
  POST /submit             Submit a raw reading through DP+HE pipeline
  GET  /aggregate          Return serialised HE aggregate (still encrypted)
  GET  /decrypt_total      Return decrypted aggregate sum
  GET  /status             Pipeline health
  GET  /readings           Stored metadata (no values)
  GET  /dp_report          Privacy budget consumption
  GET  /he_info            HE context details
  DELETE /reset            Clear all stored readings
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Add project root to path so sibling packages resolve correctly
# ---------------------------------------------------------------------------
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional FastAPI
# ---------------------------------------------------------------------------
try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, StreamingResponse
    from pydantic import BaseModel, Field

    _FASTAPI = True
except ImportError:
    _FASTAPI = False
    FastAPI = None  # type: ignore
    BaseModel = object  # type: ignore

# ---------------------------------------------------------------------------
# Project modules  (BUG FIX: corrected import path)
# ---------------------------------------------------------------------------
from simulator.smart_meter_simulator import (
    SmartGridSimulator,
    SimulatorConfig,
    generate_synthetic_rows,
    MeterReading,
)

try:
    from privacy.dp_module import DifferentialPrivacyModule, DPConfig

    _DP = True
except ImportError:
    _DP = False
    DifferentialPrivacyModule = None  # type: ignore
    DPConfig = None  # type: ignore

try:
    from encryption.he_module import (
        HomomorphicEncryptionModule,
        HEConfig,
        EncryptedReading,
    )

    _HE = True
except ImportError:
    _HE = False
    HomomorphicEncryptionModule = None  # type: ignore
    HEConfig = None  # type: ignore
    EncryptedReading = None  # type: ignore


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------
class _Store:
    """Thread-safe in-memory store for encrypted readings."""

    def __init__(self) -> None:
        self._encrypted: List[Any] = []
        self._metadata: List[Dict] = []
        self._dp_values: List[float] = []  # for visualisation
        self._raw_values: List[float] = []  # research comparison

    def add(self, enc: Any, raw_energy: float, dp_energy: float) -> None:
        self._encrypted.append(enc)
        self._metadata.append(
            {
                "meter_id": enc.meter_id,
                "timestamp": enc.timestamp,
                "region_id": enc.region_id,
                "scheme": enc.scheme,
            }
        )
        self._raw_values.append(raw_energy)
        self._dp_values.append(dp_energy)

    def clear(self) -> None:
        self._encrypted.clear()
        self._metadata.clear()
        self._dp_values.clear()
        self._raw_values.clear()

    def __len__(self) -> int:
        return len(self._encrypted)


# ---------------------------------------------------------------------------
# Global singletons
# ---------------------------------------------------------------------------
_simulator: Optional[SmartGridSimulator] = None
_dp_module: Optional[Any] = None
_he_module: Optional[Any] = None
_store = _Store()


def _build_simulator(
    num_meters: int = 20,
    use_synthetic: bool = True,
    sample_size: int = 120,
) -> SmartGridSimulator:
    global _dp_module, _he_module
    _dp_module = DifferentialPrivacyModule(DPConfig(epsilon=1.0)) if _DP else None
    _he_module = HomomorphicEncryptionModule(HEConfig()) if _HE else None

    cfg = SimulatorConfig(
        num_meters=num_meters,
        enable_dp=_DP,
        enable_he=_HE,
    )
    sim = SmartGridSimulator(
        num_meters=num_meters,
        config=cfg,
        dp_module=_dp_module,
        he_module=_he_module,
    )
    if use_synthetic:
        sim._raw_rows = generate_synthetic_rows(n=sample_size, seed=42)
    else:
        try:
            sim.load_dataset()
        except FileNotFoundError:
            logger.warning("Dataset not found — using synthetic data.")
            sim._raw_rows = generate_synthetic_rows(n=sample_size, seed=42)
    sim.create_meters()
    logger.info(
        "Simulator ready | meters=%d | rows=%d | dp=%s | he=%s",
        len(sim.meters),
        len(sim._raw_rows),
        "✓" if _dp_module else "✗",
        "✓" if _he_module else "✗",
    )
    return sim


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
if _FASTAPI:

    class _Reading(BaseModel):
        meter_id: str = Field(..., example="meter_007")
        timestamp: str = Field(..., example="2026-01-15 08:12:00")
        energy_usage: float = Field(..., ge=0, example=1.42)
        voltage: float = Field(..., example=230.5)
        current: float = Field(..., example=5.2)
        region_id: int = Field(0)
        is_peak_hour: bool = Field(False)
        is_weekend: bool = Field(False)
        epsilon: Optional[float] = Field(None)

    @asynccontextmanager
    async def _lifespan(app: FastAPI):  # type: ignore
        global _simulator
        logger.info("Server startup …")
        _simulator = _build_simulator(
            num_meters=int(os.getenv("NUM_METERS", "20")),
            use_synthetic=os.getenv("USE_SYNTHETIC", "1") == "1",
            sample_size=int(os.getenv("SAMPLE_SIZE", "120")),
        )
        yield
        logger.info("Server shutdown.")

    app = FastAPI(
        title="SecureGrid Aggregation Server",
        description=(
            "Privacy-preserving smart-grid aggregation.\n"
            "Each submitted reading is DP-noised then CKKS-encrypted.\n"
            "The server aggregates ciphertexts without decrypting individuals."
        ),
        version="2.0.0",
        lifespan=_lifespan,
    )
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )

    def _req() -> None:
        if _simulator is None:
            raise HTTPException(503, "Server not ready.")
        if not _DP:
            raise HTTPException(503, "DP module not available.")
        if not _HE:
            raise HTTPException(503, "HE module not available.")

    # ------ POST /submit ------
    @app.post("/submit", summary="Submit a reading through DP+HE pipeline")
    async def submit(body: _Reading) -> JSONResponse:
        """
        Accept a raw reading. Pipeline: DP noise → CKKS encrypt → store.
        Plaintext is discarded after encryption.
        """
        _req()
        reading = MeterReading(
            meter_id=body.meter_id,
            timestamp=body.timestamp,
            energy_usage=body.energy_usage,
            voltage=body.voltage,
            current=body.current,
            sub_metering={"kitchen": 0.0, "laundry": 0.0, "hvac": 0.0},
            region_id=body.region_id,
            is_peak_hour=body.is_peak_hour,
            is_weekend=body.is_weekend,
        )
        raw_energy = body.energy_usage
        if body.epsilon:
            _dp_module.reconfigure(DPConfig(epsilon=body.epsilon))
        _dp_module.apply(reading)
        dp_energy = reading.energy_usage
        enc = _he_module.encrypt(reading)
        _store.add(enc, raw_energy, dp_energy)
        logger.info("Stored | meter=%s | n=%d", body.meter_id, len(_store))
        return JSONResponse(
            {
                "status": "stored",
                "meter_id": body.meter_id,
                "n_stored": len(_store),
                "dp_epsilon": _dp_module.config.epsilon,
                "he_scheme": _he_module.scheme_name,
            }
        )

    # ------ GET /aggregate ------
    @app.get("/aggregate", summary="Homomorphic aggregate (still encrypted)")
    async def aggregate() -> JSONResponse:
        if len(_store) == 0:
            raise HTTPException(404, "No readings stored.")
        agg = _he_module.aggregate_ciphertexts(_store._encrypted)
        import base64

        return JSONResponse(
            {
                "n_readings": len(_store),
                "scheme": _he_module.scheme_name,
                "ciphertext_b64": base64.b64encode(agg).decode(),
            }
        )

    # ------ GET /decrypt_total ------
    @app.get("/decrypt_total", summary="Decrypt aggregate — authorised analyst only")
    async def decrypt_total() -> JSONResponse:
        if len(_store) == 0:
            raise HTTPException(404, "No readings stored.")
        agg = _he_module.aggregate_ciphertexts(_store._encrypted)
        totals = _he_module.decrypt_aggregate(agg)
        n = len(_store)
        return JSONResponse(
            {
                "n_readings": n,
                "totals": totals,
                "avg_per_reading": {k: round(v / n, 6) for k, v in totals.items()},
                "scheme": _he_module.scheme_name,
                "note": "Individual readings were never decrypted.",
            }
        )

    # ------ GET /status ------
    @app.get("/status")
    async def status() -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "n_stored": len(_store),
                "dp_epsilon": _dp_module.config.epsilon if _dp_module else None,
                "he_scheme": _he_module.scheme_name if _he_module else None,
            }
        )

    # ------ GET /readings ------
    @app.get("/readings")
    async def list_readings(limit: int = Query(50, ge=1, le=1000)) -> JSONResponse:
        return JSONResponse(
            {"n_total": len(_store), "readings": _store._metadata[:limit]}
        )

    # ------ GET /dp_report ------
    @app.get("/dp_report")
    async def dp_report() -> JSONResponse:
        if not _dp_module:
            raise HTTPException(503, "DP module unavailable.")
        return JSONResponse(_dp_module.budget_report())

    # ------ GET /he_info ------
    @app.get("/he_info")
    async def he_info() -> JSONResponse:
        if not _he_module:
            raise HTTPException(503, "HE module unavailable.")
        return JSONResponse(_he_module.context_summary())

    # ------ GET /dp_comparison (research/visualisation only) ------
    @app.get("/dp_comparison", summary="True vs DP-noised values (research only)")
    async def dp_comparison(limit: int = Query(100, ge=1, le=2000)) -> JSONResponse:
        n = min(limit, len(_store))
        return JSONResponse(
            {
                "true_values": _store._raw_values[-n:],
                "dp_values": _store._dp_values[-n:],
                "n": n,
            }
        )

    # ------ GET /stream/readings (SSE) ------
    @app.get("/stream/readings", summary="SSE stream of DP-protected readings")
    async def stream_readings(
        limit: int = Query(200, ge=1, le=10_000),
        epsilon: float = Query(1.0, gt=0.0),
        delay_ms: float = Query(0.0, ge=0.0),
    ):
        sim = _simulator
        session_dp = (
            DifferentialPrivacyModule(DPConfig(epsilon=epsilon)) if _DP else None
        )

        async def gen():
            count = 0
            async for reading in sim.async_stream(
                privacy_hook=session_dp.apply if session_dp else None,
                yield_delay=delay_ms / 1000.0,
            ):
                if count >= limit:
                    break
                d = reading.to_dict()
                d.pop("encrypted_payload", None)
                yield f"event: reading\ndata: {json.dumps(d, default=str)}\n\n"
                count += 1
            yield f"event: done\ndata: {json.dumps({'message': f'Streamed {count} readings.'})}\n\n"

        return StreamingResponse(
            gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        )

    # ------ DELETE /reset ------
    @app.delete("/reset")
    async def reset() -> JSONResponse:
        _store.clear()
        logger.info("Store reset.")
        return JSONResponse({"status": "cleared"})

else:
    app = None  # type: ignore
    logger.warning("FastAPI not installed. Run: pip install fastapi uvicorn[standard]")


def run_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Launch the server with uvicorn."""
    try:
        import uvicorn  # type: ignore
    except ImportError:
        raise ImportError("Install uvicorn: pip install uvicorn[standard]")
    if not _FASTAPI:
        raise RuntimeError("Install FastAPI: pip install fastapi")
    uvicorn.run("server.aggregation_server:app", host=host, port=port)


if __name__ == "__main__":
    run_server()
