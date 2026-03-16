"""
api_server.py
=============
FastAPI Streaming Endpoint for Smart Grid Simulator
----------------------------------------------------
Provides a production-quality FastAPI application with:

  /stream/readings      – Server-Sent Events (SSE) real-time meter stream
  /stream/encrypted     – SSE stream of DP-noised + HE-encrypted readings
  /batch/export         – REST endpoint: export N readings as JSON
  /aggregate            – REST endpoint: HE aggregate over N readings
  /status               – REST endpoint: pipeline health + budget report
  /ws/readings          – WebSocket endpoint (alternative to SSE)

Security pipeline per request
------------------------------
    HTTP request
        │
        ▼
    SmartGridSimulator.async_stream()
        │
        ├─► DifferentialPrivacyModule.apply()
        │
        ├─► HomomorphicEncryptionModule.encrypt()
        │
        ├─► KafkaStreamProducer.publish_reading()
        │
        └─► StreamingResponse (SSE / WebSocket / JSON)

Running
-------
    # Install:  pip install fastapi uvicorn[standard] sse-starlette
    # Run:
    uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload

    # Or programmatically:
    python api_server.py

Endpoints
---------
    GET /stream/readings?limit=100&epsilon=1.0
    GET /stream/encrypted?limit=100&epsilon=0.5
    GET /batch/export?limit=500&output_path=batch.jsonl
    POST /aggregate   body: {"limit": 50}
    GET /status
    WS  /ws/readings?limit=100

"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

logger = logging.getLogger("api_server")

# ---------------------------------------------------------------------------
# Optional dependency imports with helpful error messages
# ---------------------------------------------------------------------------
try:
    from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, StreamingResponse

    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    FastAPI = None  # type: ignore

try:
    from sse_starlette.sse import EventSourceResponse  # type: ignore

    _SSE_AVAILABLE = True
except ImportError:
    _SSE_AVAILABLE = False
    EventSourceResponse = None  # type: ignore

# ---------------------------------------------------------------------------
# Smart-grid modules
# ---------------------------------------------------------------------------
from smart_meter_simulator import (
    SmartGridSimulator,
    SimulatorConfig,
    MeterReading,
    generate_synthetic_rows,
)

try:
    from dp_module import DifferentialPrivacyModule, DPConfig

    _DP_AVAILABLE = True
except ImportError:
    _DP_AVAILABLE = False
    DifferentialPrivacyModule = None  # type: ignore
    DPConfig = None  # type: ignore

try:
    from he_module import HomomorphicEncryptionModule, HEConfig

    _HE_AVAILABLE = True
except ImportError:
    _HE_AVAILABLE = False
    HomomorphicEncryptionModule = None  # type: ignore
    HEConfig = None  # type: ignore

try:
    from kafka_producer import KafkaStreamProducer, KafkaConfig

    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False
    KafkaStreamProducer = None  # type: ignore
    KafkaConfig = None  # type: ignore


# ===========================================================================
# Application bootstrap
# ===========================================================================

# These are populated during lifespan startup and shared across all requests.
_simulator: Optional[SmartGridSimulator] = None
_dp_module: Optional[Any] = None
_he_module: Optional[Any] = None
_kafka_producer: Optional[Any] = None


def _build_simulator(
    dataset_path: str = "household_power_consumption.txt",
    num_meters: int = 100,
    sample_size: int = 2880,
    use_synthetic: bool = False,
) -> SmartGridSimulator:
    """
    Construct and initialise the simulator with all available security modules.
    Called once at application startup.
    """
    global _dp_module, _he_module, _kafka_producer

    # Security modules
    _dp_module = (
        DifferentialPrivacyModule(DPConfig(epsilon=1.0)) if _DP_AVAILABLE else None
    )
    _he_module = HomomorphicEncryptionModule(HEConfig()) if _HE_AVAILABLE else None
    _kafka_producer = (
        KafkaStreamProducer(KafkaConfig(use_mock=True))  # mock broker by default
        if _KAFKA_AVAILABLE
        else None
    )

    cfg = SimulatorConfig(
        num_meters=num_meters,
        sample_size=sample_size,
        enable_dp=_DP_AVAILABLE,
        enable_he=_HE_AVAILABLE,
        enable_kafka=_KAFKA_AVAILABLE,
    )

    sim = SmartGridSimulator(
        dataset_path=dataset_path,
        num_meters=num_meters,
        config=cfg,
        dp_module=_dp_module,
        he_module=_he_module,
        kafka_producer=_kafka_producer,
    )

    # Load data
    if use_synthetic or not os.path.exists(dataset_path):
        logger.warning("Dataset not found or synthetic mode – using synthetic data.")
        sim._raw_rows = generate_synthetic_rows(n=sample_size, seed=42)
        logger.info("Synthetic rows: %d", len(sim._raw_rows))
    else:
        sim.load_dataset()

    sim.create_meters()
    logger.info("Simulator ready. Security status: %s", sim.security_status())
    return sim


# ---------------------------------------------------------------------------
# Lifespan context manager (FastAPI startup / shutdown)
# ---------------------------------------------------------------------------
if _FASTAPI_AVAILABLE:

    @asynccontextmanager
    async def lifespan(application: "FastAPI"):  # type: ignore[name-defined]
        """Startup: build simulator.  Shutdown: flush Kafka."""
        global _simulator
        logger.info("API startup – initialising SmartGridSimulator …")
        _simulator = _build_simulator(
            dataset_path=os.getenv("DATASET_PATH", "household_power_consumption.txt"),
            num_meters=int(os.getenv("NUM_METERS", "100")),
            sample_size=int(os.getenv("SAMPLE_SIZE", "2880")),
            use_synthetic=os.getenv("USE_SYNTHETIC", "1") == "1",
        )
        logger.info("Startup complete.")
        yield
        # Shutdown
        if _kafka_producer is not None:
            try:
                await _kafka_producer.stop()
            except Exception:
                pass
        logger.info("API shutdown complete.")

    # -----------------------------------------------------------------------
    # FastAPI application
    # -----------------------------------------------------------------------
    app = FastAPI(
        title="Smart Grid Secure Aggregation API",
        description=(
            "Streams smart-meter readings through a DP + HE security pipeline. "
            "Supports SSE, WebSocket, and batch REST endpoints."
        ),
        version="2.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -----------------------------------------------------------------------
    # Utility
    # -----------------------------------------------------------------------

    def _get_simulator() -> SmartGridSimulator:
        if _simulator is None:
            raise RuntimeError("Simulator not initialised.")
        return _simulator

    def _sse_event(data: Dict[str, Any], event_type: str = "reading") -> str:
        """Format a dict as an SSE event string."""
        return f"event: {event_type}\ndata: {json.dumps(data, default=str)}\n\n"

    # -----------------------------------------------------------------------
    # /status
    # -----------------------------------------------------------------------

    @app.get("/status", summary="Pipeline health and budget report")
    async def get_status() -> JSONResponse:
        """
        Returns the health of all attached security modules, DP budget,
        HE context, and Kafka producer stats.
        """
        sim = _get_simulator()
        return JSONResponse(
            content={
                "status": "ok",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "dataset_rows": len(sim._raw_rows),
                "meters": len(sim.meters),
                "security": sim.security_status(),
            }
        )

    # -----------------------------------------------------------------------
    # /stream/readings  –  SSE plaintext (DP-noised only)
    # -----------------------------------------------------------------------

    @app.get(
        "/stream/readings",
        summary="SSE stream of DP-protected meter readings",
    )
    async def stream_readings(
        limit: int = Query(
            default=200, ge=1, le=100_000, description="Max readings to stream"
        ),
        epsilon: float = Query(
            default=1.0, gt=0.0, description="DP epsilon for this session"
        ),
        delay_ms: float = Query(
            default=0.0, ge=0.0, description="Artificial delay between events (ms)"
        ),
    ) -> "EventSourceResponse | StreamingResponse":
        """
        Streams MeterReading objects as Server-Sent Events (SSE).

        Each event carries one JSON-encoded MeterReading after DP noise has
        been applied.  The HE ciphertext is excluded from this endpoint
        (use /stream/encrypted for the full encrypted payload).

        Query Parameters
        ----------------
        limit    : Number of readings to emit before closing the stream.
        epsilon  : Privacy budget ε used for this session's Laplace noise.
        delay_ms : Milliseconds of simulated latency per reading.
        """
        sim = _get_simulator()

        # Per-request DP module with caller-specified epsilon
        session_dp = (
            DifferentialPrivacyModule(DPConfig(epsilon=epsilon))
            if _DP_AVAILABLE
            else None
        )

        async def event_generator() -> AsyncGenerator[str, None]:
            count = 0
            async for reading in sim.async_stream(
                privacy_hook=session_dp.apply if session_dp else None,
                yield_delay=delay_ms / 1000.0,
            ):
                if count >= limit:
                    break
                # Strip encrypted_payload from this endpoint
                d = reading.to_dict()
                d.pop("encrypted_payload", None)
                yield _sse_event(d, event_type="reading")
                count += 1

            yield _sse_event(
                {"message": f"Stream complete – {count} readings sent."},
                event_type="done",
            )

        if _SSE_AVAILABLE:
            return EventSourceResponse(event_generator())
        else:
            # Fallback: plain text/event-stream without sse-starlette
            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

    # -----------------------------------------------------------------------
    # /stream/encrypted  –  SSE full pipeline (DP + HE)
    # -----------------------------------------------------------------------

    @app.get(
        "/stream/encrypted",
        summary="SSE stream of DP + HE encrypted readings",
    )
    async def stream_encrypted(
        limit: int = Query(default=100, ge=1, le=10_000),
        epsilon: float = Query(default=0.5, gt=0.0),
    ) -> "EventSourceResponse | StreamingResponse":
        """
        Streams EncryptedReading objects after the full DP + HE pipeline.

        The ``ciphertext_vector`` field contains a base64-encoded CKKS
        ciphertext.  The aggregation server can sum ciphertexts without
        decrypting individual readings.
        """
        if not _HE_AVAILABLE or _he_module is None:
            return JSONResponse(
                status_code=503,
                content={"error": "HE module not available. Install tenseal."},
            )

        sim = _get_simulator()
        session_dp = (
            DifferentialPrivacyModule(DPConfig(epsilon=epsilon))
            if _DP_AVAILABLE
            else None
        )

        async def enc_event_generator() -> AsyncGenerator[str, None]:
            enc_readings = []
            count = 0
            async for reading in sim.async_stream(
                privacy_hook=session_dp.apply if session_dp else None,
            ):
                if count >= limit:
                    break
                enc = _he_module.encrypt(reading)
                enc_readings.append(enc)
                yield _sse_event(enc.to_dict(), event_type="encrypted_reading")
                count += 1

            # Emit the homomorphic aggregate over the streamed batch
            if enc_readings:
                try:
                    agg_ct = _he_module.aggregate_ciphertexts(enc_readings)
                    agg = _he_module.decrypt_aggregate(agg_ct)
                    yield _sse_event(
                        {"aggregate": agg, "n_readings": count},
                        event_type="aggregate",
                    )
                except Exception as exc:
                    logger.error("Aggregation failed: %s", exc)

            yield _sse_event(
                {"message": f"Stream complete – {count} readings."}, event_type="done"
            )

        if _SSE_AVAILABLE:
            return EventSourceResponse(enc_event_generator())
        else:
            return StreamingResponse(
                enc_event_generator(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

    # -----------------------------------------------------------------------
    # /batch/export  –  REST batch endpoint
    # -----------------------------------------------------------------------

    @app.get("/batch/export", summary="Export a batch of readings as JSON")
    async def batch_export(
        limit: int = Query(default=500, ge=1, le=50_000),
        epsilon: float = Query(default=1.0, gt=0.0),
        output_path: Optional[str] = Query(
            default=None, description="Optional server-side JSONL path"
        ),
    ) -> JSONResponse:
        """
        Returns up to ``limit`` DP-protected readings as a JSON array.
        Optionally writes to a JSONL file on the server.
        """
        sim = _get_simulator()
        session_dp = (
            DifferentialPrivacyModule(DPConfig(epsilon=epsilon))
            if _DP_AVAILABLE
            else None
        )
        batch = sim.export_batch(
            max_records=limit,
            output_path=output_path,
            privacy_hook=session_dp.apply if session_dp else None,
        )
        return JSONResponse(content={"count": len(batch), "readings": batch})

    # -----------------------------------------------------------------------
    # /aggregate  –  Homomorphic aggregation
    # -----------------------------------------------------------------------

    @app.post("/aggregate", summary="HE aggregate over N readings")
    async def he_aggregate(
        limit: int = Query(default=50, ge=1, le=1000),
        epsilon: float = Query(default=0.5, gt=0.0),
    ) -> JSONResponse:
        """
        Runs the full DP + HE pipeline over ``limit`` readings, sums the
        ciphertexts homomorphically, then decrypts only the aggregate.

        This is the core privacy-preserving aggregation pattern:
          - Individual readings are **never** decrypted.
          - Only the sum is revealed.
        """
        if not _HE_AVAILABLE or _he_module is None:
            return JSONResponse(
                status_code=503,
                content={"error": "HE module not available. Install tenseal."},
            )

        sim = _get_simulator()
        session_dp = (
            DifferentialPrivacyModule(DPConfig(epsilon=epsilon))
            if _DP_AVAILABLE
            else None
        )

        # Collect encrypted readings (synchronous; runs in thread pool via executor)
        loop = asyncio.get_event_loop()
        enc_readings = await loop.run_in_executor(
            None,
            lambda: sim.collect_encrypted_batch(max_records=limit),
        )

        agg_ct = _he_module.aggregate_ciphertexts(enc_readings)
        agg = _he_module.decrypt_aggregate(agg_ct)

        budget = _dp_module.budget_report() if _dp_module else {}

        return JSONResponse(
            content={
                "n_readings_aggregated": len(enc_readings),
                "he_scheme": _he_module.scheme_name,
                "aggregate_sums": agg,
                "dp_budget_report": budget,
            }
        )

    # -----------------------------------------------------------------------
    # /ws/readings  –  WebSocket endpoint
    # -----------------------------------------------------------------------

    @app.websocket("/ws/readings")
    async def ws_readings(
        websocket: WebSocket,
        limit: int = Query(default=200),
        epsilon: float = Query(default=1.0),
    ) -> None:
        """
        WebSocket alternative to the SSE stream.

        Sends JSON-encoded MeterReading objects one per message.
        The client can disconnect at any time to stop the stream.
        """
        await websocket.accept()
        sim = _get_simulator()
        session_dp = (
            DifferentialPrivacyModule(DPConfig(epsilon=epsilon))
            if _DP_AVAILABLE
            else None
        )
        count = 0
        try:
            async for reading in sim.async_stream(
                privacy_hook=session_dp.apply if session_dp else None,
            ):
                if count >= limit:
                    break
                d = reading.to_dict()
                d.pop("encrypted_payload", None)
                await websocket.send_json(d)
                count += 1

            await websocket.send_json(
                {"event": "done", "message": f"Streamed {count} readings."}
            )
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected after %d readings.", count)
        except Exception as exc:
            logger.error("WebSocket error: %s", exc)
            await websocket.close(code=1011)

    # -----------------------------------------------------------------------
    # Kafka diagnostics endpoint
    # -----------------------------------------------------------------------

    @app.get("/kafka/stats", summary="Kafka producer statistics")
    async def kafka_stats() -> JSONResponse:
        """Returns the Kafka (or mock broker) publish statistics."""
        if _kafka_producer is None:
            return JSONResponse(
                status_code=503,
                content={"error": "Kafka producer not available."},
            )
        return JSONResponse(content=_kafka_producer.stats())

else:
    # FastAPI not installed – define a placeholder so the file can still be imported
    # and the simulator / security modules can be used directly.
    app = None  # type: ignore
    logger.warning(
        "FastAPI is not installed.  Install with:  pip install fastapi uvicorn[standard] sse-starlette\n"
        "The simulator, DP module, and HE module are still usable directly."
    )


# ===========================================================================
# Standalone runner (without uvicorn CLI)
# ===========================================================================


def run_server(
    host: str = "0.0.0.0",
    port: int = 8000,
    reload: bool = False,
    log_level: str = "info",
) -> None:
    """
    Launch the FastAPI server programmatically.

    Requires:  pip install uvicorn[standard]

    Parameters
    ----------
    host      : Bind address.
    port      : TCP port.
    reload    : Enable hot-reload (development only).
    log_level : Uvicorn log level.
    """
    try:
        import uvicorn  # type: ignore
    except ImportError:
        raise ImportError(
            "uvicorn is required to run the server.\n"
            "Install with:  pip install uvicorn[standard]"
        )

    logger.info("Starting Smart Grid API server on %s:%d", host, port)
    uvicorn.run(
        "api_server:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level,
    )


# ===========================================================================
# Demo without server (for environments without FastAPI)
# ===========================================================================


def demo_without_server() -> None:
    """
    Run a full pipeline demonstration without starting the HTTP server.
    Useful for testing in environments where FastAPI / uvicorn are unavailable.
    """
    import json as _json

    print("\n" + "=" * 72)
    print("  Smart Grid Pipeline Demo  (no HTTP server)")
    print("=" * 72)

    # Build simulator
    sim = _build_simulator(use_synthetic=True, num_meters=10, sample_size=60)

    # Sync stream – first 5 readings
    print("\n[1] First 5 readings from simulate_stream():\n")
    for i, reading in enumerate(sim.simulate_stream()):
        if i >= 5:
            break
        print(reading.to_json())
        print("-" * 72)

    # DP budget
    if _dp_module:
        print("\n[2] DP Budget Report:")
        print(_json.dumps(_dp_module.budget_report(), indent=2))

    # HE aggregation demo
    if _he_module:
        print("\n[3] HE Aggregation over 10 encrypted readings:")
        enc_batch = sim.collect_encrypted_batch(max_records=10)
        agg_ct = _he_module.aggregate_ciphertexts(enc_batch)
        agg = _he_module.decrypt_aggregate(agg_ct)
        print(_json.dumps({"n": len(enc_batch), "sums": agg}, indent=2))

    # Kafka stats
    if _kafka_producer:
        print("\n[4] Kafka Stats:")
        print(_json.dumps(_kafka_producer.stats(), indent=2))

    print("\n✓ Demo complete.\n")


if __name__ == "__main__":
    import sys

    if not _FASTAPI_AVAILABLE:
        print("FastAPI not installed — running offline demo instead.")
        demo_without_server()
        sys.exit(0)

    # Parse simple CLI arguments
    args = sys.argv[1:]
    if "--demo" in args:
        demo_without_server()
    else:
        run_server(
            host=os.getenv("HOST", "0.0.0.0"),
            port=int(os.getenv("PORT", "8000")),
            reload="--reload" in args,
        )
