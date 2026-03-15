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
