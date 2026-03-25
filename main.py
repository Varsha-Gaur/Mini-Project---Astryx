"""
main.py
=======
SecureGrid — Full Pipeline Runner
-----------------------------------
Executes the complete 5-step pipeline:

  1. Smart Meter Simulation   (generate or load data)
  2. Differential Privacy     (Laplace-mechanism noise)
  3. Homomorphic Encryption   (CKKS encrypt each reading)
  4. Secure Aggregation       (HE sum — no individual decryption)
  5. Analytics Engine         (pandas reports + JSON output)

Usage
-----
  python main.py                          # defaults
  python main.py --meters 50 --epsilon 0.5 --samples 1000
  python main.py --server                 # also start FastAPI server
  streamlit run dashboard/dashboard.py   # launch dashboard separately
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

from config import AppConfig
from simulator.smart_meter_simulator import SimulatorConfig  # noqa: E402
from privacy.dp_module import DPConfig
from encryption.he_module import HEConfig
from simulator.smart_meter_simulator import SmartGridSimulator, MeterReading
from privacy.dp_module import DifferentialPrivacyModule
from encryption.he_module import HomomorphicEncryptionModule, EncryptedReading
from analytics.energy_analysis import build_summary_report, privacy_noise_analysis


def _banner(msg: str) -> None:
    line = "─" * 65
    logger.info("")
    logger.info(line)
    logger.info("  %s", msg)
    logger.info(line)


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def step_simulate(cfg: SimulatorConfig, max_records: int) -> List[MeterReading]:
    _banner("STEP 1  ·  Smart Meter Simulation")
    sim = SmartGridSimulator(config=cfg)
    sim.setup(synthetic_minutes=max(max_records // max(cfg.num_meters, 1) + 1, 60))
    readings = sim.simulate_batch(max_records)
    logger.info("Collected %d raw readings.", len(readings))
    if readings:
        r = readings[0]
        logger.info(
            "  Sample: %s | %s | %.4f kW | region %s",
            r.meter_id,
            r.timestamp,
            r.energy_usage,
            r.region_id,
        )
    return readings


def step_apply_dp(readings, dp_cfg):
    _banner(f"STEP 2  ·  Differential Privacy  (ε = {dp_cfg.epsilon})")
    dp = DifferentialPrivacyModule(dp_cfg)
    true_e: List[float] = []
    noisy_e: List[float] = []
    for r in readings:
        true_e.append(r.energy_usage)
        dp.apply(r)
        noisy_e.append(r.energy_usage)
    for i in range(min(3, len(readings))):
        logger.info(
            "  [%s] true=%.4f → noised=%.4f  (Δ=%+.4f)",
            readings[i].meter_id,
            true_e[i],
            noisy_e[i],
            noisy_e[i] - true_e[i],
        )
    logger.info("Budget: %s", dp.budget_report())
    return readings, true_e, noisy_e


def step_encrypt(readings, he_cfg):
    _banner("STEP 3  ·  Homomorphic Encryption  (CKKS)")
    he = HomomorphicEncryptionModule(he_cfg)
    enc = [he.encrypt(r) for r in readings]
    logger.info(
        "Encrypted %d readings | scheme=%s | real_crypto=%s",
        len(enc),
        he.scheme_name,
        he.is_real_he,
    )
    return enc, he


def step_aggregate(encrypted, he, n):
    _banner("STEP 4  ·  Secure Homomorphic Aggregation")
    logger.info(
        "Aggregating %d ciphertexts without decrypting individually …", len(encrypted)
    )
    t0 = time.perf_counter()
    agg_ct = he.aggregate_ciphertexts(encrypted)
    totals = he.decrypt_aggregate(agg_ct)
    elapsed = round(time.perf_counter() - t0, 4)
    logger.info("Aggregate decrypted: %s", totals)
    logger.info("Aggregation time: %.4f s", elapsed)
    return {
        "n_readings": n,
        "he_aggregate_totals": totals,
        "aggregation_time_s": elapsed,
        "scheme": he.scheme_name,
    }


def step_analytics(readings, true_e, noisy_e):
    _banner("STEP 5  ·  Analytics Engine")
    df = pd.DataFrame([r.to_dict() for r in readings])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    report = build_summary_report(df, true_e, noisy_e)
    s = report["summary"]
    logger.info("Total readings:  %d", s["total_readings"])
    logger.info("Active meters:   %d", s["active_meters"])
    logger.info("Total energy:    %.4f kW", s["total_kwh"])
    logger.info("Avg energy:      %.6f kW", s["avg_kwh"])
    if "privacy_analysis" in report:
        pa = report["privacy_analysis"]
        logger.info("DP MAE:          %.6f kW", pa["mae"])
        logger.info("DP RMSE:         %.6f kW", pa["rmse"])
        logger.info("Relative error:  %.4f %%", pa["relative_error_pct"])
    for row in report["regional"]:
        logger.info(
            "  %-10s total=%.3f kW  share=%.1f%%  meters=%d",
            row.get("region", row.get("region_id", "")),
            row["total_kwh"],
            row.get("share_pct", 0),
            row["meter_count"],
        )
    return report


def save_report(data: Dict[str, Any], path: str = "pipeline_report.json") -> None:
    def _clean(obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return str(obj)
        raise TypeError(type(obj))

    with open(path, "w") as fh:
        json.dump(data, fh, indent=2, default=_clean)
    logger.info("Report saved → %s", path)


# ---------------------------------------------------------------------------
# Optional server
# ---------------------------------------------------------------------------


def start_server_thread(host="0.0.0.0", port=8000):
    import threading

    try:
        import uvicorn
        from server.aggregation_server import app
    except ImportError as exc:
        logger.warning("Cannot start server: %s", exc)
        return

    def _run():
        uvicorn.run(app, host=host, port=port, log_level="warning")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    time.sleep(1.5)
    logger.info("Aggregation server running at http://%s:%d/docs", host, port)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(description="SecureGrid Pipeline Runner")
    p.add_argument("--meters", type=int, default=20)
    p.add_argument("--samples", type=int, default=400)
    p.add_argument("--epsilon", type=float, default=1.0)
    p.add_argument("--server", action="store_true")
    p.add_argument("--output", default="pipeline_report.json")
    args = p.parse_args()

    logger.info("=" * 65)
    logger.info("  SecureGrid — Hybrid HE + DP Smart Grid Aggregation")
    logger.info(
        "  meters=%d | samples=%d | ε=%.2f", args.meters, args.samples, args.epsilon
    )
    logger.info("=" * 65)

    t_start = time.perf_counter()

    if args.server:
        _banner("PRE-STEP  ·  Starting Aggregation Server")
        start_server_thread()

    sim_cfg = SimulatorConfig(num_meters=args.meters)
    dp_cfg = DPConfig(epsilon=args.epsilon)
    he_cfg = HEConfig()

    readings = step_simulate(sim_cfg, args.samples)
    readings, true_e, ne = step_apply_dp(readings, dp_cfg)
    encrypted, he = step_encrypt(readings, he_cfg)
    agg = step_aggregate(encrypted, he, len(readings))
    analytics = step_analytics(readings, true_e, ne)

    elapsed = round(time.perf_counter() - t_start, 3)
    report = {
        "pipeline": "SecureGrid Hybrid HE + DP",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "parameters": {
            "num_meters": args.meters,
            "samples": args.samples,
            "epsilon": args.epsilon,
            "he_scheme": he.scheme_name,
        },
        "aggregation": agg,
        "analytics": analytics,
        "elapsed_s": elapsed,
    }
    save_report(report, args.output)

    _banner("Pipeline Complete")
    logger.info("Elapsed: %.3f s", elapsed)
    logger.info("HE totals: %s", agg["he_aggregate_totals"])
    logger.info("")
    logger.info("Next: streamlit run dashboard/dashboard.py")
    if args.server:
        logger.info("API: http://localhost:8000/docs")
    logger.info("")


if __name__ == "__main__":
    main()
