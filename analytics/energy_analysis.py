"""
analytics/energy_analysis.py
Analytics Engine — pandas-based analysis over smart-meter DataFrames.
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
_REQUIRED = {"meter_id", "timestamp", "energy_usage", "voltage", "current"}


def _validate(df):
    missing = _REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing columns: {missing}")


def _ts(df):
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _region_col(df):
    return "region" if "region" in df.columns else "region_id"


def total_consumption_by_time(df, freq="15min"):
    """Resample total grid energy by time window."""
    _validate(df)
    df = _ts(df)
    r = df.set_index("timestamp")["energy_usage"].resample(freq).sum().reset_index()
    r.columns = ["timestamp", "energy_usage"]
    return r


def regional_consumption(df, normalize=False):
    """Aggregate energy by region."""
    _validate(df)
    rc = _region_col(df)
    result = (
        df.groupby(rc)
        .agg(
            total_kwh=("energy_usage", "sum"),
            avg_kwh=("energy_usage", "mean"),
            meter_count=("meter_id", "nunique"),
        )
        .reset_index()
        .sort_values("total_kwh", ascending=False)
    )
    result["total_kwh"] = result["total_kwh"].round(4)
    result["avg_kwh"] = result["avg_kwh"].round(6)
    if normalize:
        t = result["total_kwh"].sum()
        result["share_pct"] = (result["total_kwh"] / t * 100).round(2)
    return result


def peak_load_detection(df, percentile=90.0, freq="15min"):
    """Flag intervals where load exceeds a percentile threshold."""
    _validate(df)
    df = _ts(df)
    load = df.set_index("timestamp")["energy_usage"].resample(freq).sum().reset_index()
    load.columns = ["timestamp", "total_load"]
    thr = float(np.percentile(load["total_load"].dropna(), percentile))
    load["is_peak"] = load["total_load"] >= thr
    load["threshold"] = round(thr, 4)
    logger.info(
        "Peak detection | threshold=%.4f kW | peaks=%d/%d",
        thr,
        int(load["is_peak"].sum()),
        len(load),
    )
    return load


def meter_statistics(df):
    """Per-meter summary statistics."""
    _validate(df)
    rc = _region_col(df)
    stats = (
        df.groupby(["meter_id", rc])
        .agg(
            mean_kwh=("energy_usage", "mean"),
            std_kwh=("energy_usage", "std"),
            min_kwh=("energy_usage", "min"),
            max_kwh=("energy_usage", "max"),
            total_kwh=("energy_usage", "sum"),
            reading_count=("energy_usage", "count"),
            avg_voltage=("voltage", "mean"),
            avg_current=("current", "mean"),
        )
        .reset_index()
    )
    for c in (
        "mean_kwh",
        "std_kwh",
        "min_kwh",
        "max_kwh",
        "total_kwh",
        "avg_voltage",
        "avg_current",
    ):
        stats[c] = stats[c].round(4)
    return stats


def privacy_noise_analysis(true_values, noisy_values):
    """Quantify privacy-utility trade-off between true and DP-noised values."""
    if len(true_values) != len(noisy_values):
        raise ValueError("Lists must have equal length.")
    if not true_values:
        return {}
    t = np.array(true_values, dtype=float)
    n = np.array(noisy_values, dtype=float)
    errors = n - t
    mean_t = float(np.mean(t)) or 1e-9
    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(errors**2)))
    return {
        "mae": round(mae, 6),
        "rmse": round(rmse, 6),
        "max_error": round(float(np.max(np.abs(errors))), 6),
        "mean_noise": round(float(np.mean(errors)), 6),
        "std_noise": round(float(np.std(errors)), 6),
        "relative_error_pct": round(mae / mean_t * 100, 4),
        "privacy_utility_score": round(rmse / mean_t, 6),
        "n_samples": len(true_values),
    }


def anomaly_detection(df, column="energy_usage", z_threshold=3.0):
    """Flag outliers with Z-score method."""
    result = df.copy()
    mu, sigma = result[column].mean(), result[column].std()
    if sigma == 0:
        result["z_score"] = 0.0
        result["is_anomaly"] = False
        return result
    result["z_score"] = ((result[column] - mu) / sigma).round(4)
    result["is_anomaly"] = result["z_score"].abs() > z_threshold
    logger.info(
        "Anomaly detection on '%s' | z>%.1f | anomalies=%d/%d",
        column,
        z_threshold,
        int(result["is_anomaly"].sum()),
        len(result),
    )
    return result


def hourly_load_profile(df, region=None):
    """Average energy by hour of day (0-23)."""
    _validate(df)
    df = _ts(df)
    if region:
        rc = _region_col(df)
        df = df[df[rc] == region]
    work = df.copy()
    work["hour"] = work["timestamp"].dt.hour
    profile = (
        work.groupby("hour")["energy_usage"]
        .agg(mean_kwh="mean", std_kwh="std")
        .reset_index()
    )
    profile["std_kwh"] = profile["std_kwh"].fillna(0).round(4)
    profile["mean_kwh"] = profile["mean_kwh"].round(4)
    return profile


def build_summary_report(df, true_values=None, noisy_values=None):
    """Compile a complete analytics report dictionary."""
    _validate(df)
    rc = _region_col(df)
    report = {
        "summary": {
            "total_readings": len(df),
            "active_meters": int(df["meter_id"].nunique()),
            "regions": df[rc].unique().tolist(),
            "total_kwh": round(float(df["energy_usage"].sum()), 4),
            "avg_kwh": round(float(df["energy_usage"].mean()), 6),
            "avg_voltage": round(float(df["voltage"].mean()), 2),
            "avg_current": round(float(df["current"].mean()), 3),
        },
        "regional": regional_consumption(df, normalize=True).to_dict("records"),
        "top_consumers": (
            meter_statistics(df)
            .sort_values("total_kwh", ascending=False)
            .head(10)
            .to_dict("records")
        ),
        "anomalies": (
            anomaly_detection(df)
            .query("is_anomaly")
            .head(20)[["meter_id", "timestamp", "energy_usage", "z_score"]]
            .to_dict("records")
        ),
    }
    if true_values and noisy_values:
        report["privacy_analysis"] = privacy_noise_analysis(true_values, noisy_values)
    return report
