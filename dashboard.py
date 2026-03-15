"""
dashboard/dashboard.py
========================
SecureGrid Research Dashboard
-------------------------------
Interactive Streamlit dashboard for the Hybrid Secure Data Aggregation
research project.  Visualises smart-meter data flowing through the
Differential Privacy + Homomorphic Encryption pipeline.

Aesthetic direction: Deep-space data-terminal.
Obsidian backgrounds, electric-teal accents for HE data,
saffron for energy signals, crimson for threat/attack events.
Typography: IBM Plex Mono for data labels, Outfit for UI text.

Run:
    pip install streamlit pandas plotly numpy
    streamlit run dashboard/dashboard.py

"""

from __future__ import annotations

import hashlib
import logging
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ---------------------------------------------------------------------------
# Path setup — allow imports from project root
# ---------------------------------------------------------------------------
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ---------------------------------------------------------------------------
# Optional project module imports
# ---------------------------------------------------------------------------
try:
    from simulator.smart_meter_simulator import SmartGridSimulator, REGIONS

    _SIM_AVAILABLE = True
except ImportError:
    _SIM_AVAILABLE = False
    REGIONS = ["north", "south", "east", "west", "central"]

try:
    from privacy.differential_privacy import DifferentialPrivacyEngine, DPConfig

    _DP_AVAILABLE = True
except ImportError:
    _DP_AVAILABLE = False

try:
    from analytics.energy_analysis import (
        regional_consumption,
        peak_load_detection,
        meter_statistics,
        privacy_noise_analysis,
        hourly_load_profile,
        anomaly_detection,
    )

    _ANALYTICS_AVAILABLE = True
except ImportError:
    _ANALYTICS_AVAILABLE = False

# ============================================================
# PAGE CONFIG  — must be first Streamlit call
# ============================================================
st.set_page_config(
    page_title="SecureGrid · Research Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# GLOBAL DESIGN TOKENS
# ============================================================
C = dict(
    bg0="#080c14",
    bg1="#0d1220",
    bg2="#111827",
    bg3="#1a2235",
    border="#1e2d45",
    teal="#00c8b4",
    teal2="#5ef0e0",
    saffron="#f7b731",
    saffron2="#ffd166",
    crimson="#ef4444",
    violet="#a78bfa",
    green="#34d399",
    muted="#4b5a70",
    text="#dde4f0",
    text2="#7a8fa8",
)

STYLE = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&family=Outfit:wght@300;400;500;600&display=swap');

:root {{
  --bg0:{C["bg0"]};--bg1:{C["bg1"]};--bg2:{C["bg2"]};--bg3:{C["bg3"]};
  --border:{C["border"]};--teal:{C["teal"]};--teal2:{C["teal2"]};
  --saffron:{C["saffron"]};--saffron2:{C["saffron2"]};
  --crimson:{C["crimson"]};--violet:{C["violet"]};--green:{C["green"]};
  --muted:{C["muted"]};--text:{C["text"]};--text2:{C["text2"]};
}}

/* ── Shell ── */
.stApp {{ background: var(--bg0); color: var(--text); font-family:'Outfit',sans-serif; }}
.main .block-container {{ padding:1.5rem 2.2rem 3rem; max-width:1440px; }}
#MainMenu, footer, header {{ visibility:hidden; }}
.stDeployButton {{ display:none; }}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {{
  background: var(--bg1);
  border-right:1px solid var(--border);
}}
section[data-testid="stSidebar"] label {{
  color:var(--text2) !important;
  font-family:'IBM Plex Mono',monospace;
  font-size:0.71rem;
  letter-spacing:0.1em;
  text-transform:uppercase;
}}

/* ── Metric cards ── */
div[data-testid="stMetric"] {{
  background:var(--bg2);
  border:1px solid var(--border);
  border-radius:12px;
  padding:1.1rem 1.3rem;
  position:relative;
  overflow:hidden;
}}
div[data-testid="stMetric"]::before {{
  content:'';
  position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--saffron),var(--teal));
}}
div[data-testid="stMetric"] label {{
  color:var(--text2) !important;
  font-family:'IBM Plex Mono',monospace !important;
  font-size:0.68rem !important;
  letter-spacing:0.12em;
  text-transform:uppercase;
}}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
  color:var(--saffron2) !important;
  font-family:'IBM Plex Mono',monospace !important;
  font-size:1.65rem !important;
  letter-spacing:-0.01em;
}}

/* ── Section headers ── */
.sg-title {{
  font-family:'IBM Plex Mono',monospace;
  font-size:0.68rem;
  letter-spacing:0.2em;
  text-transform:uppercase;
  color:var(--teal);
  border-bottom:1px solid var(--border);
  padding-bottom:0.45rem;
  margin-bottom:1rem;
}}

/* ── Hero ── */
.sg-hero {{
  background:linear-gradient(135deg,var(--bg2) 0%,var(--bg3) 100%);
  border:1px solid var(--border);
  border-radius:16px;
  padding:2.2rem 2.8rem;
  margin-bottom:1.8rem;
  position:relative;
  overflow:hidden;
}}
.sg-hero::before {{
  content:'';
  position:absolute;top:0;left:0;right:0;bottom:0;
  background:radial-gradient(ellipse at 80% 50%,rgba(0,200,180,.06),transparent 60%);
  pointer-events:none;
}}
.sg-hero h1 {{
  font-family:'IBM Plex Mono',monospace;
  font-size:1.55rem;
  color:var(--saffron2);
  margin:0 0 0.4rem;
  letter-spacing:0.03em;
}}
.sg-hero .subtitle {{
  font-size:0.9rem;
  color:var(--text2);
  margin:0 0 1.1rem;
  line-height:1.6;
}}
.pipe-row {{
  display:flex;flex-wrap:wrap;gap:0.4rem;align-items:center;
}}
.pipe-node {{
  background:var(--bg0);
  border:1px solid var(--border);
  border-radius:6px;
  padding:0.28rem 0.75rem;
  font-family:'IBM Plex Mono',monospace;
  font-size:0.68rem;
  color:var(--teal);
  letter-spacing:0.06em;
  white-space:nowrap;
}}
.pipe-arrow {{ color:var(--muted);font-size:0.8rem; }}

/* ── Status badges ── */
.badge-live  {{background:#0a2e28;color:var(--teal2);border:1px solid var(--teal);border-radius:4px;padding:2px 9px;font-family:'IBM Plex Mono',monospace;font-size:0.68rem;}}
.badge-dp    {{background:#2e2a08;color:var(--saffron2);border:1px solid var(--saffron);border-radius:4px;padding:2px 9px;font-family:'IBM Plex Mono',monospace;font-size:0.68rem;}}
.badge-he    {{background:#0a1a2e;color:var(--violet);border:1px solid var(--violet);border-radius:4px;padding:2px 9px;font-family:'IBM Plex Mono',monospace;font-size:0.68rem;}}
.badge-warn  {{background:#2e0a0a;color:var(--crimson);border:1px solid var(--crimson);border-radius:4px;padding:2px 9px;font-family:'IBM Plex Mono',monospace;font-size:0.68rem;}}

/* ── Cards ── */
.info-card {{
  background:var(--bg2);border:1px solid var(--border);
  border-radius:10px;padding:1.1rem 1.4rem;height:100%;
}}
.he-card {{
  background:linear-gradient(135deg,#080f20,#0a1530);
  border:1px solid var(--violet);
  border-radius:10px;padding:1.3rem 1.6rem;
}}
.he-card h4 {{
  font-family:'IBM Plex Mono',monospace;color:var(--violet);
  font-size:0.82rem;letter-spacing:0.08em;margin:0 0 0.6rem;
}}
.atk-card {{
  background:linear-gradient(135deg,#1a0808,#2a0f0f);
  border:1px solid var(--crimson);
  border-radius:10px;padding:1.3rem 1.6rem;
}}
.atk-card h4 {{
  font-family:'IBM Plex Mono',monospace;color:var(--crimson);
  font-size:0.82rem;letter-spacing:0.08em;margin:0 0 0.6rem;
}}
.mono {{ font-family:'IBM Plex Mono',monospace;font-size:0.75rem;color:var(--teal2);word-break:break-all; }}
.mono-sm {{ font-family:'IBM Plex Mono',monospace;font-size:0.68rem;color:var(--text2); }}

/* ── Buttons ── */
.stButton>button {{
  background:linear-gradient(135deg,#0a2218,#082a20) !important;
  color:var(--green) !important;
  border:1px solid var(--green) !important;
  border-radius:8px !important;
  font-family:'IBM Plex Mono',monospace !important;
  font-size:0.78rem !important;
  letter-spacing:0.08em !important;
  padding:0.5rem 1.4rem !important;
  transition:all .2s !important;
}}
.stButton>button:hover {{
  background:var(--green) !important;
  color:#080c14 !important;
  box-shadow:0 0 20px rgba(52,211,153,.3) !important;
}}

/* ── Tables ── */
div[data-testid="stDataFrame"] thead tr th {{
  background:var(--bg3) !important;
  color:var(--text2) !important;
  font-family:'IBM Plex Mono',monospace;
  font-size:0.65rem;
  letter-spacing:0.12em;
  text-transform:uppercase;
}}
div[data-testid="stDataFrame"] tbody tr td {{
  font-size:0.8rem;
  color:var(--text);
}}

/* ── Expander ── */
div[data-testid="stExpander"] {{
  border:1px solid var(--border) !important;
  border-radius:10px !important;
  background:var(--bg1) !important;
}}
div[data-testid="stExpander"] summary {{
  font-family:'IBM Plex Mono',monospace;
  font-size:0.78rem;
  letter-spacing:0.05em;
}}

/* ── Dividers ── */
hr {{ border-color:var(--border) !important;margin:1.8rem 0 !important; }}

/* ── Slider / select ── */
div[data-testid="stSlider"] .stSlider {{ color:var(--teal) !important; }}
</style>
"""

# ============================================================
# PLOTLY SHARED THEME
# ============================================================
# Root cause of "got multiple values for keyword argument 'legend'":
#   fig.update_layout(**CHART_THEME, legend=dict(...))  <- SAME key twice
# Fix: use _layout() which merges via dict.update() so overrides always win
# and no key is ever duplicated in a single call.
_CT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(13,18,32,0.7)",
    font=dict(family="Outfit, sans-serif", color=C["text2"], size=11),
    xaxis=dict(
        gridcolor="#1e2d45",
        linecolor="#1e2d45",
        tickfont=dict(size=9),
        title_font=dict(size=10),
    ),
    yaxis=dict(
        gridcolor="#1e2d45",
        linecolor="#1e2d45",
        tickfont=dict(size=9),
        title_font=dict(size=10),
    ),
    margin=dict(l=48, r=16, t=36, b=36),
    legend=dict(
        bgcolor="rgba(13,18,32,.85)",
        bordercolor="#1e2d45",
        borderwidth=1,
        font=dict(size=9),
    ),
)
_CT = _CT_BASE  # backward-compat alias


def _layout(height: int = 340, **overrides) -> dict:
    """
    Return a merged Plotly layout dict: base theme + caller overrides.

    Overrides win via dict.update(), so the same key can never appear
    twice in one update_layout() call — eliminating the TypeError:
        "got multiple values for keyword argument 'legend'"

    Usage
    -----
    fig.update_layout(**_layout(300, legend=dict(x=1, y=0.5), title=...))
    """
    merged = dict(_CT_BASE)  # shallow copy
    merged["height"] = height
    merged.update(overrides)  # overrides win; no duplicate key possible
    return merged


REGION_COLORS = {
    "north": C["saffron"],
    "south": C["teal"],
    "east": C["green"],
    "west": C["violet"],
    "central": "#fb923c",
}

# ============================================================
# DATA GENERATION
# ============================================================

_DIURNAL = [
    0.30,
    0.28,
    0.26,
    0.25,
    0.25,
    0.28,
    0.50,
    0.75,
    0.85,
    0.72,
    0.65,
    0.65,
    0.70,
    0.68,
    0.65,
    0.68,
    0.75,
    0.95,
    1.00,
    0.98,
    0.90,
    0.80,
    0.65,
    0.45,
]


def _diurnal(hour: int) -> float:
    return _DIURNAL[hour % 24]


def generate_sample_data(
    n_meters: int = 20,
    n_minutes: int = 120,
    noise_level: float = 0.08,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic smart-meter readings with realistic patterns.

    Includes true values, DP-noised values, encrypted flag,
    sub-metering breakdown, and regional clustering.
    """
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    # Per-meter fixed characteristics
    meta: Dict[str, Dict] = {}
    for i in range(n_meters):
        mid = f"meter_{i:03d}"
        meta[mid] = dict(
            region=REGIONS[i % len(REGIONS)],
            scale=rng.uniform(0.55, 1.65),
            v_off=rng.uniform(-6.0, 6.0),
            i_off=rng.uniform(-0.5, 0.5),
            w_k=rng.uniform(0.1, 1.0),
            w_l=rng.uniform(0.1, 1.0),
            w_h=rng.uniform(0.1, 1.0),
        )

    base_ts = datetime(2026, 3, 15, 8, 0, 0)
    rows: List[Dict] = []

    for minute in range(n_minutes):
        ts = base_ts + timedelta(minutes=minute)
        hour = ts.hour
        d = _diurnal(hour)

        for mid, m in meta.items():
            base_e = max(0.05, (d * 2.0 + np_rng.normal(0, 0.12)) * m["scale"])

            # Peak-hour spike (weekday 17-21)
            if ts.weekday() < 5 and 17 <= hour < 21:
                base_e *= 1.20
            # Weekend reduction
            if ts.weekday() >= 5:
                base_e *= 0.85
            # Occasional appliance spike
            if rng.random() < 0.025:
                base_e += rng.uniform(0.4, 1.8)

            base_e = max(0.05, round(base_e, 4))

            # Voltage / current
            voltage = round(230.0 + m["v_off"] + np_rng.normal(0, 0.7), 2)
            current = round(
                max(
                    0.1,
                    base_e * 1000 / max(voltage, 1.0)
                    + m["i_off"]
                    + np_rng.normal(0, 0.04),
                ),
                3,
            )

            # Sub-metering
            total_sm = max(0.0, base_e * 3 + np_rng.normal(0, 0.5))
            tw = m["w_k"] + m["w_l"] + m["w_h"]
            kitchen = round(total_sm * m["w_k"] / tw, 3)
            laundry = round(total_sm * m["w_l"] / tw, 3)
            hvac = round(total_sm * m["w_h"] / tw, 3)

            # DP noise — Laplace
            scale_lap = noise_level * max(base_e, 0.01)
            dp_noise = np_rng.laplace(0.0, scale_lap)
            noisy_e = max(0.0, round(base_e + dp_noise, 4))

            # Mock HE token
            he_token = (
                hashlib.sha256(f"{noisy_e:.6f}|secret".encode())
                .hexdigest()[:20]
                .upper()
            )

            rows.append(
                dict(
                    meter_id=mid,
                    timestamp=ts,
                    energy_usage=base_e,
                    noisy_energy_usage=noisy_e,
                    voltage=voltage,
                    current=current,
                    region=m["region"],
                    kitchen=kitchen,
                    laundry=laundry,
                    hvac=hvac,
                    privacy_noise=round(abs(dp_noise), 6),
                    encrypted=True,
                    he_token=he_token,
                )
            )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values(["timestamp", "meter_id"]).reset_index(drop=True)


def append_live_tick(df: pd.DataFrame, noise_level: float = 0.08) -> pd.DataFrame:
    """Append one new minute of readings to the DataFrame."""
    rng = np.random.default_rng()
    ts = df["timestamp"].max() + timedelta(minutes=1)
    hour = ts.hour
    d = _diurnal(hour)
    new_rows = []

    for mid in df["meter_id"].unique():
        subset = df[df["meter_id"] == mid]
        region = subset["region"].iloc[0]
        scale = subset["energy_usage"].mean() / max(d * 2.0, 0.01)
        scale = float(np.clip(scale, 0.4, 2.5))
        base_e = max(0.05, (d * 2.0 + rng.normal(0, 0.12)) * scale)
        base_e = round(base_e, 4)
        voltage = round(230.0 + rng.normal(0, 0.7), 2)
        current = round(max(0.1, base_e * 1000 / max(voltage, 1.0)), 3)
        dp_noise = rng.laplace(0.0, noise_level * max(base_e, 0.01))
        noisy_e = max(0.0, round(base_e + dp_noise, 4))
        he_token = (
            hashlib.sha256(f"{noisy_e:.6f}|secret".encode()).hexdigest()[:20].upper()
        )

        total_sm = max(0.0, base_e * 3 + rng.normal(0, 0.5))
        new_rows.append(
            dict(
                meter_id=mid,
                timestamp=ts,
                energy_usage=base_e,
                noisy_energy_usage=noisy_e,
                voltage=voltage,
                current=current,
                region=region,
                kitchen=round(total_sm * 0.35, 3),
                laundry=round(total_sm * 0.30, 3),
                hvac=round(total_sm * 0.35, 3),
                privacy_noise=round(abs(dp_noise), 6),
                encrypted=True,
                he_token=he_token,
            )
        )
    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)


# ============================================================
# CHART BUILDERS
# ============================================================


def _apply_theme(fig: go.Figure, height: int = 340) -> go.Figure:
    """Thin wrapper kept for compatibility. Prefer _layout() for new code."""
    fig.update_layout(**_layout(height))
    return fig


def chart_timeseries(df: pd.DataFrame, meters: List[str]) -> go.Figure:
    """Multi-meter energy time-series."""
    fig = go.Figure()
    palette = px.colors.qualitative.Bold
    sub = df[df["meter_id"].isin(meters)]
    for idx, mid in enumerate(meters[:12]):
        mdf = sub[sub["meter_id"] == mid].sort_values("timestamp")
        fig.add_trace(
            go.Scatter(
                x=mdf["timestamp"],
                y=mdf["energy_usage"],
                name=mid,
                mode="lines",
                line=dict(width=1.7, color=palette[idx % len(palette)]),
                hovertemplate=f"<b>{mid}</b><br>%{{x|%H:%M}}<br>%{{y:.4f}} kW<extra></extra>",
            )
        )
    _apply_theme(fig, 340)
    fig.update_layout(
        title=dict(
            text="Energy Consumption — Live Stream",
            font=dict(size=12, color=C["text"]),
            x=0,
        ),
        xaxis_title="Time",
        yaxis_title="Energy (kW)",
        hovermode="x unified",
    )
    return fig


def chart_noisy_vs_true(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """Side-by-side real vs DP-noised for one meter."""
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(80)
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("True (Private)", "Published (DP-Noised)"),
        horizontal_spacing=0.08,
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["energy_usage"],
            fill="tozeroy",
            fillcolor="rgba(247,183,49,.12)",
            line=dict(color=C["saffron"], width=1.8),
            name="True",
            hovertemplate="True: %{y:.4f}<extra></extra>",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["noisy_energy_usage"],
            fill="tozeroy",
            fillcolor="rgba(0,200,180,.10)",
            line=dict(color=C["teal"], width=1.8),
            name="Noised",
            hovertemplate="Noised: %{y:.4f}<extra></extra>",
        ),
        row=1,
        col=2,
    )
    _apply_theme(fig, 310)
    fig.update_layout(
        title=dict(
            text=f"Differential Privacy Comparison — {meter_id}",
            font=dict(size=12, color=C["text"]),
            x=0,
        ),
        showlegend=False,
    )
    for ax in ["xaxis", "xaxis2", "yaxis", "yaxis2"]:
        fig.layout[ax].update(
            gridcolor="#1e2d45",
            linecolor="#1e2d45",
            tickfont=dict(size=8, color=C["muted"]),
        )
    for ann in fig.layout.annotations:
        ann.font.color = C["text2"]
        ann.font.size = 10
    return fig


def chart_noise_histogram(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """Laplace noise distribution histogram."""
    mdf = df[df["meter_id"] == meter_id]
    fig = go.Figure(
        go.Histogram(
            x=mdf["privacy_noise"],
            nbinsx=35,
            marker_color=C["saffron"],
            marker_line=dict(color=C["bg0"], width=0.5),
            opacity=0.85,
            hovertemplate="Noise: %{x:.5f}<br>Count: %{y}<extra></extra>",
        )
    )
    _apply_theme(fig, 240)
    fig.update_layout(
        title=dict(
            text="Laplace Noise Distribution", font=dict(size=12, color=C["text"]), x=0
        ),
        xaxis_title="Noise |Δ| (kW)",
        yaxis_title="Count",
        showlegend=False,
    )
    return fig


def chart_regional_bar(df: pd.DataFrame) -> go.Figure:
    """Regional total energy bar chart."""
    agg = (
        df.groupby("region")["energy_usage"]
        .sum()
        .reset_index()
        .sort_values("energy_usage", ascending=False)
    )
    colours = [REGION_COLORS.get(r, C["saffron"]) for r in agg["region"]]
    fig = go.Figure(
        go.Bar(
            x=agg["region"],
            y=agg["energy_usage"],
            marker=dict(color=colours, opacity=0.82),
            text=agg["energy_usage"].round(1).astype(str) + " kW",
            textposition="outside",
            textfont=dict(size=10, color=C["text"]),
            hovertemplate="<b>%{x}</b><br>Total: %{y:.2f} kW<extra></extra>",
        )
    )
    _apply_theme(fig, 290)
    fig.update_layout(
        title=dict(
            text="Grid Load by Region", font=dict(size=12, color=C["text"]), x=0
        ),
        xaxis_title="Region",
        yaxis_title="Total Energy (kW)",
        showlegend=False,
    )
    return fig


def chart_region_donut(df: pd.DataFrame) -> go.Figure:
    """Regional share donut chart."""
    agg = df.groupby("region")["energy_usage"].sum().reset_index()
    colours = [REGION_COLORS.get(r, C["saffron"]) for r in agg["region"]]
    fig = go.Figure(
        go.Pie(
            labels=agg["region"],
            values=agg["energy_usage"].round(2),
            hole=0.58,
            marker=dict(colors=colours, line=dict(color=C["bg0"], width=2)),
            textfont=dict(size=9, color=C["text"]),
            hovertemplate="<b>%{label}</b><br>%{value:.2f} kW (%{percent})<extra></extra>",
        )
    )
    # FIX: Use _layout() so legend override is merged safely — no duplicate key.
    fig.update_layout(
        **_layout(
            height=290,
            title=dict(text="Region Share", font=dict(size=12, color=C["text"]), x=0),
            showlegend=True,
            legend=dict(
                orientation="v",
                x=1.0,
                y=0.5,
                bgcolor="rgba(13,18,32,.85)",
                bordercolor="#1e2d45",
                borderwidth=1,
                font=dict(size=9),
            ),
            annotations=[
                dict(
                    text="Load<br>Share",
                    x=0.5,
                    y=0.5,
                    font=dict(size=10, color=C["text2"], family="IBM Plex Mono"),
                    showarrow=False,
                )
            ],
        )
    )
    return fig


def chart_attack_simulation(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """Show adversary's reconstruction attempt vs true + published values."""
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(50)
    attacker = mdf["noisy_energy_usage"].rolling(6, min_periods=1).mean()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["energy_usage"],
            name="True (hidden)",
            mode="lines",
            line=dict(color=C["saffron"], width=2.0, dash="dot"),
            hovertemplate="True: %{y:.4f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["noisy_energy_usage"],
            name="Published (DP-noised)",
            mode="lines",
            line=dict(color=C["teal"], width=1.6),
            hovertemplate="Published: %{y:.4f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=attacker,
            name="Attacker estimate",
            mode="lines",
            line=dict(color=C["crimson"], width=1.8, dash="dash"),
            hovertemplate="Attacker: %{y:.4f}<extra></extra>",
        )
    )
    _apply_theme(fig, 300)
    fig.update_layout(
        title=dict(
            text="Attack Reconstruction vs DP Protection",
            font=dict(size=12, color=C["text"]),
            x=0,
        ),
        xaxis_title="Time",
        yaxis_title="Energy (kW)",
        hovermode="x unified",
    )
    return fig


def chart_voltage_heatmap(df: pd.DataFrame) -> go.Figure:
    """Voltage levels across meters over time."""
    pivot = (
        df.sort_values("timestamp")
        .groupby("meter_id")
        .tail(30)
        .pivot_table(
            index="meter_id", columns="timestamp", values="voltage", aggfunc="mean"
        )
    )
    cols = pivot.columns[-20:] if len(pivot.columns) > 20 else pivot.columns
    pivot = pivot[cols]
    fig = go.Figure(
        go.Heatmap(
            z=pivot.values,
            x=[str(c.strftime("%H:%M")) for c in pivot.columns],
            y=pivot.index.tolist(),
            colorscale=[
                [0, "#0a2e28"],
                [0.35, C["saffron"]],
                [0.65, C["saffron2"]],
                [1, C["crimson"]],
            ],
            colorbar=dict(
                title="V",
                titlefont=dict(size=9, color=C["text2"]),
                tickfont=dict(size=8, color=C["text2"]),
                thickness=10,
            ),
            hovertemplate="Meter: %{y}<br>Time: %{x}<br>Voltage: %{z:.1f} V<extra></extra>",
        )
    )
    # FIX: _layout() merges xaxis override safely — no duplicate xaxis key.
    fig.update_layout(
        **_layout(
            height=310,
            title=dict(
                text="Voltage Heatmap — All Meters",
                font=dict(size=12, color=C["text"]),
                x=0,
            ),
            xaxis=dict(
                tickangle=-45,
                tickfont=dict(size=7),
                gridcolor="rgba(0,0,0,0)",
                linecolor="#1e2d45",
            ),
            yaxis=dict(
                tickfont=dict(size=8), gridcolor="rgba(0,0,0,0)", linecolor="#1e2d45"
            ),
        )
    )
    return fig


def chart_hourly_profile(df: pd.DataFrame) -> go.Figure:
    """Average hourly load profile."""
    df2 = df.copy()
    df2["hour"] = df2["timestamp"].dt.hour
    hp = df2.groupby("hour")["energy_usage"].agg(mean="mean", std="std").reset_index()
    hp["std"] = hp["std"].fillna(0)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=list(hp["hour"]) + list(reversed(hp["hour"].tolist())),
            y=list(hp["mean"] + hp["std"])
            + list(reversed((hp["mean"] - hp["std"]).tolist())),
            fill="toself",
            fillcolor="rgba(0,200,180,.08)",
            line=dict(color="rgba(0,0,0,0)"),
            name="±1σ band",
            showlegend=True,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=hp["hour"],
            y=hp["mean"],
            mode="lines+markers",
            line=dict(color=C["teal"], width=2.2),
            marker=dict(size=5, color=C["teal2"]),
            name="Mean load",
            hovertemplate="Hour %{x}:00 → %{y:.4f} kW<extra></extra>",
        )
    )
    # FIX: _layout() merges xaxis override safely — no duplicate xaxis key.
    fig.update_layout(
        **_layout(
            height=280,
            title=dict(
                text="Hourly Load Profile", font=dict(size=12, color=C["text"]), x=0
            ),
            xaxis=dict(
                title="Hour of Day",
                tickvals=list(range(0, 24, 2)),
                gridcolor="#1e2d45",
                linecolor="#1e2d45",
                tickfont=dict(size=9),
                title_font=dict(size=10),
            ),
            yaxis_title="Avg Energy (kW)",
        )
    )
    return fig


def chart_sub_metering(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """Stacked area chart of sub-metering for a single meter."""
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(60)
    fig = go.Figure()
    for col, colour, name in [
        ("kitchen", C["saffron"], "Kitchen"),
        ("laundry", C["teal"], "Laundry"),
        ("hvac", C["violet"], "HVAC"),
    ]:
        fig.add_trace(
            go.Scatter(
                x=mdf["timestamp"],
                y=mdf[col],
                name=name,
                stackgroup="sub",
                line=dict(color=colour, width=1),
                fillcolor=colour.replace(")", ", 0.35)").replace("#", "rgba(")
                if "rgba" not in colour
                else colour,
                hovertemplate=f"{name}: %{{y:.3f}}<extra></extra>",
            )
        )
    _apply_theme(fig, 260)
    fig.update_layout(
        title=dict(
            text=f"Sub-Metering — {meter_id}", font=dict(size=12, color=C["text"]), x=0
        ),
        xaxis_title="Time",
        yaxis_title="Wh",
    )
    return fig


# ============================================================
# SESSION STATE
# ============================================================


def init_state() -> None:
    if "df" not in st.session_state:
        st.session_state.df = generate_sample_data(n_meters=20, n_minutes=120)
    if "simulating" not in st.session_state:
        st.session_state.simulating = False
    if "tick" not in st.session_state:
        st.session_state.tick = 0
    if "noise_level" not in st.session_state:
        st.session_state.noise_level = 0.08


# ============================================================
# SIDEBAR
# ============================================================


def render_sidebar(df: pd.DataFrame):
    st.sidebar.markdown(
        f"""
        <div style='padding:.8rem 0 .4rem;'>
          <p style='font-family:IBM Plex Mono,monospace;font-size:1.05rem;
                    color:{C["saffron2"]};margin:0;letter-spacing:.06em;'>⚡ SECUREGRID</p>
          <p style='font-family:Outfit,sans-serif;font-size:.7rem;
                    color:{C["muted"]};margin:.2rem 0 0;'>Research Platform v2.0</p>
        </div>
        <hr style='border-color:{C["border"]};margin:.6rem 0;'>
        """,
        unsafe_allow_html=True,
    )

    # Live simulation controls
    st.sidebar.markdown("##### 🔴  Simulation")
    ca, cb = st.sidebar.columns(2)
    with ca:
        if st.button("▶ Start", key="btn_start", use_container_width=True):
            st.session_state.simulating = True
    with cb:
        if st.button("■ Stop", key="btn_stop", use_container_width=True):
            st.session_state.simulating = False

    sim_badge = (
        f'<span class="badge-live">● LIVE</span>'
        if st.session_state.simulating
        else f'<span class="mono-sm">● STATIC</span>'
    )
    st.sidebar.markdown(f"Status: {sim_badge}", unsafe_allow_html=True)
    st.sidebar.markdown("---")

    # Privacy controls
    st.sidebar.markdown("##### 🔒  Privacy Budget")
    noise = st.sidebar.slider(
        "DP Noise σ",
        0.001,
        0.30,
        st.session_state.noise_level,
        step=0.001,
        format="%.3f",
    )
    st.session_state.noise_level = noise
    eps = round(1.0 / max(noise, 1e-4), 1)
    st.sidebar.caption(f"≈ ε = {eps}  (1/σ proxy)")
    st.sidebar.markdown("---")

    # Filters
    st.sidebar.markdown("##### 🔌  Meters")
    all_meters = sorted(df["meter_id"].unique().tolist())
    sel_meters = st.sidebar.multiselect(
        "Select meters", all_meters, default=all_meters[:8]
    )
    if not sel_meters:
        sel_meters = all_meters

    st.sidebar.markdown("##### 🗺  Regions")
    all_regions = sorted(df["region"].unique().tolist())
    sel_regions = st.sidebar.multiselect(
        "Select regions", all_regions, default=all_regions
    )
    if not sel_regions:
        sel_regions = all_regions

    st.sidebar.markdown("##### 🕒  Time Range")
    ts_min = df["timestamp"].min().to_pydatetime()
    ts_max = df["timestamp"].max().to_pydatetime()
    t_start = st.sidebar.slider("From", ts_min, ts_max, ts_min, format="HH:mm")
    t_end = st.sidebar.slider("To", ts_min, ts_max, ts_max, format="HH:mm")
    st.sidebar.markdown("---")

    if st.sidebar.button("↺ Reset Data", use_container_width=True):
        st.session_state.df = generate_sample_data(
            n_meters=20, n_minutes=120, noise_level=noise
        )
        st.session_state.tick = 0
        st.session_state.simulating = False
        st.rerun()

    st.sidebar.markdown(
        f"""<div style='font-family:Outfit,sans-serif;font-size:.68rem;
                       color:{C["muted"]};padding:.8rem 0;line-height:1.65;'>
            Hybrid Secure Data Aggregation<br>
            for Smart Grids using HE + DP<br>
            <em>University Research Prototype</em>
            </div>""",
        unsafe_allow_html=True,
    )

    return sel_meters, sel_regions, (t_start, t_end), noise


# ============================================================
# SECTION RENDERERS
# ============================================================


def s_hero() -> None:
    st.markdown(
        f"""
        <div class="sg-hero">
          <h1>⚡ SecureGrid Research Platform</h1>
          <p class="subtitle">
            Hybrid Secure Data Aggregation for Smart Grids<br>
            using <strong style="color:{C["saffron2"]};">Homomorphic Encryption</strong>
            &amp; <strong style="color:{C["teal2"]};">Differential Privacy</strong>
          </p>
          <div class="pipe-row">
            <span class="pipe-node">Smart Meter</span>
            <span class="pipe-arrow">→</span>
            <span class="pipe-node">DP Module</span>
            <span class="pipe-arrow">→</span>
            <span class="pipe-node">CKKS Encrypt</span>
            <span class="pipe-arrow">→</span>
            <span class="pipe-node">Kafka Stream</span>
            <span class="pipe-arrow">→</span>
            <span class="pipe-node">HE Aggregate</span>
            <span class="pipe-arrow">→</span>
            <span class="pipe-node">FastAPI Server</span>
            <span class="pipe-arrow">→</span>
            <span class="pipe-node">Analytics</span>
            <span class="pipe-arrow">→</span>
            <span class="pipe-node">Dashboard</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def s_badges(simulating: bool) -> None:
    cols = st.columns([1, 1, 1, 1, 6])
    with cols[0]:
        if simulating:
            st.markdown(
                '<span class="badge-live">● LIVE</span>', unsafe_allow_html=True
            )
        else:
            st.markdown(
                f'<span class="mono-sm" style="color:{C["muted"]};">● STATIC</span>',
                unsafe_allow_html=True,
            )
    cols[1].markdown('<span class="badge-dp">🔐 DP</span>', unsafe_allow_html=True)
    cols[2].markdown('<span class="badge-he">🔑 HE</span>', unsafe_allow_html=True)
    cols[3].markdown(
        '<span class="badge-warn">⚠ ATK SIM</span>', unsafe_allow_html=True
    )


def s_kpis(df: pd.DataFrame) -> None:
    st.markdown(
        '<p class="sg-title">📊 Grid Monitoring — KPIs</p>', unsafe_allow_html=True
    )
    total_e = df["energy_usage"].sum()
    active_m = df["meter_id"].nunique()
    avg_v = df["voltage"].mean()
    avg_i = df["current"].mean()
    enc_pct = df["encrypted"].mean() * 100

    cut = df["timestamp"].quantile(0.90)
    r = df[df["timestamp"] >= cut]
    p = df[df["timestamp"] < cut]
    d_e = round(r["energy_usage"].mean() - p["energy_usage"].mean(), 4) if len(p) else 0
    d_v = round(r["voltage"].mean() - p["voltage"].mean(), 2) if len(p) else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("⚡ Total Energy", f"{total_e:.1f} kW", delta=f"{d_e:+.3f}")
    c2.metric("🔌 Active Meters", f"{active_m}", delta=None)
    c3.metric("🔋 Avg Voltage", f"{avg_v:.1f} V", delta=f"{d_v:+.2f} V")
    c4.metric("⚙ Avg Current", f"{avg_i:.2f} A", delta=None)
    c5.metric("🔒 Encrypted", f"{enc_pct:.0f}%", delta=None)


def s_timeseries(df: pd.DataFrame, sel_meters: List[str]) -> None:
    st.markdown(
        '<p class="sg-title">📈 Real-Time Energy Consumption</p>',
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        chart_timeseries(df, sel_meters),
        use_container_width=True,
        config={"displayModeBar": False},
    )


def s_regional(df: pd.DataFrame) -> None:
    st.markdown('<p class="sg-title">🗺 Regional Grid Load</p>', unsafe_allow_html=True)
    ca, cb = st.columns([2, 1])
    with ca:
        st.plotly_chart(
            chart_regional_bar(df),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with cb:
        st.plotly_chart(
            chart_region_donut(df),
            use_container_width=True,
            config={"displayModeBar": False},
        )


def s_dp_privacy(df: pd.DataFrame, sel_meters: List[str]) -> None:
    st.markdown(
        '<p class="sg-title">🔐 Differential Privacy Protection</p>',
        unsafe_allow_html=True,
    )
    dp_meter = st.selectbox(
        "Select meter for DP comparison", sel_meters, key="dp_meter"
    )
    ca, cb = st.columns([3, 1])
    with ca:
        st.plotly_chart(
            chart_noisy_vs_true(df, dp_meter),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with cb:
        st.plotly_chart(
            chart_noise_histogram(df, dp_meter),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # Compute privacy stats
    mdf = df[df["meter_id"] == dp_meter]
    true_v = mdf["energy_usage"].tolist()
    noisy_v = mdf["noisy_energy_usage"].tolist()
    mae = round(float(np.mean(np.abs(np.array(true_v) - np.array(noisy_v)))), 5)
    rmse = round(
        float(np.sqrt(np.mean((np.array(true_v) - np.array(noisy_v)) ** 2))), 5
    )

    with st.expander("ℹ️  Differential Privacy — Technical Details"):
        st.markdown(
            f"""
            **Laplace Mechanism** — `published = true + Laplace(0, Δf/ε)`

            | Metric | Value |
            |--------|-------|
            | MAE (mean abs error) | `{mae}` kW |
            | RMSE | `{rmse}` kW |
            | Noise distribution | Laplace(0, σ) |

            A smaller `ε` provides stronger privacy guarantees at the cost of higher
            distortion.  The histogram shows the empirical noise distribution over this session.
            Individual meter readings are **never revealed** to the aggregation server —
            only the final decrypted aggregate is disclosed to the authorised analyst.
            """,
        )


def s_he_panel(df: pd.DataFrame) -> None:
    st.markdown(
        '<p class="sg-title">🔑 Homomorphic Encryption Aggregation</p>',
        unsafe_allow_html=True,
    )

    # Mock HE aggregate
    values = df["noisy_energy_usage"].tolist()
    total = sum(values)
    n = len(values)
    agg_token = hashlib.sha256(f"{total:.6f}|agg_key".encode()).hexdigest()[:32].upper()

    c1, c2, c3 = st.columns(3)
    c1.metric("🔒 Encrypted Records", f"{n:,}")
    c2.metric("∑ HE Aggregate", f"{total:.3f} kW")
    c3.metric("🔓 Decrypted Total", f"{total:.3f} kW")

    st.markdown(
        f"""
        <div class="he-card" style="margin-top:.8rem;">
          <h4>🔑  CKKS Ciphertext Aggregate (simulated)</h4>
          <p class="mono">CT_AGG: {agg_token}</p>
          <p style="font-family:Outfit,sans-serif;font-size:.8rem;
                    color:{C["text2"]};margin-top:.6rem;line-height:1.6;">
            The aggregation server receives only ciphertexts.
            It computes the sum using CKKS homomorphic addition —
            <em>no individual reading is ever decrypted</em>.
            Only the final aggregate is revealed to the authorised analyst.
            <br><br>
            <strong style="color:{C["teal2"]};">
              Enc(a) ⊕ Enc(b) = Enc(a + b)
            </strong>
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("ℹ️  CKKS Homomorphic Encryption — Technical Details"):
        st.markdown(
            f"""
            **CKKS Scheme** (Cheon-Kim-Kim-Song) supports approximate arithmetic over
            real-valued ciphertexts.

            ```
            # Encrypt
            ct_i = CKKS.encrypt(dp_noised_energy_i)

            # Aggregate (server — no decryption)
            CT_sum = ct_0 ⊕ ct_1 ⊕ … ⊕ ct_n

            # Decrypt (authorised analyst only)
            total = CKKS.decrypt(CT_sum)
            ```

            | Parameter | Value |
            |-----------|-------|
            | Scheme | CKKS (approximate HE) |
            | Poly modulus degree n | 8192 |
            | Scale bits | 40 |
            | Backend | Pyfhel / TenSEAL / MockHE |

            Install real HE backend:  `pip install Pyfhel` or `pip install tenseal`
            """,
        )


def s_meter_table(df: pd.DataFrame) -> None:
    st.markdown(
        '<p class="sg-title">📋 Smart Meter Activity Log</p>', unsafe_allow_html=True
    )
    latest = (
        df.sort_values("timestamp", ascending=False)
        .head(150)[
            [
                "meter_id",
                "timestamp",
                "energy_usage",
                "voltage",
                "current",
                "region",
                "encrypted",
            ]
        ]
        .copy()
    )
    latest["timestamp"] = latest["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
    latest["encrypted"] = latest["encrypted"].map({True: "🔒 Yes", False: "🔓 No"})
    st.dataframe(
        latest,
        use_container_width=True,
        height=270,
        hide_index=True,
        column_config={
            "meter_id": st.column_config.TextColumn("Meter ID"),
            "timestamp": st.column_config.TextColumn("Timestamp"),
            "energy_usage": st.column_config.NumberColumn("Energy (kW)", format="%.4f"),
            "voltage": st.column_config.NumberColumn("Voltage (V)", format="%.2f"),
            "current": st.column_config.NumberColumn("Current (A)", format="%.3f"),
            "region": st.column_config.TextColumn("Region"),
            "encrypted": st.column_config.TextColumn("Encrypted"),
        },
    )


def s_attack_panel(df: pd.DataFrame, sel_meters: List[str]) -> None:
    st.markdown(
        '<p class="sg-title">⚠️  Attack Simulation Panel</p>', unsafe_allow_html=True
    )
    atk_meter = st.selectbox("Target meter", sel_meters, key="atk_meter")
    mdf = df[df["meter_id"] == atk_meter].sort_values("timestamp").tail(40)
    true_m = mdf["energy_usage"].mean()
    noisy_m = mdf["noisy_energy_usage"].mean()
    atk_err = abs(true_m - noisy_m) / max(true_m, 1e-6) * 100

    ca, cb = st.columns([2, 1])
    with ca:
        st.plotly_chart(
            chart_attack_simulation(df, atk_meter),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with cb:
        st.markdown(
            f"""
            <div class="atk-card" style="min-height:260px;">
              <h4>🎯 Reconstruction Analysis</h4>
              <table style="font-family:IBM Plex Mono,monospace;font-size:.73rem;
                            color:{C["text2"]};border-collapse:collapse;width:100%;">
                <tr><td style="padding:4px 0;color:{C["muted"]};">Target</td>
                    <td style="color:{C["text"]};">{atk_meter}</td></tr>
                <tr><td style="padding:4px 0;color:{C["muted"]};">Method</td>
                    <td style="color:{C["text"]};">Rolling-mean (k=6)</td></tr>
                <tr><td style="padding:4px 0;color:{C["muted"]};">True mean</td>
                    <td style="color:{C["saffron2"]};">{true_m:.4f} kW</td></tr>
                <tr><td style="padding:4px 0;color:{C["muted"]};">Att. estimate</td>
                    <td style="color:{C["crimson"]};">{noisy_m:.4f} kW</td></tr>
                <tr><td style="padding:4px 0;color:{C["muted"]};">Estimation err.</td>
                    <td style="color:{C["green"]};">{atk_err:.1f}%</td></tr>
              </table>
              <p style="margin-top:.8rem;font-family:Outfit,sans-serif;
                        font-size:.76rem;color:{C["muted"]};line-height:1.55;">
                Higher σ → larger estimation error → stronger DP protection.
                The attacker cannot distinguish whether any individual meter's
                reading contributed to the published noisy values.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def s_advanced_analytics(df: pd.DataFrame, sel_meters: List[str]) -> None:
    """Hourly profile, sub-metering, and voltage heatmap in expanders."""
    with st.expander("📊  Hourly Load Profile", expanded=False):
        st.plotly_chart(
            chart_hourly_profile(df),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    with st.expander("🔌  Sub-Metering Breakdown", expanded=False):
        sub_m = st.selectbox("Select meter", sel_meters, key="sub_meter")
        st.plotly_chart(
            chart_sub_metering(df, sub_m),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    with st.expander("🌡  Voltage Heatmap", expanded=False):
        st.plotly_chart(
            chart_voltage_heatmap(df),
            use_container_width=True,
            config={"displayModeBar": False},
        )


# ============================================================
# MAIN APPLICATION
# ============================================================


def main() -> None:
    st.markdown(STYLE, unsafe_allow_html=True)
    init_state()

    # ── Sidebar ──────────────────────────────────────────────
    sel_meters, sel_regions, time_range, noise_level = render_sidebar(
        st.session_state.df
    )

    # ── Live tick ────────────────────────────────────────────
    if st.session_state.simulating:
        st.session_state.df = append_live_tick(
            st.session_state.df, noise_level=noise_level
        )
        st.session_state.tick += 1
        # Cap DataFrame size for memory safety
        max_rows = 20 * 400  # 20 meters × 400 minutes
        if len(st.session_state.df) > max_rows:
            st.session_state.df = st.session_state.df.iloc[-max_rows:]

    # ── Filter ───────────────────────────────────────────────
    df_all = st.session_state.df
    mask = (
        df_all["meter_id"].isin(sel_meters)
        & df_all["region"].isin(sel_regions)
        & (df_all["timestamp"] >= pd.Timestamp(time_range[0]))
        & (df_all["timestamp"] <= pd.Timestamp(time_range[1]))
    )
    df = df_all[mask].copy()

    if df.empty:
        st.warning("⚠ No data matches current filters. Adjust sidebar settings.")
        return

    # ════════════════════════════════════════════════════════
    # SECTIONS
    # ════════════════════════════════════════════════════════
    s_hero()
    s_badges(st.session_state.simulating)
    st.markdown("---")
    s_kpis(df)
    st.markdown("---")
    s_timeseries(df, sel_meters)
    st.markdown("---")
    s_regional(df)
    st.markdown("---")
    s_dp_privacy(df, sel_meters)
    st.markdown("---")
    s_he_panel(df)
    st.markdown("---")
    s_meter_table(df)
    st.markdown("---")
    s_attack_panel(df, sel_meters)
    st.markdown("---")
    s_advanced_analytics(df, sel_meters)

    # Footer
    st.markdown(
        f"""
        <hr>
        <div style="text-align:center;font-family:IBM Plex Mono,monospace;
                    font-size:.65rem;color:{C["muted"]};padding:.4rem 0 1.5rem;">
            SecureGrid Research Platform · Hybrid HE + DP Smart Grid Aggregation ·
            Built with Streamlit + Plotly
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Auto-rerun for live simulation ───────────────────────
    if st.session_state.simulating:
        time.sleep(1.5)
        st.rerun()


if __name__ == "__main__":
    main()
