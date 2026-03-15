"""
dashboard.py
============
Hybrid Secure Data Aggregation for Smart Grids
Research Dashboard  ·  Streamlit + Plotly
-----------------------------------------
Visualises smart-meter readings flowing through a
Differential Privacy + Homomorphic Encryption pipeline.

Run:
    pip install streamlit pandas plotly numpy
    streamlit run dashboard.py
"""

from __future__ import annotations

import hashlib
import math
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ============================================================
# PAGE CONFIG  (must be the very first Streamlit call)
# ============================================================
st.set_page_config(
    page_title="SecureGrid · Research Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# GLOBAL STYLE  — industrial dark-tech palette
# ============================================================
STYLE = """
<style>
/* ── Google Font ── */
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=DM+Sans:wght@300;400;500;600&display=swap');

/* ── Root palette ── */
:root {
    --bg0:      #0a0e1a;
    --bg1:      #0f1422;
    --bg2:      #151c30;
    --bg3:      #1c2540;
    --border:   #263050;
    --amber:    #f5a623;
    --amber2:   #ffc857;
    --cyan:     #00d4e8;
    --cyan2:    #7af4ff;
    --green:    #4ade80;
    --red:      #f87171;
    --muted:    #64748b;
    --text:     #e2e8f0;
    --text2:    #94a3b8;
}

/* ── App shell ── */
.stApp { background: var(--bg0); color: var(--text); }
.main .block-container { padding: 1.5rem 2rem 3rem; max-width: 1400px; }

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: var(--bg1);
    border-right: 1px solid var(--border);
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stMultiSelect label,
section[data-testid="stSidebar"] .stSlider label {
    color: var(--text2) !important;
    font-family: 'DM Sans', sans-serif;
    font-size: 0.78rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

/* ── Metric cards ── */
div[data-testid="stMetric"] {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1rem 1.25rem;
    position: relative;
    overflow: hidden;
}
div[data-testid="stMetric"]::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--amber), var(--cyan));
}
div[data-testid="stMetric"] label {
    color: var(--text2) !important;
    font-size: 0.72rem !important;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-family: 'Share Tech Mono', monospace !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: var(--amber2) !important;
    font-family: 'Share Tech Mono', monospace !important;
    font-size: 1.7rem !important;
}
div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
    font-size: 0.75rem !important;
}

/* ── Section headers ── */
.sg-section-title {
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.72rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--amber);
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem;
    margin-bottom: 1rem;
}

/* ── Hero header ── */
.sg-hero {
    background: linear-gradient(135deg, var(--bg2) 0%, var(--bg3) 100%);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 2rem 2.5rem;
    margin-bottom: 1.75rem;
    position: relative;
    overflow: hidden;
}
.sg-hero::after {
    content: '⚡';
    position: absolute;
    right: 2rem; top: 50%;
    transform: translateY(-50%);
    font-size: 6rem;
    opacity: 0.05;
}
.sg-hero h1 {
    font-family: 'Share Tech Mono', monospace;
    font-size: 1.6rem;
    color: var(--amber2);
    margin: 0 0 0.3rem;
    letter-spacing: 0.05em;
}
.sg-hero p { color: var(--text2); font-size: 0.9rem; margin: 0.2rem 0; }

/* ── Arch pills ── */
.arch-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-top: 1rem;
}
.arch-pill {
    background: var(--bg0);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 0.3rem 0.85rem;
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.72rem;
    color: var(--cyan);
    letter-spacing: 0.06em;
}
.arch-arrow {
    color: var(--muted);
    font-size: 0.85rem;
    display: flex;
    align-items: center;
}

/* ── Info/status badges ── */
.badge-enc  { background:#0e3a3f; color:var(--cyan2);  border:1px solid var(--cyan);  border-radius:4px; padding:2px 8px; font-family:'Share Tech Mono',monospace; font-size:0.7rem; }
.badge-dp   { background:#3a2e0e; color:var(--amber2); border:1px solid var(--amber); border-radius:4px; padding:2px 8px; font-family:'Share Tech Mono',monospace; font-size:0.7rem; }
.badge-live { background:#0e3a1a; color:var(--green);  border:1px solid var(--green); border-radius:4px; padding:2px 8px; font-family:'Share Tech Mono',monospace; font-size:0.7rem; }

/* ── Tables ── */
div[data-testid="stDataFrame"] table { font-size: 0.82rem; }
div[data-testid="stDataFrame"] thead tr th {
    background: var(--bg3) !important;
    color: var(--text2) !important;
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.68rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
}

/* ── Expander ── */
div[data-testid="stExpander"] {
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    background: var(--bg1) !important;
}
div[data-testid="stExpander"] summary {
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.8rem;
    letter-spacing: 0.06em;
    color: var(--text) !important;
}

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, #1a2f1a, #0e3a1a) !important;
    color: var(--green) !important;
    border: 1px solid var(--green) !important;
    border-radius: 7px !important;
    font-family: 'Share Tech Mono', monospace !important;
    font-size: 0.82rem !important;
    letter-spacing: 0.08em !important;
    padding: 0.5rem 1.5rem !important;
    transition: all 0.2s !important;
}
.stButton > button:hover {
    background: var(--green) !important;
    color: #0a0e1a !important;
    box-shadow: 0 0 18px rgba(74,222,128,0.35) !important;
}
.stop-btn > button {
    background: linear-gradient(135deg, #2f1a1a, #3a0e0e) !important;
    color: var(--red) !important;
    border: 1px solid var(--red) !important;
}
.stop-btn > button:hover {
    background: var(--red) !important;
    color: #0a0e1a !important;
    box-shadow: 0 0 18px rgba(248,113,113,0.35) !important;
}

/* ── Plotly chart containers ── */
.js-plotly-plot { border-radius: 10px; }

/* ── Attack panel ── */
.attack-box {
    background: linear-gradient(135deg,#1a0e0e,#2a1010);
    border: 1px solid #7f1d1d;
    border-radius: 10px;
    padding: 1.2rem 1.5rem;
}
.attack-box h4 {
    font-family:'Share Tech Mono',monospace;
    color:var(--red);
    font-size:0.85rem;
    margin:0 0 0.5rem;
    letter-spacing:0.08em;
}

/* ── HE panel ── */
.he-box {
    background: linear-gradient(135deg,#0a1a2a,#0e2a3a);
    border:1px solid var(--cyan);
    border-radius:10px;
    padding:1.2rem 1.5rem;
}
.he-box h4 {
    font-family:'Share Tech Mono',monospace;
    color:var(--cyan2);
    font-size:0.85rem;
    margin:0 0 0.5rem;
    letter-spacing:0.08em;
}
.mono { font-family:'Share Tech Mono',monospace; font-size:0.78rem; color:var(--cyan); }

/* ── Divider ── */
hr { border-color: var(--border) !important; margin: 1.75rem 0 !important; }
</style>
"""

# ============================================================
# PLOTLY CHART THEME  — shared across all charts
# ============================================================
CHART_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(15,20,34,0.6)",
    font=dict(family="DM Sans, sans-serif", color="#94a3b8", size=12),
    xaxis=dict(
        gridcolor="#1c2540",
        linecolor="#263050",
        tickfont=dict(size=10),
        title_font=dict(size=11),
    ),
    yaxis=dict(
        gridcolor="#1c2540",
        linecolor="#263050",
        tickfont=dict(size=10),
        title_font=dict(size=11),
    ),
    margin=dict(l=50, r=20, t=40, b=40),
    legend=dict(
        bgcolor="rgba(15,20,34,0.8)",
        bordercolor="#263050",
        borderwidth=1,
        font=dict(size=10),
    ),
)

# Colour palette for regions / meters
REGION_COLOURS = {
    "north": "#f5a623",
    "south": "#00d4e8",
    "east": "#4ade80",
    "west": "#a78bfa",
    "central": "#fb923c",
}
METER_PALETTE = px.colors.qualitative.Bold


# ============================================================
# DATA GENERATION
# ============================================================

REGIONS = ["north", "south", "east", "west", "central"]
NUM_METERS = 20


def _diurnal(hour: int) -> float:
    """Return a realistic consumption multiplier for the given hour."""
    if 0 <= hour < 6:
        return 0.35
    if 6 <= hour < 9:
        return 0.70 + 0.30 * (hour - 6) / 3
    if 9 <= hour < 17:
        return 0.65
    if 17 <= hour < 21:
        return 1.0
    return 0.55


def generate_sample_data(
    n_meters: int = NUM_METERS,
    n_minutes: int = 120,
    noise_level: float = 0.05,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic smart-meter readings that mimic real UCI data patterns.

    Parameters
    ----------
    n_meters  : Number of virtual meters.
    n_minutes : Number of 1-minute intervals to simulate.
    noise_level : Differential-privacy noise σ (fraction of reading value).
    seed      : Random seed for reproducibility.

    Returns
    -------
    DataFrame with columns:
        meter_id, timestamp, energy_usage, voltage, current,
        region, privacy_noise, encrypted, noisy_energy_usage
    """
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    # Assign each meter a fixed region + scaling factor
    meter_meta: Dict[str, Dict] = {}
    for i in range(n_meters):
        meter_id = f"meter_{i:02d}"
        meter_meta[meter_id] = {
            "region": REGIONS[i % len(REGIONS)],
            "scale": rng.uniform(0.6, 1.5),
            "v_off": rng.uniform(-5.0, 5.0),
            "i_off": rng.uniform(-0.3, 0.3),
        }

    base_ts = datetime(2026, 3, 15, 8, 0, 0)
    rows: List[Dict] = []

    for minute in range(n_minutes):
        ts = base_ts + timedelta(minutes=minute)
        hour = ts.hour
        mult = _diurnal(hour)

        for meter_id, meta in meter_meta.items():
            base_e = max(0.1, (mult * 1.8 + np_rng.normal(0, 0.15)) * meta["scale"])
            # Add occasional appliance spike
            if rng.random() < 0.03:
                base_e += rng.uniform(0.5, 2.0)

            voltage = round(230.0 + meta["v_off"] + np_rng.normal(0, 0.8), 2)
            current = round(max(0.1, base_e * 1000 / voltage + meta["i_off"]), 3)

            # Differential privacy: Laplace noise
            dp_noise = np_rng.laplace(0, noise_level * max(base_e, 0.01))
            noisy_e = max(0.0, base_e + dp_noise)

            rows.append(
                {
                    "meter_id": meter_id,
                    "timestamp": ts,
                    "energy_usage": round(base_e, 4),
                    "noisy_energy_usage": round(noisy_e, 4),
                    "voltage": voltage,
                    "current": current,
                    "region": meta["region"],
                    "privacy_noise": round(abs(dp_noise), 5),
                    "encrypted": True,
                }
            )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values(["timestamp", "meter_id"]).reset_index(drop=True)


def append_live_row(df: pd.DataFrame, noise_level: float = 0.05) -> pd.DataFrame:
    """Append one new round of readings (one per meter) at 'now'."""
    rng = np.random.default_rng()
    ts = df["timestamp"].max() + timedelta(minutes=1)
    hour = ts.hour
    mult = _diurnal(hour)
    new_rows = []
    for meter_id in df["meter_id"].unique():
        meta_region = df[df["meter_id"] == meter_id]["region"].iloc[0]
        scale = df[df["meter_id"] == meter_id]["energy_usage"].mean() / (
            mult * 1.8 + 1e-9
        )
        scale = max(0.5, min(scale, 2.0))
        base_e = max(0.1, (mult * 1.8 + rng.normal(0, 0.15)) * scale)
        voltage = round(230.0 + rng.normal(0, 0.8), 2)
        current = round(max(0.1, base_e * 1000 / voltage), 3)
        dp_noise = rng.laplace(0, noise_level * max(base_e, 0.01))
        noisy_e = max(0.0, base_e + dp_noise)
        new_rows.append(
            {
                "meter_id": meter_id,
                "timestamp": ts,
                "energy_usage": round(base_e, 4),
                "noisy_energy_usage": round(noisy_e, 4),
                "voltage": voltage,
                "current": current,
                "region": meta_region,
                "privacy_noise": round(abs(dp_noise), 5),
                "encrypted": True,
            }
        )
    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)


# ============================================================
# HOMOMORPHIC ENCRYPTION SIMULATION HELPERS
# ============================================================


def mock_he_encrypt(value: float, key: int = 12345) -> str:
    """
    Simulate CKKS encryption: return a deterministic hex token.
    In production replace with TenSEAL ts.ckks_vector().
    """
    raw = f"{value:.6f}|{key}".encode()
    return hashlib.sha256(raw).hexdigest()[:24].upper()


def mock_he_aggregate(values: List[float]) -> Tuple[str, float]:
    """
    Simulate homomorphic summation.
    Returns (aggregate_ciphertext_token, plaintext_sum).
    """
    total = sum(values)
    token = mock_he_encrypt(total, key=99999)
    return token, round(total, 4)


# ============================================================
# FILTERED DATA HELPER
# ============================================================


def apply_filters(
    df: pd.DataFrame,
    selected_meters: List[str],
    selected_regions: List[str],
    time_range: Tuple[datetime, datetime],
) -> pd.DataFrame:
    """Return a filtered slice of the main DataFrame."""
    mask = (
        (df["meter_id"].isin(selected_meters))
        & (df["region"].isin(selected_regions))
        & (df["timestamp"] >= pd.Timestamp(time_range[0]))
        & (df["timestamp"] <= pd.Timestamp(time_range[1]))
    )
    return df[mask].copy()


# ============================================================
# CHART BUILDERS
# ============================================================


def make_timeseries_chart(df: pd.DataFrame, selected_meters: List[str]) -> go.Figure:
    """Interactive multi-meter energy time-series."""
    fig = go.Figure()
    palette = METER_PALETTE
    subset = df[df["meter_id"].isin(selected_meters)]

    for idx, mid in enumerate(selected_meters):
        mdf = subset[subset["meter_id"] == mid].sort_values("timestamp")
        colour = palette[idx % len(palette)]
        fig.add_trace(
            go.Scatter(
                x=mdf["timestamp"],
                y=mdf["energy_usage"],
                name=mid,
                mode="lines",
                line=dict(width=1.8, color=colour),
                hovertemplate=(
                    f"<b>{mid}</b><br>"
                    "Time: %{x|%H:%M}<br>"
                    "Energy: %{y:.3f} kW<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        **CHART_THEME,
        height=340,
        title=dict(
            text="Energy Consumption — Multi-Meter",
            font=dict(size=13, color="#e2e8f0"),
            x=0,
        ),
        xaxis_title="Time",
        yaxis_title="Energy (kW)",
        hovermode="x unified",
    )
    return fig


def make_regional_bar(df: pd.DataFrame) -> go.Figure:
    """Regional energy consumption bar chart."""
    region_agg = (
        df.groupby("region")["energy_usage"]
        .sum()
        .reset_index()
        .sort_values("energy_usage", ascending=False)
    )
    colours = [REGION_COLOURS.get(r, "#f5a623") for r in region_agg["region"]]

    fig = go.Figure(
        go.Bar(
            x=region_agg["region"],
            y=region_agg["energy_usage"],
            marker=dict(
                color=colours,
                line=dict(color="rgba(0,0,0,0)", width=0),
                opacity=0.85,
            ),
            text=region_agg["energy_usage"].round(1).astype(str) + " kW",
            textposition="outside",
            textfont=dict(size=10, color="#e2e8f0"),
            hovertemplate="<b>%{x}</b><br>Total: %{y:.2f} kW<extra></extra>",
        )
    )
    fig.update_layout(
        **CHART_THEME,
        height=300,
        title=dict(
            text="Grid Load by Region", font=dict(size=13, color="#e2e8f0"), x=0
        ),
        xaxis_title="Region",
        yaxis_title="Total Energy (kW)",
        showlegend=False,
    )
    return fig


def make_dp_comparison_chart(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """Side-by-side real vs DP-noised energy for a single meter."""
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(60)

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Real Energy Usage", "DP-Noised Energy (Published)"),
        horizontal_spacing=0.08,
    )

    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["energy_usage"],
            fill="tozeroy",
            fillcolor="rgba(245,166,35,0.15)",
            line=dict(color="#f5a623", width=1.8),
            name="Real",
            hovertemplate="Real: %{y:.4f} kW<extra></extra>",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["noisy_energy_usage"],
            fill="tozeroy",
            fillcolor="rgba(0,212,232,0.12)",
            line=dict(color="#00d4e8", width=1.8),
            name="DP-Noised",
            hovertemplate="Noised: %{y:.4f} kW<extra></extra>",
        ),
        row=1,
        col=2,
    )

    fig.update_layout(
        **CHART_THEME,
        height=320,
        showlegend=False,
        title=dict(
            text=f"Differential Privacy  ·  {meter_id}",
            font=dict(size=13, color="#e2e8f0"),
            x=0,
        ),
    )
    for ax in ["xaxis", "xaxis2", "yaxis", "yaxis2"]:
        fig.layout[ax].update(
            gridcolor="#1c2540",
            linecolor="#263050",
            tickfont=dict(size=9, color="#64748b"),
        )
    # Annotation titles
    for ann in fig.layout.annotations:
        ann.font.color = "#94a3b8"
        ann.font.size = 11

    return fig


def make_noise_distribution(df: pd.DataFrame) -> go.Figure:
    """Histogram of privacy noise magnitudes."""
    fig = go.Figure(
        go.Histogram(
            x=df["privacy_noise"],
            nbinsx=40,
            marker_color="#f5a623",
            marker_line=dict(color="#0a0e1a", width=0.5),
            opacity=0.8,
            hovertemplate="Noise bin: %{x:.5f}<br>Count: %{y}<extra></extra>",
        )
    )
    fig.update_layout(
        **CHART_THEME,
        height=260,
        title=dict(
            text="Privacy Noise Distribution (Laplace)",
            font=dict(size=13, color="#e2e8f0"),
            x=0,
        ),
        xaxis_title="Noise Magnitude (kW)",
        yaxis_title="Count",
        showlegend=False,
    )
    return fig


def make_attack_chart(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """
    Simulates an adversary trying to reconstruct individual consumption
    from noisy published values over time.
    """
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(40)
    # Attacker's estimate: running mean of noisy values
    attacker_estimate = mdf["noisy_energy_usage"].rolling(5, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["energy_usage"],
            name="True (hidden)",
            mode="lines",
            line=dict(color="#f5a623", width=2, dash="dot"),
            hovertemplate="True: %{y:.4f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["noisy_energy_usage"],
            name="Published (noisy)",
            mode="lines",
            line=dict(color="#00d4e8", width=1.5),
            hovertemplate="Published: %{y:.4f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=attacker_estimate,
            name="Attacker estimate",
            mode="lines",
            line=dict(color="#f87171", width=1.8, dash="dash"),
            hovertemplate="Attacker: %{y:.4f}<extra></extra>",
        )
    )

    fig.update_layout(
        **CHART_THEME,
        height=300,
        title=dict(
            text="Attack Simulation — Reconstruction Attempt",
            font=dict(size=13, color="#e2e8f0"),
            x=0,
        ),
        xaxis_title="Time",
        yaxis_title="Energy (kW)",
        hovermode="x unified",
    )
    return fig


def make_voltage_heatmap(df: pd.DataFrame) -> go.Figure:
    """Voltage levels across meters over time (last 30 rows per meter)."""
    pivot_df = (
        df.sort_values("timestamp")
        .groupby("meter_id")
        .tail(30)
        .pivot_table(
            index="meter_id", columns="timestamp", values="voltage", aggfunc="mean"
        )
    )
    # Trim columns for readability
    cols = pivot_df.columns[-20:] if len(pivot_df.columns) > 20 else pivot_df.columns
    pivot_df = pivot_df[cols]

    fig = go.Figure(
        go.Heatmap(
            z=pivot_df.values,
            x=[str(c.strftime("%H:%M")) for c in pivot_df.columns],
            y=pivot_df.index.tolist(),
            colorscale=[
                [0.0, "#0e3a1a"],
                [0.35, "#f5a623"],
                [0.65, "#ffc857"],
                [1.0, "#f87171"],
            ],
            colorbar=dict(
                title="Voltage (V)",
                titlefont=dict(size=10, color="#94a3b8"),
                tickfont=dict(size=9, color="#94a3b8"),
                thickness=12,
            ),
            hovertemplate="Meter: %{y}<br>Time: %{x}<br>Voltage: %{z:.1f} V<extra></extra>",
        )
    )
    fig.update_layout(
        **CHART_THEME,
        height=320,
        title=dict(
            text="Voltage Heatmap — All Meters",
            font=dict(size=13, color="#e2e8f0"),
            x=0,
        ),
        xaxis=dict(tickangle=-45, tickfont=dict(size=8), gridcolor="rgba(0,0,0,0)"),
        yaxis=dict(tickfont=dict(size=9), gridcolor="rgba(0,0,0,0)"),
    )
    return fig


# ============================================================
# SESSION STATE INITIALISATION
# ============================================================


def init_session_state() -> None:
    """Ensure all session-state keys exist before any widget renders."""
    if "df" not in st.session_state:
        st.session_state.df = generate_sample_data(n_meters=NUM_METERS, n_minutes=120)
    if "simulating" not in st.session_state:
        st.session_state.simulating = False
    if "sim_tick" not in st.session_state:
        st.session_state.sim_tick = 0
    if "noise_level" not in st.session_state:
        st.session_state.noise_level = 0.05


# ============================================================
# SIDEBAR
# ============================================================


def render_sidebar(df: pd.DataFrame):
    """Render all sidebar controls; return active filter values."""
    st.sidebar.markdown(
        """
        <div style='padding:1rem 0 0.5rem;'>
            <p style='font-family:Share Tech Mono,monospace;font-size:1rem;
                      color:#f5a623;margin:0;letter-spacing:0.08em;'>⚡ SECUREGRID</p>
            <p style='font-family:DM Sans,sans-serif;font-size:0.72rem;
                      color:#64748b;margin:0.25rem 0 0;'>Research Dashboard v2.0</p>
        </div>
        <hr style='border-color:#263050;margin:0.75rem 0;'>
        """,
        unsafe_allow_html=True,
    )

    # ── Simulation controls ──────────────────────────────────
    st.sidebar.markdown("##### 🔴  Live Simulation")
    col_a, col_b = st.sidebar.columns(2)
    with col_a:
        start_clicked = st.button("▶ Start", key="btn_start", use_container_width=True)
    with col_b:
        stop_clicked = st.button("■ Stop", key="btn_stop", use_container_width=True)

    if start_clicked:
        st.session_state.simulating = True
    if stop_clicked:
        st.session_state.simulating = False

    status_label = (
        '<span class="badge-live">● LIVE</span>'
        if st.session_state.simulating
        else '<span style="font-family:Share Tech Mono,monospace;font-size:0.72rem;color:#64748b;">● STATIC</span>'
    )
    st.sidebar.markdown(f"Status: {status_label}", unsafe_allow_html=True)
    st.sidebar.markdown("---")

    # ── Noise / privacy budget ───────────────────────────────
    st.sidebar.markdown("##### 🔒  Privacy Budget")
    noise_level = st.sidebar.slider(
        "DP Noise Level (σ)",
        min_value=0.001,
        max_value=0.30,
        value=st.session_state.noise_level,
        step=0.001,
        format="%.3f",
        help="Laplace noise scale as a fraction of the reading. Lower ε → higher noise.",
    )
    st.session_state.noise_level = noise_level
    epsilon_approx = round(1.0 / max(noise_level, 0.001), 1)
    st.sidebar.caption(f"≈ ε = {epsilon_approx}  (1/σ proxy)")
    st.sidebar.markdown("---")

    # ── Meter filter ─────────────────────────────────────────
    st.sidebar.markdown("##### 🔌  Meter Selection")
    all_meters = sorted(df["meter_id"].unique().tolist())
    default_meters = all_meters[:6]
    selected_meters = st.sidebar.multiselect(
        "Meters",
        all_meters,
        default=default_meters,
        placeholder="Choose meters…",
    )
    if not selected_meters:
        selected_meters = all_meters

    # ── Region filter ────────────────────────────────────────
    st.sidebar.markdown("##### 🗺  Region Filter")
    all_regions = sorted(df["region"].unique().tolist())
    selected_regions = st.sidebar.multiselect(
        "Regions",
        all_regions,
        default=all_regions,
        placeholder="Choose regions…",
    )
    if not selected_regions:
        selected_regions = all_regions

    # ── Time range ───────────────────────────────────────────
    st.sidebar.markdown("##### 🕒  Time Range")
    ts_min = df["timestamp"].min().to_pydatetime()
    ts_max = df["timestamp"].max().to_pydatetime()
    time_start = st.sidebar.slider(
        "From",
        min_value=ts_min,
        max_value=ts_max,
        value=ts_min,
        format="HH:mm",
    )
    time_end = st.sidebar.slider(
        "To",
        min_value=ts_min,
        max_value=ts_max,
        value=ts_max,
        format="HH:mm",
    )
    st.sidebar.markdown("---")

    # ── Data reset ───────────────────────────────────────────
    if st.sidebar.button("↺ Reset Data", use_container_width=True):
        st.session_state.df = generate_sample_data(
            n_meters=NUM_METERS, n_minutes=120, noise_level=noise_level
        )
        st.session_state.sim_tick = 0
        st.session_state.simulating = False
        st.rerun()

    st.sidebar.markdown(
        """
        <div style='padding:1rem 0 0;font-family:DM Sans,sans-serif;
                    font-size:0.7rem;color:#475569;line-height:1.6;'>
            Research project:<br>
            <em>Hybrid Secure Data Aggregation<br>
            for Smart Grids using HE + DP</em>
        </div>
        """,
        unsafe_allow_html=True,
    )

    return selected_meters, selected_regions, (time_start, time_end), noise_level


# ============================================================
# SECTION RENDERERS
# ============================================================


def render_hero() -> None:
    """Section 1 — Project header / architecture summary."""
    st.markdown(
        """
        <div class="sg-hero">
            <h1>⚡ SecureGrid Research Dashboard</h1>
            <p>Hybrid Secure Data Aggregation for Smart Grids<br>
               using <strong style="color:#ffc857;">Homomorphic Encryption</strong> &amp;
               <strong style="color:#00d4e8;">Differential Privacy</strong></p>
            <div class="arch-row">
                <span class="arch-pill">Smart Meter</span>
                <span class="arch-arrow">→</span>
                <span class="arch-pill">DP Module</span>
                <span class="arch-arrow">→</span>
                <span class="arch-pill">HE Encrypt</span>
                <span class="arch-arrow">→</span>
                <span class="arch-pill">Kafka Stream</span>
                <span class="arch-arrow">→</span>
                <span class="arch-pill">HE Aggregate</span>
                <span class="arch-arrow">→</span>
                <span class="arch-pill">Decrypt Sum</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_cards(df: pd.DataFrame) -> None:
    """Section 2 — KPI metric cards."""
    st.markdown(
        '<p class="sg-section-title">📊 Real-Time Grid Monitoring</p>',
        unsafe_allow_html=True,
    )

    total_energy = df["energy_usage"].sum()
    active_meters = df["meter_id"].nunique()
    avg_voltage = df["voltage"].mean()
    avg_current = df["current"].mean()

    # Compute deltas from previous 10% of time window
    cutoff = df["timestamp"].quantile(0.90)
    df_recent = df[df["timestamp"] >= cutoff]
    df_prev = df[df["timestamp"] < cutoff]

    delta_e = (
        round(df_recent["energy_usage"].mean() - df_prev["energy_usage"].mean(), 3)
        if len(df_prev) > 0
        else 0
    )
    delta_v = (
        round(df_recent["voltage"].mean() - df_prev["voltage"].mean(), 2)
        if len(df_prev) > 0
        else 0
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("⚡ Total Energy", f"{total_energy:.1f} kW", delta=f"{delta_e:+.3f} kW")
    c2.metric("🔌 Active Meters", f"{active_meters}", delta=None)
    c3.metric("🔋 Avg Voltage", f"{avg_voltage:.1f} V", delta=f"{delta_v:+.2f} V")
    c4.metric("⚙ Avg Current", f"{avg_current:.2f} A", delta=None)
    enc_pct = df["encrypted"].mean() * 100
    c5.metric("🔒 Encrypted", f"{enc_pct:.0f}%", delta=None)


def render_timeseries(df: pd.DataFrame, selected_meters: List[str]) -> None:
    """Section 3 — Live energy consumption line chart."""
    st.markdown(
        '<p class="sg-section-title">📈 Energy Consumption — Time Series</p>',
        unsafe_allow_html=True,
    )
    fig = make_timeseries_chart(df, selected_meters[:10])  # cap for readability
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_regional_load(df: pd.DataFrame) -> None:
    """Section 4 — Regional bar chart."""
    st.markdown(
        '<p class="sg-section-title">🗺 Regional Grid Load</p>', unsafe_allow_html=True
    )

    col_chart, col_donut = st.columns([2, 1])

    with col_chart:
        fig_bar = make_regional_bar(df)
        st.plotly_chart(
            fig_bar, use_container_width=True, config={"displayModeBar": False}
        )

    with col_donut:
        # Donut showing share of each region
        region_agg = df.groupby("region")["energy_usage"].sum().reset_index()
        colours = [REGION_COLOURS.get(r, "#f5a623") for r in region_agg["region"]]
        fig_pie = go.Figure(
            go.Pie(
                labels=region_agg["region"],
                values=region_agg["energy_usage"].round(2),
                hole=0.55,
                marker=dict(colors=colours, line=dict(color="#0a0e1a", width=2)),
                textfont=dict(size=10, color="#e2e8f0"),
                hovertemplate="<b>%{label}</b><br>%{value:.2f} kW  (%{percent})<extra></extra>",
            )
        )
        fig_pie.update_layout(
            **CHART_THEME,
            height=300,
            showlegend=True,
            title=dict(text="Region Share", font=dict(size=13, color="#e2e8f0"), x=0),
            legend=dict(orientation="v", x=1.0, y=0.5, font=dict(size=9)),
            annotations=[
                dict(
                    text="Load<br>Share",
                    x=0.5,
                    y=0.5,
                    font=dict(size=11, color="#94a3b8", family="Share Tech Mono"),
                    showarrow=False,
                )
            ],
        )
        st.plotly_chart(
            fig_pie, use_container_width=True, config={"displayModeBar": False}
        )


def render_dp_panel(df: pd.DataFrame, selected_meters: List[str]) -> None:
    """Section 5 — Differential privacy visualisation."""
    st.markdown(
        '<p class="sg-section-title">🔐 Differential Privacy Protection</p>',
        unsafe_allow_html=True,
    )

    dp_meter = st.selectbox(
        "Select meter for DP comparison",
        options=selected_meters,
        key="dp_meter_select",
    )

    col_left, col_right = st.columns([3, 1])
    with col_left:
        fig_dp = make_dp_comparison_chart(df, dp_meter)
        st.plotly_chart(
            fig_dp, use_container_width=True, config={"displayModeBar": False}
        )

    with col_right:
        fig_hist = make_noise_distribution(df[df["meter_id"] == dp_meter])
        st.plotly_chart(
            fig_hist, use_container_width=True, config={"displayModeBar": False}
        )

    with st.expander("ℹ️  How Differential Privacy works here"):
        st.markdown(
            """
            **Laplace Mechanism**

            Before any reading leaves the smart meter, calibrated Laplace noise is added:

            ```
            published_value = true_value + Laplace(0, sensitivity/ε)
            ```

            - **ε (epsilon)** — the privacy budget. Smaller ε → stronger privacy, more distortion.
            - **Sensitivity** — the maximum change one individual meter can cause to the query result.
            - The noise distribution shown above is the empirical Laplace distribution over this session.

            An attacker who sees only the published values cannot reliably reconstruct the
            true reading — see the *Attack Simulation* panel below.
            """,
        )


def render_he_panel(df: pd.DataFrame, selected_regions: List[str]) -> None:
    """Section 6 — Homomorphic encryption aggregation panel."""
    st.markdown(
        '<p class="sg-section-title">🔑 Homomorphic Encryption Aggregation</p>',
        unsafe_allow_html=True,
    )

    he_df = df[df["region"].isin(selected_regions)]
    values = he_df["noisy_energy_usage"].tolist()
    n_enc = len(values)
    agg_token, agg_plain = mock_he_aggregate(values)

    col1, col2, col3 = st.columns(3)
    col1.metric("🔒 Encrypted Records", f"{n_enc:,}")
    col2.metric("∑ HE Aggregate", f"{agg_plain:.3f} kW")
    col3.metric("🔓 Decrypted Result", f"{agg_plain:.3f} kW")

    st.markdown(
        f"""
        <div class="he-box" style="margin-top:0.75rem;">
            <h4>🔑  CKKS Ciphertext Token (simulated)</h4>
            <p class="mono">AGG_CT: {agg_token}</p>
            <p style="font-family:DM Sans,sans-serif;font-size:0.8rem;
                      color:#94a3b8;margin-top:0.6rem;">
                The aggregation server receives <em>only</em> the encrypted values.
                It computes the sum directly on ciphertexts using the CKKS homomorphic
                addition property — no individual reading is ever decrypted.
                Only the final aggregate is revealed to the authorised analyst.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("ℹ️  How Homomorphic Encryption works here"):
        st.markdown(
            """
            **CKKS Scheme (Cheon-Kim-Kim-Song)**

            CKKS supports approximate arithmetic over real-valued ciphertexts:

            ```
            Enc(a) ⊕ Enc(b)  =  Enc(a + b)    ← addition is homomorphic
            ```

            The pipeline:

            1. Each meter encrypts its DP-noised reading: `ct_i = Encrypt(noisy_eᵢ)`
            2. The aggregation server computes: `CT_sum = ct₀ ⊕ ct₁ ⊕ … ⊕ ctₙ`
            3. Only the authorised aggregator decrypts: `Decrypt(CT_sum) = Σ noisy_eᵢ`

            Individual readings are **never decrypted** at any server.
            In production, `TenSEAL` or `OpenFHE` provides the CKKS implementation.
            """,
        )


def render_activity_table(df: pd.DataFrame) -> None:
    """Section 7 — Latest meter readings table."""
    st.markdown(
        '<p class="sg-section-title">📋 Smart Meter Activity Log</p>',
        unsafe_allow_html=True,
    )

    latest = (
        df.sort_values("timestamp", ascending=False)
        .head(100)[
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
    latest["energy_usage"] = latest["energy_usage"].round(4)
    latest["voltage"] = latest["voltage"].round(2)
    latest["current"] = latest["current"].round(3)
    latest["encrypted"] = latest["encrypted"].map({True: "🔒 Yes", False: "🔓 No"})

    st.dataframe(
        latest,
        use_container_width=True,
        height=280,
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


def render_attack_panel(df: pd.DataFrame, selected_meters: List[str]) -> None:
    """Section 8 — Attack simulation."""
    st.markdown(
        '<p class="sg-section-title">⚠️  Attack Simulation Panel</p>',
        unsafe_allow_html=True,
    )

    atk_meter = st.selectbox(
        "Target meter for attack simulation",
        options=selected_meters,
        key="atk_meter_select",
    )

    mdf = df[df["meter_id"] == atk_meter].sort_values("timestamp").tail(40)
    true_mean = mdf["energy_usage"].mean()
    noisy_mean = mdf["noisy_energy_usage"].mean()
    att_err = abs(true_mean - noisy_mean) / true_mean * 100

    col_chart, col_info = st.columns([2, 1])
    with col_chart:
        fig_atk = make_attack_chart(df, atk_meter)
        st.plotly_chart(
            fig_atk, use_container_width=True, config={"displayModeBar": False}
        )

    with col_info:
        st.markdown(
            f"""
            <div class="attack-box" style="height:100%;min-height:260px;">
                <h4>🎯 Attack Analysis</h4>
                <table style="font-family:Share Tech Mono,monospace;
                              font-size:0.75rem;color:#94a3b8;
                              border-collapse:collapse;width:100%;">
                    <tr><td style="padding:4px 0;color:#64748b;">Target</td>
                        <td style="color:#e2e8f0;">{atk_meter}</td></tr>
                    <tr><td style="padding:4px 0;color:#64748b;">Strategy</td>
                        <td style="color:#e2e8f0;">Rolling-mean inference</td></tr>
                    <tr><td style="padding:4px 0;color:#64748b;">True mean</td>
                        <td style="color:#f5a623;">{true_mean:.4f} kW</td></tr>
                    <tr><td style="padding:4px 0;color:#64748b;">Attacker estimate</td>
                        <td style="color:#f87171;">{noisy_mean:.4f} kW</td></tr>
                    <tr><td style="padding:4px 0;color:#64748b;">Estimation error</td>
                        <td style="color:#4ade80;">{att_err:.1f}%</td></tr>
                </table>
                <p style="margin-top:0.8rem;font-family:DM Sans,sans-serif;
                          font-size:0.78rem;color:#64748b;line-height:1.55;">
                    The attacker's rolling-mean estimate diverges from the
                    true signal because DP noise prevents convergence.
                    Higher noise → larger estimation error → stronger privacy.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_voltage_heatmap(df: pd.DataFrame) -> None:
    """Bonus — Voltage heatmap across all meters."""
    st.markdown(
        '<p class="sg-section-title">🌡  Voltage Heatmap</p>', unsafe_allow_html=True
    )
    fig = make_voltage_heatmap(df)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# ============================================================
# MAIN APPLICATION LOOP
# ============================================================


def main() -> None:
    # ── Inject CSS ──────────────────────────────────────────
    st.markdown(STYLE, unsafe_allow_html=True)

    # ── Session state ────────────────────────────────────────
    init_session_state()

    # ── Sidebar — returns active filter values ───────────────
    selected_meters, selected_regions, time_range, noise_level = render_sidebar(
        st.session_state.df
    )

    # ── Live simulation tick ─────────────────────────────────
    if st.session_state.simulating:
        st.session_state.df = append_live_row(
            st.session_state.df, noise_level=noise_level
        )
        st.session_state.sim_tick += 1
        # Keep DataFrame from growing unbounded in demo
        if len(st.session_state.df) > NUM_METERS * 300:
            st.session_state.df = st.session_state.df.iloc[-NUM_METERS * 200 :]

    # ── Apply filters ────────────────────────────────────────
    df_full = st.session_state.df
    df_filtered = apply_filters(df_full, selected_meters, selected_regions, time_range)

    if df_filtered.empty:
        st.warning("No data matches the current filters. Adjust the sidebar settings.")
        return

    # ════════════════════════════════════════════════════════
    # SECTION 1 — Hero / Architecture
    # ════════════════════════════════════════════════════════
    render_hero()

    # Live / simulation status badge row
    col_badges = st.columns([1, 1, 1, 4])
    with col_badges[0]:
        if st.session_state.simulating:
            st.markdown(
                '<span class="badge-live">● LIVE STREAM</span>', unsafe_allow_html=True
            )
        else:
            st.markdown(
                '<span style="font-family:Share Tech Mono,monospace;font-size:0.7rem;color:#475569;">● STATIC MODE</span>',
                unsafe_allow_html=True,
            )
    with col_badges[1]:
        st.markdown(
            '<span class="badge-dp">🔐 DP ACTIVE</span>', unsafe_allow_html=True
        )
    with col_badges[2]:
        st.markdown(
            '<span class="badge-enc">🔑 HE ACTIVE</span>', unsafe_allow_html=True
        )

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # SECTION 2 — KPI Cards
    # ════════════════════════════════════════════════════════
    render_kpi_cards(df_filtered)

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # SECTION 3 — Time Series
    # ════════════════════════════════════════════════════════
    render_timeseries(df_filtered, selected_meters)

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # SECTION 4 — Regional Load
    # ════════════════════════════════════════════════════════
    render_regional_load(df_filtered)

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # SECTION 5 — Differential Privacy
    # ════════════════════════════════════════════════════════
    render_dp_panel(df_filtered, selected_meters)

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # SECTION 6 — Homomorphic Encryption
    # ════════════════════════════════════════════════════════
    render_he_panel(df_filtered, selected_regions)

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # SECTION 7 — Activity Table
    # ════════════════════════════════════════════════════════
    render_activity_table(df_filtered)

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # SECTION 8 — Attack Simulation
    # ════════════════════════════════════════════════════════
    render_attack_panel(df_filtered, selected_meters)

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # BONUS — Voltage Heatmap
    # ════════════════════════════════════════════════════════
    with st.expander("🌡  Expand: Voltage Heatmap — All Meters", expanded=False):
        render_voltage_heatmap(df_filtered)

    # ════════════════════════════════════════════════════════
    # Footer
    # ════════════════════════════════════════════════════════
    st.markdown(
        """
        <hr>
        <div style="text-align:center;font-family:Share Tech Mono,monospace;
                    font-size:0.68rem;color:#334155;padding:0.5rem 0 1.5rem;">
            SecureGrid Research Dashboard · Hybrid HE + DP Smart Grid Aggregation
            · Built with Streamlit + Plotly
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Auto-rerun when simulating ───────────────────────────
    if st.session_state.simulating:
        time.sleep(1.2)
        st.rerun()


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()


# ============================================================
# ── HOW TO RUN ──────────────────────────────────────────────
# ============================================================
#
#   1. Install dependencies:
#
#      pip install streamlit pandas plotly numpy
#
#   2. Launch the dashboard:
#
#      streamlit run dashboard.py
#
#   3. Optional — connect real simulator output:
#      Replace the generate_sample_data() call in init_session_state()
#      with a call to your SmartGridSimulator:
#
#      from smart_meter_simulator import SmartGridSimulator, generate_synthetic_rows
#      sim = SmartGridSimulator("household_power_consumption.txt", num_meters=20)
#      sim.load_dataset()
#      sim.create_meters()
#      st.session_state.df = pd.DataFrame([
#          r.to_dict() for r in itertools.islice(sim.simulate_stream(), 2000)
#      ])
#
#   4. Optional — enable DP + HE pipeline:
#      pip install diffprivlib tenseal
#      (see dp_module.py and he_module.py for integration)
#
# ============================================================
