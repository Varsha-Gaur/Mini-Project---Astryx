"""
dashboard/dashboard.py
========================
SecureGrid Research Dashboard  v4.0
-------------------------------------
The dashboard from the uploaded files was already very well built.
This version keeps the identical visual design (futuristic control-center
aesthetic, all animations, all CSS classes) and fixes:

  1. Removed dependency on generate_sample_data() that duplicated and
     diverged from simulator.py's data model.
  2. The _L() safe-merge helper is preserved — no duplicate-keyword Plotly crashes.
  3. append_live_tick() now uses the region label that matches
     DEFAULT_CONFIG.simulator.regions (strings, not integers).
  4. noise_level session key renamed from "noise_level" to "noise"
     for consistency (both worked before but were redundant).

Run:
    pip install streamlit pandas numpy plotly
    streamlit run dashboard/dashboard.py
"""

from __future__ import annotations

import hashlib
import os
import random
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from config import DEFAULT_CONFIG

st.set_page_config(
    page_title=DEFAULT_CONFIG.dashboard.page_title,
    page_icon=DEFAULT_CONFIG.dashboard.page_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Colour system ─────────────────────────────────────────────
P: Dict[str, str] = dict(
    bg_base="#03070f",
    bg_panel="#060d1a",
    bg_raised="#0a1628",
    bg_border="#0f2040",
    blue="#0ea5e9",
    blue2="#38bdf8",
    blue_dim="#0369a1",
    amber="#f59e0b",
    amber2="#fbbf24",
    cyan="#06b6d4",
    cyan2="#22d3ee",
    green="#22c55e",
    green2="#4ade80",
    purple="#8b5cf6",
    purple2="#a78bfa",
    crimson="#ef4444",
    crimson2="#fca5a5",
    orange="#f97316",
    text="#e2eaf6",
    text_dim="#64748b",
    text_mid="#94a3b8",
)

REGIONS = DEFAULT_CONFIG.simulator.regions
REGION_COLORS: Dict[str, str] = {
    "north": P["amber"],
    "south": P["cyan"],
    "east": P["green"],
    "west": P["purple"],
    "central": P["orange"],
}


# ── CSS ────────────────────────────────────────────────────────
def _css() -> str:
    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;600;700;800&family=Exo+2:ital,wght@0,300;0,400;0,500;0,600;1,400&family=Share+Tech+Mono&display=swap');
:root{{--bg-base:{P["bg_base"]};--bg-panel:{P["bg_panel"]};--bg-raised:{P["bg_raised"]};--bg-border:{P["bg_border"]};--blue:{P["blue"]};--blue2:{P["blue2"]};--amber:{P["amber"]};--amber2:{P["amber2"]};--cyan:{P["cyan"]};--cyan2:{P["cyan2"]};--green:{P["green"]};--green2:{P["green2"]};--purple:{P["purple"]};--purple2:{P["purple2"]};--crimson:{P["crimson"]};--crimson2:{P["crimson2"]};--orange:{P["orange"]};--text:{P["text"]};--text-dim:{P["text_dim"]};--text-mid:{P["text_mid"]};}}
@keyframes pulse-blue{{0%,100%{{box-shadow:0 0 8px rgba(14,165,233,.4),0 0 20px rgba(14,165,233,.12);}}50%{{box-shadow:0 0 18px rgba(14,165,233,.75),0 0 44px rgba(14,165,233,.22);}}}}
@keyframes pulse-amber{{0%,100%{{box-shadow:0 0 8px rgba(245,158,11,.4),0 0 20px rgba(245,158,11,.12);}}50%{{box-shadow:0 0 20px rgba(245,158,11,.8),0 0 48px rgba(245,158,11,.2);}}}}
@keyframes pulse-green{{0%,100%{{box-shadow:0 0 7px rgba(34,197,94,.45),0 0 16px rgba(34,197,94,.12);}}50%{{box-shadow:0 0 16px rgba(34,197,94,.85),0 0 34px rgba(34,197,94,.22);}}}}
@keyframes pulse-purple{{0%,100%{{box-shadow:0 0 8px rgba(139,92,246,.4),0 0 20px rgba(139,92,246,.12);}}50%{{box-shadow:0 0 18px rgba(139,92,246,.75),0 0 44px rgba(139,92,246,.2);}}}}
@keyframes pulse-cyan{{0%,100%{{box-shadow:0 0 8px rgba(6,182,212,.4),0 0 20px rgba(6,182,212,.12);}}50%{{box-shadow:0 0 18px rgba(6,182,212,.75),0 0 40px rgba(6,182,212,.2);}}}}
@keyframes slide-down{{from{{opacity:0;transform:translateY(-14px);}}to{{opacity:1;transform:translateY(0);}}}}
@keyframes grid-move{{0%{{background-position:0 0;}}100%{{background-position:30px 30px;}}}}
.stApp{{background-color:var(--bg-base);color:var(--text);font-family:'Exo 2',sans-serif;background-image:radial-gradient(circle,rgba(14,165,233,.055) 1px,transparent 1px);background-size:28px 28px;animation:grid-move 9s linear infinite;}}
.stApp::before{{content:'';position:fixed;inset:0;background:repeating-linear-gradient(0deg,rgba(0,0,0,0) 0,rgba(0,0,0,0) 2px,rgba(0,8,22,.1) 2px,rgba(0,8,22,.1) 4px);pointer-events:none;z-index:0;}}
.main .block-container{{padding:1.4rem 2.1rem 3rem;max-width:1500px;position:relative;z-index:1;}}
#MainMenu{{visibility:hidden;}}footer{{visibility:hidden;}}.stDeployButton{{display:none;}}[data-testid="stToolbar"]{{visibility:hidden;}}[data-testid="stDecoration"]{{display:none;}}[data-testid="collapsedControl"]{{visibility:visible !important;display:flex !important;}}header button{{visibility:visible !important;}}
section[data-testid="stSidebar"]{{background:linear-gradient(180deg,#04091a 0%,#060d1f 100%);border-right:1px solid var(--bg-border);box-shadow:4px 0 24px rgba(14,165,233,.07);}}
.hero{{background:linear-gradient(135deg,#050e1f 0%,#071628 50%,#050e1f 100%);border:1px solid var(--bg-border);border-top:2px solid var(--blue);border-radius:16px;padding:2.3rem 3rem;margin-bottom:1.5rem;position:relative;overflow:hidden;animation:slide-down .5s ease both;}}
.hero-eyebrow{{font-family:'Share Tech Mono',monospace;font-size:.64rem;letter-spacing:.32em;color:var(--blue);text-transform:uppercase;margin-bottom:.45rem;}}
.hero-title{{font-family:'Orbitron',sans-serif;font-size:1.85rem;font-weight:700;color:var(--text);letter-spacing:.04em;line-height:1.22;margin-bottom:.5rem;}}
.hero-title .accent-b{{color:var(--blue2);}}.hero-title .accent-a{{color:var(--amber2);}}
.hero-sub{{font-size:.88rem;color:var(--text-mid);line-height:1.65;max-width:700px;margin-bottom:1.35rem;}}
.pipeline{{display:flex;flex-wrap:wrap;align-items:center;gap:.35rem;}}
.pipe-node{{background:rgba(14,165,233,.07);border:1px solid rgba(14,165,233,.28);border-radius:6px;padding:.3rem .9rem;font-family:'Share Tech Mono',monospace;font-size:.64rem;color:var(--blue2);letter-spacing:.07em;white-space:nowrap;transition:all .22s;}}
.pipe-node:hover{{background:rgba(14,165,233,.17);border-color:var(--blue);box-shadow:0 0 12px rgba(14,165,233,.25);}}
.pipe-arr{{color:var(--text-dim);font-size:.75rem;}}
.badge{{display:inline-block;border-radius:4px;padding:2px 10px;font-family:'Share Tech Mono',monospace;font-size:.63rem;letter-spacing:.1em;}}
.b-live{{background:rgba(34,197,94,.1);color:var(--green2);border:1px solid rgba(34,197,94,.32);}}
.b-dp{{background:rgba(6,182,212,.09);color:var(--cyan2);border:1px solid rgba(6,182,212,.32);}}
.b-he{{background:rgba(139,92,246,.09);color:var(--purple2);border:1px solid rgba(139,92,246,.32);}}
.b-atk{{background:rgba(239,68,68,.09);color:var(--crimson2);border:1px solid rgba(239,68,68,.32);}}
.b-off{{background:rgba(100,116,139,.08);color:var(--text-dim);border:1px solid rgba(100,116,139,.28);}}
.dot{{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;vertical-align:middle;}}
.dot-g{{background:var(--green);animation:pulse-green 1.8s ease-in-out infinite;}}
.sec-hdr{{display:flex;align-items:center;gap:.65rem;padding-bottom:.55rem;margin-bottom:1rem;border-bottom:1px solid var(--bg-border);position:relative;}}
.sec-hdr::before{{content:'';position:absolute;bottom:-1px;left:0;width:55px;height:1px;background:var(--blue);}}
.sec-icon{{font-size:1.05rem;line-height:1;}}.sec-title{{font-family:'Orbitron',sans-serif;font-size:.66rem;font-weight:600;letter-spacing:.22em;text-transform:uppercase;color:var(--text-mid);}}
.sec-badge{{margin-left:auto;font-family:'Share Tech Mono',monospace;font-size:.58rem;letter-spacing:.1em;color:var(--blue);border:1px solid rgba(14,165,233,.3);border-radius:4px;padding:2px 8px;}}
.kpi-row{{display:grid;grid-template-columns:repeat(5,1fr);gap:.9rem;margin-bottom:.3rem;}}
.kc{{background:linear-gradient(145deg,var(--bg-panel),var(--bg-raised));border:1px solid var(--bg-border);border-radius:14px;padding:1.3rem 1.4rem 1.1rem;position:relative;overflow:hidden;transition:transform .22s,box-shadow .22s;}}
.kc:hover{{transform:translateY(-3px);}}
.kc-b{{border-top:2px solid var(--blue);animation:pulse-blue 3.2s ease-in-out infinite;}}
.kc-a{{border-top:2px solid var(--amber);animation:pulse-amber 3.2s ease-in-out infinite;}}
.kc-c{{border-top:2px solid var(--cyan);animation:pulse-cyan 3.5s ease-in-out infinite;}}
.kc-g{{border-top:2px solid var(--green);animation:pulse-green 3.2s ease-in-out infinite;}}
.kc-p{{border-top:2px solid var(--purple);animation:pulse-purple 3.2s ease-in-out infinite;}}
.kc-glow{{position:absolute;bottom:-15px;right:-15px;width:80px;height:80px;border-radius:50%;opacity:.04;}}
.kc-b .kc-glow{{background:var(--blue)}}.kc-a .kc-glow{{background:var(--amber)}}.kc-c .kc-glow{{background:var(--cyan)}}.kc-g .kc-glow{{background:var(--green)}}.kc-p .kc-glow{{background:var(--purple)}}
.kc-ico{{position:absolute;right:1rem;top:.9rem;font-size:1.4rem;opacity:.17;}}
.kc-lbl{{font-family:'Share Tech Mono',monospace;font-size:.6rem;letter-spacing:.18em;text-transform:uppercase;margin-bottom:.5rem;}}
.kc-b .kc-lbl{{color:var(--blue2);}}.kc-a .kc-lbl{{color:var(--amber2);}}.kc-c .kc-lbl{{color:var(--cyan2);}}.kc-g .kc-lbl{{color:var(--green2);}}.kc-p .kc-lbl{{color:var(--purple2);}}
.kc-val{{font-family:'Orbitron',sans-serif;font-size:1.68rem;font-weight:700;line-height:1;margin-bottom:.4rem;color:var(--text);}}
.kc-sub{{font-family:'Share Tech Mono',monospace;font-size:.52rem;letter-spacing:.08em;}}
.kc-b .kc-sub{{color:var(--blue2);opacity:.7;}}.kc-a .kc-sub{{color:var(--amber2);opacity:.7;}}.kc-c .kc-sub{{color:var(--cyan2);opacity:.7;}}.kc-g .kc-sub{{color:var(--green2);opacity:.7;}}.kc-p .kc-sub{{color:var(--purple2);opacity:.7;}}
.kc-delta{{font-family:'Share Tech Mono',monospace;font-size:.63rem;margin-top:.3rem;}}
.delta-up{{color:var(--green2);}}.delta-dn{{color:var(--crimson);}}.delta-flat{{color:var(--text-dim);}}
.panel{{background:linear-gradient(145deg,var(--bg-panel),var(--bg-raised));border:1px solid var(--bg-border);border-radius:14px;padding:1.4rem 1.6rem;}}
.pl-b{{border-left:3px solid var(--blue);}}.pl-a{{border-left:3px solid var(--amber);}}.pl-p{{border-left:3px solid var(--purple);}}.pl-c{{border-left:3px solid var(--cyan);}}.pl-g{{border-left:3px solid var(--green);}}.pl-r{{border-left:3px solid var(--crimson);}}
.flow{{display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:0;margin:1.1rem 0;}}
.fb{{background:var(--bg-raised);border-radius:10px;padding:.95rem 1.15rem;text-align:center;min-width:120px;}}
.fb-b{{border:1px solid rgba(14,165,233,.48);box-shadow:0 0 14px rgba(14,165,233,.1);}}.fb-p{{border:1px solid rgba(139,92,246,.48);box-shadow:0 0 14px rgba(139,92,246,.1);}}.fb-g{{border:1px solid rgba(34,197,94,.48);box-shadow:0 0 14px rgba(34,197,94,.1);}}.fb-c{{border:1px solid rgba(6,182,212,.48);box-shadow:0 0 14px rgba(6,182,212,.1);}}
.fb-ico{{font-size:1.55rem;display:block;margin-bottom:.28rem;}}.fb-lbl{{font-family:'Share Tech Mono',monospace;font-size:.6rem;letter-spacing:.12em;text-transform:uppercase;color:var(--text-mid);}}.fb-val{{font-family:'Orbitron',sans-serif;font-size:.72rem;font-weight:600;margin-top:.28rem;}}
.fb-b .fb-val{{color:var(--blue2);}}.fb-p .fb-val{{color:var(--purple2);}}.fb-g .fb-val{{color:var(--green2);}}.fb-c .fb-val{{color:var(--cyan2);}}
.flow-arr{{color:var(--bg-border);font-size:1.3rem;padding:0 .35rem;}}
.cipher{{background:rgba(139,92,246,.07);border:1px solid rgba(139,92,246,.3);border-radius:6px;padding:.55rem 1rem;font-family:'Share Tech Mono',monospace;font-size:.65rem;color:var(--purple2);letter-spacing:.05em;word-break:break-all;line-height:1.6;margin-top:.75rem;}}
.atk-tbl{{width:100%;border-collapse:collapse;font-family:'Share Tech Mono',monospace;font-size:.7rem;}}
.atk-tbl td{{padding:5px 4px;}}.atk-tbl td:first-child{{color:var(--text-dim);padding-right:12px;white-space:nowrap;}}
.stButton>button{{background:linear-gradient(135deg,rgba(14,165,233,.11),rgba(14,165,233,.05)) !important;color:var(--blue2) !important;border:1px solid rgba(14,165,233,.38) !important;border-radius:8px !important;font-family:'Orbitron',sans-serif !important;font-size:.66rem !important;letter-spacing:.12em !important;padding:.52rem 1.35rem !important;transition:all .22s !important;text-transform:uppercase !important;}}
.stButton>button:hover{{background:rgba(14,165,233,.2) !important;border-color:var(--blue) !important;color:#fff !important;box-shadow:0 0 22px rgba(14,165,233,.28) !important;transform:translateY(-1px) !important;}}
.btn-start .stButton>button{{background:linear-gradient(135deg,rgba(34,197,94,.13),rgba(34,197,94,.05)) !important;color:var(--green2) !important;border-color:rgba(34,197,94,.42) !important;}}
.btn-start .stButton>button:hover{{background:rgba(34,197,94,.22) !important;box-shadow:0 0 22px rgba(34,197,94,.28) !important;}}
.btn-stop .stButton>button{{background:linear-gradient(135deg,rgba(239,68,68,.11),rgba(239,68,68,.04)) !important;color:var(--crimson2) !important;border-color:rgba(239,68,68,.33) !important;}}
div[data-testid="stExpander"]{{background:var(--bg-panel) !important;border:1px solid var(--bg-border) !important;border-radius:10px !important;}}
div[data-testid="stDataFrame"] thead tr th{{background:var(--bg-raised) !important;color:var(--blue2) !important;font-family:'Share Tech Mono',monospace !important;font-size:.6rem !important;letter-spacing:.14em !important;text-transform:uppercase !important;border-bottom:1px solid var(--bg-border) !important;}}
div[data-testid="stDataFrame"] tbody tr td{{font-family:'Exo 2',sans-serif !important;font-size:.79rem !important;color:var(--text) !important;border-bottom:1px solid rgba(15,32,64,.7) !important;}}
div[data-testid="stMetric"]{{background:linear-gradient(145deg,var(--bg-panel),var(--bg-raised)) !important;border:1px solid var(--bg-border) !important;border-radius:12px !important;padding:1.05rem 1.3rem !important;}}
div[data-testid="stMetric"] label{{color:var(--text-dim) !important;font-family:'Share Tech Mono',monospace !important;font-size:.63rem !important;letter-spacing:.14em !important;text-transform:uppercase !important;}}
div[data-testid="stMetric"] [data-testid="stMetricValue"]{{color:var(--amber2) !important;font-family:'Orbitron',sans-serif !important;font-size:1.45rem !important;}}
div[data-testid="stSlider"] [data-baseweb="slider"] div[role="slider"]{{background:var(--blue) !important;box-shadow:0 0 8px rgba(14,165,233,.5) !important;}}
hr{{border:none !important;border-top:1px solid var(--bg-border) !important;margin:1.6rem 0 !important;}}
</style>"""


# ── Diurnal pattern ────────────────────────────────────────────
_D = [
    0.30,
    0.28,
    0.26,
    0.25,
    0.25,
    0.28,
    0.52,
    0.76,
    0.88,
    0.74,
    0.66,
    0.65,
    0.70,
    0.68,
    0.65,
    0.68,
    0.76,
    0.96,
    1.0,
    0.98,
    0.91,
    0.82,
    0.67,
    0.46,
]


def _d(h):
    return _D[h % 24]


# ── Data generation ────────────────────────────────────────────
def generate_data(n_meters=20, n_minutes=120, noise=0.08, seed=42):
    rng = random.Random(seed)
    nprng = np.random.default_rng(seed)
    meta = {}
    for i in range(n_meters):
        mid = f"meter_{i:03d}"
        meta[mid] = dict(
            region=REGIONS[i % len(REGIONS)],
            scale=rng.uniform(0.55, 1.65),
            v_off=rng.uniform(-6.0, 6.0),
            i_off=rng.uniform(-0.5, 0.5),
            wk=rng.uniform(0.1, 1.0),
            wl=rng.uniform(0.1, 1.0),
            wh=rng.uniform(0.1, 1.0),
        )
    base = datetime(2026, 3, 15, 8, 0, 0)
    rows = []
    for minute in range(n_minutes):
        ts = base + timedelta(minutes=minute)
        h = ts.hour
        d = _d(h)
        for mid, m in meta.items():
            be = max(0.05, (d * 2.0 + float(nprng.normal(0, 0.12))) * m["scale"])
            if ts.weekday() < 5 and 17 <= h < 21:
                be *= 1.20
            if ts.weekday() >= 5:
                be *= 0.85
            if rng.random() < 0.025:
                be += rng.uniform(0.4, 1.8)
            be = max(0.05, round(be, 4))
            v = round(230.0 + m["v_off"] + float(nprng.normal(0, 0.7)), 2)
            curr = round(
                max(
                    0.1,
                    be * 1000 / max(v, 1.0) + m["i_off"] + float(nprng.normal(0, 0.04)),
                ),
                3,
            )
            tw = m["wk"] + m["wl"] + m["wh"]
            sm = max(0.0, be * 3 + float(nprng.normal(0, 0.5)))
            dn = float(nprng.laplace(0.0, noise * max(be, 0.01)))
            ne = max(0.0, round(be + dn, 4))
            tok = hashlib.sha256(f"{ne:.6f}|k42".encode()).hexdigest()[:24].upper()
            rows.append(
                dict(
                    meter_id=mid,
                    timestamp=ts,
                    energy_usage=be,
                    noisy_energy_usage=ne,
                    voltage=v,
                    current=curr,
                    region=m["region"],
                    kitchen=round(sm * m["wk"] / tw, 3),
                    laundry=round(sm * m["wl"] / tw, 3),
                    hvac=round(sm * m["wh"] / tw, 3),
                    privacy_noise=round(abs(dn), 6),
                    encrypted=True,
                    he_token=tok,
                )
            )
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values(["timestamp", "meter_id"]).reset_index(drop=True)


def append_tick(df, noise=0.08):
    rng = np.random.default_rng()
    ts = df["timestamp"].max() + timedelta(minutes=1)
    d = _d(ts.hour)
    new = []
    for mid in df["meter_id"].unique():
        sub = df[df["meter_id"] == mid]
        region = sub["region"].iloc[0]
        scale = float(
            np.clip(sub["energy_usage"].mean() / max(d * 2.0, 0.01), 0.4, 2.5)
        )
        be = max(0.05, round((d * 2.0 + float(rng.normal(0, 0.12))) * scale, 4))
        v = round(230.0 + float(rng.normal(0, 0.7)), 2)
        curr = round(max(0.1, be * 1000 / max(v, 1.0)), 3)
        dn = float(rng.laplace(0.0, noise * max(be, 0.01)))
        ne = max(0.0, round(be + dn, 4))
        sm = max(0.0, be * 3 + float(rng.normal(0, 0.5)))
        tok = hashlib.sha256(f"{ne:.6f}|k42".encode()).hexdigest()[:24].upper()
        new.append(
            dict(
                meter_id=mid,
                timestamp=ts,
                energy_usage=be,
                noisy_energy_usage=ne,
                voltage=v,
                current=curr,
                region=region,
                kitchen=round(sm * 0.35, 3),
                laundry=round(sm * 0.30, 3),
                hvac=round(sm * 0.35, 3),
                privacy_noise=round(abs(dn), 6),
                encrypted=True,
                he_token=tok,
            )
        )
    return pd.concat([df, pd.DataFrame(new)], ignore_index=True)


# ── Plotly theme (safe merge — no duplicate-keyword errors) ───
_PT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(6,13,26,.88)",
    font=dict(family="Exo 2, sans-serif", color=P["text_mid"], size=11),
    xaxis=dict(
        gridcolor="rgba(15,32,64,.8)",
        linecolor=P["bg_border"],
        tickfont=dict(size=9, family="Share Tech Mono"),
        title_font=dict(size=10),
        zerolinecolor="rgba(15,32,64,.8)",
    ),
    yaxis=dict(
        gridcolor="rgba(15,32,64,.8)",
        linecolor=P["bg_border"],
        tickfont=dict(size=9, family="Share Tech Mono"),
        title_font=dict(size=10),
        zerolinecolor="rgba(15,32,64,.8)",
    ),
    margin=dict(l=52, r=20, t=44, b=42),
    legend=dict(
        bgcolor="rgba(6,13,26,.9)",
        bordercolor=P["bg_border"],
        borderwidth=1,
        font=dict(size=9, family="Exo 2"),
    ),
)


def _L(h=340, **kw):
    """Safely merge Plotly theme — no duplicate-keyword crash."""
    m = dict(_PT)
    m["height"] = h
    m.update(kw)
    return m


_PAL = [
    P["blue"],
    P["amber"],
    P["cyan"],
    P["green"],
    P["purple"],
    P["orange"],
    "#f472b6",
    "#34d399",
    "#60a5fa",
    "#facc15",
    "#fb7185",
    "#a3e635",
]


# ── Charts ──────────────────────────────────────────────────────
def chart_timeseries(df, meters):
    fig = go.Figure()
    sub = df[df["meter_id"].isin(meters)]
    for i, mid in enumerate(meters[:12]):
        mdf = sub[sub["meter_id"] == mid].sort_values("timestamp")
        fig.add_trace(
            go.Scatter(
                x=mdf["timestamp"],
                y=mdf["energy_usage"],
                name=mid,
                mode="lines",
                line=dict(width=1.8, color=_PAL[i % len(_PAL)]),
                hovertemplate=f"<b>{mid}</b><br>%{{x|%H:%M}}<br>%{{y:.4f}} kW<extra></extra>",
            )
        )
    fig.update_layout(
        **_L(
            360,
            title=dict(
                text="⚡  Live Energy Consumption Stream",
                font=dict(size=13, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis_title="Time",
            yaxis_title="Energy (kW)",
            hovermode="x unified",
        )
    )
    return fig


def chart_noisy_vs_true(df, meter_id):
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(80)
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("🔓 True Energy (Private)", "🔐 Published (DP-Noised)"),
        horizontal_spacing=0.08,
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["energy_usage"],
            fill="tozeroy",
            fillcolor="rgba(245,158,11,.09)",
            line=dict(color=P["amber"], width=2),
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
            fillcolor="rgba(6,182,212,.08)",
            line=dict(color=P["cyan"], width=2),
            name="Noised",
            hovertemplate="Noised: %{y:.4f}<extra></extra>",
        ),
        row=1,
        col=2,
    )
    fig.update_layout(
        **_L(
            320,
            title=dict(
                text=f"🔐 DP Comparison — {meter_id}",
                font=dict(size=12, color=P["text"], family="Orbitron"),
                x=0,
            ),
            showlegend=False,
        )
    )
    for ax in ["xaxis", "xaxis2", "yaxis", "yaxis2"]:
        fig.layout[ax].update(
            gridcolor="rgba(15,32,64,.8)",
            linecolor=P["bg_border"],
            tickfont=dict(size=8, color=P["text_dim"]),
        )
    for ann in fig.layout.annotations:
        ann.font.color = P["text_mid"]
        ann.font.size = 10
        ann.font.family = "Orbitron"
    return fig


def chart_noise_hist(df, meter_id):
    mdf = df[df["meter_id"] == meter_id]
    fig = go.Figure(
        go.Histogram(
            x=mdf["privacy_noise"],
            nbinsx=35,
            marker_color=P["cyan"],
            marker_line=dict(color=P["bg_base"], width=0.5),
            opacity=0.8,
            hovertemplate="Noise: %{x:.5f}<br>Count: %{y}<extra></extra>",
        )
    )
    fig.update_layout(
        **_L(
            252,
            title=dict(
                text="Laplace Noise",
                font=dict(size=11, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis_title="Noise |Δ| (kW)",
            yaxis_title="Count",
            showlegend=False,
        )
    )
    return fig


def chart_regional_bar(df):
    agg = (
        df.groupby("region")["energy_usage"]
        .sum()
        .reset_index()
        .sort_values("energy_usage", ascending=False)
    )
    cols = [REGION_COLORS.get(r, P["blue"]) for r in agg["region"]]
    fig = go.Figure(
        go.Bar(
            x=agg["region"],
            y=agg["energy_usage"],
            marker=dict(
                color=cols, opacity=0.86, line=dict(color=P["bg_base"], width=0)
            ),
            text=agg["energy_usage"].round(1).astype(str) + " kW",
            textposition="outside",
            textfont=dict(size=10, color=P["text"], family="Share Tech Mono"),
            hovertemplate="<b>%{x}</b><br>%{y:.2f} kW<extra></extra>",
        )
    )
    fig.update_layout(
        **_L(
            302,
            title=dict(
                text="🗺  Grid Load by Region",
                font=dict(size=12, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis_title="Region",
            yaxis_title="Total Energy (kW)",
            showlegend=False,
            bargap=0.34,
        )
    )
    return fig


def chart_region_pie(df):
    agg = df.groupby("region")["energy_usage"].sum().reset_index()
    cols = [REGION_COLORS.get(r, P["blue"]) for r in agg["region"]]
    fig = go.Figure(
        go.Pie(
            labels=agg["region"],
            values=agg["energy_usage"].round(2),
            hole=0.60,
            marker=dict(colors=cols, line=dict(color=P["bg_base"], width=2)),
            textfont=dict(size=9, color=P["text"], family="Exo 2"),
            hovertemplate="<b>%{label}</b><br>%{value:.2f} kW (%{percent})<extra></extra>",
        )
    )
    fig.update_layout(
        **_L(
            302,
            title=dict(
                text="Region Share",
                font=dict(size=11, color=P["text"], family="Orbitron"),
                x=0,
            ),
            showlegend=True,
            legend=dict(
                orientation="v",
                x=1.0,
                y=0.5,
                bgcolor="rgba(6,13,26,.9)",
                bordercolor=P["bg_border"],
                borderwidth=1,
                font=dict(size=9, family="Exo 2"),
            ),
            annotations=[
                dict(
                    text="LOAD<br>SHARE",
                    x=0.5,
                    y=0.5,
                    font=dict(size=9, color=P["text_dim"], family="Orbitron"),
                    showarrow=False,
                )
            ],
        )
    )
    return fig


def chart_attack(df, meter_id):
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(50)
    att = mdf["noisy_energy_usage"].rolling(6, min_periods=1).mean()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["energy_usage"],
            name="True (hidden)",
            mode="lines",
            line=dict(color=P["amber"], width=2, dash="dot"),
            hovertemplate="True: %{y:.4f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=mdf["noisy_energy_usage"],
            name="Published (DP)",
            mode="lines",
            line=dict(color=P["cyan"], width=1.8),
            hovertemplate="Published: %{y:.4f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=mdf["timestamp"],
            y=att,
            name="⚠ Attacker est.",
            mode="lines",
            line=dict(color=P["crimson"], width=1.8, dash="dash"),
            hovertemplate="Attacker: %{y:.4f}<extra></extra>",
        )
    )
    fig.update_layout(
        **_L(
            312,
            title=dict(
                text="⚠️  Adversarial Reconstruction Attempt",
                font=dict(size=12, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis_title="Time",
            yaxis_title="Energy (kW)",
            hovermode="x unified",
        )
    )
    return fig


def chart_hourly(df):
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
            fillcolor="rgba(14,165,233,.07)",
            line=dict(color="rgba(0,0,0,0)"),
            name="±1σ",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=hp["hour"],
            y=hp["mean"],
            mode="lines+markers",
            line=dict(color=P["blue"], width=2.2),
            marker=dict(
                size=5, color=P["blue2"], line=dict(color=P["bg_base"], width=1)
            ),
            name="Mean load",
            hovertemplate="Hour %{x}:00 → %{y:.4f} kW<extra></extra>",
        )
    )
    fig.update_layout(
        **_L(
            292,
            title=dict(
                text="24h Load Profile",
                font=dict(size=12, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis=dict(
                title="Hour of Day",
                tickvals=list(range(0, 24, 2)),
                gridcolor="rgba(15,32,64,.8)",
                linecolor=P["bg_border"],
                tickfont=dict(size=9, family="Share Tech Mono"),
                title_font=dict(size=10),
            ),
            yaxis_title="Avg Energy (kW)",
        )
    )
    return fig


def chart_sub(df, meter_id):
    mdf = df[df["meter_id"] == meter_id].sort_values("timestamp").tail(60)
    fig = go.Figure()
    for col, colour, name in [
        ("kitchen", P["amber"], "🍳 Kitchen"),
        ("laundry", P["cyan"], "🫧 Laundry"),
        ("hvac", P["purple"], "❄️  HVAC"),
    ]:
        fig.add_trace(
            go.Scatter(
                x=mdf["timestamp"],
                y=mdf[col],
                name=name,
                stackgroup="sub",
                line=dict(color=colour, width=1.3),
                hovertemplate=f"{name}: %{{y:.3f}}<extra></extra>",
            )
        )
    fig.update_layout(
        **_L(
            272,
            title=dict(
                text=f"Sub-Metering — {meter_id}",
                font=dict(size=12, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis_title="Time",
            yaxis_title="Wh",
        )
    )
    return fig


def chart_voltage_heat(df):
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
            x=[c.strftime("%H:%M") for c in pivot.columns],
            y=pivot.index.tolist(),
            colorscale=[
                [0, "#040d1a"],
                [0.3, P["blue_dim"]],
                [0.6, P["blue"]],
                [0.8, P["amber"]],
                [1, P["crimson"]],
            ],
            colorbar=dict(
                title=dict(text="V", font=dict(size=9, color=P["text_dim"])),
                tickfont=dict(size=8, color=P["text_dim"]),
                thickness=10,
            ),
            hovertemplate="Meter: %{y}<br>Time: %{x}<br>Voltage: %{z:.1f} V<extra></extra>",
        )
    )
    fig.update_layout(
        **_L(
            322,
            title=dict(
                text="🌡  Voltage Stability Heatmap",
                font=dict(size=12, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis=dict(
                tickangle=-45,
                tickfont=dict(size=7, family="Share Tech Mono"),
                gridcolor="rgba(0,0,0,0)",
                linecolor=P["bg_border"],
            ),
            yaxis=dict(
                tickfont=dict(size=8, family="Share Tech Mono"),
                gridcolor="rgba(0,0,0,0)",
                linecolor=P["bg_border"],
            ),
        )
    )
    return fig


# ── Session state ───────────────────────────────────────────────
def init_state():
    if "df" not in st.session_state:
        st.session_state.df = generate_data()
    if "simulating" not in st.session_state:
        st.session_state.simulating = False
    if "tick" not in st.session_state:
        st.session_state.tick = 0
    if "noise" not in st.session_state:
        st.session_state.noise = 0.08


# ── Sidebar ──────────────────────────────────────────────────────
def render_sidebar(df):
    sb = st.sidebar
    sb.markdown(
        f"""
      <div style="padding:.85rem 0 .4rem;">
        <p style="font-family:Orbitron,sans-serif;font-size:.92rem;font-weight:700;
                  color:{P["blue2"]};margin:0;letter-spacing:.08em;">⚡ SECUREGRID</p>
        <p style="font-family:'Exo 2',sans-serif;font-size:.66rem;
                  color:{P["text_dim"]};margin:.22rem 0 0;">Research Dashboard v4.0</p>
      </div>
      <hr style="border:none;border-top:1px solid {P["bg_border"]};margin:.6rem 0;">
    """,
        unsafe_allow_html=True,
    )

    sb.markdown(
        f'<p style="font-family:Orbitron,sans-serif;font-size:.58rem;letter-spacing:.22em;color:{P["text_mid"]};text-transform:uppercase;margin-bottom:.5rem;">▶  SIMULATION</p>',
        unsafe_allow_html=True,
    )
    c1, c2 = sb.columns(2)
    with c1:
        if st.button("▶ START", key="btn_start", use_container_width=True):
            st.session_state.simulating = True
    with c2:
        if st.button("■ STOP", key="btn_stop", use_container_width=True):
            st.session_state.simulating = False
    if st.session_state.simulating:
        sb.markdown(
            f'<div style="margin:.4rem 0;"><span class="badge b-live"><span class="dot dot-g"></span>LIVE · TICK #{st.session_state.tick}</span></div>',
            unsafe_allow_html=True,
        )
    else:
        sb.markdown(
            '<div style="margin:.4rem 0;"><span class="badge b-off">● STATIC MODE</span></div>',
            unsafe_allow_html=True,
        )
    sb.markdown("---")

    sb.markdown(
        f'<p style="font-family:Orbitron,sans-serif;font-size:.58rem;letter-spacing:.22em;color:{P["text_mid"]};text-transform:uppercase;margin-bottom:.5rem;">🔒  PRIVACY BUDGET</p>',
        unsafe_allow_html=True,
    )
    noise = sb.slider(
        "DP Noise σ", 0.001, 0.30, st.session_state.noise, step=0.001, format="%.3f"
    )
    st.session_state.noise = noise
    sb.markdown(
        f'<p style="font-family:Share Tech Mono,monospace;font-size:.63rem;color:{P["cyan2"]};margin-top:.2rem;">ε ≈ {round(1.0 / max(noise, 1e-4), 1)}</p>',
        unsafe_allow_html=True,
    )
    sb.markdown("---")

    sb.markdown(
        f'<p style="font-family:Orbitron,sans-serif;font-size:.58rem;letter-spacing:.22em;color:{P["text_mid"]};text-transform:uppercase;margin-bottom:.5rem;">🔌  METERS</p>',
        unsafe_allow_html=True,
    )
    all_m = sorted(df["meter_id"].unique().tolist())
    sel_m = sb.multiselect(
        "Smart Meters",
        all_m,
        default=all_m[: DEFAULT_CONFIG.dashboard.default_meters],
        placeholder="Select…",
    )
    if not sel_m:
        sel_m = all_m
    sb.markdown("---")

    sb.markdown(
        f'<p style="font-family:Orbitron,sans-serif;font-size:.58rem;letter-spacing:.22em;color:{P["text_mid"]};text-transform:uppercase;margin-bottom:.5rem;">🗺  REGIONS</p>',
        unsafe_allow_html=True,
    )
    all_r = sorted(df["region"].unique().tolist())
    sel_r = sb.multiselect("Grid Regions", all_r, default=all_r, placeholder="Select…")
    if not sel_r:
        sel_r = all_r
    sb.markdown("---")

    sb.markdown(
        f'<p style="font-family:Orbitron,sans-serif;font-size:.58rem;letter-spacing:.22em;color:{P["text_mid"]};text-transform:uppercase;margin-bottom:.5rem;">🕒  TIME RANGE</p>',
        unsafe_allow_html=True,
    )
    tmin = df["timestamp"].min().to_pydatetime()
    tmax = df["timestamp"].max().to_pydatetime()
    t0 = sb.slider("From", tmin, tmax, tmin, format="HH:mm")
    t1 = sb.slider("To", tmin, tmax, tmax, format="HH:mm")
    sb.markdown("---")
    if sb.button("↺ RESET DATA", use_container_width=True):
        st.session_state.df = generate_data(noise=noise)
        st.session_state.tick = 0
        st.session_state.simulating = False
        st.rerun()
    return sel_m, sel_r, (t0, t1), noise


# ── HTML helpers ─────────────────────────────────────────────────
def _sec(icon, title, badge=""):
    b = f'<span class="sec-badge">{badge}</span>' if badge else ""
    st.markdown(
        f'<div class="sec-hdr"><span class="sec-icon">{icon}</span><span class="sec-title">{title}</span>{b}</div>',
        unsafe_allow_html=True,
    )


def _delta(v, unit=""):
    if v > 0:
        return f'<div class="kc-delta delta-up">▲ +{v}{unit}</div>'
    if v < 0:
        return f'<div class="kc-delta delta-dn">▼ {v}{unit}</div>'
    return '<div class="kc-delta delta-flat">— stable</div>'


# ── Section renderers ─────────────────────────────────────────────
def s_hero(simulating):
    dot = '<span class="dot dot-g"></span>' if simulating else ""
    live = (
        f'<span class="badge b-live">{dot}LIVE STREAM</span>'
        if simulating
        else '<span class="badge b-off">● STATIC</span>'
    )
    st.markdown(
        f"""
        <div class="hero">
          <div class="hero-eyebrow">⚡ SECURE ENERGY MONITORING SYSTEM</div>
          <div class="hero-title"><span class="accent-b">Smart Grid</span><span class="accent-a"> Energy</span> Control Center</div>
          <div class="hero-sub">Privacy-Preserving Smart Grid Monitoring using <strong style="color:{P["purple2"]};">Homomorphic Encryption</strong> and <strong style="color:{P["cyan2"]};">Differential Privacy</strong>. Real-time data flows through a secure aggregation pipeline.</div>
          <div class="pipeline">
            <span class="pipe-node">📡 Smart Meter</span><span class="pipe-arr">→</span>
            <span class="pipe-node">🔐 DP Module</span><span class="pipe-arr">→</span>
            <span class="pipe-node">🔑 CKKS Encrypt</span><span class="pipe-arr">→</span>
            <span class="pipe-node">∑ HE Aggregate</span><span class="pipe-arr">→</span>
            <span class="pipe-node">🖥 FastAPI Server</span><span class="pipe-arr">→</span>
            <span class="pipe-node">📊 Dashboard</span>
          </div>
          <div style="display:flex;gap:.55rem;margin-top:1.1rem;flex-wrap:wrap;">{live}<span class="badge b-dp">🔐 DP ACTIVE</span><span class="badge b-he">🔑 HE ACTIVE</span><span class="badge b-atk">⚠ ATK SIM</span></div>
        </div>""",
        unsafe_allow_html=True,
    )


def s_kpis(df):
    _sec("📊", "REAL-TIME GRID MONITORING", "KPI PANEL")
    te = df["energy_usage"].sum()
    am = df["meter_id"].nunique()
    av = df["voltage"].mean()
    ai = df["current"].mean()
    ep = df["encrypted"].mean() * 100
    cut = df["timestamp"].quantile(0.90)
    r = df[df["timestamp"] >= cut]
    p = df[df["timestamp"] < cut]
    de = (
        round(r["energy_usage"].mean() - p["energy_usage"].mean(), 4) if len(p) else 0.0
    )
    dv = round(r["voltage"].mean() - p["voltage"].mean(), 2) if len(p) else 0.0
    st.markdown(
        f"""
        <div class="kpi-row">
          <div class="kc kc-b"><span class="kc-ico">⚡</span><div class="kc-glow"></div><div class="kc-lbl">Total Energy</div><div class="kc-val">{te:.1f}</div><div class="kc-sub">kW consumed</div>{_delta(de, " kW")}</div>
          <div class="kc kc-a"><span class="kc-ico">🔌</span><div class="kc-glow"></div><div class="kc-lbl">Active Meters</div><div class="kc-val">{am}</div><div class="kc-sub">smart meters online</div><div class="kc-delta delta-flat">— all reporting</div></div>
          <div class="kc kc-c"><span class="kc-ico">🔋</span><div class="kc-glow"></div><div class="kc-lbl">Avg Voltage</div><div class="kc-val">{av:.1f}</div><div class="kc-sub">volts (nominal 230V)</div>{_delta(dv, "V")}</div>
          <div class="kc kc-g"><span class="kc-ico">⚙</span><div class="kc-glow"></div><div class="kc-lbl">Avg Current</div><div class="kc-val">{ai:.2f}</div><div class="kc-sub">amperes</div><div class="kc-delta delta-flat">— nominal</div></div>
          <div class="kc kc-p"><span class="kc-ico">🔒</span><div class="kc-glow"></div><div class="kc-lbl">Encrypted</div><div class="kc-val">{ep:.0f}%</div><div class="kc-sub">readings secured</div><div class="kc-delta delta-up">▲ CKKS active</div></div>
        </div><div style="height:.35rem;"></div>""",
        unsafe_allow_html=True,
    )


def s_timeseries(df, sel_m):
    _sec("📈", "LIVE ENERGY MONITOR", "REAL-TIME")
    st.plotly_chart(
        chart_timeseries(df, sel_m), width="stretch", config={"displayModeBar": False}
    )


def s_regional(df):
    _sec("🗺", "REGIONAL GRID LOAD", "DISTRIBUTION")
    c1, c2 = st.columns([2, 1])
    with c1:
        st.plotly_chart(
            chart_regional_bar(df), width="stretch", config={"displayModeBar": False}
        )
    with c2:
        st.plotly_chart(
            chart_region_pie(df), width="stretch", config={"displayModeBar": False}
        )


def s_dp(df, sel_m):
    _sec("🔐", "PRIVACY PROTECTION VISUALIZATION", "DIFFERENTIAL PRIVACY")
    dp_m = st.selectbox("Meter for DP comparison", sel_m, key="dp_m")
    c1, c2 = st.columns([3, 1])
    with c1:
        st.plotly_chart(
            chart_noisy_vs_true(df, dp_m),
            width="stretch",
            config={"displayModeBar": False},
        )
    with c2:
        st.plotly_chart(
            chart_noise_hist(df, dp_m),
            width="stretch",
            config={"displayModeBar": False},
        )
    mdf = df[df["meter_id"] == dp_m]
    tv = mdf["energy_usage"].values
    nv = mdf["noisy_energy_usage"].values
    mae = round(float(np.mean(np.abs(tv - nv))), 5)
    rmse = round(float(np.sqrt(np.mean((tv - nv) ** 2))), 5)
    eps = round(1.0 / max(st.session_state.noise, 1e-4), 2)
    st.markdown(
        f"""
        <div class="panel pl-c" style="margin-top:.6rem;">
          <div style="display:flex;gap:2.2rem;flex-wrap:wrap;">
            <div><div style="font-family:'Share Tech Mono',monospace;font-size:.58rem;letter-spacing:.16em;color:{P["text_dim"]};text-transform:uppercase;margin-bottom:.2rem;">Mechanism</div><div style="font-family:Orbitron,sans-serif;font-size:.73rem;color:{P["cyan2"]};">Laplace(0, Δf/ε)</div></div>
            <div><div style="font-family:'Share Tech Mono',monospace;font-size:.58rem;letter-spacing:.16em;color:{P["text_dim"]};text-transform:uppercase;margin-bottom:.2rem;">ε (budget)</div><div style="font-family:Orbitron,sans-serif;font-size:.73rem;color:{P["cyan2"]};">{eps}</div></div>
            <div><div style="font-family:'Share Tech Mono',monospace;font-size:.58rem;letter-spacing:.16em;color:{P["text_dim"]};text-transform:uppercase;margin-bottom:.2rem;">MAE</div><div style="font-family:Orbitron,sans-serif;font-size:.73rem;color:{P["amber2"]};">{mae} kW</div></div>
            <div><div style="font-family:'Share Tech Mono',monospace;font-size:.58rem;letter-spacing:.16em;color:{P["text_dim"]};text-transform:uppercase;margin-bottom:.2rem;">RMSE</div><div style="font-family:Orbitron,sans-serif;font-size:.73rem;color:{P["amber2"]};">{rmse} kW</div></div>
          </div>
        </div>""",
        unsafe_allow_html=True,
    )
    with st.expander("ℹ️  How Differential Privacy Protects Households"):
        st.markdown(f"""
**Laplace Mechanism** — `published = true_value + Laplace(0, Δf/ε)`

| Parameter | Meaning |
|---|---|
| ε (epsilon) | Privacy budget — smaller ε → stronger privacy |
| Δf (sensitivity) | Max change one record causes in the query |
| Noise scale | Δf / ε |

An adversary seeing only published values cannot reliably reconstruct any individual reading.
The histogram shows the empirical Laplace distribution for `{dp_m}`.
""")


def s_he(df):
    _sec("🔑", "ENCRYPTED AGGREGATION PIPELINE", "CKKS · HOMOMORPHIC ENCRYPTION")
    vals = df["noisy_energy_usage"].tolist()
    total = sum(vals)
    n = len(vals)
    agg_t = hashlib.sha256(f"{total:.6f}|mk".encode()).hexdigest().upper()
    samp = df["he_token"].iloc[0] if len(df) else "N/A"
    v0 = vals[0] if vals else 0.0
    st.markdown(
        f"""
        <div class="panel pl-p">
          <div class="flow">
            <div class="fb fb-b"><span class="fb-ico">📡</span><div class="fb-lbl">Raw Reading</div><div class="fb-val">{v0:.4f} kW</div></div>
            <div class="flow-arr">→</div>
            <div class="fb fb-c"><span class="fb-ico">🔐</span><div class="fb-lbl">DP Noised</div><div class="fb-val">{v0:.4f} kW</div></div>
            <div class="flow-arr">→</div>
            <div class="fb fb-p"><span class="fb-ico">🔑</span><div class="fb-lbl">CKKS Encrypt</div><div class="fb-val">Enc(·)</div></div>
            <div class="flow-arr">→</div>
            <div class="fb fb-p"><span class="fb-ico">∑</span><div class="fb-lbl">HE Aggregate</div><div class="fb-val">Enc(Σ)</div></div>
            <div class="flow-arr">→</div>
            <div class="fb fb-g"><span class="fb-ico">🔓</span><div class="fb-lbl">Decrypt Sum</div><div class="fb-val">{total:.4f} kW</div></div>
          </div>
          <div class="cipher">AGGREGATE CT: {agg_t[:32]}…{agg_t[-8:]}<br>SAMPLE CT:  {samp}</div>
        </div>""",
        unsafe_allow_html=True,
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔒 Encrypted Records", f"{n:,}")
    c2.metric("∑ HE Aggregate", f"{total:.3f} kW")
    c3.metric("🔓 Decrypted Total", f"{total:.3f} kW")
    c4.metric("🔑 Scheme", "CKKS (MockHE)")
    with st.expander("ℹ️  How HE Enables Private Aggregation"):
        st.markdown("""
**CKKS** — approximate arithmetic on real-valued ciphertexts:
```python
ct_i    = CKKS.encrypt(dp_noised_energy_i)        # each meter encrypts locally
CT_sum  = ct_0 ⊕ ct_1 ⊕ … ⊕ ct_n               # server adds — no decryption
total   = CKKS.decrypt(CT_sum)                    # only authorised analyst
```
**Individual readings are never decrypted.** Only the aggregate is revealed.
Install real backend: `pip install tenseal`
""")


def s_table(df):
    _sec("📋", "SMART METER ACTIVITY LOG", f"{min(len(df), 150)} READINGS")
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
        height=285,
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


def s_attack(df, sel_m):
    _sec("⚠️", "ADVERSARIAL ATTACK SIMULATION", "THREAT ANALYSIS")
    atk = st.selectbox("Target meter", sel_m, key="atk")
    mdf = df[df["meter_id"] == atk].sort_values("timestamp").tail(40)
    tm = mdf["energy_usage"].mean()
    nm = mdf["noisy_energy_usage"].mean()
    err = abs(tm - nm) / max(tm, 1e-6) * 100
    prot = "✅ PROTECTED" if err > 5 else "⚠️ MARGINAL"
    c1, c2 = st.columns([2, 1])
    with c1:
        st.plotly_chart(
            chart_attack(df, atk), width="stretch", config={"displayModeBar": False}
        )
    with c2:
        st.markdown(
            f"""
            <div class="panel pl-r" style="min-height:290px;">
              <div style="font-family:Orbitron,sans-serif;font-size:.6rem;letter-spacing:.18em;color:{P["crimson"]};text-transform:uppercase;margin-bottom:.75rem;">⚠ Reconstruction Analysis</div>
              <table class="atk-tbl">
                <tr><td>Target</td><td style="color:{P["text"]};">{atk}</td></tr>
                <tr><td>Method</td><td style="color:{P["text"]};">Rolling mean (k=6)</td></tr>
                <tr><td>True mean</td><td style="color:{P["amber2"]};">{tm:.4f} kW</td></tr>
                <tr><td>Att. est.</td><td style="color:{P["crimson"]};">{nm:.4f} kW</td></tr>
                <tr><td>Error</td><td style="color:{P["green2"]};">{err:.1f}%</td></tr>
                <tr><td>Status</td><td style="color:{P["green2"]};">{prot}</td></tr>
              </table>
              <p style="margin-top:.85rem;font-family:'Exo 2',sans-serif;font-size:.74rem;color:{P["text_dim"]};line-height:1.55;">Higher σ → larger estimation error → stronger DP protection.</p>
            </div>""",
            unsafe_allow_html=True,
        )


def s_advanced(df, sel_m):
    _sec("🔬", "ADVANCED ANALYTICS", "EXPANDED VIEW")
    with st.expander("📊  24-Hour Load Profile"):
        st.plotly_chart(
            chart_hourly(df), width="stretch", config={"displayModeBar": False}
        )
    with st.expander("🔌  Sub-Metering Appliance Breakdown"):
        sm = st.selectbox("Select meter", sel_m, key="sub_m")
        st.plotly_chart(
            chart_sub(df, sm), width="stretch", config={"displayModeBar": False}
        )
    with st.expander("🌡  Voltage Stability Heatmap"):
        st.plotly_chart(
            chart_voltage_heat(df), width="stretch", config={"displayModeBar": False}
        )


# ── Main ─────────────────────────────────────────────────────────

# This file contains only the additions — it will be merged into dashboard_base.py

import sys, os, time, secrets, hmac as _hmac, hashlib
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ── Security palette additions ─────────────────────────────────
SEC = dict(
    threat_critical="#ef4444",
    threat_high="#f97316",
    threat_medium="#f59e0b",
    threat_low="#22c55e",
    threat_none="#22c55e",
    panel_dark="#04080f",
    panel_border="#1a0a1e",
)


# ── Security CSS additions ─────────────────────────────────────
def _sec_css():
    return f"""
<style>
@keyframes threat-pulse-red{{0%,100%{{box-shadow:0 0 10px rgba(239,68,68,.6),0 0 30px rgba(239,68,68,.2);}}50%{{box-shadow:0 0 22px rgba(239,68,68,1),0 0 55px rgba(239,68,68,.35);}}}}
@keyframes threat-pulse-orange{{0%,100%{{box-shadow:0 0 8px rgba(249,115,22,.5);}}50%{{box-shadow:0 0 20px rgba(249,115,22,.9);}}}}
@keyframes scan-line{{0%{{top:-5%;}}100%{{top:105%;}}}}
.sec-panel{{background:linear-gradient(145deg,{SEC["panel_dark"]},#08050f);border:1px solid {SEC["panel_border"]};border-radius:14px;padding:1.3rem 1.5rem;position:relative;overflow:hidden;}}
.sec-panel::before{{content:'';position:absolute;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,rgba(239,68,68,.5),transparent);animation:scan-line 4s linear infinite;}}
.threat-critical{{border-top:2px solid #ef4444 !important;animation:threat-pulse-red 2s ease-in-out infinite;}}
.threat-high    {{border-top:2px solid #f97316 !important;animation:threat-pulse-orange 2s ease-in-out infinite;}}
.threat-medium  {{border-top:2px solid #f59e0b !important;}}
.threat-low     {{border-top:2px solid #22c55e !important;}}
.alert-row{{display:flex;align-items:flex-start;gap:.65rem;padding:.55rem .7rem;border-radius:7px;margin-bottom:.4rem;font-family:'Exo 2',sans-serif;font-size:.78rem;line-height:1.4;}}
.alert-critical{{background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.35);}}
.alert-warning {{background:rgba(249,115,22,.09);border:1px solid rgba(249,115,22,.3);}}
.alert-info    {{background:rgba(14,165,233,.07);border:1px solid rgba(14,165,233,.2);}}
.alert-ok      {{background:rgba(34,197,94,.07);border:1px solid rgba(34,197,94,.2);}}
.alert-icon{{font-size:1rem;line-height:1.4;flex-shrink:0;}}
.alert-body{{flex:1;}}
.alert-meta{{font-family:'Share Tech Mono',monospace;font-size:.6rem;letter-spacing:.08em;opacity:.6;margin-top:.15rem;}}
.threat-meter{{display:flex;align-items:center;gap:.7rem;padding:.7rem 1rem;border-radius:10px;background:rgba(0,0,0,.3);margin-bottom:.6rem;}}
.threat-bar-bg{{flex:1;height:8px;border-radius:4px;background:rgba(255,255,255,.08);overflow:hidden;}}
.threat-bar-fill{{height:100%;border-radius:4px;transition:width .5s ease;}}
.stat-pill{{display:inline-flex;align-items:center;gap:.35rem;background:rgba(0,0,0,.3);border-radius:6px;padding:.3rem .8rem;font-family:'Share Tech Mono',monospace;font-size:.65rem;letter-spacing:.08em;margin:.2rem;}}
.attack-card{{background:linear-gradient(135deg,#080510,#0a0618);border-radius:12px;padding:1rem 1.2rem;margin-bottom:.6rem;transition:transform .2s;}}
.attack-card:hover{{transform:translateY(-2px);}}
.attack-card.blocked{{border:1px solid rgba(34,197,94,.35);}}
.attack-card.warned {{border:1px solid rgba(249,115,22,.35);}}
.attack-card.failed {{border:1px solid rgba(239,68,68,.35);}}
</style>"""


# ── Lightweight standalone security state (no external imports) ──
class _SecurityState:
    """
    Self-contained security simulation state stored in session_state.
    No dependency on security_core.py — works standalone.
    """

    def __init__(self):
        self.events: List[Dict] = []
        self.attack_results: List[Dict] = []
        self.nonce_store: dict = {}
        self.rate_counts: dict = defaultdict(int)
        self.auth_failures: dict = defaultdict(int)
        self.anomaly_scores: dict = defaultdict(list)
        self.registered_meters: set = set()
        # Pre-register 20 meters
        for i in range(20):
            self.registered_meters.add(f"meter_{i:03d}")

    def add_event(self, severity, etype, meter_id, description, extra=None):
        now = time.time()
        self.events.append(
            {
                "timestamp": now,
                "time_str": time.strftime("%H:%M:%S", time.localtime(now)),
                "severity": severity,
                "event_type": etype,
                "meter_id": meter_id,
                "description": description,
                **(extra or {}),
            }
        )
        if len(self.events) > 500:
            self.events.pop(0)

    def threat_level(self):
        recent = self.events[-20:] if self.events else []
        crits = sum(1 for e in recent if e["severity"] == "CRITICAL")
        warns = sum(1 for e in recent if e["severity"] == "WARNING")
        if crits >= 3:
            return "CRITICAL", "#ef4444"
        if crits >= 1:
            return "HIGH", "#f97316"
        if warns >= 3:
            return "MEDIUM", "#f59e0b"
        return "LOW", "#22c55e"

    def counts(self):
        c = defaultdict(int)
        for e in self.events:
            c[e["event_type"]] += 1
        return dict(c)

    def simulate_attack(self, attack_type: str, meter_id: str):
        """Simulate a specific attack and log detection events."""
        import random, secrets as _sec

        if attack_type == "REPLAY":
            self.add_event(
                "CRITICAL",
                "REPLAY",
                meter_id,
                f"Replay detected: duplicate nonce from {meter_id} — packet REJECTED",
                {"detail": "Nonce already consumed within 30s window"},
            )

        elif attack_type == "TAMPER":
            fake_e = round(random.uniform(15, 22), 4)
            self.add_event(
                "CRITICAL",
                "TAMPER",
                meter_id,
                f"Data tampering: {meter_id} energy={fake_e}kW exceeds 12.5kW threshold — BLOCKED",
                {"forged_value": fake_e, "detail": "HMAC verification failed"},
            )

        elif attack_type == "MITM":
            self.add_event(
                "CRITICAL",
                "MITM",
                meter_id,
                f"MITM detected: {meter_id} signature mismatch (foreign key) — REJECTED",
                {"detail": "Server key ≠ attacker key"},
            )

        elif attack_type == "AUTH_FAIL":
            self.auth_failures[meter_id] += 1
            self.add_event(
                "WARNING",
                "AUTH_FAIL",
                meter_id,
                f"Authentication failure #{self.auth_failures[meter_id]} from {meter_id}",
                {"attempts": self.auth_failures[meter_id]},
            )

        elif attack_type == "ANOMALY":
            z = round(random.uniform(3.6, 6.5), 2)
            e = round(random.uniform(12, 20), 4)
            self.anomaly_scores[meter_id].append(z)
            self.add_event(
                "WARNING",
                "ANOMALY",
                meter_id,
                f"Anomalous reading from {meter_id}: {e} kW (z={z}σ)",
                {"z_score": z, "energy": e},
            )

        elif attack_type == "INFERENCE":
            self.add_event(
                "WARNING",
                "INFERENCE",
                meter_id,
                f"Inference attempt on {meter_id} — DP noise disrupted reconstruction (ε=1.0)",
                {"detail": "Error > 0.3 kW after DP noise"},
            )

        elif attack_type == "RATE_LIMIT":
            self.rate_counts[meter_id] += 1
            self.add_event(
                "WARNING",
                "RATE_LIMIT",
                meter_id,
                f"Rate limit exceeded: {meter_id} sent 62 requests/min (max=60) — THROTTLED",
                {"rpm": 62},
            )


def _init_security():
    if "sec" not in st.session_state:
        st.session_state.sec = _SecurityState()
        # Seed with a few initial OK events
        for i in range(5):
            mid = f"meter_{i:03d}"
            st.session_state.sec.add_event(
                "INFO", "OK", mid, f"Meter {mid} authenticated successfully"
            )
    return st.session_state.sec


# ── Security chart builders ────────────────────────────────────


def chart_attack_timeline(events: List[Dict]) -> "go.Figure":
    """Bar chart of security events by type over the last 50 events."""
    if not events:
        return go.Figure()
    import pandas as pd

    df = pd.DataFrame(events[-50:])
    color_map = {
        "CRITICAL": P["crimson"],
        "WARNING": P["amber"],
        "INFO": P["green"],
        "OK": P["green"],
    }
    df["color"] = df["severity"].map(lambda s: color_map.get(s, P["blue"]))
    fig = go.Figure()
    for sev, col in [
        ("CRITICAL", P["crimson"]),
        ("WARNING", P["amber"]),
        ("INFO", P["blue"]),
    ]:
        sub = df[df["severity"] == sev] if "severity" in df.columns else pd.DataFrame()
        if len(sub):
            fig.add_trace(
                go.Bar(
                    x=sub["time_str"],
                    y=[1] * len(sub),
                    name=sev,
                    marker_color=col,
                    opacity=0.85,
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>%{customdata[1]}<br>"
                        "%{x}<extra></extra>"
                    ),
                    customdata=list(
                        zip(sub["event_type"].tolist(), sub["description"].tolist())
                    ),
                )
            )
    fig.update_layout(
        **_L(
            200,
            title=dict(
                text="Security Event Timeline",
                font=dict(size=11, color=P["text"], family="Orbitron"),
                x=0,
            ),
            barmode="stack",
            showlegend=True,
            xaxis=dict(showticklabels=False, gridcolor="rgba(0,0,0,0)"),
            yaxis=dict(showticklabels=False, gridcolor="rgba(0,0,0,0)"),
            margin=dict(l=10, r=10, t=36, b=10),
        )
    )
    return fig


def chart_anomaly_scores(sec: "_SecurityState", sel_m: List[str]) -> "go.Figure":
    """Line chart of running anomaly Z-scores per meter."""
    fig = go.Figure()
    for i, mid in enumerate(sel_m[:8]):
        scores = sec.anomaly_scores.get(mid, [])
        if scores:
            fig.add_trace(
                go.Scatter(
                    y=scores,
                    mode="lines+markers",
                    name=mid,
                    line=dict(width=1.8, color=_PAL[i % len(_PAL)]),
                    marker=dict(size=5),
                    hovertemplate=f"<b>{mid}</b><br>z=%{{y:.2f}}σ<extra></extra>",
                )
            )
    # Draw threshold line
    if any(sec.anomaly_scores.get(m) for m in sel_m[:8]):
        max_len = max((len(v) for v in sec.anomaly_scores.values() if v), default=1)
        fig.add_trace(
            go.Scatter(
                x=list(range(max_len)),
                y=[3.5] * max_len,
                name="Threshold (3.5σ)",
                mode="lines",
                line=dict(color=P["crimson"], width=1.5, dash="dash"),
            )
        )
    fig.update_layout(
        **_L(
            260,
            title=dict(
                text="Anomaly Z-Score per Meter",
                font=dict(size=11, color=P["text"], family="Orbitron"),
                x=0,
            ),
            xaxis_title="Reading #",
            yaxis_title="Z-Score (σ)",
        )
    )
    return fig


def chart_auth_failures(sec: "_SecurityState") -> "go.Figure":
    """Bar chart of authentication failures per meter."""
    if not sec.auth_failures:
        return go.Figure()
    mids = list(sec.auth_failures.keys())
    counts = [sec.auth_failures[m] for m in mids]
    colors = [
        P["crimson"] if c >= 3 else P["amber"] if c >= 1 else P["green"] for c in counts
    ]
    fig = go.Figure(
        go.Bar(
            x=mids,
            y=counts,
            marker_color=colors,
            opacity=0.85,
            text=counts,
            textposition="outside",
            textfont=dict(size=10, color=P["text"], family="Share Tech Mono"),
            hovertemplate="<b>%{x}</b><br>Failures: %{y}<extra></extra>",
        )
    )
    fig.update_layout(
        **_L(
            240,
            title=dict(
                text="Auth Failures per Meter",
                font=dict(size=11, color=P["text"], family="Orbitron"),
                x=0,
            ),
            showlegend=False,
            bargap=0.3,
            xaxis=dict(tickfont=dict(size=8, family="Share Tech Mono")),
        )
    )
    return fig


def chart_attack_radar(counts: Dict[str, int]) -> "go.Figure":
    """Radar chart of attack type frequencies."""
    labels = [
        "REPLAY",
        "TAMPER",
        "MITM",
        "AUTH_FAIL",
        "ANOMALY",
        "INFERENCE",
        "RATE_LIMIT",
    ]
    values = [counts.get(l, 0) for l in labels]
    if sum(values) == 0:
        values = [0] * len(labels)
    fig = go.Figure(
        go.Scatterpolar(
            r=values + [values[0]],
            theta=labels + [labels[0]],
            fill="toself",
            fillcolor="rgba(239,68,68,.15)",
            line=dict(color=P["crimson"], width=2),
            mode="lines+markers",
            marker=dict(size=6, color=P["crimson"]),
            hovertemplate="<b>%{theta}</b><br>Count: %{r}<extra></extra>",
        )
    )
    fig.update_layout(
        height=280,
        paper_bgcolor="rgba(0,0,0,0)",
        polar=dict(
            bgcolor="rgba(6,13,26,.88)",
            radialaxis=dict(
                visible=True,
                gridcolor="rgba(239,68,68,.2)",
                tickfont=dict(size=8, color=P["text_mid"]),
                showline=False,
            ),
            angularaxis=dict(
                gridcolor="rgba(239,68,68,.2)",
                tickfont=dict(size=9, color=P["text_mid"], family="Share Tech Mono"),
            ),
        ),
        margin=dict(l=40, r=40, t=30, b=30),
        showlegend=False,
    )
    return fig


# ── Security section renderers ────────────────────────────────


def s_threat_banner(sec: "_SecurityState"):
    """Full-width threat level banner."""
    level, color = sec.threat_level()
    counts = sec.counts()
    total = sum(counts.values())
    crits = counts.get("TAMPER", 0) + counts.get("REPLAY", 0) + counts.get("MITM", 0)
    warns = (
        counts.get("AUTH_FAIL", 0)
        + counts.get("ANOMALY", 0)
        + counts.get("RATE_LIMIT", 0)
    )
    ok_cnt = counts.get("OK", 0) + counts.get("INFERENCE", 0)

    threat_class = f"threat-{level.lower()}"
    icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(
        level, "🟢"
    )

    bar_pct = {"CRITICAL": "92", "HIGH": "65", "MEDIUM": "38", "LOW": "12"}.get(
        level, "12"
    )
    bar_color = color

    st.markdown(
        f"""
    <div class="sec-panel {threat_class}" style="margin-bottom:1rem;">
      <div style="display:flex;align-items:center;gap:1.2rem;flex-wrap:wrap;">
        <div>
          <div style="font-family:'Share Tech Mono',monospace;font-size:.6rem;letter-spacing:.2em;color:{P["text_dim"]};text-transform:uppercase;margin-bottom:.25rem;">CURRENT THREAT LEVEL</div>
          <div style="font-family:'Orbitron',sans-serif;font-size:1.5rem;font-weight:700;color:{color};">{icon} {level}</div>
        </div>
        <div class="threat-meter" style="flex:1;min-width:180px;">
          <div class="threat-bar-bg"><div class="threat-bar-fill" style="width:{bar_pct}%;background:{bar_color};"></div></div>
          <span style="font-family:'Share Tech Mono',monospace;font-size:.68rem;color:{color};">{bar_pct}%</span>
        </div>
        <div style="display:flex;gap:.4rem;flex-wrap:wrap;">
          <span class="stat-pill" style="color:{P["crimson"]};">💀 CRITICAL: {crits}</span>
          <span class="stat-pill" style="color:{P["amber"]};">⚠ WARNINGS: {warns}</span>
          <span class="stat-pill" style="color:{P["green"]};">✓ ACCEPTED: {ok_cnt}</span>
          <span class="stat-pill" style="color:{P["blue2"]};">📊 TOTAL: {total}</span>
        </div>
      </div>
    </div>""",
        unsafe_allow_html=True,
    )


def s_live_alerts(sec: "_SecurityState"):
    """Scrolling live alert feed."""
    _sec2("🚨", "LIVE SECURITY ALERTS", "REAL-TIME FEED")
    events = list(reversed(sec.events[-15:]))
    if not events:
        st.markdown(
            f'<div style="color:{P["text_dim"]};font-size:.8rem;padding:.5rem;">No security events yet. Run an attack simulation to populate this feed.</div>',
            unsafe_allow_html=True,
        )
        return

    icon_map = {
        "CRITICAL": ("🔴", "alert-critical"),
        "WARNING": ("🟠", "alert-warning"),
        "INFO": ("🔵", "alert-info"),
    }
    for ev in events:
        sev = ev.get("severity", "INFO")
        ico, cls = icon_map.get(sev, ("⚪", "alert-info"))
        etype = ev.get("event_type", "")
        etype_colors = {
            "REPLAY": "#38bdf8",
            "TAMPER": "#ef4444",
            "MITM": "#ef4444",
            "AUTH_FAIL": "#f59e0b",
            "ANOMALY": "#f59e0b",
            "INFERENCE": "#a78bfa",
            "RATE_LIMIT": "#f97316",
            "OK": "#4ade80",
            "TRAFFIC_ANALYSIS": "#22d3ee",
        }
        etype_color = etype_colors.get(etype, P["text_mid"])
        st.markdown(
            f"""
        <div class="alert-row {cls}">
          <span class="alert-icon">{ico}</span>
          <div class="alert-body">
            <div><span style="font-family:'Share Tech Mono',monospace;font-size:.65rem;color:{etype_color};font-weight:600;margin-right:.5rem;">[{etype}]</span>
            <span style="color:{P["text"]};">{ev["description"]}</span></div>
            <div class="alert-meta">🕐 {ev["time_str"]} · meter: {ev["meter_id"]}</div>
          </div>
        </div>""",
            unsafe_allow_html=True,
        )


def s_attack_results(sec: "_SecurityState"):
    """Attack simulation results — run all 5 attacks and show outcomes."""
    _sec2("⚔️", "ATTACK SIMULATION RESULTS", "CYBER THREAT SCENARIOS")

    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from attack_simulator import run_all_attacks

        results = [r.to_dict() for r in run_all_attacks(gateway=None)]
        use_real = True
    except ImportError:
        use_real = False
        results = _builtin_attack_results()

    for r in results:
        success_u = r.get("succeeded_unprotected", True)
        detected = r.get("detected_protected", True)
        sev = r.get("severity", "HIGH")
        card_cls = "blocked" if detected else ("warned" if success_u else "failed")
        u_icon = "✅ Succeeded" if success_u else "❌ Failed"
        d_icon = "🛡 DETECTED & BLOCKED" if detected else "⚠️ Residual risk"
        sev_col = {
            "CRITICAL": P["crimson"],
            "HIGH": P["orange"],
            "MEDIUM": P["amber"],
            "LOW": P["green"],
        }.get(sev, P["amber"])

        st.markdown(
            f"""
        <div class="attack-card {card_cls}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:.5rem;margin-bottom:.5rem;">
            <div>
              <span style="font-family:'Orbitron',sans-serif;font-size:.7rem;color:{P["text"]};font-weight:600;">{r["attack_name"]}</span>
              <span style="font-family:'Share Tech Mono',monospace;font-size:.6rem;color:{sev_col};margin-left:.6rem;border:1px solid {sev_col};border-radius:3px;padding:1px 7px;">{sev}</span>
            </div>
            <span style="font-family:'Share Tech Mono',monospace;font-size:.62rem;color:{P["green2"] if detected else P["crimson"]};">{d_icon}</span>
          </div>
          <div style="font-size:.76rem;color:{P["text_dim"]};margin-bottom:.4rem;">{r["description"]}</div>
          <div style="display:flex;gap:1.2rem;flex-wrap:wrap;font-size:.72rem;">
            <div><span style="color:{P["text_dim"]};">Unprotected: </span><span style="color:{P["amber"] if success_u else P["green"]};">{u_icon}</span></div>
            <div><span style="color:{P["text_dim"]};">Defence: </span><span style="color:{P["cyan2"]};font-family:'Share Tech Mono',monospace;font-size:.65rem;">{r["detection_mechanism"]}</span></div>
          </div>
        </div>""",
            unsafe_allow_html=True,
        )


def _builtin_attack_results() -> List[Dict]:
    """Fallback results used if attack_simulator.py is not found."""
    return [
        {
            "attack_name": "Data Inference",
            "description": "Reconstruct individual usage from aggregate differences.",
            "succeeded_unprotected": True,
            "detected_protected": True,
            "detection_mechanism": "Differential Privacy (ε=1.0)",
            "severity": "HIGH",
            "attacker_result": "Error=0.003 kW (unprotected)",
            "defender_action": "DP noise → error=0.41 kW (inference disrupted)",
        },
        {
            "attack_name": "Replay Attack",
            "description": "Re-submit old valid packet to inflate aggregate.",
            "succeeded_unprotected": True,
            "detected_protected": True,
            "detection_mechanism": "Nonce uniqueness + timestamp window",
            "severity": "CRITICAL",
            "attacker_result": "Duplicate accepted (unprotected)",
            "defender_action": "Nonce already consumed — REJECTED",
        },
        {
            "attack_name": "Data Tampering",
            "description": "Inflate energy reading ×10 in transit.",
            "succeeded_unprotected": True,
            "detected_protected": True,
            "detection_mechanism": "HMAC-SHA-256 signature",
            "severity": "CRITICAL",
            "attacker_result": "Inflated value accepted (no sig check)",
            "defender_action": "HMAC mismatch → TAMPER DETECTED",
        },
        {
            "attack_name": "Man-in-the-Middle",
            "description": "Intercept, modify, re-sign with attacker key.",
            "succeeded_unprotected": True,
            "detected_protected": True,
            "detection_mechanism": "Server-side key derivation (no master secret)",
            "severity": "CRITICAL",
            "attacker_result": "Forged packet forwarded",
            "defender_action": "Attacker key ≠ server key → REJECTED",
        },
        {
            "attack_name": "Traffic Analysis",
            "description": "Infer occupancy from packet timing metadata.",
            "succeeded_unprotected": True,
            "detected_protected": False,
            "detection_mechanism": "⚠ Residual risk — requires traffic shaping",
            "severity": "MEDIUM",
            "attacker_result": "r=0.82 (strong correlation)",
            "defender_action": "DP/HE protect values, not metadata",
        },
    ]


def s_security_metrics(sec: "_SecurityState", df, sel_m: List[str]):
    """Charts: event timeline, anomaly scores, auth failures, attack radar."""
    counts = sec.counts()
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            chart_attack_timeline(sec.events),
            width="stretch",
            config={"displayModeBar": False},
        )
    with c2:
        st.plotly_chart(
            chart_attack_radar(counts),
            width="stretch",
            config={"displayModeBar": False},
        )

    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(
            chart_anomaly_scores(sec, sel_m),
            width="stretch",
            config={"displayModeBar": False},
        )
    with c4:
        st.plotly_chart(
            chart_auth_failures(sec), width="stretch", config={"displayModeBar": False}
        )


def s_suspicious_meters(sec: "_SecurityState", df):
    """Table of most suspicious meters based on event counts."""
    _sec2("🔍", "SUSPICIOUS METER DETECTION", "BEHAVIORAL ANALYSIS")
    meter_risk: Dict[str, Dict] = {}
    for ev in sec.events:
        mid = ev.get("meter_id", "unknown")
        if mid not in meter_risk:
            meter_risk[mid] = {"CRITICAL": 0, "WARNING": 0, "INFO": 0, "total": 0}
        meter_risk[mid][ev.get("severity", "INFO")] += 1
        meter_risk[mid]["total"] += 1

    if not meter_risk:
        st.markdown(
            f'<div style="color:{P["text_dim"]};font-size:.8rem;">No suspicious activity detected yet.</div>',
            unsafe_allow_html=True,
        )
        return

    rows = []
    for mid, counts in sorted(
        meter_risk.items(), key=lambda x: x[1]["CRITICAL"], reverse=True
    )[:10]:
        c_cnt = counts["CRITICAL"]
        w_cnt = counts["WARNING"]
        risk = (
            "🔴 CRITICAL"
            if c_cnt >= 2
            else (
                "🟠 HIGH" if c_cnt >= 1 else ("🟡 MEDIUM" if w_cnt >= 2 else "🟢 LOW")
            )
        )
        z_vals = sec.anomaly_scores.get(mid, [0])
        max_z = max(z_vals) if z_vals else 0.0
        rows.append(
            {
                "Meter": mid,
                "Risk": risk,
                "Critical": c_cnt,
                "Warnings": w_cnt,
                "Max Z-Score": round(max_z, 2),
                "Auth Fails": sec.auth_failures.get(mid, 0),
            }
        )

    import pandas as pd

    df_risk = pd.DataFrame(rows)
    st.dataframe(
        df_risk,
        use_container_width=True,
        height=260,
        hide_index=True,
        column_config={
            "Meter": st.column_config.TextColumn("Meter ID"),
            "Risk": st.column_config.TextColumn("Risk Level"),
            "Critical": st.column_config.NumberColumn("🔴 Critical"),
            "Warnings": st.column_config.NumberColumn("🟠 Warnings"),
            "Max Z-Score": st.column_config.NumberColumn("Max Z", format="%.2f"),
            "Auth Fails": st.column_config.NumberColumn("Auth Fails"),
        },
    )


def s_privacy_indicator(sec: "_SecurityState"):
    """Privacy risk indicator based on epsilon and attack history."""
    _sec2("🛡", "PRIVACY RISK INDICATOR", "DP + HE STATUS")
    noise = st.session_state.get("noise", 0.08)
    eps = round(1.0 / max(noise, 1e-4), 2)
    inf_cnt = sum(1 for e in sec.events if e.get("event_type") == "INFERENCE")

    # Privacy score: 0=worst, 100=best
    eps_score = max(0, min(100, int(100 - eps * 8)))
    inf_score = max(0, 100 - inf_cnt * 15)
    priv_score = int((eps_score + inf_score) / 2)
    priv_color = (
        P["green2"]
        if priv_score >= 70
        else (P["amber2"] if priv_score >= 40 else P["crimson2"])
    )
    priv_label = (
        "STRONG" if priv_score >= 70 else ("MODERATE" if priv_score >= 40 else "WEAK")
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔐 DP Privacy Score", f"{priv_score}/100", delta=priv_label)
    c2.metric("ε (Privacy Budget)", f"{eps}", delta="Lower = more private")
    c3.metric("🔑 HE Scheme", "CKKS", delta="Enc(a)⊕Enc(b)=Enc(a+b)")
    c4.metric(
        "🕵 Inference Attempts",
        str(inf_cnt),
        delta="All disrupted by DP" if inf_cnt else "None detected",
    )

    # Privacy explanation
    st.markdown(
        f"""
    <div class="sec-panel" style="margin-top:.6rem;">
      <div style="display:flex;gap:2rem;flex-wrap:wrap;align-items:center;">
        <div style="text-align:center;min-width:100px;">
          <div style="font-family:'Orbitron',sans-serif;font-size:2rem;font-weight:700;color:{priv_color};">{priv_score}</div>
          <div style="font-family:'Share Tech Mono',monospace;font-size:.6rem;color:{priv_color};letter-spacing:.15em;">{priv_label} PRIVACY</div>
        </div>
        <div style="flex:1;">
          <div style="font-size:.78rem;color:{P["text_dim"]};line-height:1.65;">
            <strong style="color:{P["cyan2"]};">Differential Privacy</strong> adds Laplace noise Lap(0, Δf/ε) to every published reading — 
            preventing inference attacks even against an unbounded adversary.<br>
            <strong style="color:{P["purple2"]};">Homomorphic Encryption</strong> (CKKS) ensures the aggregation server 
            computes <code>Enc(Σ eᵢ)</code> without ever decrypting individual readings.
          </div>
        </div>
        <div style="display:flex;flex-direction:column;gap:.3rem;">
          <span class="stat-pill" style="color:{P["green2"]};">✓ DP active (ε={eps})</span>
          <span class="stat-pill" style="color:{P["purple2"]};">✓ CKKS encrypted</span>
          <span class="stat-pill" style="color:{P["blue2"]};">✓ HMAC-SHA-256 signing</span>
          <span class="stat-pill" style="color:{P["amber2"]};">✓ Nonce replay guard</span>
        </div>
      </div>
    </div>""",
        unsafe_allow_html=True,
    )


def s_simulation_controls(sec: "_SecurityState", sel_m: List[str]):
    """Interactive attack simulation launcher."""
    _sec2("🎮", "ATTACK SIMULATION LAUNCHER", "RUN INDIVIDUAL ATTACKS")
    st.markdown(
        f'<p style="font-size:.8rem;color:{P["text_dim"]};margin-bottom:.8rem;">Click any button to simulate that attack. The security system will detect and log it in real time.</p>',
        unsafe_allow_html=True,
    )
    import random

    meter = random.choice(sel_m[:10]) if sel_m else "meter_000"

    cols = st.columns(4)
    with cols[0]:
        if st.button("🔁 Replay Attack", key="sim_replay", use_container_width=True):
            sec.simulate_attack("REPLAY", meter)
            st.toast("🔴 Replay attack simulated!", icon="🔁")
    with cols[1]:
        if st.button("✏️ Data Tampering", key="sim_tamper", use_container_width=True):
            sec.simulate_attack("TAMPER", meter)
            st.toast("🔴 Tampering attack simulated!", icon="✏️")
    with cols[2]:
        if st.button("🕵️ MITM Attack", key="sim_mitm", use_container_width=True):
            sec.simulate_attack("MITM", meter)
            st.toast("🔴 MITM attack simulated!", icon="🕵️")
    with cols[3]:
        if st.button("🔑 Auth Failure", key="sim_auth", use_container_width=True):
            sec.simulate_attack("AUTH_FAIL", meter)
            st.toast("🟠 Auth failure simulated!", icon="🔑")

    cols2 = st.columns(4)
    with cols2[0]:
        if st.button(
            "📈 Anomaly Injection", key="sim_anomaly", use_container_width=True
        ):
            sec.simulate_attack("ANOMALY", meter)
            st.toast("🟠 Anomaly injected!", icon="📈")
    with cols2[1]:
        if st.button("🔍 Inference Attack", key="sim_infer", use_container_width=True):
            sec.simulate_attack("INFERENCE", meter)
            st.toast("🟣 Inference attack simulated!", icon="🔍")
    with cols2[2]:
        if st.button("🌊 Rate Limit Flood", key="sim_rate", use_container_width=True):
            sec.simulate_attack("RATE_LIMIT", meter)
            st.toast("🟠 Rate limit triggered!", icon="🌊")
    with cols2[3]:
        if st.button("💣 Run All Attacks", key="sim_all", use_container_width=True):
            for t in [
                "REPLAY",
                "TAMPER",
                "MITM",
                "AUTH_FAIL",
                "ANOMALY",
                "INFERENCE",
                "RATE_LIMIT",
            ]:
                sec.simulate_attack(
                    t, random.choice(sel_m[:10]) if sel_m else "meter_000"
                )
            st.toast("🚨 Full attack barrage simulated!", icon="💣")

    # Auto-inject random events during live simulation
    if st.session_state.get("simulating", False):
        if random.random() < 0.3:
            random_types = ["ANOMALY", "AUTH_FAIL", "RATE_LIMIT", "REPLAY"]
            sec.simulate_attack(
                random.choice(random_types),
                random.choice(sel_m[:10]) if sel_m else "meter_000",
            )


def _sec2(icon, title, badge=""):
    """Section header variant for security panel."""
    b = f'<span class="sec-badge">{badge}</span>' if badge else ""
    st.markdown(
        f'<div class="sec-hdr">'
        f'<span class="sec-icon">{icon}</span>'
        f'<span class="sec-title" style="color:#ef4444;">{title}</span>{b}'
        f"</div>",
        unsafe_allow_html=True,
    )


# ── Enhanced main() — all original sections + security panel ──


def main():
    st.markdown(_css(), unsafe_allow_html=True)
    st.markdown(_sec_css(), unsafe_allow_html=True)
    _init_state()
    sec = _init_security()

    sel_m, sel_r, t_range, noise = render_sidebar(st.session_state.df)

    # ── Live simulation tick ──
    if st.session_state.simulating:
        st.session_state.df = append_tick(st.session_state.df, noise=noise)
        st.session_state.tick += 1
        if len(st.session_state.df) > 20 * 400:
            st.session_state.df = st.session_state.df.iloc[-20 * 300 :]
        # Log a few OK events during live run
        import random as _r

        if _r.random() < 0.4:
            mid = _r.choice(sel_m[:5]) if sel_m else "meter_000"
            sec.add_event(
                "INFO", "OK", mid, f"Reading accepted (z=0.{_r.randint(10, 90)})"
            )
        # Occasional random attack event
        s_simulation_controls(sec, sel_m)  # also fires random events
        s_simulation_controls.__doc__  # no-op to keep reference

    # ── Filter ──
    df_all = st.session_state.df
    mask = (
        df_all["meter_id"].isin(sel_m)
        & df_all["region"].isin(sel_r)
        & (df_all["timestamp"] >= pd.Timestamp(t_range[0]))
        & (df_all["timestamp"] <= pd.Timestamp(t_range[1]))
    )
    df = df_all[mask].copy()
    if df.empty:
        st.warning("⚠ No data matches current filters — adjust the sidebar.")
        return

    # ═══════════════════════════════════════════════════════════
    # ORIGINAL SECTIONS (unchanged)
    # ═══════════════════════════════════════════════════════════
    s_hero(st.session_state.simulating)
    st.markdown("---")
    s_kpis(df)
    st.markdown("---")
    s_timeseries(df, sel_m)
    st.markdown("---")
    s_regional(df)
    st.markdown("---")
    s_dp(df, sel_m)
    st.markdown("---")
    s_he(df)
    st.markdown("---")
    s_table(df)
    st.markdown("---")
    s_attack(df, sel_m)
    st.markdown("---")
    s_advanced(df, sel_m)

    # ═══════════════════════════════════════════════════════════
    # ▼ NEW: CYBER SECURITY MONITORING PANEL
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown(
        f"""
    <div style="background:linear-gradient(135deg,#0a0510,#10050a);border:1px solid #3a0f1a;
                border-top:2px solid #ef4444;border-radius:16px;padding:1.8rem 2.4rem;
                margin-bottom:1.2rem;position:relative;overflow:hidden;">
      <div style="position:absolute;top:0;left:0;right:0;height:1px;
                  background:linear-gradient(90deg,transparent,rgba(239,68,68,.7),transparent);"></div>
      <div style="font-family:'Share Tech Mono',monospace;font-size:.62rem;letter-spacing:.3em;
                  color:#ef4444;text-transform:uppercase;margin-bottom:.4rem;">
        ⚡ CYBER THREAT INTELLIGENCE
      </div>
      <div style="font-family:'Orbitron',sans-serif;font-size:1.55rem;font-weight:700;
                  color:{P["text"]};letter-spacing:.04em;">
        <span style="color:#ef4444;">Real-Time</span> Security Monitor
      </div>
      <div style="font-size:.85rem;color:{P["text_mid"]};margin:.4rem 0 .9rem;max-width:650px;">
        Live cyber-attack detection, HMAC integrity verification, replay prevention,
        differential privacy protection, and anomaly-based intrusion detection.
      </div>
    </div>""",
        unsafe_allow_html=True,
    )

    # Threat level banner
    s_threat_banner(sec)

    # Row 1: Live alerts + Attack simulation launcher
    c_left, c_right = st.columns([3, 2])
    with c_left:
        s_live_alerts(sec)
    with c_right:
        s_simulation_controls(sec, sel_m)

    st.markdown("---")

    # Row 2: Charts
    s_security_metrics(sec, df, sel_m)

    st.markdown("---")

    # Row 3: Suspicious meters + Privacy indicator
    s_suspicious_meters(sec, df)

    st.markdown("---")
    s_privacy_indicator(sec)

    st.markdown("---")

    # Attack simulation results
    s_attack_results(sec)

    # Footer
    st.markdown(
        f"""
    <hr>
    <div style="text-align:center;font-family:'Share Tech Mono',monospace;font-size:.6rem;
                color:{P["text_dim"]};padding:.5rem 0 2rem;letter-spacing:.07em;">
      ⚡ SECUREGRID RESEARCH DASHBOARD · Hybrid HE + DP · Real-Time Cyber Security Monitor · Streamlit + Plotly
    </div>""",
        unsafe_allow_html=True,
    )

    if st.session_state.simulating:
        time.sleep(DEFAULT_CONFIG.dashboard.refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()
