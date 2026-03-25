# Mini-Project---Astryx
# ⚡ SecureGrid — Hybrid Secure Data Aggregation for Smart Grids

> **Hybrid Secure Data Aggregation for Smart Grids using Homomorphic Encryption and Differential Privacy**
> University Research Prototype · Python · Streamlit · FastAPI

---

## Overview

SecureGrid demonstrates **privacy-preserving energy aggregation** for smart electricity grids. A fleet of virtual smart meters generates readings, each reading is protected with **Differential Privacy (Laplace mechanism)**, encrypted with **Homomorphic Encryption (CKKS)**, and aggregated at a server — without any individual reading ever being exposed.

---

## System Architecture

```
Smart Meter Fleet                Security Pipeline              Aggregation
──────────────────    ──────────────────────────────────    ───────────────
meter_000  ──┐
meter_001  ──┤  → DP Noise → CKKS Encrypt → CT_0 ──┐
   ...      ──┤                                      ├── HE Sum → Decrypt
meter_N-1 ──┘  → DP Noise → CKKS Encrypt → CT_N ──┘   (authorised only)
                                                         → Analytics → Dashboard
```

| Step | What happens | Privacy guarantee |
|------|-------------|-------------------|
| DP noise | `published = true + Lap(0, Δf/ε)` | Individual readings obfuscated |
| HE encrypt | `ct_i = CKKS.encrypt(noisy_eᵢ)` | Ciphertext sent to server |
| HE aggregate | `CT = ct_0 ⊕ ct_1 ⊕ … ⊕ ct_n` | Server never sees plaintext |
| Decrypt total | `CKKS.decrypt(CT) = Σ noisy_eᵢ` | Only aggregate revealed |

---

## Project Structure

```
smart_grid_project/
├── config.py                       ← All parameters in one place
├── main.py                         ← CLI pipeline runner
├── requirements.txt
├── README.md
│
├── simulator/
│   ├── __init__.py
│   └── smart_meter_simulator.py    ← Multi-meter data generator (generator-based)
│
├── privacy/
│   ├── __init__.py
│   └── dp_module.py                ← Laplace/Gaussian DP + budget ledger
│
├── encryption/
│   ├── __init__.py
│   └── he_module.py                ← CKKS HE (TenSEAL or MockCKKS fallback)
│
├── server/
│   ├── __init__.py
│   └── aggregation_server.py       ← FastAPI REST aggregation server
│
├── analytics/
│   ├── __init__.py
│   └── energy_analysis.py          ← pandas analytics engine (8 functions)
│
└── dashboard/
    ├── __init__.py
    └── dashboard.py                ← Streamlit interactive dashboard
```

---

## How to Run

### 1. Install

```bash
pip install -r requirements.txt
```

### 2. Full pipeline (console output)

```bash
python main.py                                      # 20 meters, 400 readings, ε=1.0
python main.py --meters 50 --epsilon 0.5            # tighter privacy
python main.py --meters 20 --samples 200 --server   # also start FastAPI
```

### 3. Dashboard

```bash
streamlit run dashboard/dashboard.py
```

Open http://localhost:8501 · Click **▶ START** in the sidebar to begin live simulation.

### 4. API server

```bash
uvicorn server.aggregation_server:app --reload
# Swagger UI: http://localhost:8000/docs
```

---

## Configuration

All parameters live in `config.py`. Change values there — every module picks them up:

```python
from config import AppConfig
cfg = AppConfig()
cfg.dp.epsilon = 0.1          # very private
cfg.simulator.num_meters = 100
```

---

## Security Design

### Differential Privacy
```
published_value = true_value + Laplace(0, sensitivity / ε)
```
- **ε** — privacy budget. Smaller ε → stronger protection, more distortion.
- Attacker observing published values cannot reliably reconstruct any individual reading.

### Homomorphic Encryption (CKKS)
```python
ct_i    = CKKS.encrypt(noisy_reading_i)          # at each meter
CT_sum  = ct_0 ⊕ ct_1 ⊕ … ⊕ ct_n               # server — no decryption
total   = CKKS.decrypt(CT_sum)                    # authorised analyst only
```
**Individual readings are never decrypted by the server.**

### Backends
| Backend | Status | Install |
|---------|--------|---------|
| TenSEAL CKKS | Real crypto | `pip install tenseal` |
| Pyfhel CKKS | Real crypto | `pip install Pyfhel` |
| MockCKKS | Correct algebra, no crypto | Built-in (no install needed) |
| diffprivlib | Validated DP | `pip install diffprivlib` |

---
