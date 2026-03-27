import streamlit as st
import tenseal as ts
import pandas as pd
import numpy as np
import sqlite3
import datetime
import hashlib
import time

# ─────────────────────────────────────────────────
# test change
# PAGE CONFIG
# ─────────────────────────────────────────────────
st.set_page_config(
    page_title="SecureGrid Platform",
    layout="wide",
    page_icon="🛡️",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────
st.markdown("""
<style>
    .stMetric > label { font-size: 0.85rem; color: #6b7280; }
    .role-badge {
        background: linear-gradient(90deg, #1e3a5f, #2563eb);
        color: white; border-radius: 8px; padding: 4px 12px;
        font-size: 0.8rem; font-weight: 600; display: inline-block;
    }
    .alert-success {
        background: #d1fae5; border-left: 4px solid #10b981;
        padding: 10px 16px; border-radius: 4px; color: #065f46;
    }
    .alert-error {
        background: #fee2e2; border-left: 4px solid #ef4444;
        padding: 10px 16px; border-radius: 4px; color: #991b1b;
    }
    div[data-testid="metric-container"] {
        background: #f8fafc; border: 1px solid #e2e8f0;
        border-radius: 8px; padding: 12px;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────
# USER CREDENTIALS (hashed passwords)
# In production, use a proper auth system
# ─────────────────────────────────────────────────
USERS = {
    "aggregator": {
        "password_hash": hashlib.sha256(b"agg_pass_123").hexdigest(),
        "role": "aggregator",
        "display": "Aggregator Node",
        "icon": "🏢",
    },
    "control": {
        "password_hash": hashlib.sha256(b"ctrl_pass_456").hexdigest(),
        "role": "control",
        "display": "Control Center",
        "icon": "🔐",
    },
}

# ─────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────
@st.cache_resource
def get_db():
    conn = sqlite3.connect("smartgrid.db", check_same_thread=False)
    cur = conn.cursor()

    # Create tables if they don't exist (original schema)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            meter_id        TEXT,
            encrypted_value BLOB,
            noisy_value     REAL,
            timestamp       TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_trail (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            event     TEXT,
            username  TEXT,
            role      TEXT,
            details   TEXT,
            timestamp TEXT
        )
    """)

    # ── Migration: add missing columns to existing databases ──
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(logs)")}
    migrations = {
        "actual_value": "ALTER TABLE logs ADD COLUMN actual_value REAL DEFAULT 0",
        "epsilon":      "ALTER TABLE logs ADD COLUMN epsilon REAL DEFAULT 1.0",
        "session_id":   "ALTER TABLE logs ADD COLUMN session_id TEXT DEFAULT 'legacy'",
    }
    for col, sql in migrations.items():
        if col not in existing_cols:
            cur.execute(sql)

    conn.commit()
    return conn

conn = get_db()

def log_audit(event: str, username: str, role: str, details: str = ""):
    conn.execute(
        "INSERT INTO audit_trail (event, username, role, details, timestamp) VALUES (?,?,?,?,?)",
        (event, username, role, details, datetime.datetime.now().isoformat())
    )
    conn.commit()

# ─────────────────────────────────────────────────
# ENCRYPTION CONTEXT
# ─────────────────────────────────────────────────
@st.cache_resource
def create_context():
    ctx = ts.context(
        ts.SCHEME_TYPE.CKKS,
        poly_modulus_degree=8192,
        coeff_mod_bit_sizes=[60, 40, 40, 60]
    )
    ctx.global_scale = 2**40
    ctx.generate_relin_keys()
    ctx.generate_galois_keys()
    return ctx

context = create_context()
secret_key = context.secret_key()

# ─────────────────────────────────────────────────
# SESSION STATE INIT
# ─────────────────────────────────────────────────
for key, default in {
    "role": None,
    "username": None,
    "login_attempts": 0,
    "last_session_id": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ─────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────
def login(username: str, password: str) -> bool:
    if st.session_state.login_attempts >= 5:
        st.error("Too many failed attempts. Please refresh the page.")
        return False
    user = USERS.get(username)
    if user and hashlib.sha256(password.encode()).hexdigest() == user["password_hash"]:
        st.session_state.role = user["role"]
        st.session_state.username = username
        st.session_state.login_attempts = 0
        log_audit("LOGIN", username, user["role"])
        return True
    st.session_state.login_attempts += 1
    return False

def logout():
    log_audit("LOGOUT", st.session_state.username or "unknown", st.session_state.role or "unknown")
    st.session_state.role = None
    st.session_state.username = None

def require_role(required_role: str):
    if st.session_state.role != required_role:
        st.markdown(
            f'<div class="alert-error">🚫 Access Denied — '
            f'This page requires the <strong>{required_role}</strong> role.</div>',
            unsafe_allow_html=True
        )
        st.stop()

def generate_session_id() -> str:
    return hashlib.md5(
        f"{datetime.datetime.now().isoformat()}{np.random.rand()}".encode()
    ).hexdigest()[:8].upper()

# ─────────────────────────────────────────────────
# LOGIN PAGE
# ─────────────────────────────────────────────────
if st.session_state.role is None:
    col_l, col_m, col_r = st.columns([1, 1.5, 1])
    with col_m:
        st.markdown("## 🛡️ SecureGrid Platform")
        st.markdown("Homomorphic Encryption + Differential Privacy for Smart Grid Data")
        st.divider()

        with st.form("login_form"):
            st.markdown("**Sign In**")
            username = st.selectbox("User", options=list(USERS.keys()),
                                    format_func=lambda u: f"{USERS[u]['icon']} {USERS[u]['display']}")
            password = st.text_input("Password", type="password",
                                     placeholder="Enter password…")
            submitted = st.form_submit_button("Login", use_container_width=True)

            if submitted:
                if not password:
                    st.warning("Please enter your password.")
                elif login(username, password):
                    st.success("Login successful! Redirecting…")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    remaining = max(0, 5 - st.session_state.login_attempts)
                    st.error(f"Invalid credentials. {remaining} attempts remaining.")

        st.caption("🔒 Demo passwords: aggregator → `agg_pass_123` | control → `ctrl_pass_456`")
    st.stop()

# ─────────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ─────────────────────────────────────────────────
user_info = USERS[st.session_state.username]

with st.sidebar:
    st.markdown(f"### 🛡️ SecureGrid")
    st.markdown(
        f'<span class="role-badge">{user_info["icon"]} {user_info["display"]}</span>',
        unsafe_allow_html=True
    )
    st.divider()

    # Only show pages relevant to the role
    all_pages = {
        "📊 Dashboard": "Dashboard",
        "🚀 Run Aggregation": "Run Aggregation",
        "🗂 Logs": "Logs",
        "📈 Privacy Analytics": "Privacy Analytics",
        "🔍 Audit Trail": "Audit Trail",
    }
    restricted = {
        "Run Aggregation": "aggregator",
        "Privacy Analytics": "control",
        "Audit Trail": "control",
    }

    available_pages = [
        label for label, name in all_pages.items()
        if name not in restricted or restricted[name] == st.session_state.role
    ]

    page_label = st.radio("Navigate", available_pages)
    page = all_pages[page_label]

    st.divider()
    if st.button("🚪 Logout", use_container_width=True):
        logout()
        st.rerun()

    with st.expander("ℹ️ System Info"):
        st.caption(f"Scheme: CKKS")
        st.caption(f"Poly Degree: 8192")
        st.caption(f"Scale: 2^40")
        st.caption(f"DB: smartgrid.db")

# ─────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────
if page == "Dashboard":
    st.title("📊 System Dashboard")

    # KPI row
    total_logs = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
    sessions = conn.execute("SELECT COUNT(DISTINCT session_id) FROM logs").fetchone()[0]
    avg_epsilon = conn.execute("SELECT AVG(epsilon) FROM logs").fetchone()[0]
    latest_ts = conn.execute("SELECT MAX(timestamp) FROM logs").fetchone()[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 Encrypted Records", total_logs)
    c2.metric("🔄 Aggregation Sessions", sessions)
    c3.metric("🔒 Avg Privacy (ε)", f"{avg_epsilon:.2f}" if avg_epsilon else "N/A")
    c4.metric("🕒 Last Activity", latest_ts[:16] if latest_ts else "None")

    st.divider()
    st.info("**SecureGrid** uses **CKKS Homomorphic Encryption** to allow arithmetic on encrypted meter readings, combined with **Laplace Differential Privacy** noise to prevent inference of individual values.")

    if total_logs > 0:
        st.subheader("Recent Activity")
        df = pd.read_sql_query(
            "SELECT meter_id, noisy_value, actual_value, epsilon, timestamp FROM logs ORDER BY id DESC LIMIT 20",
            conn
        )
        # Error (noise) column
        df["noise"] = df["noisy_value"] - df["actual_value"]
        st.dataframe(
            df.style.format({"noisy_value": "{:.3f}", "actual_value": "{:.3f}",
                             "noise": "{:+.3f}", "epsilon": "{:.2f}"}),
            use_container_width=True, hide_index=True
        )

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Noisy vs Actual Readings")
            chart_df = df[["actual_value", "noisy_value"]].rename(
                columns={"actual_value": "Actual (kWh)", "noisy_value": "Noisy (kWh)"}
            )
            st.line_chart(chart_df)
        with col_b:
            st.subheader("Noise Distribution")
            st.bar_chart(df["noise"].value_counts(bins=10, sort=False))

# ─────────────────────────────────────────────────
# RUN AGGREGATION
# ─────────────────────────────────────────────────
elif page == "Run Aggregation":
    require_role("aggregator")
    st.title("🚀 Secure Aggregation")

    with st.expander("⚙️ Privacy & Meter Configuration", expanded=True):
        col_l, col_r = st.columns(2)
        with col_l:
            epsilon = st.slider(
                "Privacy Budget (ε)",
                min_value=0.1, max_value=10.0, value=1.0, step=0.1,
                help="Lower ε = more privacy, more noise. Higher ε = less noise but weaker privacy."
            )
            sensitivity = st.number_input(
                "Query Sensitivity (Δf)", min_value=0.1, value=1.0, step=0.1,
                help="Maximum change a single meter reading can cause."
            )
        with col_r:
            num_meters = st.slider("Number of Smart Meters", 2, 10, 3)
            st.markdown(f"""
            **Current Noise Scale:** `{sensitivity/epsilon:.4f}` (Laplace scale = Δf/ε)  
            **Privacy Guarantee:** ε = `{epsilon}` differential privacy
            """)

    st.subheader("📟 Meter Readings")
    cols = st.columns(min(num_meters, 5))
    meter_values = []
    for i in range(num_meters):
        col = cols[i % 5]
        val = col.number_input(
            f"Meter {i+1:03d} (kWh)",
            min_value=0.0, max_value=10000.0,
            value=float(np.random.randint(30, 150)),
            step=0.5, key=f"m{i}"
        )
        meter_values.append(val)

    col1, col2 = st.columns([1, 3])
    run = col1.button("▶️ Execute Secure Aggregation", use_container_width=True)
    col2.button("🔀 Randomize Values", on_click=lambda: [st.session_state.pop(f"m{i}", None) for i in range(num_meters)])

    if run:
        session_id = generate_session_id()
        st.session_state.last_session_id = session_id

        with st.spinner("Encrypting and aggregating…"):
            progress = st.progress(0, text="Initializing…")
            encrypted_vectors = []
            noisy_values = []
            noise_scale = sensitivity / epsilon

            for i, value in enumerate(meter_values):
                noise = np.random.laplace(0, noise_scale)
                dp_value = value + noise
                noisy_values.append(dp_value)

                enc = ts.ckks_vector(context, [dp_value])
                encrypted_vectors.append(enc)

                conn.execute(
                    """INSERT INTO logs
                       (meter_id, encrypted_value, noisy_value, actual_value, epsilon, session_id, timestamp)
                       VALUES (?,?,?,?,?,?,?)""",
                    (f"M-{i+1:03d}", enc.serialize(), dp_value, value,
                     epsilon, session_id, datetime.datetime.now().isoformat())
                )
                progress.progress((i + 1) / num_meters, text=f"Encrypting Meter {i+1}/{num_meters}…")

            conn.commit()

            # Homomorphic sum
            encrypted_sum = encrypted_vectors[0].copy()
            for enc in encrypted_vectors[1:]:
                encrypted_sum += enc

            decrypted_sum = encrypted_sum.decrypt(secret_key)[0]
            actual_sum = sum(meter_values)
            noise_total = sum(noisy_values) - actual_sum
            progress.empty()

        log_audit("AGGREGATION", st.session_state.username, st.session_state.role,
                  f"session={session_id}, meters={num_meters}, ε={epsilon}")

        st.markdown(f'<div class="alert-success">✅ Aggregation complete — Session ID: <strong>{session_id}</strong></div>',
                    unsafe_allow_html=True)
        st.divider()

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🔓 Decrypted Sum", f"{decrypted_sum:.2f} kWh")
        m2.metric("📊 Actual Sum", f"{actual_sum:.2f} kWh")
        m3.metric("〰️ Total Noise", f"{noise_total:+.2f} kWh")
        m4.metric("📉 Relative Error", f"{abs((decrypted_sum - actual_sum) / actual_sum * 100):.4f}%")

        st.subheader("Per-Meter Breakdown")
        result_df = pd.DataFrame({
            "Meter": [f"M-{i+1:03d}" for i in range(num_meters)],
            "Actual (kWh)": meter_values,
            "Noisy (kWh)": noisy_values,
            "Added Noise": [n - v for n, v in zip(noisy_values, meter_values)],
        })
        st.dataframe(
            result_df.style.format({
                "Actual (kWh)": "{:.2f}", "Noisy (kWh)": "{:.2f}", "Added Noise": "{:+.4f}"
            }).bar(subset=["Added Noise"], color=["#fca5a5", "#86efac"], align="zero"),
            use_container_width=True, hide_index=True
        )

        col_ch1, col_ch2 = st.columns(2)
        with col_ch1:
            st.caption("Actual vs Noisy Readings")
            st.bar_chart(result_df.set_index("Meter")[["Actual (kWh)", "Noisy (kWh)"]])
        with col_ch2:
            st.caption("Noise per Meter")
            st.bar_chart(result_df.set_index("Meter")["Added Noise"])

# ─────────────────────────────────────────────────
# LOGS PAGE
# ─────────────────────────────────────────────────
elif page == "Logs":
    st.title("🗂 Encrypted Logs")

    df = pd.read_sql_query(
        "SELECT id, meter_id, noisy_value, actual_value, epsilon, session_id, timestamp FROM logs ORDER BY id DESC",
        conn
    )

    if df.empty:
        st.warning("No records found. Run an aggregation first.")
    else:
        # Filter controls
        col_a, col_b, col_c = st.columns(3)
        sessions = ["All"] + sorted(df["session_id"].dropna().unique().tolist(), reverse=True)
        sel_session = col_a.selectbox("Filter by Session", sessions)
        sel_meter = col_b.selectbox("Filter by Meter", ["All"] + sorted(df["meter_id"].unique().tolist()))

        if sel_session != "All":
            df = df[df["session_id"] == sel_session]
        if sel_meter != "All":
            df = df[df["meter_id"] == sel_meter]

        col_c.metric("Showing Records", len(df))

        df["noise"] = df["noisy_value"] - df["actual_value"]
        st.dataframe(
            df.style.format({
                "noisy_value": "{:.3f}", "actual_value": "{:.3f}",
                "noise": "{:+.4f}", "epsilon": "{:.2f}"
            }),
            use_container_width=True, hide_index=True
        )

        # Download
        csv = df.to_csv(index=False).encode()
        st.download_button("⬇️ Export CSV", csv, "securegrid_logs.csv", "text/csv")

# ─────────────────────────────────────────────────
# PRIVACY ANALYTICS
# ─────────────────────────────────────────────────
elif page == "Privacy Analytics":
    require_role("control")
    st.title("📈 Privacy Analytics")

    df = pd.read_sql_query(
        "SELECT noisy_value, actual_value, epsilon, meter_id, session_id, timestamp FROM logs",
        conn
    )

    if df.empty:
        st.warning("No data available. Ask the Aggregator to run some sessions first.")
    else:
        df["noise"] = df["noisy_value"] - df["actual_value"]
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Summary stats
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Mean Noise", f"{df['noise'].mean():.4f}")
        c2.metric("Noise Std Dev", f"{df['noise'].std():.4f}")
        c3.metric("Max |Noise|", f"{df['noise'].abs().max():.4f}")
        c4.metric("Unique ε Values", df["epsilon"].nunique())

        st.divider()
        tab1, tab2, tab3 = st.tabs(["Noise Analysis", "Privacy Budget", "Session Comparison"])

        with tab1:
            st.subheader("Noise Distribution (Histogram)")
            bins = pd.cut(df["noise"], bins=20)
            hist = df.groupby(bins, observed=True)["noise"].count()
            st.bar_chart(hist)

            st.subheader("Noise vs Actual Value")
            scatter_df = df[["actual_value", "noise"]].rename(
                columns={"actual_value": "Actual Reading (kWh)", "noise": "Applied Noise"}
            )
            st.scatter_chart(scatter_df, x="Actual Reading (kWh)", y="Applied Noise")

        with tab2:
            st.subheader("Privacy Budget (ε) Usage Over Time")
            eps_df = df.groupby("session_id")["epsilon"].first().reset_index()
            eps_df.columns = ["Session", "ε"]
            st.bar_chart(eps_df.set_index("Session"))

            st.subheader("Noise Scale by ε")
            st.markdown("Laplace scale = sensitivity / ε. Lower ε → larger noise → stronger privacy.")
            epsilons = np.linspace(0.1, 5.0, 100)
            theory_df = pd.DataFrame({
                "ε": epsilons,
                "Noise Scale (sensitivity=1)": 1 / epsilons
            }).set_index("ε")
            st.line_chart(theory_df)

        with tab3:
            st.subheader("Per-Session Aggregates")
            sess_df = df.groupby("session_id").agg(
                meters=("meter_id", "count"),
                total_actual=("actual_value", "sum"),
                total_noisy=("noisy_value", "sum"),
                epsilon=("epsilon", "first"),
                mean_noise=("noise", "mean"),
                std_noise=("noise", "std"),
            ).reset_index()
            sess_df["total_error"] = sess_df["total_noisy"] - sess_df["total_actual"]
            st.dataframe(
                sess_df.style.format({
                    "total_actual": "{:.2f}", "total_noisy": "{:.2f}",
                    "epsilon": "{:.2f}", "mean_noise": "{:+.4f}",
                    "std_noise": "{:.4f}", "total_error": "{:+.4f}"
                }),
                use_container_width=True, hide_index=True
            )

# ─────────────────────────────────────────────────
# AUDIT TRAIL
# ─────────────────────────────────────────────────
elif page == "Audit Trail":
    require_role("control")
    st.title("🔍 Audit Trail")
    st.caption("Immutable log of all system events")

    audit_df = pd.read_sql_query(
        "SELECT event, username, role, details, timestamp FROM audit_trail ORDER BY id DESC",
        conn
    )

    if audit_df.empty:
        st.info("No audit events recorded yet.")
    else:
        event_types = ["All"] + sorted(audit_df["event"].unique().tolist())
        sel_event = st.selectbox("Filter by Event", event_types)
        if sel_event != "All":
            audit_df = audit_df[audit_df["event"] == sel_event]

        def color_event(val):
            colors = {"LOGIN": "#d1fae5", "LOGOUT": "#fef3c7", "AGGREGATION": "#dbeafe"}
            return f"background-color: {colors.get(val, '#f3f4f6')}"

        st.dataframe(
            audit_df.style.applymap(color_event, subset=["event"]),
            use_container_width=True, hide_index=True
        )

        csv = audit_df.to_csv(index=False).encode()
        st.download_button("⬇️ Export Audit CSV", csv, "securegrid_audit.csv", "text/csv")
