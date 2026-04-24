import streamlit as st      # turns this script into a web app
import pandas as pd
import plotly.graph_objects as go  # interactive charts
import sqlite3
import os
import sys
import glob
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB_PATH, FINANCIAL_SYMBOLS, WEATHER_SENSITIVE_SYMBOLS

ALL_SYMBOLS = FINANCIAL_SYMBOLS + WEATHER_SENSITIVE_SYMBOLS

# ── Page config ───────────────────────────────────────────────────────────────
# This must be the first Streamlit call in the script
st.set_page_config(
    page_title="Data Agent Dashboard",
    page_icon="📊",
    layout="wide"
)

# ── Data loading ──────────────────────────────────────────────────────────────
# st.cache_data tells Streamlit: "cache the result of this function"
# Without it, every tiny interaction (clicking a tab, etc.) would re-query the DB
# ttl=300 means the cache expires after 5 minutes, forcing a fresh DB read

@st.cache_data(ttl=300)
def load_financial_data():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM financial ORDER BY timestamp", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_weather_data():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM weather ORDER BY timestamp", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_anomalies():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM anomalies ORDER BY score ASC", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

def load_latest_report():
    """Read the most recently generated Claude report from the reports/ folder."""
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    files = glob.glob(os.path.join(reports_dir, "report_*.txt"))
    if not files:
        return None, None
    latest = max(files, key=os.path.getmtime)
    with open(latest, encoding="utf-8") as f:
        return f.read(), os.path.basename(latest)


# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("Data Agent")
st.sidebar.markdown("Autonomous anomaly detection + AI analysis")
st.sidebar.divider()

if st.sidebar.button("Run Full Pipeline", type="primary", use_container_width=True):
    # Running the pipeline from inside Streamlit using subprocess
    # subprocess lets Python call other Python scripts as if you typed them in the terminal
    import subprocess

    project_root = os.path.dirname(os.path.dirname(__file__))

    with st.sidebar:
        with st.spinner("Running backfill..."):
            subprocess.run([sys.executable, os.path.join(project_root, "ingestion", "backfill.py")])
        with st.spinner("Running anomaly detection..."):
            subprocess.run([sys.executable, os.path.join(project_root, "detection", "anomaly_detector.py")])
        with st.spinner("Generating Claude report..."):
            subprocess.run([sys.executable, os.path.join(project_root, "alerts", "report_generator.py")])

    st.sidebar.success("Pipeline complete!")
    # Clear the cache so the dashboard reloads fresh data
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()
st.sidebar.caption(f"DB: {DB_PATH}")


# ── Main content ──────────────────────────────────────────────────────────────

st.title("Data Agent Dashboard")
st.caption("Real-time financial & weather anomaly detection")

# Load all data upfront
fin_df      = load_financial_data()
weather_df  = load_weather_data()
anomalies_df = load_anomalies()

# ── Overview metrics ──────────────────────────────────────────────────────────

st.subheader("Overview")

col1, col2, col3, col4 = st.columns(4)

total     = len(anomalies_df)
financial = len(anomalies_df[anomalies_df["source"] == "financial"]) if "source" in anomalies_df.columns else 0
combined  = len(anomalies_df[anomalies_df["source"] == "combined"])  if "source" in anomalies_df.columns else 0
weather   = len(anomalies_df[anomalies_df["source"] == "weather"])   if "source" in anomalies_df.columns else 0

# st.metric() renders a nice big number card with a label
col1.metric("Total Anomalies",     total)
col2.metric("Financial Only",      financial)
col3.metric("Weather + Financial", combined)
col4.metric("Weather Only",        weather)

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
# st.tabs() creates clickable tab headers — each "with" block is one tab's content
tab1, tab2, tab3, tab4 = st.tabs(["Price Charts", "Weather", "Anomalies", "Claude Report"])


# ── Tab 1: Price Charts ───────────────────────────────────────────────────────
with tab1:
    st.subheader("Price History with Anomalies Highlighted")

    selected = st.selectbox("Select symbol", ALL_SYMBOLS)

    if fin_df.empty or "symbol" not in fin_df.columns:
        st.warning("No data yet. Click 'Run Full Pipeline' in the sidebar first.")
    else:
        sym_df = fin_df[fin_df["symbol"] == selected].copy()
        sym_anomalies = anomalies_df[anomalies_df["symbol"] == selected] if "symbol" in anomalies_df.columns else pd.DataFrame()

        if sym_df.empty:
            st.warning(f"No data for {selected}. Run the pipeline first.")
        else:
            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=sym_df["timestamp"],
                y=sym_df["price"],
                mode="lines",
                name="Price",
                line=dict(color="#4C9BE8", width=2)
            ))

            if not sym_anomalies.empty:
                anom_dates = sym_anomalies["timestamp"].values
                anom_prices = sym_df[sym_df["timestamp"].isin(anom_dates)]["price"]
                anom_ts = sym_df[sym_df["timestamp"].isin(anom_dates)]["timestamp"]

                fig.add_trace(go.Scatter(
                    x=anom_ts,
                    y=anom_prices,
                    mode="markers",
                    name="Anomaly",
                    marker=dict(color="red", size=10, symbol="circle")
                ))

            fig.update_layout(
                title=f"{selected} — Last 90 Days",
                xaxis_title="Date",
                yaxis_title="Price (USD)",
                hovermode="x unified",
                height=450
            )

            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(sym_df)} trading days  |  {len(sym_anomalies)} anomalies flagged")


# ── Tab 2: Weather ────────────────────────────────────────────────────────────
with tab2:
    st.subheader("London Weather — Last 90 Days")

    if weather_df.empty:
        st.warning("No weather data. Run the pipeline first.")
    else:
        wx_anomalies = anomalies_df[anomalies_df["symbol"] == "London"] if "symbol" in anomalies_df.columns else pd.DataFrame()

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=weather_df["timestamp"],
            y=weather_df["temp_c"],
            mode="lines",
            name="Max Temp (°C)",
            line=dict(color="#F4845F", width=2)
        ))

        if not wx_anomalies.empty:
            anom_dates = wx_anomalies["timestamp"].values
            anom_wx = weather_df[weather_df["timestamp"].isin(anom_dates)]
            fig2.add_trace(go.Scatter(
                x=anom_wx["timestamp"],
                y=anom_wx["temp_c"],
                mode="markers",
                name="Weather Anomaly",
                marker=dict(color="red", size=10)
            ))

        fig2.update_layout(
            title="London Daily Max Temperature",
            xaxis_title="Date",
            yaxis_title="Temperature (°C)",
            height=350
        )
        st.plotly_chart(fig2, use_container_width=True)

        col_a, col_b = st.columns(2)
        col_a.metric("Avg Temp",      f"{weather_df['temp_c'].mean():.1f} °C")
        col_b.metric("Avg Windspeed", f"{weather_df['windspeed'].mean():.1f} km/h")


# ── Tab 3: Anomalies table ────────────────────────────────────────────────────
with tab3:
    st.subheader("All Detected Anomalies")

    if anomalies_df.empty:
        st.info("No anomalies detected yet. Run the pipeline from the sidebar.")
    else:
        # Format score to 4 decimal places for readability
        display_df = anomalies_df[["source", "symbol", "timestamp", "score", "details"]].copy()
        display_df["score"] = display_df["score"].round(4)
        display_df.columns = ["Source", "Symbol", "Date", "Score", "Details"]

        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.caption("Score: more negative = more anomalous")


# ── Tab 4: Claude Report ──────────────────────────────────────────────────────
with tab4:
    st.subheader("Latest Claude Analysis Report")

    report_text, report_name = load_latest_report()

    if report_text is None:
        st.info("No report found. Click 'Run Full Pipeline' in the sidebar to generate one.")
    else:
        st.caption(f"Report: {report_name}")
        st.markdown(report_text)
