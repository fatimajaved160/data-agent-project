import sqlite3
import pandas as pd                              # for loading DB data into DataFrames
import numpy as np
from sklearn.ensemble import IsolationForest     # our anomaly detection model
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB_PATH, FINANCIAL_SYMBOLS, WEATHER_SENSITIVE_SYMBOLS, ANOMALY_CONTAMINATION


# ── Database setup ────────────────────────────────────────────────────────────

def get_connection():
    return sqlite3.connect(DB_PATH)


def create_anomalies_table(conn):
    """Create the anomalies table to store every flagged reading."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            detected_at TEXT,  -- timestamp of when we ran this detection
            source      TEXT,  -- "financial" or "weather"
            symbol      TEXT,  -- ticker symbol or "London" for weather
            timestamp   TEXT,  -- date of the anomalous reading
            score       REAL,  -- Isolation Forest score: more negative = more anomalous
            details     TEXT   -- human-readable summary of the anomalous values
        )
    """)
    conn.commit()


def save_anomaly(conn, source, symbol, timestamp, score, details):
    """Insert one detected anomaly into the anomalies table."""
    conn.execute("""
        INSERT INTO anomalies (detected_at, source, symbol, timestamp, score, details)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (datetime.utcnow().isoformat(), source, symbol, timestamp, score, details))


# ── Core detection logic ──────────────────────────────────────────────────────

def run_isolation_forest(df, feature_cols):
    """
    Fit an Isolation Forest on the given DataFrame columns and return scores + labels.

    Isolation Forest works like this:
    - It builds many random decision trees, each splitting the data at random
    - Points that are easy to isolate (take few splits to separate) = anomalies
    - Points that blend in with the crowd (take many splits) = normal
    - score_samples() returns a negative score: closer to 0 = normal, more negative = anomalous
    - predict() returns -1 for anomaly, 1 for normal

    contamination= tells the model what fraction of the data to treat as anomalous.
    """
    model = IsolationForest(contamination=ANOMALY_CONTAMINATION, random_state=42)

    features = df[feature_cols].values   # convert DataFrame columns to a numpy array

    model.fit(features)                  # train: learn what "normal" looks like
    labels = model.predict(features)     # -1 = anomaly, 1 = normal
    scores = model.score_samples(features)  # raw anomaly score per row

    return labels, scores


# ── Financial anomaly detection ───────────────────────────────────────────────

def detect_financial_anomalies(conn):
    """Run anomaly detection on each financial symbol separately."""
    print("\n── Financial anomaly detection ─────────────────")
    total_flagged = 0

    for symbol in FINANCIAL_SYMBOLS:
        # pd.read_sql loads a SQL query result directly into a pandas DataFrame
        # This is one of the most useful tools for data scientists working with databases
        df = pd.read_sql(
            "SELECT timestamp, price, volume FROM financial WHERE symbol=? ORDER BY timestamp",
            conn,
            params=(symbol,)
        )

        if len(df) < 10:
            print(f"  {symbol}: not enough data, skipping.")
            continue

        # Normalise volume — it can be billions for S&P500 but tiny for BTC
        # Without this, volume would dominate the distance calculation
        df["volume_norm"] = (df["volume"] - df["volume"].mean()) / (df["volume"].std() + 1e-9)
        df["price_norm"]  = (df["price"]  - df["price"].mean())  / (df["price"].std()  + 1e-9)

        labels, scores = run_isolation_forest(df, ["price_norm", "volume_norm"])

        # Find the rows the model flagged as anomalies (label == -1)
        anomaly_idx = np.where(labels == -1)[0]
        flagged = len(anomaly_idx)
        total_flagged += flagged

        print(f"  {symbol}: {len(df)} days analysed, {flagged} anomalies flagged")

        for idx in anomaly_idx:
            row = df.iloc[idx]
            details = f"price={row['price']:.2f}, volume={row['volume']:.0f}"
            save_anomaly(conn, "financial", symbol, row["timestamp"], float(scores[idx]), details)
            print(f"    ⚠  {row['timestamp']}  {details}  score={scores[idx]:.4f}")

    conn.commit()
    return total_flagged


# ── Weather anomaly detection ─────────────────────────────────────────────────

def detect_weather_anomalies(conn):
    """Run anomaly detection on weather data."""
    print("\n── Weather anomaly detection ───────────────────")

    df = pd.read_sql(
        "SELECT timestamp, temp_c, windspeed FROM weather ORDER BY timestamp",
        conn
    )

    if len(df) < 10:
        print("  Not enough weather data, skipping.")
        return 0

    labels, scores = run_isolation_forest(df, ["temp_c", "windspeed"])

    anomaly_idx = np.where(labels == -1)[0]
    flagged = len(anomaly_idx)

    print(f"  London weather: {len(df)} days analysed, {flagged} anomalies flagged")

    for idx in anomaly_idx:
        row = df.iloc[idx]
        details = f"temp={row['temp_c']:.1f}C, windspeed={row['windspeed']:.1f}km/h"
        save_anomaly(conn, "weather", "London", row["timestamp"], float(scores[idx]), details)
        print(f"    ⚠  {row['timestamp']}  {details}  score={scores[idx]:.4f}")

    conn.commit()
    return flagged


# ── Combined weather + financial detection ────────────────────────────────────

def detect_combined_anomalies(conn):
    """
    For weather-sensitive symbols, join financial data with weather data on the
    same date and run one Isolation Forest across all 4 features together.

    Joining two tables on a shared column is called a JOIN — same concept as
    pd.merge() which you likely know from pandas. Here we merge on 'timestamp'
    so each row has that day's price, volume, temperature AND windspeed together.
    The model then flags days where the COMBINATION is unusual, not just one stream.
    """
    print("\n── Combined weather + financial detection ───────")
    total_flagged = 0

    # Load all weather data once — we'll merge it with each symbol's data
    weather_df = pd.read_sql(
        "SELECT timestamp, temp_c, windspeed FROM weather ORDER BY timestamp",
        conn
    )

    if len(weather_df) < 10:
        print("  Not enough weather data for combined detection, skipping.")
        return 0

    for symbol in WEATHER_SENSITIVE_SYMBOLS:
        fin_df = pd.read_sql(
            "SELECT timestamp, price, volume FROM financial WHERE symbol=? ORDER BY timestamp",
            conn,
            params=(symbol,)
        )

        if len(fin_df) < 10:
            print(f"  {symbol}: not enough data, skipping.")
            continue

        # pd.merge() joins two DataFrames on a shared column — here 'timestamp'
        # how="inner" means only keep dates that exist in BOTH tables
        merged = pd.merge(fin_df, weather_df, on="timestamp", how="inner")

        if len(merged) < 10:
            print(f"  {symbol}: not enough overlapping dates with weather data, skipping.")
            continue

        # Normalise all 4 features to the same scale before feeding to the model
        for col in ["price", "volume", "temp_c", "windspeed"]:
            merged[f"{col}_norm"] = (
                (merged[col] - merged[col].mean()) / (merged[col].std() + 1e-9)
            )

        features = ["price_norm", "volume_norm", "temp_c_norm", "windspeed_norm"]
        labels, scores = run_isolation_forest(merged, features)

        anomaly_idx = np.where(labels == -1)[0]
        flagged = len(anomaly_idx)
        total_flagged += flagged

        print(f"  {symbol}: {len(merged)} days analysed, {flagged} combined anomalies flagged")

        for idx in anomaly_idx:
            row = merged.iloc[idx]
            details = (
                f"price={row['price']:.2f}, volume={row['volume']:.0f}, "
                f"temp={row['temp_c']:.1f}C, wind={row['windspeed']:.1f}km/h"
            )
            save_anomaly(conn, "combined", symbol, row["timestamp"], float(scores[idx]), details)
            print(f"    ⚠  {row['timestamp']}  {details}  score={scores[idx]:.4f}")

    conn.commit()
    return total_flagged


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to database...")
    conn = get_connection()

    print("Setting up anomalies table...")
    create_anomalies_table(conn)

    # Clear previous anomaly runs so we don't accumulate duplicates
    conn.execute("DELETE FROM anomalies")
    conn.commit()

    fin_flagged      = detect_financial_anomalies(conn)
    combined_flagged = detect_combined_anomalies(conn)
    wx_flagged       = detect_weather_anomalies(conn)

    total = fin_flagged + combined_flagged + wx_flagged
    print(f"\n── Summary ─────────────────────────────────────")
    print(f"  Total anomalies detected: {total}")
    print(f"  Saved to anomalies table in {DB_PATH}")

    conn.close()


if __name__ == "__main__":
    main()
