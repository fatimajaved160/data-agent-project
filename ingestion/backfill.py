import yfinance as yf
import requests
import sqlite3
import sys
import os
from datetime import datetime, timedelta

# Add the project root so we can import config.py
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB_PATH, FINANCIAL_SYMBOLS, WEATHER_SENSITIVE_SYMBOLS, WEATHER_PARAMS


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_connection():
    """Open a connection to the SQLite database and return it."""
    return sqlite3.connect(DB_PATH)


def row_exists(conn, table, where_clause, values):
    """Return True if a matching row already exists — prevents duplicates."""
    # This is a SELECT query: we ask the DB "does a row like this already exist?"
    # The ? placeholders are filled in safely by sqlite3 (prevents SQL injection)
    cursor = conn.execute(f"SELECT 1 FROM {table} WHERE {where_clause}", values)
    return cursor.fetchone() is not None


# ── Financial backfill ────────────────────────────────────────────────────────

def setup_financial_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS financial (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            symbol    TEXT,
            price     REAL,
            volume    REAL
        )
    """)
    conn.commit()


def backfill_financial(conn):
    """Fetch 90 days of daily prices for each symbol and save to DB."""
    print("\n── Financial backfill ──────────────────────────")

    all_symbols = FINANCIAL_SYMBOLS + WEATHER_SENSITIVE_SYMBOLS
    for symbol in all_symbols:
        print(f"  Fetching {symbol}...")

        ticker = yf.Ticker(symbol)
        # "3mo" = 3 months ≈ 90 days of daily data returned as a DataFrame
        hist = ticker.history(period="3mo", interval="1d")

        if hist.empty:
            print(f"    No data returned for {symbol}, skipping.")
            continue

        inserted = 0
        for date, row in hist.iterrows():
            # date is a pandas Timestamp — convert to a plain date string
            timestamp = str(date.date())

            # Skip if this symbol+date is already in the DB
            if row_exists(conn, "financial", "timestamp=? AND symbol=?", (timestamp, symbol)):
                continue

            conn.execute(
                "INSERT INTO financial (timestamp, symbol, price, volume) VALUES (?, ?, ?, ?)",
                (timestamp, symbol, row["Close"], row["Volume"])
            )
            inserted += 1

        conn.commit()
        print(f"    Inserted {inserted} new rows for {symbol}")


# ── Weather backfill ──────────────────────────────────────────────────────────

def setup_weather_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT,
            temp_c      REAL,
            windspeed   REAL,
            weathercode INTEGER
        )
    """)
    conn.commit()


def backfill_weather(conn):
    """Fetch 90 days of daily weather data from Open-Meteo archive API."""
    print("\n── Weather backfill ────────────────────────────")

    # Calculate the date range: 90 days ago up to yesterday
    # (the archive API doesn't include today — use the live API for that)
    end_date   = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%d")
    print(f"  Fetching {start_date} to {end_date}...")

    # Open-Meteo archive endpoint — different URL from the live forecast API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":           WEATHER_PARAMS["latitude"],
        "longitude":          WEATHER_PARAMS["longitude"],
        "start_date":         start_date,
        "end_date":           end_date,
        # Ask for daily summaries: max temp, max wind, and weather code
        "daily":              "temperature_2m_max,windspeed_10m_max,weathercode",
        "timezone":           "UTC"
    }

    response = requests.get(url, params=params)
    data = response.json()

    # The response has parallel lists: one date per index position
    # e.g. daily["time"][0] matches daily["temperature_2m_max"][0]
    daily = data["daily"]
    dates       = daily["time"]
    temps       = daily["temperature_2m_max"]
    windspeeds  = daily["windspeed_10m_max"]
    weathercodes= daily["weathercode"]

    inserted = 0
    for i in range(len(dates)):
        timestamp = dates[i]

        if row_exists(conn, "weather", "timestamp=?", (timestamp,)):
            continue

        conn.execute(
            "INSERT INTO weather (timestamp, temp_c, windspeed, weathercode) VALUES (?, ?, ?, ?)",
            (timestamp, temps[i], windspeeds[i], weathercodes[i])
        )
        inserted += 1

    conn.commit()
    print(f"  Inserted {inserted} new weather rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to database...")
    conn = get_connection()

    setup_financial_table(conn)
    setup_weather_table(conn)

    backfill_financial(conn)
    backfill_weather(conn)

    # Print final row counts so we can confirm the data is there
    fin_count = conn.execute("SELECT COUNT(*) FROM financial").fetchone()[0]
    wx_count  = conn.execute("SELECT COUNT(*) FROM weather").fetchone()[0]

    print(f"\nDone!")
    print(f"  financial table: {fin_count} rows")
    print(f"  weather table:   {wx_count} rows")

    conn.close()


if __name__ == "__main__":
    main()
