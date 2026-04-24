import yfinance as yf  # third-party library that wraps Yahoo Finance data
import sqlite3          # built-in Python library for SQLite databases
import sys
import os
from datetime import datetime

# Add the project root to the path so we can import config.py
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB_PATH, FINANCIAL_SYMBOLS


def create_table(conn):
    """Create the financial table if it doesn't already exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS financial (
            id        INTEGER PRIMARY KEY AUTOINCREMENT, -- unique row ID
            timestamp TEXT,                              -- when we fetched this
            symbol    TEXT,                              -- ticker e.g. "GC=F"
            price     REAL,                              -- latest price in USD
            volume    REAL                               -- number of shares/units traded
        )
    """)
    conn.commit()


def fetch_prices(symbols):
    """Fetch the latest price and volume for each symbol from Yahoo Finance."""
    results = []

    for symbol in symbols:
        # yf.Ticker() creates an object representing one financial instrument
        ticker = yf.Ticker(symbol)

        # .history() fetches recent price data
        # "5d" = last 5 days, "1d" interval = one row per day
        # This always works even outside market hours (1m data only exists when markets are open)
        hist = ticker.history(period="5d", interval="1d")

        if hist.empty:
            print(f"  Warning: no data returned for {symbol}, skipping.")
            continue

        # .iloc[-1] gets the last row — the most recent price bar
        latest = hist.iloc[-1]

        results.append({
            "timestamp": datetime.utcnow().isoformat(),  # current UTC time as a string
            "symbol":    symbol,
            "price":     latest["Close"],   # closing price of that 1-minute bar
            "volume":    latest["Volume"]   # volume traded in that bar
        })

    return results


def save_prices(conn, prices):
    """Insert all fetched prices into the database."""
    for row in prices:
        conn.execute("""
            INSERT INTO financial (timestamp, symbol, price, volume)
            VALUES (?, ?, ?, ?)
        """, (row["timestamp"], row["symbol"], row["price"], row["volume"]))
    conn.commit()


def main():
    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    print("Setting up financial table...")
    create_table(conn)

    print(f"Fetching prices for: {', '.join(FINANCIAL_SYMBOLS)}\n")
    prices = fetch_prices(FINANCIAL_SYMBOLS)

    print("Latest prices:")
    for row in prices:
        print(f"  {row['symbol']:<10} ${row['price']:>10.2f}   volume: {row['volume']:.0f}")

    print("\nSaving to database...")
    save_prices(conn, prices)

    print(f"Done! {len(prices)} records saved to {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    main()
