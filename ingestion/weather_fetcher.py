import requests   # for making HTTP requests to the API
import sqlite3     # built into Python — lets us work with SQLite databases
import sys
import os

# Add the project root to the path so we can import config.py
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB_PATH, WEATHER_API_URL, WEATHER_PARAMS


def create_table(conn):
    """Create the weather table if it doesn't already exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id        INTEGER PRIMARY KEY AUTOINCREMENT, -- unique row ID, auto-assigned
            timestamp TEXT,                              -- when the reading was taken
            temp_c    REAL,                              -- temperature in Celsius
            windspeed REAL,                              -- wind speed in km/h
            weathercode INTEGER                          -- WMO code (e.g. 0 = clear sky)
        )
    """)
    conn.commit()  # save the change to disk


def fetch_weather():
    """Call the Open-Meteo API and return the current weather data."""
    # requests.get() sends an HTTP GET request — like typing a URL in a browser
    # params= automatically adds ?latitude=...&longitude=... to the URL
    response = requests.get(WEATHER_API_URL, params=WEATHER_PARAMS)

    # .json() converts the response text into a Python dictionary
    data = response.json()

    # Pull out just the "current_weather" section
    return data["current_weather"]


def save_weather(conn, weather):
    """Insert a weather reading into the database."""
    conn.execute("""
        INSERT INTO weather (timestamp, temp_c, windspeed, weathercode)
        VALUES (?, ?, ?, ?)
    """, (
        weather["time"],           # timestamp from the API
        weather["temperature"],    # temperature value
        weather["windspeed"],      # wind speed value
        weather["weathercode"]     # weather condition code
    ))
    conn.commit()  # save the insert to disk


def main():
    print("Connecting to database...")
    # sqlite3.connect() opens (or creates) the database file at DB_PATH
    conn = sqlite3.connect(DB_PATH)

    print("Setting up database table...")
    create_table(conn)

    print("Fetching weather data from Open-Meteo API...")
    weather = fetch_weather()

    print(f"\nWeather in London right now:")
    print(f"  Time:        {weather['time']}")
    print(f"  Temperature: {weather['temperature']} C")
    print(f"  Wind Speed:  {weather['windspeed']} km/h")
    print(f"  Weather Code:{weather['weathercode']}")

    print("\nSaving to database...")
    save_weather(conn, weather)

    print(f"Done! Data saved to {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    # This block only runs when you execute this file directly
    # It won't run if another file imports this one
    main()
