import os

# Path to the SQLite database file (will be created automatically on first run)
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "agent.db")

# Isolation Forest contamination — the fraction of data we expect to be anomalies
# 0.05 means "flag the most unusual 5% of readings"
ANOMALY_CONTAMINATION = 0.05

# Symbols with no meaningful weather correlation — detected independently
FINANCIAL_SYMBOLS = ["^GSPC", "BTC-USD"]

# Symbols that are genuinely sensitive to weather conditions
# These will be detected using a combined model: price + volume + weather
WEATHER_SENSITIVE_SYMBOLS = [
    "CL=F",  # Crude oil
    "NG=F",  # Natural gas
    "ZW=F",  # Wheat
    "ZC=F",  # Corn
    "ZS=F",  # Soybeans
    "XHB",   # Homebuilders ETF (housing proxy)
]

# Claude model to use for report generation
CLAUDE_MODEL = "claude-haiku-4-5-20251001"  # fast and cheap — good for automated reports

# Open-Meteo API: free weather API, no key needed
# Fetching data for London (latitude/longitude coordinates)
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_PARAMS = {
    "latitude": 51.5,       # London latitude
    "longitude": -0.12,     # London longitude
    "current_weather": True # Ask for current conditions only
}
