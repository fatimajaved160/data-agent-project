import sqlite3
import anthropic          # the official Anthropic Python SDK
import os
import sys
from datetime import datetime
from dotenv import load_dotenv  # reads our .env file and loads the values

# Load environment variables from .env so ANTHROPIC_API_KEY is available
load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB_PATH, CLAUDE_MODEL


# ── Load anomalies from DB ────────────────────────────────────────────────────

def load_anomalies(conn):
    """Read all anomalies from the last detection run."""
    cursor = conn.execute("""
        SELECT source, symbol, timestamp, score, details
        FROM anomalies
        ORDER BY score ASC  -- most anomalous (lowest score) first
    """)
    return cursor.fetchall()


# ── Format anomalies into readable text ──────────────────────────────────────

def format_anomalies(anomalies):
    """
    Turn raw DB rows into a structured text block we'll send to Claude.
    Claude needs context about what the numbers mean, not just raw values.
    """
    if not anomalies:
        return "No anomalies were detected in the latest run."

    lines = ["DETECTED ANOMALIES\n" + "=" * 40]

    for source, symbol, timestamp, score, details in anomalies:
        lines.append(
            f"[{source.upper()}] {symbol} on {timestamp}\n"
            f"  Values: {details}\n"
            f"  Anomaly score: {score:.4f} (more negative = more unusual)\n"
        )

    return "\n".join(lines)


# ── Call Claude API ───────────────────────────────────────────────────────────

def generate_report(anomaly_text):
    """
    Send the anomaly data to Claude and ask for an analysis.

    How the Claude API works:
    - We create a client object using our API key
    - We call client.messages.create() with:
        model:    which Claude model to use
        max_tokens: the maximum length of Claude's reply
        messages: a list of conversation turns, each with a "role" and "content"
                  role="user" is our message, role="assistant" would be Claude's reply
    - Claude returns a response object; the text is in response.content[0].text
    """
    # The API key is automatically picked up from the ANTHROPIC_API_KEY env variable
    client = anthropic.Anthropic()

    prompt = f"""You are a financial and weather data analyst reviewing automated anomaly detection results.

Below are anomalies detected across financial markets and weather data for London.
The system monitors:
- S&P 500 and Bitcoin (financial signals only)
- Crude oil, natural gas, wheat, corn, soybeans, and housing (combined weather + financial signals)
- London weather conditions independently

For each anomaly, provide:
1. A plain-English explanation of what the anomaly means
2. A possible real-world cause or explanation
3. A risk level: LOW / MEDIUM / HIGH
4. Whether it warrants immediate attention

Be concise but insightful. Assume the reader understands data science but not necessarily financial markets.

{anomaly_text}
"""

    # This is the actual API call — sending our prompt and getting Claude's response
    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1500,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    # Extract the text from Claude's response
    return response.content[0].text


# ── Save report to file ───────────────────────────────────────────────────────

def save_report(report_text, anomaly_text):
    """Save the full report to a timestamped file in the reports/ folder."""
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    os.makedirs(reports_dir, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(reports_dir, f"report_{timestamp}.txt")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"Data Agent Report — {timestamp} UTC\n")
        f.write("=" * 60 + "\n\n")
        f.write("RAW ANOMALY DATA\n")
        f.write("-" * 40 + "\n")
        f.write(anomaly_text + "\n\n")
        f.write("CLAUDE ANALYSIS\n")
        f.write("-" * 40 + "\n")
        f.write(report_text)

    return filename


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key or api_key == "your-api-key-here":
        print("ERROR: Please add your Anthropic API key to the .env file.")
        print("  Open .env and replace 'your-api-key-here' with your actual key.")
        return

    print("Loading anomalies from database...")
    conn = sqlite3.connect(DB_PATH)
    anomalies = load_anomalies(conn)
    conn.close()

    print(f"Found {len(anomalies)} anomalies to analyse.")
    anomaly_text = format_anomalies(anomalies)

    print("Sending to Claude for analysis...")
    report = generate_report(anomaly_text)

    print("\n" + "=" * 60)
    print("CLAUDE ANALYSIS REPORT")
    print("=" * 60)
    print(report)

    filepath = save_report(report, anomaly_text)
    print(f"\nReport saved to: {filepath}")


if __name__ == "__main__":
    main()
