import sqlite3
import glob
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# FastAPI is the web framework — it handles incoming HTTP requests
# JSONResponse lets us return Python dicts as proper JSON responses
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

load_dotenv()

# Add project root so we can import our own modules
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DB_PATH

# Import our pipeline functions — reusing what we already built
from ingestion.backfill import backfill_financial, backfill_weather, setup_financial_table, setup_weather_table, get_connection as bf_conn
from detection.anomaly_detector import (
    create_anomalies_table, detect_financial_anomalies,
    detect_combined_anomalies, detect_weather_anomalies, get_connection as ad_conn
)
from alerts.report_generator import load_anomalies, format_anomalies, generate_report, save_report


# ── Create the FastAPI app ────────────────────────────────────────────────────
# This one line creates the entire web application object
# title and description appear in the auto-generated docs at /docs
app = FastAPI(
    title="Data Agent API",
    description="Autonomous financial and weather anomaly detection agent",
    version="1.0.0"
)


# ── Endpoints ─────────────────────────────────────────────────────────────────
# Each function below is an endpoint.
# The decorator (@app.get or @app.post) above each function tells FastAPI:
#   - what HTTP method to use (GET = fetch data, POST = trigger action)
#   - what URL path maps to this function

@app.get("/")
def health_check():
    """Health check — confirms the API is running."""
    return {"status": "ok", "message": "Data Agent API is running"}


@app.post("/run-pipeline")
def run_pipeline():
    """
    Run the full pipeline: backfill → anomaly detection → Claude report.
    This is a POST endpoint because it triggers an action that changes data.
    """
    try:
        # Step 1: Backfill
        conn = bf_conn()
        setup_financial_table(conn)
        setup_weather_table(conn)
        backfill_financial(conn)
        backfill_weather(conn)
        conn.close()

        # Step 2: Anomaly detection
        conn = ad_conn()
        create_anomalies_table(conn)
        conn.execute("DELETE FROM anomalies")
        conn.commit()
        detect_financial_anomalies(conn)
        detect_combined_anomalies(conn)
        detect_weather_anomalies(conn)
        conn.close()

        # Step 3: Claude report
        conn = sqlite3.connect(DB_PATH)
        anomalies = load_anomalies(conn)
        conn.close()
        anomaly_text = format_anomalies(anomalies)
        report = generate_report(anomaly_text)
        filepath = save_report(report, anomaly_text)

        return {
            "status": "success",
            "anomalies_found": len(anomalies),
            "report_saved_to": filepath,
            "ran_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        # HTTPException sends a proper error response with a status code
        # 500 = "Internal Server Error" — something went wrong on our side
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomalies")
def get_anomalies():
    """Return all anomalies from the latest detection run as JSON."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT source, symbol, timestamp, score, details FROM anomalies ORDER BY score ASC"
        )
        rows = cursor.fetchall()
        conn.close()

        # Convert each row (a tuple) into a dictionary so FastAPI can serialise it as JSON
        return [
            {
                "source":    row[0],
                "symbol":    row[1],
                "timestamp": row[2],
                "score":     row[3],
                "details":   row[4]
            }
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/report", response_class=PlainTextResponse)
def get_report():
    """Return the most recent Claude report as plain text."""
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")
    files = glob.glob(os.path.join(reports_dir, "report_*.txt"))

    if not files:
        raise HTTPException(status_code=404, detail="No reports found. Run /run-pipeline first.")

    latest = max(files, key=os.path.getmtime)
    with open(latest, encoding="utf-8") as f:
        return f.read()


@app.get("/status")
def get_status():
    """Return row counts from the DB — useful to check how fresh the data is."""
    try:
        conn = sqlite3.connect(DB_PATH)

        fin_count  = conn.execute("SELECT COUNT(*) FROM financial").fetchone()[0]
        wx_count   = conn.execute("SELECT COUNT(*) FROM weather").fetchone()[0]
        anom_count = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]

        # Get the most recent date in the financial table
        latest_fin = conn.execute(
            "SELECT MAX(timestamp) FROM financial"
        ).fetchone()[0]

        conn.close()

        return {
            "financial_rows": fin_count,
            "weather_rows":   wx_count,
            "anomaly_rows":   anom_count,
            "latest_data":    latest_fin,
            "checked_at":     datetime.utcnow().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
