"""
Villages Listing Tracker backend - FIXED FOR IFRAME VERSION
Loads Angular Homefinder directly from:
https://development.avengers.thevillages.com/homefinder/?hideHeader
"""

import os
import sqlite3
import json
import time
import logging
from datetime import datetime
from typing import List, Dict

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

from apscheduler.schedulers.background import BackgroundScheduler

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DB_PATH = os.environ.get("DB_PATH", "counts.db")

# ⛔ DO NOT USE thevillages.com wrapper
# ✅ USE THE RAW ANGULAR APP IN THE IFRAME
HOMEFINDER_URL = (
    "https://development.avengers.thevillages.com/homefinder/?hideHeader"
)

app = FastAPI(title="Villages Listing Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------- REGION DEFINITIONS -------------
REGION_DEFS = {
    "South of 44": [
        "Fenney", "DeLuna", "Marsh Bend", "Chitty Chatty", "Bradford",
        "Citrus Grove", "Hawkins", "Linden", "Monarch Grove", "St. Catherine",
        "St. Johns", "St. Lucy", "Lake Denham", "Dabney"
    ]
}

def classify_region(village: str) -> str:
    if not village:
        return "Unknown"
    v = village.lower()
    for region, villages in REGION_DEFS.items():
        for name in villages:
            if name.lower() in v:
                return region
    return "Unknown"


# -------- DB INITIALIZATION ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_counts(
            id INTEGER PRIMARY KEY,
            run_at TEXT,
            total_active INTEGER,
            total_pending INTEGER,
            payload_json TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# -------- SELENIUM SETUP ----------
def make_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-web-security")
    return webdriver.Chrome(options=chrome_options)


# -------- REAL SCRAPER (FIXED) ----------
def scrape_listings() -> List[Dict]:
    logger.info("Launching Selenium...")
    driver = make_driver()

    try:
        logger.info(f"Loading Homefinder IFRAME APP: {HOMEFINDER_URL}")
        driver.get(HOMEFINDER_URL)

        # Allow Angular to bootstrap
        time.sleep(5)

        # The list lives inside md-card.propertyCard
        logger.info("Waiting for listing cards...")
        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "md-card.propertyCard")
                )
            )
        except Exception:
            logger.error("ERROR: Cards never loaded! Dumping screenshot.")
            driver.save_screenshot("/tmp/hf_debug.png")
            return []

        time.sleep(1)

        # Infinite scroll
        last_count = -1
        stability = 0

        while True:
            cards = driver.find_elements(By.CSS_SELECTOR, "md-card.propertyCard")
            count = len(cards)

            if count == last_count:
                stability += 1
                if stability >= 3:
                    break
            else:
                stability = 0
                last_count = count

            driver.execute_script("window.scrollBy(0, 1100);")
            time.sleep(1)

        logger.info(f"Total cards found: {len(cards)}")

        # Extract data
        listings = []
        for c in cards:
            txt = c.text
            lower = txt.lower()

            status = "active"
            if "pending" in lower or "under contract" in lower:
                status = "pending"

            village = ""
            try:
                v = c.find_element(By.CSS_SELECTOR, ".prop_village")
                village = v.text.strip()
            except:
                # Try fallback: search for "Village of X" in text
                for line in txt.splitlines():
                    if "village" in line.lower():
                        village = line.strip()

            listings.append({
                "title": txt[:150],
                "status": status,
                "village": village,
                "region": classify_region(village)
            })

        logger.info(f"Scraped {len(listings)} listings.")
        return listings

    finally:
        driver.quit()


# -------- RUN COUNT ----------
def run_count():
    listings = scrape_listings()

    total_active = sum(1 for l in listings if l["status"] == "active")
    total_pending = sum(1 for l in listings if l["status"] == "pending")

    row = {
        "run_at": datetime.utcnow().isoformat(),
        "total_active": total_active,
        "total_pending": total_pending,
    }

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO daily_counts(run_at,total_active,total_pending,payload_json) VALUES (?,?,?,?)",
        (row["run_at"], total_active, total_pending, json.dumps(listings)),
    )
    conn.commit()
    conn.close()

    return row


# -------- API ENDPOINTS ----------
@app.get("/status")
def status():
    return {"status": "ok"}


def debug_run():
    try:
        run_count()
    except Exception as e:
        logger.error(str(e))


@app.post("/run")
def run(background_tasks: BackgroundTasks):
    background_tasks.add_task(debug_run)
    return {"status": "started"}


@app.get("/latest")
def latest():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM daily_counts ORDER BY id DESC LIMIT 1")
    row = c.fetchone()
    conn.close()

    if not row:
        return {}

    _id, run_at, active, pending, payload_json = row
    return {
        "run_at": run_at,
        "total_active": active,
        "total_pending": pending,
        "listings": json.loads(payload_json),
    }


@app.get("/debug-screenshot")
def screenshot():
    path = "/tmp/hf_debug.png"
    if not os.path.exists(path):
        return {"error": "No screenshot available"}
    return FileResponse(path, media_type="image/png")


# -------- SCHEDULE DAILY --------
scheduler = BackgroundScheduler()
scheduler.add_job(run_count, "cron", hour=6, minute=0)
scheduler.start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
