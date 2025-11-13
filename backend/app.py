"""
Villages Listing Tracker backend

Run locally:
    uvicorn app:app --reload --host 0.0.0.0 --port 8000
"""

import os
import sqlite3
import json
import time
import logging
from datetime import datetime
from typing import List, Dict

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DB_PATH = os.environ.get("DB_PATH", "counts.db")
HOMEFINDER_URL = "https://www.thevillages.com/homefinder/#/homes"

app = FastAPI(title="Villages Listing Tracker")

# CORS so frontend on another domain can call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # you can tighten this to your exact frontend origin later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------- Regions & village grouping ---------

REGION_DEFS = {
    "North of 466": [
        "Orange Blossom Gardens", "Silver Lake", "Spanish Springs", "Santo Domingo",
        "Rio Grande", "La Reynalda", "La Zamora", "La Crescenta", "Chula Vista",
        "El Cortez", "El Santiago", "El Cortez"
    ],
    "Between 466 & 466A": [
        "Belvedere", "Ashland", "Amelia", "Bonnybrook", "Liberty Park", "Hadley",
        "Hemingway", "Duval", "Caroline", "Mallory Square", "Sabastian",
        "Sunset Pointe", "Virginia Trace", "Lake Sumter Landing"
    ],
    "South of 466A": [
        "St. Charles", "St. James", "Tamarind Grove", "Buttonwood", "St. James",
        "Sanibel", "Hillsborough", "Collier", "Pinellas", "Charlotte"
    ],
    "South of 44": [
        "Fenney", "DeLuna", "Marsh Bend", "Chitty Chatty", "Bradford", "Citrus Grove",
        "Hawkins", "Linden", "Monarch Grove", "St. Catherine", "St. Johns",
        "St. Lucy", "Lake Denham", "Dabney"
    ],
    "New Southern / Future": [
        "Eastport", "Newell", "Lake Denham East", "Future Development"
    ]
}


def classify_region(village: str) -> str:
    if not village:
        return "Unknown"
    v = village.strip().lower()
    for region, villages in REGION_DEFS.items():
        for name in villages:
            if name.lower() in v:
                return region
    # simple keyword fallbacks
    if "denham" in v:
        return "South of 44"
    if "dabney" in v or "eastport" in v or "newell" in v:
        return "New Southern / Future"
    return "Unknown"


# --------- DB helpers ---------


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_counts (
            id INTEGER PRIMARY KEY,
            run_at TEXT,
            total_active INTEGER,
            total_pending INTEGER,
            payload_json TEXT
        )
        """
    )
    conn.commit()
    conn.close()


init_db()

# --------- Selenium helpers & scraping ---------
def make_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def scrape_listings() -> List[Dict]:
    logger.info("Launching Selenium WebDriver...")
    driver = make_driver()

    try:
        logger.info("Loading Homefinder list view…")
        driver.get("https://www.thevillages.com/homefinder/#/homes?view=list")

        # Give Angular router time to bootstrap
        time.sleep(5)

        # Wait for ANY property cards to appear
        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "md-card.propertyCard, md-card._md")
                )
            )
            logger.info("Listing cards detected.")
        except:
            logger.error("ERROR: No listing cards ever appeared.")
            return []

        time.sleep(2)

        # Scroll until no new cards appear
        cards_seen = 0
        attempts = 0

        while True:
            cards = driver.find_elements(
                By.CSS_SELECTOR, "md-card.propertyCard, md-card._md"
            )

            if len(cards) == cards_seen:
                attempts += 1
                if attempts >= 4:
                    break
            else:
                attempts = 0
                cards_seen = len(cards)

            driver.execute_script("window.scrollBy(0, 1200);")
            time.sleep(1.2)

        logger.info(f"Total md-card elements detected: {cards_seen}")

        results = []

        for c in cards:
            try:
                full_text = c.text
                lower = full_text.lower()

                # Listing status
                status = "active"
                if "pending" in lower or "under contract" in lower:
                    status = "pending"

                # New home vs preowned
                list_type = "preowned"
                if "new home" in lower or "model" in lower:
                    list_type = "new"

                # Village extraction
                village = ""
                try:
                    village_el = c.find_element(By.CSS_SELECTOR, ".prop_village")
                    village = village_el.text.strip()
                except:
                    # Fallback heuristic
                    for line in full_text.splitlines():
                        if "village" in line.lower():
                            village = line.strip()
                            break

                region = classify_region(village)

                results.append({
                    "title": full_text[:150],
                    "status": status,
                    "type": list_type,
                    "village": village,
                    "region": region
                })

            except Exception as e:
                logger.error(f"Error parsing card: {e}")
                continue

        logger.info(f"Scraped {len(results)} listings total.")
        return results

    finally:
        try:
            driver.quit()
        except:
            pass

def run_count() -> Dict:
    listings = scrape_listings()

    total_active = sum(1 for r in listings if r.get("status", "").lower() == "active")
    total_pending = sum(1 for r in listings if r.get("status", "").lower() == "pending")

    # group by region and then village (alphabetical inside region)
    grouped: Dict[str, Dict[str, Dict[str, int]]] = {}
    for r in listings:
        region = r.get("region") or classify_region(r.get("village", ""))
        village = r.get("village") or "Unknown"
        status = r.get("status", "Active").lower()

        region_dict = grouped.setdefault(region, {})
        village_dict = region_dict.setdefault(
            village, {"active": 0, "pending": 0, "total": 0}
        )

        if status == "active":
            village_dict["active"] += 1
        elif status == "pending":
            village_dict["pending"] += 1
        village_dict["total"] += 1

    # sort villages inside each region alphabetically
    grouped_sorted: Dict[str, Dict[str, Dict[str, int]]] = {}
    for region, villages in grouped.items():
        grouped_sorted[region] = dict(sorted(villages.items(), key=lambda kv: kv[0]))

    row = {
        "run_at": datetime.utcnow().isoformat(),
        "total_active": total_active,
        "total_pending": total_pending,
        "grouped": grouped_sorted,
    }

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO daily_counts(run_at, total_active, total_pending, payload_json) "
        "VALUES (?, ?, ?, ?)",
        (
            row["run_at"],
            row["total_active"],
            row["total_pending"],
            json.dumps(listings),
        ),
    )
    conn.commit()
    conn.close()

    return row


# --------- API endpoints ---------


@app.get("/status")
def status():
    return {"status": "ok"}


def debug_run_count():
    logger.info("Background task started.")
    try:
        result = run_count()
        logger.info(
            "Background task completed successfully. Result summary: "
            f"{result.get('total_active')} active, "
            f"{result.get('total_pending')} pending."
        )
    except Exception as e:
        logger.error(f"Error during run_count(): {str(e)}", exc_info=True)


@app.post("/run")
def trigger_run(background_tasks: BackgroundTasks):
    logger.info("RUN endpoint received request — starting background task.")
    background_tasks.add_task(debug_run_count)
    return JSONResponse({"status": "started"})


@app.get("/latest")
def latest():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM daily_counts ORDER BY id DESC LIMIT 1")
    row = c.fetchone()
    conn.close()
    if not row:
        return {}
    _id, run_at, total_active, total_pending, payload_json = row

    listings = json.loads(payload_json)
    grouped: Dict[str, Dict[str, Dict[str, int]]] = {}
    for r in listings:
        region = r.get("region") or classify_region(r.get("village", ""))
        village = r.get("village") or "Unknown"
        status = r.get("status", "Active").lower()

        region_dict = grouped.setdefault(region, {})
        village_dict = region_dict.setdefault(
            village, {"active": 0, "pending": 0, "total": 0}
        )

        if status == "active":
            village_dict["active"] += 1
        elif status == "pending":
            village_dict["pending"] += 1
        village_dict["total"] += 1

    grouped_sorted: Dict[str, Dict[str, Dict[str, int]]] = {}
    for region, villages in grouped.items():
        grouped_sorted[region] = dict(sorted(villages.items(), key=lambda kv: kv[0]))

    return {
        "run_at": run_at,
        "total_active": total_active,
        "total_pending": total_pending,
        "grouped": grouped_sorted,
    }


@app.get("/history")
def history(days: int = 30):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT run_at, total_active, total_pending "
        "FROM daily_counts ORDER BY id DESC LIMIT ?",
        (days,),
    )
    rows = c.fetchall()
    conn.close()
    data = [{"run_at": r[0], "active": r[1], "pending": r[2]} for r in rows]
    return {"data": data}


@app.get("/export.csv")
def export_csv(days: int = 365):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT run_at, total_active, total_pending "
        "FROM daily_counts ORDER BY id DESC LIMIT ?",
        (days,),
    )
    rows = c.fetchall()
    conn.close()

    def iter_csv():
        yield "run_at,total_active,total_pending\n"
        for r in rows:
            yield f"{r[0]},{r[1]},{r[2]}\n"

    return StreamingResponse(iter_csv(), media_type="text/csv")


# --------- Scheduler for daily 6 AM run ---------


scheduler = BackgroundScheduler()
scheduler.add_job(run_count, "cron", hour=6, minute=0)
scheduler.start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
