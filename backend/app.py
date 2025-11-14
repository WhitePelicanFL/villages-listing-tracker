"""
Villages Listing Tracker (FINAL VERSION)

This scraper loads the REAL homefinder SPA backend directly:
    https://development.avengers.thevillages.com/homefinder/?hideHeader

This bypasses the WordPress wrapper and allows full access
to all listing cards, new + preowned, active + pending.
"""

import os
import json
import time
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from apscheduler.schedulers.background import BackgroundScheduler

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# --------------------------------------------
# CONFIG
# --------------------------------------------

DB_PATH = os.environ.get("DB_PATH", "counts.db")

# **REAL backend SPA that contains ALL listings**
HOMEFINDER_URL = (
    "https://development.avengers.thevillages.com/homefinder/?hideHeader"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------------------------------
# REGION MAPPING
# --------------------------------------------

REGION_DEFS = {
    "North of 466": [
        "Orange Blossom Gardens", "Silver Lake", "Spanish Springs",
        "Santo Domingo", "Rio Grande", "La Reynalda", "La Zamora",
        "La Crescenta", "Chula Vista", "El Cortez", "El Santiago"
    ],
    "Between 466 & 466A": [
        "Belvedere", "Ashland", "Amelia", "Bonnybrook", "Liberty Park",
        "Hadley", "Hemingway", "Duval", "Caroline", "Mallory Square",
        "Sabastian", "Sunset Pointe", "Virginia Trace",
        "Lake Sumter Landing"
    ],
    "South of 466A": [
        "St. Charles", "St. James", "Tamarind Grove", "Buttonwood",
        "Sanibel", "Hillsborough", "Collier", "Pinellas", "Charlotte"
    ],
    "South of 44": [
        "Fenney", "DeLuna", "Marsh Bend", "Chitty Chatty", "Bradford",
        "Citrus Grove", "Hawkins", "Linden", "Monarch Grove",
        "St. Catherine", "St. Johns", "St. Lucy", "Lake Denham", "Dabney"
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
    if "denham" in v:
        return "South of 44"
    if any(x in v for x in ["dabney", "eastport", "newell"]):
        return "New Southern / Future"
    return "Unknown"


# --------------------------------------------
# DB INIT
# --------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_counts (
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


# --------------------------------------------
# SELENIUM DRIVER
# --------------------------------------------

def make_driver():
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-gpu")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=opt)


# --------------------------------------------
# SCRAPING LOGIC
# --------------------------------------------

def scrape_listings() -> List[Dict]:
    logger.info("Launching Selenium WebDriver…")
    driver = make_driver()

    try:
        # Load REAL backend
        logger.info(f"Loading Homefinder SPA: {HOMEFINDER_URL}")
        driver.get(HOMEFINDER_URL)

        # Sort: Price Low → High (makes scrolling deterministic)
        try:
            sort_dropdown = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "md-select[label='Sort By']"))
            )
            sort_dropdown.click()
            option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "md-option[value='priceLowToHigh']"))
            )
            option.click()
            time.sleep(1)
            logger.info("Sorting set to Price Low → High")
        except:
            logger.warning("Sort dropdown not found — continuing anyway.")

        # Homesites OFF
        try:
            homesite_toggle = driver.find_element(
                By.CSS_SELECTOR, "md-switch[aria-label='Homesites']"
            )
            if "md-checked" in homesite_toggle.get_attribute("class"):
                homesite_toggle.click()
                logger.info("Homesites turned OFF.")
        except:
            logger.warning("Homesites toggle not found — continuing.")

        # Wait for first card
        logger.info("Waiting for listing cards…")
        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "md-card.propertyCard"))
        )
        time.sleep(1)

        # Scroll virtual repeat to load ALL cards
        logger.info("Scrolling through virtual list…")
        SCROLL_JS = """
            var scroller = document.querySelector('.md-virtual-repeat-scroller');
            if (!scroller) return false;
            scroller.scrollTop = scroller.scrollHeight;
            return scroller.scrollHeight;
        """

        last_height = 0
        stable_count = 0

        while True:
            height = driver.execute_script(SCROLL_JS)
            cards_now = len(driver.find_elements(By.CSS_SELECTOR, "md-card.propertyCard"))

            if height == last_height:
                stable_count += 1
            else:
                stable_count = 0

            if stable_count >= 3:
                break

            last_height = height
            time.sleep(1)

        cards = driver.find_elements(By.CSS_SELECTOR, "md-card.propertyCard")
        logger.info(f"Total cards found: {len(cards)}")

        # Extract listing info
        results = []
        for c in cards:
            txt = c.text.lower()

            status = (
                "pending" if ("pending" in txt or "under contract" in txt) else "active"
            )

            list_type = (
                "new" if ("model" in txt or "new home" in txt) else "preowned"
            )

            village = ""
            try:
                v = c.find_element(By.CSS_SELECTOR, ".prop_village, .ng-binding")
                village = v.text.strip()
            except:
                pass

            region = classify_region(village)

            results.append({
                "status": status,
                "type": list_type,
                "village": village,
                "region": region,
                "raw": c.text
            })

        logger.info(f"Scraped {len(results)} listings.")
        return results

    finally:
        try:
            driver.quit()
        except:
            pass


# --------------------------------------------
# RUN COUNT
# --------------------------------------------

def run_count() -> Dict:
    listings = scrape_listings()

    total_active = sum(1 for r in listings if r["status"] == "active")
    total_pending = sum(1 for r in listings if r["status"] == "pending")

    # Group by region → village
    grouped = {}
    for r in listings:
        reg = r["region"]
        vil = r["village"] or "Unknown"
        status = r["status"]

        region_dict = grouped.setdefault(reg, {})
        village_dict = region_dict.setdefault(
            vil, {"active": 0, "pending": 0, "total": 0}
        )

        village_dict[status] += 1
        village_dict["total"] += 1

    grouped_sorted = {
        region: dict(sorted(villages.items()))
        for region, villages in grouped.items()
    }

    row = {
        "run_at": datetime.utcnow().isoformat(),
        "total_active": total_active,
        "total_pending": total_pending,
        "grouped": grouped_sorted
    }

    # Save raw listings
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO daily_counts(run_at, total_active, total_pending, payload_json) "
        "VALUES (?, ?, ?, ?)",
        (row["run_at"], total_active, total_pending, json.dumps(listings))
    )
    conn.commit()
    conn.close()

    return row


# --------------------------------------------
# FASTAPI APP
# --------------------------------------------

app = FastAPI(title="Villages Listing Tracker (VLT)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


@app.get("/status")
def status():
    return {"status": "ok"}


def debug_run():
    logger.info("Background task started…")
    try:
        result = run_count()
        logger.info(
            f"Completed. {result['total_active']} active, {result['total_pending']} pending."
        )
    except Exception as e:
        logger.error(f"run_count failed: {e}", exc_info=True)


@app.post("/run")
def trigger_run(background_tasks: BackgroundTasks):
    background_tasks.add_task(debug_run)
    return {"status": "started"}


@app.get("/latest")
def latest():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT run_at, total_active, total_pending, payload_json "
        "FROM daily_counts ORDER BY id DESC LIMIT 1"
    )
    row = c.fetchone()
    conn.close()

    if not row:
        return {}

    run_at, active, pending, payload = row
    listings = json.loads(payload)

    # regroup on-the-fly
    grp = {}
    for r in listings:
        reg = r["region"]
        vil = r["village"] or "Unknown"
        stat = r["status"]

        region_dict = grp.setdefault(reg, {})
        village_dict = region_dict.setdefault(
            vil, {"active": 0, "pending": 0, "total": 0}
        )

        village_dict[stat] += 1
        village_dict["total"] += 1

    grp_sorted = {
        reg: dict(sorted(vill.items()))
        for reg, vill in grp.items()
    }

    return {
        "run_at": run_at,
        "total_active": active,
        "total_pending": pending,
        "grouped": grp_sorted,
    }


@app.get("/history")
def history(days: int = 30):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT run_at, total_active, total_pending "
        "FROM daily_counts ORDER BY id DESC LIMIT ?",
        (days,)
    )
    rows = c.fetchall()
    conn.close()

    return {"data": [
        {"run_at": r[0], "active": r[1], "pending": r[2]}
        for r in rows
    ]}


@app.get("/export.csv")
def export_csv(days: int = 365):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT run_at, total_active, total_pending "
        "FROM daily_counts ORDER BY id DESC LIMIT ?",
        (days,)
    )
    rows = c.fetchall()
    conn.close()

    def iter_rows():
        yield "run_at,total_active,total_pending\n"
        for r in rows:
            yield f"{r[0]},{r[1]},{r[2]}\n"

    return StreamingResponse(iter_rows(), media_type="text/csv")


# --------------------------------------------
# Daily 6 AM scheduler
# --------------------------------------------

scheduler = BackgroundScheduler()
scheduler.add_job(debug_run, "cron", hour=6, minute=0)
scheduler.start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
