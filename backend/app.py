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

# -------------------------------------------------
# Config
# -------------------------------------------------

# The correct map center (YOU provided it)
VILLAGES_LAT = 28.872325543285804
VILLAGES_LNG = -81.99806323437654

DB_PATH = os.environ.get("DB_PATH", "counts.db")

# Homefinder SPA on the dev host, with:
# - lat / lng / lvl=1 to ensure:
#     * Zoom level 1 (all the way out)
#     * Map centered over The Villages
# - new & preowned & status (for-sale only)
# - hideHeader to reduce clutter
HOMEFINDER_URL = (
    "https://development.avengers.thevillages.com/homefinder/"
    f"?lat={VILLAGES_LAT}"
    f"&lng={VILLAGES_LNG}"
    f"&lvl=1"
    f"&new&preowned&status&hideHeader"
)

app = FastAPI(title="Villages Listing Tracker")

# CORS so frontend on another domain can call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # you can tighten this later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# Regions & village grouping
# -------------------------------------------------

REGION_DEFS = {
    "North of 466": [
        "Orange Blossom Gardens",
        "Silver Lake",
        "Spanish Springs",
        "Santo Domingo",
        "Rio Grande",
        "La Reynalda",
        "La Zamora",
        "La Crescenta",
        "Chula Vista",
        "El Cortez",
        "El Santiago",
        "El Cortez",
    ],
    "Between 466 & 466A": [
        "Belvedere",
        "Ashland",
        "Amelia",
        "Bonnybrook",
        "Liberty Park",
        "Hadley",
        "Hemingway",
        "Duval",
        "Caroline",
        "Mallory Square",
        "Sabastian",
        "Sunset Pointe",
        "Virginia Trace",
        "Lake Sumter Landing",
    ],
    "South of 466A": [
        "St. Charles",
        "St. James",
        "Tamarind Grove",
        "Buttonwood",
        "St. James",
        "Sanibel",
        "Hillsborough",
        "Collier",
        "Pinellas",
        "Charlotte",
    ],
    "South of 44": [
        "Fenney",
        "DeLuna",
        "Marsh Bend",
        "Chitty Chatty",
        "Bradford",
        "Citrus Grove",
        "Hawkins",
        "Linden",
        "Monarch Grove",
        "St. Catherine",
        "St. Johns",
        "St. Lucy",
        "Lake Denham",
        "Dabney",
    ],
    "New Southern / Future": [
        "Eastport",
        "Newell",
        "Lake Denham East",
        "Future Development",
    ],
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


# -------------------------------------------------
# DB helpers
# -------------------------------------------------


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

# -------------------------------------------------
# Selenium helpers & scraping
# -------------------------------------------------


def make_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def normalize_id_line(line: str) -> Dict:
    """
    Given a line like:
      'VNH# 240V045'
      'VLS# 123-456'
    return a dict with normalized id and type info.

    Normalized IDs:
      VNH# 240V045 -> VNH240V045
      VLS# 123-456 -> VLS123456
    """
    text = line.strip().upper()

    if text.startswith("VNH#"):
        prefix = "VNH"
        tail = text[4:]
    elif text.startswith("VLS#"):
        prefix = "VLS"
        tail = text[4:]
    else:
        return {"id": "", "prefix": "", "type": ""}

    # Remove non-alphanumeric from the remainder
    tail = "".join(ch for ch in tail if ch.isalnum())
    norm_id = f"{prefix}{tail}" if tail else ""

    home_type = "new" if prefix == "VNH" else "preowned"

    return {"id": norm_id, "prefix": prefix, "type": home_type}


def parse_card(card) -> Dict:
    """
    Extract structured info from a propertyCard element.

    Fix: filter out placeholder (virtual repeat) cards.
    A real listing MUST have:
      - non-empty text
      - a Village line ("The Village of ...")
    ID may be VNH/VLS or generic fallback.
    """

    full_text = card.text or ""
    if not full_text.strip():
        return {"skip": True}   # empty placeholder

    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    if not lines:
        return {"skip": True}

    # Must have a Village line to be real
    village = ""
    for line in lines:
        if "village of" in line.lower():
            village = line.strip()
            break
    if not village:
        return {"skip": True}   # placeholder card, not a real listing

    # ID detection
    uid = ""
    list_type = ""

    for line in lines:
        upper = line.upper()
        if upper.startswith("VNH#") or upper.startswith("VLS#"):
            id_info = normalize_id_line(upper)
            uid = id_info["id"]
            list_type = id_info["type"]
            break

    # If no VNH/VLS, generate fallback ID from text
    if not uid:
        # use first non-empty line as fallback
        uid = lines[0][:40]

    # Determine status
    lower = full_text.lower()
    status = "active"
    if "pending" in lower or "under contract" in lower:
        status = "pending"

    # Determine type if prefix didn't specify
    if not list_type:
        list_type = "preowned"
        if "new home" in lower or "model" in lower:
            list_type = "new"

    region = classify_region(village)

    return {
        "skip": False,
        "id": uid,
        "title": full_text[:120],
        "status": status,
        "type": list_type,
        "village": village,
        "region": region,
    }


def scrape_listings() -> List[Dict]:
    """
    Core scraping logic.

    Updated logic:
    - Loads Homefinder with lat/lng/lvl=1 so map is:
        * Zoom level 1
        * Centered over The Villages
      → this guarantees the list can show ALL listings.
    - Uses md-virtual-repeat behavior:
        * Only a handful of cards are in the DOM at once.
        * We scroll the *scroll container* to load more.
    - Harvests cards on every scroll and de-duplicates by ID.
    """
    logger.info("Launching Selenium WebDriver...")
    driver = make_driver()

    try:
        logger.info(f"Loading Homefinder URL: {HOMEFINDER_URL}")
        driver.get(HOMEFINDER_URL)

        wait = WebDriverWait(driver, 30)

        # Wait for at least one card
        try:
            sample_card = wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "md-card.propertyCard")
                )
            )

            # Walk up ancestors to find real scrollable container
            scroll_container = None
            ancestor = sample_card

            for _ in range(10):  # walk up up to 10 levels
                parent = ancestor.find_element(By.XPATH, "..")

                scroll_height = driver.execute_script(
                    "return arguments[0].scrollHeight;", parent
                )
                client_height = driver.execute_script(
                    "return arguments[0].clientHeight;", parent
                )

                # scrollable if content area is bigger than visible area
                if scroll_height > client_height + 20:
                    scroll_container = parent
                    break

                ancestor = parent

            if not scroll_container:
                raise Exception("Could not locate scrollable container for virtual list")

            logger.info("Detected TRUE scroll container for virtual list.")
            logger.info(
                "Scroll container and first listing card detected – starting scroll harvesting."
            )
        except Exception:
            logger.error(
                "ERROR: Could not find scroll container and/or listing cards – returning empty list."
            )
            return []

        # Let things stabilize a bit
        time.sleep(2)

        seen_ids = set()
        results: List[Dict] = []

        same_count_loops = 0
        max_same_loops = 8  # safety to exit once we've seen everything

        while True:
            cards = driver.find_elements(By.CSS_SELECTOR, "md-card.propertyCard")

            new_this_round = 0
            for card in cards:
                try:
                    data = parse_card(card)
                    if data.get("skip"):
                        continue
                    uid = data["id"]
                    if not uid:
                        continue

                    # *** TRUE DEDUPE ***
                    if uid in seen_ids:
                        continue

                    seen_ids.add(uid)
                    results.append(data)
                    new_this_round += 1
                except Exception:
                    # ignore parsing errors for individual cards
                    continue

            logger.info(
                "Harvest loop: saw %d cards, total unique IDs so far: %d (new_this_round=%d)",
                len(cards),
                len(seen_ids),
                new_this_round,
            )

            if new_this_round == 0:
                same_count_loops += 1
            else:
                same_count_loops = 0

            if same_count_loops >= max_same_loops:
                logger.info(
                    "No new cards for several loops – assuming end of list / all listings loaded."
                )
                break

            # Scroll the *list container* by one viewport height
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;",
                scroll_container,
            )
            time.sleep(1.0)

        logger.info("Scraped %d unique listings in total.", len(seen_ids))
        return results

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def run_count() -> Dict:
    listings = scrape_listings()

    total_active = sum(
        1 for r in listings if r.get("status", "").lower() == "active"
    )
    total_pending = sum(
        1 for r in listings if r.get("status", "").lower() == "pending"
    )

    # group by region and then village (alphabetical inside region)
    grouped: Dict[str, Dict[str, Dict[str, int]]] = {}
    for r in listings:
        region = r.get("region") or classify_region(r.get("village", ""))
        village = r.get("village") or "Unknown"
        status = r.get("status", "active").lower()

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
        """
        INSERT INTO daily_counts(run_at, total_active, total_pending, payload_json)
        VALUES (?, ?, ?, ?)
        """,
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


# -------------------------------------------------
# API endpoints
# -------------------------------------------------


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
        status = r.get("status", "active").lower()

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


# -------------------------------------------------
# Scheduler for daily 6 AM run
# -------------------------------------------------

scheduler = BackgroundScheduler()
scheduler.add_job(run_count, "cron", hour=6, minute=0)
scheduler.start()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
