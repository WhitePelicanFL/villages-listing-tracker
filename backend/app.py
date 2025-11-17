"""
Villages Listing Tracker — Production Scraper
Environment-agnostic, iframe-safe, virtual-scroll aware,
homesite-filtered, placeholder-filtered, full-inventory scraper.
"""

import os
import sqlite3
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# -------------------------------------------------
# CONFIG — production Homefinder URL (Option B center)
# -------------------------------------------------

# Geographic center (optimal for showing all Villages regions at lvl=1)
VILLAGES_LAT = 28.872325543285804
VILLAGES_LNG = -81.99806323437654

# homesites=false removes lot-only listings
HOMEFINDER_URL = (
    "https://www.thevillages.com/homefinder/"
    f"?lat={VILLAGES_LAT}&lng={VILLAGES_LNG}&lvl=1"
    "&new&preowned&status"
    "&homesites=false"
    "&hideHeader"
)

DB_PATH = os.environ.get("DB_PATH", "counts.db")

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


# -------------------------------------------------
# REGION DEFINITIONS — unchanged
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
    if "denham" in v:
        return "South of 44"
    if "dabney" in v or "eastport" in v or "newell" in v:
        return "New Southern / Future"
    return "Unknown"


# -------------------------------------------------
# DB INIT
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
# SELENIUM DRIVER — ENVIRONMENT-AGNOSTIC (Selenium Manager)
# -------------------------------------------------

def make_driver() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    # Selenium Manager auto-downloads correct driver
    service = Service()

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


# -------------------------------------------------
# ID NORMALIZATION
# -------------------------------------------------

def normalize_id_line(line: str) -> Dict:
    """
    Normalize VNH#/VLS# → VNH123456 etc.
    """
    text = line.strip().upper()

    if text.startswith("VNH#"):
        prefix = "VNH"
        tail = text[4:]
    elif text.startswith("VLS#"):
        prefix = "VLS"
        tail = text[4:]
    else:
        return {"id": "", "type": ""}

    tail = "".join(ch for ch in tail if ch.isalnum())
    norm = f"{prefix}{tail}" if tail else ""
    return {"id": norm, "type": "new" if prefix == "VNH" else "preowned"}
# -------------------------------------------------
# CARD PARSING (with placeholder + homesite filtering)
# -------------------------------------------------

def is_homesite_text(full_text: str) -> bool:
    """
    Homesites show:
      - The word "HOMESITE"
      - NO bd/ba/sqft line
    We use Option 2 (your choice) with backup rules.
    """
    T = full_text.upper()

    # Rule A: Contains HOMESITE anywhere
    if "HOMESITE" in T:
        return True

    # Rule B: Missing bd/ba/sqft (homes never miss all three)
    lacks_beds = (" BD" not in T and "BED" not in T)
    lacks_baths = (" BA" not in T and "BATH" not in T)
    lacks_sqft = ("SQFT" not in T and "FT²" not in T)

    if lacks_beds and lacks_baths and lacks_sqft:
        return True

    return False


def parse_card(card) -> Optional[Dict]:
    """
    Extract structured card data.

    Apply filtering:
      • skip empty/placeholder virtual-repeat items
      • skip homesites (Option 2 logic)
    """
    full_text = card.text or ""
    if not full_text.strip():
        return None  # placeholder / empty card

    # Homesite filtering
    if is_homesite_text(full_text):
        return None

    # Split lines
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    if not lines:
        return None

    # Find village line ("Village of ...")
    village = ""
    for ln in lines:
        if "village of" in ln.lower():
            village = ln
            break
    if not village:
        return None  # Real homes always show "Village of ..."

    # Extract ID
    uid = ""
    list_type = ""

    for ln in lines:
        up = ln.upper()
        if up.startswith("VNH#") or up.startswith("VLS#"):
            id_info = normalize_id_line(up)
            uid = id_info["id"]
            list_type = id_info["type"]
            break

    # Fallback ID if missing real VNH/VLS
    if not uid:
        uid = lines[0][:40]

    # Determine status
    low = full_text.lower()
    status = "active"
    if "pending" in low or "under contract" in low:
        status = "pending"

    # Determine type if no VNH/VLS prefix
    if not list_type:
        if "new home" in low or "model" in low:
            list_type = "new"
        else:
            list_type = "preowned"

    region = classify_region(village)

    return {
        "id": uid,
        "title": full_text[:200],
        "status": status,
        "type": list_type,
        "village": village,
        "region": region,
    }


# -------------------------------------------------
# SCROLL CONTAINER DETECTION (universal)
# -------------------------------------------------

def find_scroll_container(driver, sample_card):
    """
    UNIVERSAL SCROLL CONTAINER DETECTION
    Works across:
      • macOS, Windows, Linux
      • Chrome, Brave, Edge
      • Headless/non-headless
      • Iframe or no iframe
      • Shadow DOM or not
      • Any Homefinder deployment

    Strategy:
      • walk up ancestors from a real card
      • first ancestor where scrollHeight > clientHeight → scrollable
    """
    ancestor = sample_card

    for _ in range(15):
        try:
            parent = ancestor.find_element(By.XPATH, "..")

            scroll_height = driver.execute_script(
                "return arguments[0].scrollHeight;", parent
            )
            client_height = driver.execute_script(
                "return arguments[0].clientHeight;", parent
            )

            if scroll_height > client_height + 20:
                return parent

            ancestor = parent
        except Exception:
            break

    raise Exception("Could not locate scrollable container for listing panel")


# -------------------------------------------------
# IFRAME DETECTION (production sometimes uses it)
# -------------------------------------------------

def switch_into_iframe_if_present(driver):
    """
    Production Homefinder sometimes loads inside an iframe, depending on UA.
    We detect and switch automatically.
    """
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if len(iframes) == 1:
            driver.switch_to.frame(iframes[0])
            logger.info("Switched into Homefinder iframe.")
        elif len(iframes) > 1:
            # Choose the iframe that contains property cards
            for iframe in iframes:
                try:
                    driver.switch_to.frame(iframe)
                    driver.find_element(By.CSS_SELECTOR, "md-card.propertyCard")
                    logger.info("Switched into correct Homefinder iframe.")
                    return
                except Exception:
                    driver.switch_to.default_content()
            logger.info("Multiple iframes but none contained property cards.")
        else:
            logger.info("No iframe present — direct DOM access.")
    except Exception as e:
        logger.warning(f"Iframe detection failed: {e}")


# -------------------------------------------------
# SCRAPE LISTINGS — MAIN HARVEST LOOP
# -------------------------------------------------

def scrape_listings() -> List[Dict]:
    logger.info("Launching WebDriver…")
    driver = make_driver()

    try:
        logger.info(f"Loading Homefinder URL: {HOMEFINDER_URL}")
        driver.get(HOMEFINDER_URL)

        time.sleep(2)
        switch_into_iframe_if_present(driver)

        wait = WebDriverWait(driver, 30)

        # Wait for first listing card
        sample_card = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "md-card.propertyCard"))
        )

        # Find scrollable container
        scroll_container = find_scroll_container(driver, sample_card)
        logger.info("Scroll container detected.")

        time.sleep(1)

        seen = set()
        results: List[Dict] = []

        same_loops = 0
        max_same = 10

        while True:
            cards = driver.find_elements(By.CSS_SELECTOR, "md-card.propertyCard")

            # DEBUG: dump first 3 cards' text once so we see raw content
            if not sample_dumped:
                logger.info("DEBUG: dumping text of first 3 cards")
                for c in cards[:3]:
                    try:
                        logger.info("CARD TEXT:\n%s\n---", c.text)
                    except Exception:
                        pass
                sample_dumped = True

            new_count = 0
            for card in cards:
                try:
                    data = parse_card(card)
                    if not data:
                        continue
                    uid = data["id"]
                    if uid not in seen:
                        seen.add(uid)
                        results.append(data)
                        new_count += 1
                except Exception:
                    continue

            logger.info(
                f"Harvest loop: cards={len(cards)}, total={len(results)}, new={new_count}"
            )

            if new_count == 0:
                same_loops += 1
            else:
                same_loops = 0

            if same_loops >= max_same:
                logger.info("Reached end of listing panel — stopping scroll.")
                break

            # Scroll by one viewport height
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;",
                scroll_container,
            )
            time.sleep(1.0)

        logger.info(f"Scraped {len(results)} listings total.")
        return results

    finally:
        try:
            driver.quit()
        except:
            pass
# -------------------------------------------------
# RUN COUNT — aggregate results + store to DB
# -------------------------------------------------

def run_count() -> Dict:
    listings = scrape_listings()

    # DEBUG SUMMARY: what are we actually seeing?
    vnh = sum(1 for r in listings if str(r.get("id", "")).upper().startswith("VNH"))
    vls = sum(1 for r in listings if str(r.get("id", "")).upper().startswith("VLS"))
    type_new = sum(1 for r in listings if r.get("type") == "new")
    type_pre = sum(1 for r in listings if r.get("type") == "preowned")
    pending_ct = sum(1 for r in listings if r.get("status") == "pending")

    logger.info(
        "DEBUG SUMMARY: total=%d, VNH=%d, VLS=%d, type_new=%d, type_pre=%d, pending=%d",
        len(listings), vnh, vls, type_new, type_pre, pending_ct,
    )
    # ----- end debug summary -----

    total_active = sum(1 for r in listings if r.get("status") == "active")
    total_pending = sum(1 for r in listings if r.get("status") == "pending")

    # Group by region → village
    grouped: Dict[str, Dict[str, Dict[str, int]]] = {}

    for r in listings:
        region = r.get("region") or classify_region(r.get("village", ""))
        village = r.get("village") or "Unknown"
        status = r.get("status", "active")

        region_dict = grouped.setdefault(region, {})
        village_dict = region_dict.setdefault(
            village, {"active": 0, "pending": 0, "total": 0}
        )

        if status == "active":
            village_dict["active"] += 1
        elif status == "pending":
            village_dict["pending"] += 1
        village_dict["total"] += 1

    # Sort villages alphabetically within each region
    grouped_sorted: Dict[str, Dict[str, Dict[str, int]]] = {}
    for region, villages in grouped.items():
        grouped_sorted[region] = dict(sorted(villages.items(), key=lambda kv: kv[0]))

    row = {
        "run_at": datetime.utcnow().isoformat(),
        "total_active": total_active,
        "total_pending": total_pending,
        "grouped": grouped_sorted,
    }

    # Write to DB
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO daily_counts (run_at, total_active, total_pending, payload_json)
        VALUES (?, ?, ?, ?)
        """,
        (
            row["run_at"],
            total_active,
            total_pending,
            json.dumps(listings),
        ),
    )
    conn.commit()
    conn.close()

    return row


# -------------------------------------------------
# FASTAPI ENDPOINTS
# -------------------------------------------------

@app.get("/status")
def status():
    return {"status": "ok"}


def debug_run_count():
    logger.info("Background run started…")
    try:
        result = run_count()
        logger.info(
            f"Run complete: active={result['total_active']}, pending={result['total_pending']}"
        )
    except Exception as e:
        logger.error(f"Error during run_count(): {e}", exc_info=True)


@app.post("/run")
def trigger_run(background_tasks: BackgroundTasks):
    logger.info("/run endpoint triggered — starting background task.")
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

    # Rebuild grouping fresh (same logic as run_count)
    grouped: Dict[str, Dict[str, Dict[str, int]]] = {}

    for r in listings:
        region = r.get("region") or classify_region(r.get("village", ""))
        village = r.get("village") or "Unknown"
        status = r.get("status", "active")

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
        "SELECT run_at, total_active, total_pending FROM daily_counts ORDER BY id DESC LIMIT ?",
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
        "SELECT run_at, total_active, total_pending FROM daily_counts ORDER BY id DESC LIMIT ?",
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
# SCHEDULER — daily 6 AM run
# -------------------------------------------------

scheduler = BackgroundScheduler()
scheduler.add_job(run_count, "cron", hour=6, minute=0)
scheduler.start()


# -------------------------------------------------
# MAIN
# -------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
