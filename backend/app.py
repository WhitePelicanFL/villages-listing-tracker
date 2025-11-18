"""Villages Listing Tracker backend (map-disabled version)

Run locally:
    uvicorn app:app --reload --host 0.0.0.0 --port 8000
"""

import os
import sqlite3
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -------------------------------------------------
# Config
# -------------------------------------------------

DB_PATH = os.environ.get("DB_PATH", "villages_listings.db")
HOMEFINDER_URL = os.environ.get("HOMEFINDER_URL", "https://www.thevillages.com/homefinder")

HEADLESS = os.environ.get("HEADLESS", "1") == "1"
SELENIUM_TIMEOUT = int(os.environ.get("SELENIUM_TIMEOUT", "45"))

# safe-scroll mode (your choice B)
SCROLL_PAUSE = float(os.environ.get("SCROLL_PAUSE", "1.5"))
SCROLL_STABLE_ROUNDS = int(os.environ.get("SCROLL_STABLE_ROUNDS", "2"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# DB helpers
# -------------------------------------------------


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# -------------------------------------------------
# Backward compatibility for old UI / cron: run_count()
# -------------------------------------------------

def run_count():
    """
    Legacy helper used by old frontend buttons and run_once.py.
    It simply triggers the full scrape and returns the result.
    """
    result = run_scrape_once()
    return result

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    # basic listing table; adjust as needed to match your existing schema
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            villages_id TEXT,
            status TEXT,
            address TEXT,
            price INTEGER,
            beds TEXT,
            baths TEXT,
            sqft INTEGER,
            raw_json TEXT,
            seen_date TEXT
        );
        """
    )
    conn.commit()
    conn.close()


# -------------------------------------------------
# Selenium helpers
# -------------------------------------------------
def wait_for_homefinder_loaded(driver):
    """
    Wait until the left results panel appears, meaning React has mounted.
    """
    logger.info("Waiting for Homefinder React app to finish loading...")

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(@class,'result') or contains(@class,'listing')]")
            )
        )
        logger.info("Homefinder app fully loaded.")
    except Exception:
        logger.warning("Homefinder app did NOT fully load in time, continuing anyway.")


def build_driver() -> webdriver.Chrome:
    chrome_options = Options()
    if HEADLESS:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,900")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def wait_for_app_ready(driver: webdriver.Chrome):
    driver.get(HOMEFINDER_URL)
    logger.info("Loaded Homefinder URL: %s", HOMEFINDER_URL)

    # Wait for outer page to load
    WebDriverWait(driver, SELENIUM_TIMEOUT).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    # Try to switch into the Homefinder iframe (the real app lives there)
    try:
        WebDriverWait(driver, 15).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "iframe"))
        )
        logger.info("Switched into Homefinder iframe.")
    except Exception:
        # In case the site changes and there is no iframe, just stay at top level
        logger.info("No iframe with id='iframe' found; using top-level document.")

    # Wait for the inner app body to be ready
    WebDriverWait(driver, SELENIUM_TIMEOUT).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)  # allow the SPA to settle
    
# -------------------------------------------------
# Disable map + apply filters
# -------------------------------------------------

def disable_map_and_apply_filters(driver: webdriver.Chrome):
    logger.info("Disabling map and applying filters...")

    # Hide the map container using JS + CSS
    hide_map_js = """
        const style = document.createElement('style');
        style.innerHTML = `
            #map, .map-container, [id*="map"], .leaflet-container, .leaflet-pane {
                display: none !important;
                visibility: hidden !important;
                height: 0 !important;
                width: 0 !important;
                opacity: 0 !important;
                pointer-events: none !important;
            }
        `;
        document.head.appendChild(style);
    """
    driver.execute_script(hide_map_js)

    # Extra measure: disable map event listeners completely
    disable_map_events_js = """
        if (window.L && L.Map) {
            L.Map.prototype._initEvents = function() {};
        }
    """
    try:
        driver.execute_script(disable_map_events_js)
    except Exception:
        pass

    time.sleep(1)

    # -------------------------------------------------
    # Turn off "Homesites"
    # -------------------------------------------------

    try:
        homesite_toggle = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//label[contains(., 'Homesites')]/preceding-sibling::input")
            )
        )
        if homesite_toggle.is_selected():
            driver.execute_script("arguments[0].click();", homesite_toggle)
            logger.info("Homesites filter disabled.")
    except Exception as e:
        logger.warning("Homesites toggle not found or failed: %s", e)

    time.sleep(1)

    # -------------------------------------------------
    # Force sort order: Price Low → High
    # -------------------------------------------------

    try:
        sort_dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//select[contains(@class,'sort')]")
            )
        )
        driver.execute_script("arguments[0].value='price-asc'; arguments[0].dispatchEvent(new Event('change'));",
                              sort_dropdown)
        logger.info("Sort set: Price Low → High.")
    except Exception as e:
        logger.warning("Sort dropdown not found: %s", e)

    time.sleep(2)


# -------------------------------------------------
# Safe auto-scroll function
# -------------------------------------------------

def load_all_results(driver: webdriver.Chrome):
    """
    Scrolls the page (inside the iframe) until no new content loads.
    We watch the overall document.body.scrollHeight for stability.
    """

    logger.info("Starting safe auto-scroll to load ALL listings...")

    last_height = -1
    stable_rounds = 0

    while stable_rounds < SCROLL_STABLE_ROUNDS:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

        new_height = driver.execute_script("return document.body.scrollHeight;")

        if new_height == last_height:
            stable_rounds += 1
        else:
            stable_rounds = 0

        last_height = new_height
        logger.info(f"Window scroll height: {new_height}, stable rounds: {stable_rounds}")

    logger.info("Finished scrolling; assuming all listings are loaded.")

# -------------------------------------------------
# Extract listing cards
# -------------------------------------------------

def extract_listings(driver: webdriver.Chrome) -> List[Dict]:
    logger.info("Extracting listing cards...")

    cards = []

    # Try several reasonable selectors – stop at the first that returns results
    selectors = [
        "[data-listing-id]",                      # most reliable if present
        "[data-testid*='home-card']",
        "[data-testid*='listing-card']",
        ".result-card",
        ".listing-card",
    ]

    for sel in selectors:
        cards = driver.find_elements(By.CSS_SELECTOR, sel)
        if cards:
            logger.info("Found %d cards using selector '%s'.", len(cards), sel)
            break

    if not cards:
        logger.warning("No listing cards found with any known selector.")
        return []

    listings = []

    for c in cards:
        try:
            villages_id = c.get_attribute("data-listing-id") or ""

            # Address
            address = ""
            addr_candidates = c.find_elements(
                By.XPATH,
                ".//*[contains(@class,'address') or contains(@class,'Address') or contains(., 'The Villages, FL')]"
            )
            if addr_candidates:
                address = addr_candidates[0].text.strip()

            # Price
            price = 0
            price_candidates = c.find_elements(
                By.XPATH,
                ".//*[contains(@class,'price') or contains(@class,'Price') or contains(text(),'$')]"
            )
            if price_candidates:
                price_raw = price_candidates[0].text
                price = int(
                    price_raw.replace("$", "").replace(",", "").strip() or 0
                )

            # Beds
            beds = ""
            bed_candidates = c.find_elements(
                By.XPATH,
                ".//*[contains(@class,'bed') or contains(text(),'Bed')]"
            )
            if bed_candidates:
                beds = bed_candidates[0].text.strip()

            # Baths
            baths = ""
            bath_candidates = c.find_elements(
                By.XPATH,
                ".//*[contains(@class,'bath') or contains(text(),'Bath')]"
            )
            if bath_candidates:
                baths = bath_candidates[0].text.strip()

            # Sqft
            sqft = 0
            sqft_candidates = c.find_elements(
                By.XPATH,
                ".//*[contains(@class,'sqft') or contains(text(),'sq ft') or contains(text(),'sqft')]"
            )
            if sqft_candidates:
                sqft_raw = sqft_candidates[0].text
                sqft = int(
                    sqft_raw.lower()
                    .replace("sqft", "")
                    .replace("sq ft", "")
                    .replace(",", "")
                    .strip() or 0
                )

            listings.append(
                {
                    "villages_id": villages_id,
                    "address": address,
                    "price": price,
                    "beds": beds,
                    "baths": baths,
                    "sqft": sqft,
                    "raw_html": c.get_attribute("innerHTML"),
                }
            )

        except Exception as e:
            logger.warning("Error parsing card: %s", e)

    logger.info("Extracted %d listings.", len(listings))
    return listings

# -------------------------------------------------
# DB insert/update
# -------------------------------------------------

def save_listings_to_db(listings: List[Dict]):
    conn = get_db_connection()
    cur = conn.cursor()

    for lst in listings:
        villages_id = lst.get("villages_id")
        raw_json = json.dumps(lst)

        cur.execute(
            """
            INSERT INTO listings (villages_id, status, address, price, beds, baths, sqft, raw_json, seen_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                villages_id,
                "active_or_pending",  # You can update this as you refine status detection
                lst.get("address"),
                lst.get("price"),
                lst.get("beds"),
                lst.get("baths"),
                lst.get("sqft"),
                raw_json,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    conn.commit()
    conn.close()
    logger.info("Saved %d listings to DB.", len(listings))


# -------------------------------------------------
# Main scrape routine
# -------------------------------------------------


def run_scrape_once() -> Dict:
    logger.info("Starting full scrape...")

    driver = build_driver()

    try:
        wait_for_app_ready(driver)
        wait_for_homefinder_loaded(driver)
        disable_map_and_apply_filters(driver)
        load_all_results(driver)

        listings = extract_listings(driver)
        save_listings_to_db(listings)

        logger.info("Scrape complete. %d listings captured.", len(listings))
        return {"status": "success", "count": len(listings)}

    except Exception as e:
        logger.error("SCRAPE ERROR: %s", e)
        return {"status": "error", "message": str(e)}

    finally:
        driver.quit()


# -------------------------------------------------
# FastAPI setup
# -------------------------------------------------

app = FastAPI(title="Villages Listing Tracker API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
    
# -------------------------------------------------
# Startup
# -------------------------------------------------


@app.on_event("startup")
def on_startup():
    logger.info("Initializing database...")
    init_db()
    logger.info("Database ready.")


# -------------------------------------------------
# API routes
# -------------------------------------------------


@app.get("/health")
def health_check():
    return {"status": "ok", "message": "Villages Listing Tracker backend running."}

@app.post("/run")
def legacy_run():
    """
    Legacy endpoint used by the old UI "Run Count" button.
    Behaves exactly like /scrape.
    """
    return run_count()

@app.post("/scrape")
def scrape_now():
    """
    Trigger a full scrape:
    - Loads Homefinder
    - Disables map
    - Applies filters (Homesites off, sort by price low→high)
    - Safe-scrolls left panel to load ALL listings
    - Extracts cards and saves to DB
    """
    result = run_scrape_once()
    if result.get("status") == "error":
        return JSONResponse(status_code=500, content=result)
    return result


@app.get("/listings")
def get_listings(limit: Optional[int] = 200):
    """
    Return the most recently seen listings (up to `limit`).
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT villages_id, status, address, price, beds, baths, sqft, raw_json, seen_date
        FROM listings
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()

    listings = []
    for r in rows:
        try:
            raw = json.loads(r["raw_json"])
        except Exception:
            raw = {}

        listings.append(
            {
                "villages_id": r["villages_id"],
                "status": r["status"],
                "address": r["address"],
                "price": r["price"],
                "beds": r["beds"],
                "baths": r["baths"],
                "sqft": r["sqft"],
                "seen_date": r["seen_date"],
                "raw": raw,
            }
        )

    return {"count": len(listings), "listings": listings}


@app.get("/count")
def get_counts():
    """
    Simple row count in the listings table.
    (You can expand this later into active/pending/VNH/VLS breakdowns.)
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM listings;")
    row = cur.fetchone()
    conn.close()

    return {"total_listings_rows": row["cnt"] if row else 0}
