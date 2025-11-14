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
    driver = make_driver(_
