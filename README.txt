Villages Listing Tracker
=========================

This folder contains a minimal, Render-friendly project that:

- Scrapes TheVillages.com Homefinder (Selenium stub; CSS selectors may need tweaks).
- Counts Active vs Pending listings.
- Groups them by PRACTICAL market regions, then alphabetically by village.
- Stores history in SQLite.
- Exposes a simple web UI (static HTML) plus JSON APIs.
- Supports a daily 6 AM cron scrape via `run_once.py`.

Local dev (very rough steps)
----------------------------

1. Install Python 3.11+, Chrome/Chromium, and chromedriver.
2. In `backend`:

   pip install -r requirements.txt
   uvicorn app:app --reload --host 0.0.0.0 --port 8000

3. In another terminal, serve the `frontend/src/index.html` file via any static server
   (or just open it directly in the browser and set `window.API_BASE` to your backend URL
   in the devtools console).

Render deployment
-----------------

1. Push this folder to a GitHub repo.
2. In Render:

   - Create a Web Service from this repo for the backend:
     - Runtime: Python
     - Build Command: pip install -r backend/requirements.txt
     - Start Command: uvicorn backend.app:app --host 0.0.0.0 --port 8000

   - Create a Static Site for the frontend:
     - Publish directory: frontend/src
     - After first deploy, go to Settings -> Environment variables:
       - API_BASE = https://<your-backend-service>.onrender.com

   - Create a Cron Job:
     - Build Command: pip install -r backend/requirements.txt
     - Start Command: python backend/run_once.py
     - Schedule: 0 6 * * *  (6:00 AM daily)

3. Wait for first cron run or hit the "Run Count" button in the web page to trigger one manually.
