"""Cron helper: runs a single scrape and exits."""
from app import run_count

if __name__ == "__main__":
    result = run_count()
    print(result)
