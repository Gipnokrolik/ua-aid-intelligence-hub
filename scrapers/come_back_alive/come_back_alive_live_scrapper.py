import cloudscraper
import sqlite3
import time
import math
import random
import datetime
import logging
from pathlib import Path

# Path Configuration
# H:/ua-aid-intelligence-hub/scrapers/come_back_alive/live_scraper.py -> Project Root
BASE_DIR = Path(__file__).resolve().parent.parent.parent
MASTER_DB_PATH = BASE_DIR / "data" / "master" / "master.db"

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants
API_URL = "https://cba-transapi.savelife.in.ua/wp-json/savelife/reporting/income"
RECORDS_PER_PAGE = 100
FOUNDATION_NAME = 'come_back_alive'


def get_latest_date_from_db():
    """
    Finds the most recent date for CBA in the master database.
    """
    if not MASTER_DB_PATH.exists():
        logging.error(f"Master DB not found at {MASTER_DB_PATH}")
        return "2024-01-01"  # Safe fallback

    conn = sqlite3.connect(MASTER_DB_PATH)
    cursor = conn.cursor()
    try:
        # We look for the max date specifically for this foundation
        cursor.execute("SELECT MAX(date) FROM donations WHERE foundation_name = ?", (FOUNDATION_NAME,))
        res = cursor.fetchone()[0]
        return res if res else "2024-01-01"
    finally:
        conn.close()


def normalize_date(date_str):
    """
    Converts ISO 8601 (2025-01-01T00:09:48Z) to standard YYYY-MM-DD.
    """
    try:
        return date_str.split('T')[0]
    except Exception:
        return date_str


def save_live_records(rows):
    """
    Inserts records into the master database.
    Uses INSERT OR IGNORE to prevent duplicates based on ID.
    """
    conn = sqlite3.connect(MASTER_DB_PATH)
    cursor = conn.cursor()

    # Prepare data for master schema: (id, amount, currency, date, comment, source, foundation_name, category)
    prepared_rows = []
    for r in rows:
        prepared_rows.append((
            r['id'],
            float(r['amount']),
            r['currency'],
            normalize_date(r['date']),
            r['comment'],
            r['source'],
            FOUNDATION_NAME,
            'general'  # Default category for new live data
        ))

    try:
        cursor.executemany('''
            INSERT OR IGNORE INTO donations (id, amount, currency, date, comment, source, foundation_name, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', prepared_rows)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        logging.error(f"Failed to save to master DB: {e}")
        return 0
    finally:
        conn.close()


def run_live_update():
    """
    Main loop to fetch and save missing data.
    """
    last_date = get_latest_date_from_db()
    # Add a small buffer (start 1 day earlier to ensure no gap)
    date_from = f"{last_date}T00:00:00.000Z"
    date_to = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    logging.info(f"Syncing {FOUNDATION_NAME}: from {date_from} to {date_to}")

    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

    current_page = 1
    total_pages = 1
    params = {
        "date_from": date_from,
        "date_to": date_to,
        "per_page": RECORDS_PER_PAGE,
        "page": 1
    }

    # Initial request to get total pages
    try:
        response = scraper.get(API_URL, params=params)
        if response.status_code == 200:
            total_count = response.json().get('total_count', 0)
            total_pages = math.ceil(total_count / RECORDS_PER_PAGE)
            logging.info(f"Found {total_count} new potential records across {total_pages} pages")
        else:
            logging.error(f"API Error {response.status_code}")
            return
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        return

    # Crawling pages
    while current_page <= total_pages:
        try:
            params["page"] = current_page
            res = scraper.get(API_URL, params=params)

            if res.status_code == 200:
                rows = res.json().get('rows', [])
                if not rows: break

                inserted = save_live_records(rows)
                logging.info(f"Page {current_page}/{total_pages} | New records saved: {inserted}")

                current_page += 1
                time.sleep(random.uniform(0.3, 0.7))
            elif res.status_code == 429:
                time.sleep(60)
            else:
                break
        except Exception as e:
            logging.error(f"Error during page fetch: {e}")
            break

    logging.info("Live update completed.")


if __name__ == "__main__":
    run_live_update()