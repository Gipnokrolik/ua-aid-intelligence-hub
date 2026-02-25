import cloudscraper
import sqlite3
import time
import math
import random
import datetime
import logging
from pathlib import Path

# Path Configuration
# Moves up two levels from scrapers/savelife/ to the project root
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"/"come_back_alive"

# Ensure the directory exists
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants
API_URL = "https://cba-transapi.savelife.in.ua/wp-json/savelife/reporting/income"
TARGET_YEAR = 2025
RECORDS_PER_PAGE = 100


def init_db(db_path):
    """
    Initializes SQLite database with the required schema.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS donations (
            id INTEGER PRIMARY KEY,
            amount REAL,
            currency TEXT,
            date TEXT,
            comment TEXT,
            source TEXT
        )
    ''')
    conn.commit()
    return conn


def save_records(conn, rows):
    """
    Inserts records into the database. Skips existing IDs.
    """
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR IGNORE INTO donations (id, amount, currency, date, comment, source)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', [(r['id'], float(r['amount']), r['currency'], r['date'], r['comment'], r['source']) for r in rows])
    conn.commit()
    return cursor.rowcount


def fetch_monthly_data(year, month):
    """
    Extracts data for a specific month and saves it to data/raw.
    """
    month_str = f"{year}-{str(month).zfill(2)}"
    db_filename = f"donations_{month_str}.db"
    db_path = RAW_DATA_DIR / db_filename  # Saving to H:/ua-aid-intelligence-hub/data/raw

    date_from = f"{month_str}-01T00:00:00.000Z"
    if month == 12:
        end_date_obj = datetime.datetime(year + 1, 1, 1) - datetime.timedelta(seconds=1)
    else:
        end_date_obj = datetime.datetime(year, month + 1, 1) - datetime.timedelta(seconds=1)
    date_to = end_date_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    logging.info(f"Targeting month: {month_str} -> {db_path}")

    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    conn = init_db(db_path)

    current_page = 1
    total_pages = 1

    params = {
        "date_from": date_from,
        "date_to": date_to,
        "per_page": RECORDS_PER_PAGE,
        "page": 1
    }

    # Metadata request
    try:
        response = scraper.get(API_URL, params=params)
        if response.status_code == 200:
            total_count = response.json().get('total_count', 0)
            total_pages = math.ceil(total_count / RECORDS_PER_PAGE)
            logging.info(f"Month {month_str}: Found {total_count} records ({total_pages} pages)")
    except Exception as e:
        logging.error(f"Failed to fetch metadata for {month_str}: {e}")

    # Main extraction loop
    while current_page <= total_pages:
        try:
            params["page"] = current_page
            res = scraper.get(API_URL, params=params)

            if res.status_code == 200:
                rows = res.json().get('rows', [])
                if not rows:
                    break

                inserted = save_records(conn, rows)
                logging.info(f"[{month_str}] Page {current_page}/{total_pages} | Saved: {inserted}")

                current_page += 1
                time.sleep(random.uniform(0.2, 0.6))

            elif res.status_code == 429:
                wait = random.randint(45, 90)
                logging.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)

            elif res.status_code == 504:
                logging.warning("Gateway timeout. Retrying in 15s...")
                time.sleep(15)

            else:
                logging.error(f"Error {res.status_code}. Retrying in 30s...")
                time.sleep(30)

        except Exception as e:
            logging.error(f"Connection error: {e}. Retrying...")
            time.sleep(10)

    conn.close()
    logging.info(f"Finished {month_str}. Database ready at: {db_path}")


if __name__ == "__main__":
    logging.info(f"--- UA AID INTELLIGENCE HUB SYNC: {TARGET_YEAR} ---")

    for m in range(1, 13):
        fetch_monthly_data(TARGET_YEAR, m)
        time.sleep(3)  # Cool down between months

    logging.info("Synchronization complete.")