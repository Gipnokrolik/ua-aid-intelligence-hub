import os
import time
import math
import random
import logging
import datetime
from pathlib import Path

import psycopg2
import cloudscraper
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
BASE_DIR = Path(__file__).resolve().parent.parent.parent
PG_URI = os.getenv("DATABASE_URL")

if not PG_URI:
    raise ValueError("DATABASE_URL not found in environment variables")

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
    Retrieves the most recent donation date for the foundation.
    """
    conn = None
    try:
        conn = psycopg2.connect(PG_URI)
        cursor = conn.cursor()

        cursor.execute("SELECT MAX(date) FROM donations WHERE foundation_name = %s", (FOUNDATION_NAME,))
        res = cursor.fetchone()[0]

        return str(res)[:10] if res else "2024-01-01"
    except Exception as e:
        logging.error(f"Database error during date lookup: {e}")
        return "2024-01-01"
    finally:
        if conn:
            conn.close()


def normalize_date(date_str):
    """
    ISO 8601 string to YYYY-MM-DD.
    """
    try:
        return date_str.split('T')[0]
    except Exception:
        return date_str


def save_live_records(rows):
    """
    Batch inserts records with conflict handling.
    """
    conn = psycopg2.connect(PG_URI)
    cursor = conn.cursor()

    prepared_rows = [
        (
            r['id'],
            float(r['amount']),
            r['currency'],
            normalize_date(r['date']),
            r['comment'],
            r['source'],
            FOUNDATION_NAME,
            'general'
        ) for r in rows
    ]

    try:
        cursor.executemany('''
            INSERT INTO donations (id, amount, currency, date, comment, source, foundation_name, category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        ''', prepared_rows)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        logging.error(f"Insert failed: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()


def run_live_update():
    """
    Main ingestion process.
    """
    last_date = get_latest_date_from_db()
    date_from = f"{last_date}T00:00:00.000Z"
    date_to = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    logging.info(f"Syncing {FOUNDATION_NAME} from {date_from}")

    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

    params = {
        "date_from": date_from,
        "date_to": date_to,
        "per_page": RECORDS_PER_PAGE,
        "page": 1
    }

    try:
        response = scraper.get(API_URL, params=params)
        if response.status_code != 200:
            logging.error(f"API returned {response.status_code}")
            return

        total_count = response.json().get('total_count', 0)
        total_pages = math.ceil(total_count / RECORDS_PER_PAGE)
        logging.info(f"Total potential records: {total_count} ({total_pages} pages)")
    except Exception as e:
        logging.error(f"Initial request failed: {e}")
        return

    for current_page in range(1, total_pages + 1):
        try:
            params["page"] = current_page
            res = scraper.get(API_URL, params=params)

            if res.status_code == 200:
                rows = res.json().get('rows', [])
                if not rows:
                    break

                count = save_live_records(rows)
                logging.info(f"Page {current_page}/{total_pages} | Inserted: {count}")
                time.sleep(random.uniform(0.3, 0.7))
            elif res.status_code == 429:
                logging.warning("Rate limit hit. Waiting 60s.")
                time.sleep(60)
            else:
                break
        except Exception as e:
            logging.error(f"Error on page {current_page}: {e}")
            break

    logging.info("Update complete.")


if __name__ == "__main__":
    run_live_update()