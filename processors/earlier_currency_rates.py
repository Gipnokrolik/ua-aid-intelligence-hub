import sqlite3
import requests
import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Relative Path Logic (2 levels up from processors/)
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "master" / "master.db"


def get_backfill_range():
    """
    Determines the date range needed to fill the gap in exchange rates.
    """
    if not DB_PATH.exists():
        logger.error("Master DB not found!")
        return None, None

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    try:
        # 1. Find the earliest donation date
        cursor.execute("SELECT MIN(date) FROM donations")
        min_donation_date_str = cursor.fetchone()[0]

        # 2. Find the earliest exchange rate date
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='exchange_rates'")
        if cursor.fetchone():
            cursor.execute("SELECT MIN(date) FROM exchange_rates")
            min_rate_date_str = cursor.fetchone()[0]
        else:
            min_rate_date_str = "2025-01-01"

        if not min_donation_date_str:
            logger.warning("No donations found. Nothing to backfill.")
            return None, None

        start_dt = datetime.strptime(min_donation_date_str, '%Y-%m-%d')
        # We fill until the earliest rate we already have
        end_dt = datetime.strptime(min_rate_date_str, '%Y-%m-%d') - timedelta(days=1)

        return start_dt, end_dt
    finally:
        conn.close()


def run_backfill():
    start_date, end_date = get_backfill_range()

    if not start_date or start_date > end_date:
        logger.info("No gaps found in exchange rates. Backfill not required.")
        return

    logger.info(f"Backfilling rates from {start_date.date()} to {end_date.date()}...")

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_rates (
            date TEXT PRIMARY KEY,
            currency TEXT,
            rate_uah REAL
        )
    ''')

    current_date = start_date
    added_count = 0

    try:
        while current_date <= end_date:
            date_api = current_date.strftime('%Y%m%d')
            url = f"https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=EUR&date={date_api}&json"

            try:
                res = requests.get(url, timeout=10)
                res.raise_for_status()
                data = res.json()

                if data:
                    rate = data[0]['rate']
                    day_iso = current_date.strftime('%Y-%m-%d')
                    cursor.execute('''
                        INSERT OR REPLACE INTO exchange_rates (date, currency, rate_uah)
                        VALUES (?, ?, ?)
                    ''', (day_iso, 'EUR', rate))

                    logger.info(f"Backfill: {day_iso} | EUR: {rate}")
                    added_count += 1
            except Exception as e:
                logger.error(f"Error at {date_api}: {e}")
                time.sleep(5)  # Wait and continue

            current_date += timedelta(days=1)
            # NBU API is sensitive to many requests, let's be polite
            time.sleep(0.1)

            # Commit every 30 days to save progress
            if added_count % 30 == 0:
                conn.commit()

        conn.commit()
        logger.info(f"Historical backfill complete. Added {added_count} days of rates.")
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    run_backfill()