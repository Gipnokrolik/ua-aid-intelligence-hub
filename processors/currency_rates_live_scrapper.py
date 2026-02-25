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

# Relative Path Logic
# Script location: /processors/nbu_live_scraper.py
# .parent is /processors/
# .parent.parent is /ua-aid-intelligence-hub/ (ROOT)
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "master" / "master.db"


def get_latest_date():
    """
    Retrieves the maximum date from the exchange_rates table.
    Used for incremental loading.
    """
    if not DB_PATH.exists():
        logger.warning(f"Database not found at: {DB_PATH}")
        return None

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    try:
        # Verify if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='exchange_rates'")
        if not cursor.fetchone():
            return None

        cursor.execute("SELECT MAX(date) FROM exchange_rates WHERE currency = 'EUR'")
        res = cursor.fetchone()[0]
        return datetime.strptime(res, '%Y-%m-%d') if res else None
    except Exception as e:
        logger.error(f"Failed to check metadata: {e}")
        return None
    finally:
        conn.close()


def sync_exchange_rates():
    """
    Fetches missing EUR rates from NBU API and saves them to the master DB.
    """
    logger.info(f"Using Master DB at: {DB_PATH}")

    last_date = get_latest_date()
    # If no data, start from the beginning of 2024
    start_date = (last_date + timedelta(days=1)) if last_date else datetime(2024, 1, 1)
    end_date = datetime.now()

    if start_date > end_date:
        logger.info("Exchange rates are already up to date.")
        return

    # Ensure data/master/ exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Primary Key on date ensures unique entries per day
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_rates (
            date TEXT PRIMARY KEY,
            currency TEXT,
            rate_uah REAL
        )
    ''')

    current_date = start_date
    records_added = 0

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

                    logger.info(f"Fetched: {day_iso} | EUR: {rate}")
                    records_added += 1
            except Exception as e:
                logger.error(f"API Error at {date_api}: {e}")
                break  # Commit what we have and stop

            current_date += timedelta(days=1)
            time.sleep(random.uniform(0.1, 0.2))

        conn.commit()
        logger.info(f"Synchronization complete. Records added: {records_added}")

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    sync_exchange_rates()