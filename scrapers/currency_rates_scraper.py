import os
import requests
import logging
import random
import time
from datetime import datetime, timedelta, date
import psycopg2
from dotenv import load_dotenv

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Environment Configuration
# Technical Note: Ensure .env is accessible via absolute path in WSL/Airflow context
load_dotenv()
PG_URI = os.getenv("DATABASE_URL")

if not PG_URI:
    raise ValueError("DATABASE_URL not found in environment variables. Please check your .env file.")


def init_db():
    """
    Ensures the target table exists in PostgreSQL with proper constraints.
    """
    conn = psycopg2.connect(PG_URI)
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exchange_rates (
                date DATE PRIMARY KEY,
                currency TEXT,
                rate_uah REAL
            )
        ''')
        conn.commit()
    finally:
        conn.close()


def get_latest_date():
    """
    Retrieves the maximum date from the exchange_rates table.
    Used for incremental loading.
    """
    conn = None
    try:
        conn = psycopg2.connect(PG_URI)
        cursor = conn.cursor()

        # Check if the table exists first
        cursor.execute("SELECT to_regclass('public.exchange_rates');")
        if not cursor.fetchone()[0]:
            return None

        cursor.execute("SELECT MAX(date) FROM exchange_rates WHERE currency = 'EUR'")
        res = cursor.fetchone()[0]

        if not res:
            return None

        # Handle Postgres date/datetime returns
        if isinstance(res, datetime):
            return res
        elif isinstance(res, date):
            return datetime.combine(res, datetime.min.time())

        return datetime.strptime(str(res).split(' ')[0], '%Y-%m-%d')

    except Exception as e:
        logger.error(f"Failed to check metadata: {e}")
        return None
    finally:
        if conn:
            conn.close()


def sync_exchange_rates():
    """
    Fetches missing EUR rates from NBU API and saves them to the PostgreSQL DB.
    """
    logger.info("Initializing Database...")
    init_db()

    last_date = get_latest_date()
    # If no data, start from the beginning of 2024
    start_date = (last_date + timedelta(days=1)) if last_date else datetime(2024, 1, 1)
    end_date = datetime.now()

    # Compare only dates to avoid time-of-day execution bugs
    if start_date.date() > end_date.date():
        logger.info("Exchange rates are already up to date.")
        # Technical Note: Print 0 as the last stdout line for Airflow XCom telemetry
        print(0)
        return

    conn = psycopg2.connect(PG_URI)
    cursor = conn.cursor()

    current_date = start_date
    records_added = 0

    try:
        while current_date.date() <= end_date.date():
            date_api = current_date.strftime('%Y%m%d')
            url = f"https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=EUR&date={date_api}&json"

            try:
                res = requests.get(url, timeout=10)
                res.raise_for_status()
                data = res.json()

                if data:
                    rate = data[0]['rate']
                    day_iso = current_date.strftime('%Y-%m-%d')

                    # PostgreSQL equivalent for UPSERT
                    cursor.execute('''
                        INSERT INTO exchange_rates (date, currency, rate_uah)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (date) DO UPDATE 
                        SET rate_uah = EXCLUDED.rate_uah
                    ''', (day_iso, 'EUR', rate))

                    logger.info(f"Fetched: {day_iso} | EUR: {rate}")
                    records_added += 1
            except Exception as e:
                logger.error(f"API Error at {date_api}: {e}")
                break  # Commit what we have and stop

            current_date += timedelta(days=1)
            time.sleep(random.uniform(0.1, 0.2))

        conn.commit()
        logger.info(f"Synchronization complete. Records added/updated: {records_added}")

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        conn.rollback()
    finally:
        conn.close()

    # Technical Note: Final stdout output consumed by the downstream alerting bot
    print(records_added)


if __name__ == "__main__":
    sync_exchange_rates()