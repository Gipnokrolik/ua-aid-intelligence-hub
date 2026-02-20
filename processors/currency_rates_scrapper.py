import os
import sqlite3
import requests
import logging
from datetime import datetime, timedelta

# Professional logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'master', 'master.db')


def fetch_nbu_eur_rates_2025():
    logger.info(f"Targeting database: {DB_PATH}")

    rates_data = []
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
    current_date = start_date

    # Variable to track month transitions
    last_logged_month = None

    while current_date <= end_date:
        # LOGGING LOGIC: Check if we entered a new month
        if current_date.month != last_logged_month:
            logger.info(f"Processing data for: {current_date.strftime('%B %Y')}...")
            last_logged_month = current_date.month

        date_str = current_date.strftime('%Y%m%d')
        url = f"https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=EUR&date={date_str}&json"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data:
                rate = data[0]['rate']
                db_date = current_date.strftime('%Y-%m-%d')
                rates_data.append((db_date, 'EUR', rate))
        except Exception as e:
            logger.error(f"Failed to fetch rate for {date_str}: {e}")
            # If API fails, we break to avoid saving partial/corrupted data
            break

        current_date += timedelta(days=1)

    return rates_data


def save_rates_to_db(rates):
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Schema with Primary Key for automatic indexing
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS exchange_rates
                       (
                           date
                           TEXT
                           PRIMARY
                           KEY,
                           currency
                           TEXT,
                           rate_uah
                           REAL
                       )
                       ''')

        # Atomic bulk insert
        cursor.executemany('''
            INSERT OR REPLACE INTO exchange_rates (date, currency, rate_uah)
            VALUES (?, ?, ?)
        ''', rates)

        # Performance optimization for the JOIN in our Superset VIEW
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exchange_date_short ON exchange_rates (SUBSTR(date, 1, 10))")

        conn.commit()
        logger.info(f"Sync complete. Total processed: {len(rates)} records.")
    except sqlite3.Error as e:
        logger.error(f"Database sync failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    eur_rates = fetch_nbu_eur_rates_2025()
    if eur_rates:
        save_rates_to_db(eur_rates)