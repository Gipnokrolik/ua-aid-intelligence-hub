import os
import re
import io
import zlib
import requests
import pdfplumber
import logging
from datetime import datetime, date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

import psycopg2
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment Configuration
load_dotenv()
PG_URI = os.getenv("DATABASE_URL")

if not PG_URI:
    raise ValueError("DATABASE_URL not found in environment variables")

BASE_URL = "https://u24.gov.ua/reports"


def get_latest_u24_date():
    """
    Retrieves the maximum date specifically for United24 records in the DB.
    """
    conn = None
    try:
        conn = psycopg2.connect(PG_URI)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM donations WHERE foundation_name = 'united24'")
        res = cursor.fetchone()[0]

        if not res:
            return datetime.min

        if isinstance(res, datetime):
            return res
        elif isinstance(res, date):
            return datetime.combine(res, datetime.min.time())

        date_fmt = '%Y-%m-%d' if '-' in str(res) else '%d.%m.%Y'
        return datetime.strptime(str(res), date_fmt)
    except Exception as e:
        logging.error(f"Database check failed: {e}")
        return datetime.min
    finally:
        if conn:
            conn.close()


def get_report_links():
    """
    Uses a headless Chrome driver to render the dynamic content and extract PDF URLs.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(BASE_URL)
        import time
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        anchors = soup.find_all('a', href=True)

        pdf_links = []
        for a in anchors:
            href = a['href']
            if '.pdf' in href.lower() and 'report' in href.lower():
                full_url = href if href.startswith('http') else f"https://u24.gov.ua{href}"
                pdf_links.append(full_url)

        return list(set(pdf_links))
    finally:
        driver.quit()


def run_smart_sync():
    """
    Orchestrates the discovery, downloading, and row-level synchronization.
    """
    last_db_date = get_latest_u24_date()
    logging.info(f"Last United24 entry in DB: {last_db_date.strftime('%Y-%m-%d')}")

    links = get_report_links()
    logging.info(f"Discovered {len(links)} potential reports on the platform.")

    records_added = 0

    for url in links:
        filename = os.path.basename(url).split('?')[0]
        date_match = re.search(r'(\d{8})', filename)
        if not date_match:
            continue

        file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
        category = os.path.splitext(filename.split('-')[-1])[0].lower()

        if file_date >= last_db_date:
            logging.info(f"Processing report: {filename}")
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                parsed_rows = []
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    for page in pdf.pages[:21]:
                        table = page.extract_table()
                        if not table:
                            continue

                        for row in table:
                            try:
                                if not re.match(r'^\d{2}\.\d{2}\.\d{4}$', row[0]):
                                    continue

                                amount = float(row[1].replace(' ', '').replace(',', '.'))
                                date_str = datetime.strptime(row[0], '%d.%m.%Y').strftime('%Y-%m-%d')

                                parsed_rows.append({
                                    'date': date_str,
                                    'amount': amount,
                                    'category': category
                                })
                            except (ValueError, IndexError, TypeError):
                                continue

                if parsed_rows:
                    conn = psycopg2.connect(PG_URI)
                    try:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT date FROM donations WHERE foundation_name='united24' AND category=%s",
                            (category,)
                        )

                        known_dates = set()
                        for db_row in cursor.fetchall():
                            d = db_row[0]
                            if isinstance(d, (datetime, date)):
                                known_dates.add(d.strftime('%Y-%m-%d'))
                            else:
                                known_dates.add(str(d).split(' ')[0])

                        to_insert = []
                        for r in parsed_rows:
                            if r['date'] not in known_dates:
                                unique_str = f"u24_{r['date']}_{r['amount']}_{r['category']}"
                                record_id = zlib.crc32(unique_str.encode('utf-8'))

                                to_insert.append((
                                    record_id, r['date'], r['amount'], 'UAH', 'united24', r['category']
                                ))

                        if to_insert:
                            query = """
                                INSERT INTO donations (id, date, amount, currency, foundation_name, category) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO NOTHING
                            """
                            cursor.executemany(query, to_insert)
                            conn.commit()
                            records_added += len(to_insert)
                    finally:
                        conn.close()

            except Exception as e:
                logging.error(f"Error processing {filename}: {e}")

    logging.info(f"Sync finalized. {records_added} new entries pushed to master database.")

    # Technical Note: Final stdout line for Airflow XCom telemetry consumption
    print(records_added)


if __name__ == "__main__":
    run_smart_sync()