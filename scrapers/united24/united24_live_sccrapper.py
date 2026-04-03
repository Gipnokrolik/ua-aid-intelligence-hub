import os
import re
import io
import sqlite3
import requests
import pdfplumber
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Setup logging to track the process
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Resolve relative paths based on the script location (scrapers/united24/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "master", "master.db")

BASE_URL = "https://u24.gov.ua/reports"


def get_latest_u24_date():
    """
    Retrieves the maximum date specifically for United24 records in the DB.
    Ensures that dates from other foundations do not interfere.
    """
    if not os.path.exists(DB_PATH):
        logging.warning(f"Database file not found at: {DB_PATH}")
        return datetime.min

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # Filter strictly by foundation_name to get the correct delta point
            cursor.execute("SELECT MAX(date) FROM donations WHERE foundation_name = 'united24'")
            res = cursor.fetchone()[0]
            if not res:
                return datetime.min

            # Handle standard ISO format (YYYY-MM-DD) or legacy formats
            date_fmt = '%Y-%m-%d' if '-' in res else '%d.%m.%Y'
            return datetime.strptime(res, date_fmt)
    except Exception as e:
        logging.error(f"Database check failed: {e}")
        return datetime.min


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
        time.sleep(5)  # Wait for JS to render the list

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        anchors = soup.find_all('a', href=True)

        pdf_links = []
        for a in anchors:
            href = a['href']
            # Target only report-specific PDF files
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

        # Parse date from filename (expected format: report-YYYYMMDD-category.pdf)
        date_match = re.search(r'(\d{8})', filename)
        if not date_match:
            continue

        file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
        category = os.path.splitext(filename.split('-')[-1])[0].lower()

        # Check if the report is worth downloading based on the date delta
        if file_date >= last_db_date:
            logging.info(f"Processing report: {filename}")
            try:
                # Download file to memory stream
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                parsed_rows = []
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    # Receipts are typically located on pages 1-21
                    for page in pdf.pages[:21]:
                        table = page.extract_table()
                        if not table:
                            continue

                        for row in table:
                            try:
                                # Validate row by checking for a date pattern in the first column
                                if not re.match(r'^\d{2}\.\d{2}\.\d{4}$', row[0]):
                                    continue

                                # Clean numeric strings: "25 261,00" -> 25261.0
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
                    with sqlite3.connect(DB_PATH) as conn:
                        cursor = conn.cursor()
                        # Load existing dates for this category to ensure zero duplicates
                        cursor.execute(
                            "SELECT date FROM donations WHERE foundation_name='united24' AND category=?",
                            (category,)
                        )
                        known_dates = {r[0] for r in cursor.fetchall()}

                        to_insert = [
                            (r['date'], r['amount'], 'UAH', 'united24', r['category'])
                            for r in parsed_rows if r['date'] not in known_dates
                        ]

                        if to_insert:
                            # Mapping based on master.db schema: date, amount, currency, foundation_name, category
                            query = "INSERT INTO donations (date, amount, currency, foundation_name, category) VALUES (?, ?, ?, ?, ?)"
                            cursor.executemany(query, to_insert)
                            conn.commit()
                            records_added += len(to_insert)

            except Exception as e:
                logging.error(f"Error processing {filename}: {e}")

    logging.info(f"Sync finalized. {records_added} new entries pushed to master.db.")


if __name__ == "__main__":
    run_smart_sync()