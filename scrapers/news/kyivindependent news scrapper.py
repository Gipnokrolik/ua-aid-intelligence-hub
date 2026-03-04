import sqlite3
import requests
import logging
from datetime import datetime, timedelta
from pathlib import Path
import time

# Logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Paths (standard for your project)
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "news" / "war_news_gdelt.db"


def fetch_gdelt_news(query="Ukraine war", days_back=7):
    """
    Uses GDELT Doc API to get news articles.
    Mode: artlist (returns list of articles)
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # GDELT DOC API URL
    # format=json, sort=datedesc
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": 250,  # Max per request
        "sort": "datedesc"
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        articles = data.get('articles', [])

        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gdelt_headlines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                title TEXT,
                url TEXT,
                source TEXT,
                UNIQUE(date, title)
            )
        ''')

        new_count = 0
        for art in articles:
            # GDELT date format: "20250304T103000Z"
            raw_date = art['seendate']
            clean_date = datetime.strptime(raw_date, '%Y%m%dT%H%M%SZ').strftime('%Y-%m-%d')

            cursor.execute('''
                INSERT OR IGNORE INTO gdelt_headlines (date, title, url, source)
                VALUES (?, ?, ?, ?)
            ''', (clean_date, art['title'], art['url'], art['sourceurl']))

            if cursor.rowcount > 0:
                new_count += 1

        conn.commit()
        conn.close()
        logger.info(f"GDELT Sync: Added {new_count} unique articles.")

    except Exception as e:
        logger.error(f"GDELT API failed: {e}")


if __name__ == "__main__":
    fetch_gdelt_news()