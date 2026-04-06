import os
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
import cloudscraper
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Environment setup
BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / '.env'
load_dotenv(dotenv_path=ENV_PATH)

# Dynamic BigQuery credentials path
bq_key_path = BASE_DIR / 'keys' / 'bq_key.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(bq_key_path)

PG_URI = os.getenv("DATABASE_URL")

if not PG_URI:
    raise ValueError(f"DATABASE_URL not found at: {ENV_PATH}")

if PG_URI.startswith("postgres://"):
    PG_URI = PG_URI.replace("postgres://", "postgresql://", 1)

ENGINE = create_engine(PG_URI)


def get_latest_news_date():
    """Fetch the latest date from the Postgres database."""
    try:
        with ENGINE.connect() as conn:
            result = conn.execute(text("SELECT to_regclass('public.news');")).scalar()
            if not result:
                return "2025-01-01"

            max_date = conn.execute(text("SELECT MAX(date) FROM news")).scalar()
            return str(max_date) if max_date else "2025-01-01"
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        raise


def fetch_article_headline(url, scraper):
    """Scrape headline using cloudscraper."""
    try:
        response = scraper.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return og_title["content"].strip()

        h1_tag = soup.find("h1")
        if h1_tag:
            return h1_tag.get_text(strip=True)

        if soup.title:
            return soup.title.get_text(strip=True)

        return "Headline not found"
    except Exception as e:
        return f"Request Error: {e}"


def run_automated_pipeline():
    last_date = get_latest_news_date()
    logger.info(f"Latest news date: {last_date}. Fetching from BigQuery...")

    try:
        bq_client = bigquery.Client()
    except Exception as e:
        logger.error("BigQuery auth failed.")
        raise e

    # Query BigQuery
    query = f"""
    SELECT
      FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(date AS STRING))) AS event_date,
      LOWER(SourceCommonName) AS source,
      DocumentIdentifier AS url
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE
      _PARTITIONTIME >= TIMESTAMP("{last_date}")
      AND FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(date AS STRING))) > "{last_date}"
      AND (V2Themes LIKE '%UKRAINE%' OR V2Themes LIKE '%UKR%')
      AND (V2Themes LIKE '%WAR%' OR V2Themes LIKE '%CONFLICT%' OR V2Themes LIKE '%MILITARY%')
      AND LOWER(SourceCommonName) IN ('theguardian.com', 'kyivindependent.com')
      AND TranslationInfo IS NULL
    ORDER BY event_date DESC
    """

    logger.info("Executing query...")
    df = bq_client.query(query).to_dataframe()

    if df.empty:
        logger.info("No new articles found. Exiting.")
        print(0)
        return

    # Drop duplicate URLs
    news = df.drop_duplicates(subset=['url']).copy()
    logger.info(f"Found {len(news)} new articles.")

    # Scraper setup
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

    logger.info("Extracting headlines...")
    headlines = []
    for idx, url in enumerate(news['url']):
        if idx % 10 == 0 and idx > 0:
            logger.info(f"Processed {idx}/{len(news)} URLs...")
        headlines.append(fetch_article_headline(url, scraper))
        time.sleep(0.5)

    news['headers'] = headlines

    # Error cleanup
    initial_count = len(news)
    news = news[~news['headers'].str.contains('Error', na=False)]
    logger.info(f"Removed {initial_count - len(news)} rows due to errors.")

    if news.empty:
        logger.warning("No valid headlines. Exiting.")
        print(0)
        return

    # Data aggregation
    logger.info("Aggregating data...")
    news_final = news.groupby(['event_date', 'source'], as_index=False).agg({'headers': list})
    news_export = news_final.rename(columns={'event_date': 'date'})
    news_export['headers'] = news_export['headers'].astype(str)

    # Database export
    logger.info("Pushing to PostgreSQL...")
    try:
        with ENGINE.begin() as conn:
            news_export.to_sql('news', conn, if_exists='append', index=False)

        rows_added = len(news_export)
        logger.info(f"SUCCESS: {rows_added} rows added.")

        # Print value for Airflow XCom
        print(rows_added)

    except Exception as e:
        logger.error(f"Database export failed: {e}")
        raise


if __name__ == "__main__":
    run_automated_pipeline()
