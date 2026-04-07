import os
import sys
import time
import json
import logging
import pandas as pd
from bs4 import BeautifulSoup
import cloudscraper
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path

# Configure logging to output to stderr to prevent interference with Airflow XCom stdout captures
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Environment setup
BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_DIR / '.env'
load_dotenv(dotenv_path=ENV_PATH)

# Dynamic BigQuery credentials path
bq_key_path = BASE_DIR / 'keys' / 'bq_key.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(bq_key_path)


def get_db_engine():
    """Initialize and return the SQLAlchemy engine."""
    pg_uri = os.getenv("DATABASE_URL")
    if not pg_uri:
        raise ValueError(f"DATABASE_URL not found at: {ENV_PATH}")

    if pg_uri.startswith("postgres://"):
        pg_uri = pg_uri.replace("postgres://", "postgresql://", 1)

    return create_engine(pg_uri)


def get_latest_news_date(engine):
    """Fetch the latest date from the Postgres database."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT to_regclass('public.news');")).scalar()
            if not result:
                return "2025-01-01"

            max_date = conn.execute(text("SELECT MAX(date) FROM news")).scalar()
            return str(max_date) if max_date else "2025-01-01"
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        raise


def fetch_article_headline(url, scraper):
    """Scrape headline using cloudscraper. Returns None on failure."""
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

        return None
    except Exception as e:
        logger.error(f"Request failed for {url}: {e}")
        return None


def run_automated_pipeline():
    engine = get_db_engine()
    last_date = get_latest_news_date(engine)
    logger.info(f"Latest news date: {last_date}. Fetching from BigQuery...")

    try:
        bq_client = bigquery.Client()
    except Exception as e:
        logger.error("BigQuery auth failed.")
        raise e

    # Query BigQuery using parameters for safety against SQL injection
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("last_date", "STRING", last_date),
        ]
    )

    query = """
    SELECT
      FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(date AS STRING))) AS event_date,
      LOWER(SourceCommonName) AS source,
      DocumentIdentifier AS url
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE
      _PARTITIONTIME >= TIMESTAMP(@last_date)
      AND FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(date AS STRING))) > @last_date
      AND (V2Themes LIKE '%UKRAINE%' OR V2Themes LIKE '%UKR%')
      AND (V2Themes LIKE '%WAR%' OR V2Themes LIKE '%CONFLICT%' OR V2Themes LIKE '%MILITARY%')
      AND LOWER(SourceCommonName) IN ('theguardian.com', 'kyivindependent.com')
      AND TranslationInfo IS NULL
    ORDER BY event_date DESC
    """

    logger.info("Executing query...")
    df = bq_client.query(query, job_config=job_config).to_dataframe()

    if df.empty:
        logger.info("No new articles found. Exiting.")
        print(0, flush=True)
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

    # Error cleanup: Drop rows where headline is None
    initial_count = len(news)
    news = news.dropna(subset=['headers'])
    logger.info(f"Removed {initial_count - len(news)} rows due to request errors or missing headers.")

    if news.empty:
        logger.warning("No valid headlines. Exiting.")
        print(0, flush=True)
        return

    # Data aggregation
    logger.info("Aggregating data...")
    news_final = news.groupby(['event_date', 'source'], as_index=False).agg({'headers': list})
    news_export = news_final.rename(columns={'event_date': 'date'})

    # Serialize lists to valid JSON strings for PostgreSQL compatibility
    news_export['headers'] = news_export['headers'].apply(json.dumps)

    # Database export
    logger.info("Pushing to PostgreSQL via direct SQLAlchemy execution...")
    try:
        with engine.begin() as conn:
            # Convert DataFrame to a list of dictionaries for bulk insert
            records = news_export.to_dict(orient='records')

            if records:
                insert_stmt = text(
                    "INSERT INTO news (date, source, headers) VALUES (:date, :source, :headers)"
                )
                conn.execute(insert_stmt, records)

        rows_added = len(news_export)
        logger.info(f"SUCCESS: {rows_added} rows added.")

        # Print value for Airflow XCom with forced flush
        print(rows_added, flush=True)

    except Exception as e:
        logger.error(f"Database export failed: {e}")
        raise


if __name__ == "__main__":
    run_automated_pipeline()