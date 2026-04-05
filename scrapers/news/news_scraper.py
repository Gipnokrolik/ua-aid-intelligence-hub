import os
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
import cloudscraper
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Environment Configuration
# Tech Lead Note: Using absolute path to ensure .env is loaded correctly in Airflow/WSL contexts
ENV_PATH = '/mnt/h/ua-aid-intelligence-hub/.env'
load_dotenv(dotenv_path=ENV_PATH)

PG_URI = os.getenv("DATABASE_URL")

if not PG_URI:
    raise ValueError("DATABASE_URL not found in environment variables. Please check your .env file.")

if PG_URI.startswith("postgres://"):
    PG_URI = PG_URI.replace("postgres://", "postgresql://", 1)

ENGINE = create_engine(PG_URI)


def get_latest_news_date():
    """Fetches the latest date we have in the Postgres database."""
    try:
        with ENGINE.connect() as conn:
            # Check if table exists first
            result = conn.execute(text("SELECT to_regclass('public.news');")).scalar()
            if not result:
                return "2025-01-01"

            # Get max date
            max_date = conn.execute(text("SELECT MAX(date) FROM news")).scalar()
            return str(max_date) if max_date else "2025-01-01"
    except Exception as e:
        logger.error(f"Failed to check database: {e}")
        return "2025-01-01"


def fetch_article_headline(url, scraper):
    """Scrapes the main headline using cloudscraper to bypass anti-bot."""
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
    logger.info(f"Latest news in database is from: {last_date}. Fetching new records from BigQuery...")

    # Initialize BigQuery Client
    try:
        bq_client = bigquery.Client()
    except Exception as e:
        logger.error("BigQuery authentication failed. Ensure GOOGLE_APPLICATION_CREDENTIALS is set.")
        print(0)  # For Telegram Bot XCom
        raise e

    # 1. Fetch from BigQuery
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

    logger.info("Executing BigQuery...")
    df = bq_client.query(query).to_dataframe()

    if df.empty:
        logger.info("No new articles found in BigQuery since last update. We are up to date!")
        print(0)  # For Telegram Bot XCom
        return

    # Drop duplicate URLs
    news = df.drop_duplicates(subset=['url']).copy()
    logger.info(f"Found {len(news)} new unique articles to process.")

    # 2. Setup Scraper
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

    # 3. Fetch Headlines
    logger.info("Starting headline extraction...")
    headlines = []
    for idx, url in enumerate(news['url']):
        if idx % 10 == 0 and idx > 0:
            logger.info(f"Processed {idx}/{len(news)} URLs...")

        headlines.append(fetch_article_headline(url, scraper))
        time.sleep(0.5)

    news['headers'] = headlines

    # 4. Clean up errors
    initial_count = len(news)
    news = news[~news['headers'].str.contains('Error', na=False)]
    logger.info(f"Removed {initial_count - len(news)} rows due to extraction errors.")

    if news.empty:
        logger.warning("No valid headlines were extracted. Exiting.")
        print(0)  # For Telegram Bot XCom
        return

    # 5. Aggregate by Date and Source
    logger.info("Aggregating daily headlines...")
    news_final = news.groupby(['event_date', 'source'], as_index=False).agg({'headers': list})

    news_export = news_final.rename(columns={'event_date': 'date'})
    news_export['headers'] = news_export['headers'].astype(str)

    # 6. Push to PostgreSQL
    logger.info("Pushing data to PostgreSQL master database...")
    rows_added = 0
    try:
        # Tech Lead Note: Use context manager with begin() to handle transaction and fix 'cursor' error in SQLAlchemy 2.0
        with ENGINE.begin() as conn:
            news_export.to_sql('news', conn, if_exists='append', index=False)
        rows_added = len(news_export)
        logger.info(f"--- SUCCESS: {rows_added} new daily summaries added to 'news' table! ---")
    except Exception as e:
        logger.error(f"Database export failed: {e}")

    # Final numeric output for Airflow XCom and Telegram Bot
    print(rows_added)


if __name__ == "__main__":
    run_automated_pipeline()
