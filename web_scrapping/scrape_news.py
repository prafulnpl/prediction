#!/usr/bin/env python3
"""Web scraping and content processing module."""
import os
import sys
from contextlib import closing
from typing import Optional, Tuple
import logging
import requests
from bs4 import BeautifulSoup
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)  # Define logger for this module

# Configure absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Local imports
from src.db_connection import database_connection
from src.function.function import insert_news_to_db, generate_unique_key, insert_crypto_data
from src.cache.redis_bloom import check_duplicate_scrape, add_to_scrape_bloom

# Constants
SCRAPE_URL = "https://www.bbc.com/business"
SOURCE_ID = 1  # BBC source identifier


def fetch_content(url: str) -> Optional[Tuple[str, str]]:
    """Fetch and parse web content."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        logger.error(f"Request failed: {error}")
        return None

    try:
        soup = BeautifulSoup(response.content, "html.parser")
        elements = zip(
            soup.find_all("h2", limit=10),
            soup.find_all("p", limit=10)
        )
        content = "\n".join(
            f"Headline: {h.get_text(strip=True)}\nDescription: {d.get_text(strip=True)}"
            for h, d in elements
        )
        return content, url
    except Exception as error:
        logger.error(f"Content parsing failed: {error}")
        return None


def process_content(raw_text: str, source_url: str) -> str:
    """Process and deduplicate content."""
    deduplicated = []
    lines = [line.strip() for line in raw_text.split("\n") if line.strip()]

    for i in range(0, len(lines), 2):
        if i + 1 >= len(lines):
            continue

        headline = lines[i].replace("Headline:", "").strip()
        description = lines[i + 1].replace("Description:", "").strip()
        content_hash = generate_unique_key(headline, source_url)

        if not check_duplicate_scrape(content_hash):
            add_to_scrape_bloom(content_hash)
            deduplicated.append(f"Headline: {headline}\nDescription: {description}")

            time.sleep(1)

    return "\n".join(deduplicated)


def run_scraper() -> None:
    """Execute the web scraping pipeline."""
    logger.info("Initializing web scraper")

    try:
        content_data = fetch_content(SCRAPE_URL)
        if not content_data:
            return

        processed_content = process_content(*content_data)
        if not processed_content:
            logger.info("No new content after deduplication")
            return

        with database_connection() as (conn, cursor):
            insert_news_to_db(
                conn,
                cursor,
                processed_content,
                content_data[1],
                SOURCE_ID,
                generate_unique_key(processed_content[:50], content_data[1])
            )
            logger.info(f"Inserted {len(processed_content.splitlines())} news items")

            # Call your second function here
            insert_crypto_data(conn, cursor)
            logger.info("Crypto data inserted")

    except Exception as error:
        logger.error(f"Scraping pipeline failed: {error}", exc_info=True)
        raise


if __name__ == "__main__":
    run_scraper()
