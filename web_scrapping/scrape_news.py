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
logger = logging.getLogger(__name__)

# Configure absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Local imports
from src.db_connection import database_connection
from src.function.function import insert_news_to_db, generate_unique_key, insert_crypto_data
from src.cache.redis_bloom import check_duplicate_scrape, add_to_scrape_bloom

# Define sources with multiple URLs and corresponding selectors
SOURCES = [
    {
        "url": "https://www.bbc.com/",
        "source_id": 1,
        "name": "BBC",
        "selectors": {
            "headline": "h2",
            "description": "p"
        }
    },
    {
        "url": "https://www.globaltimes.cn/",
        "source_id": 2,
        "name": "GlobalTimes",
        "selectors": {
            "headline": "a.new_title_s",
            "description": "p"
        }
    }
]

def fetch_content(source: dict) -> Optional[Tuple[str, str]]:
    """Fetch and parse web content for the given source."""
    try:
        response = requests.get(source["url"], timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        logger.error(f"Request failed for {source['name']}: {error}")
        return None

    try:
        soup = BeautifulSoup(response.content, "html.parser")
        headlines = soup.select(source["selectors"]["headline"])
        descriptions = soup.select(source["selectors"]["description"])
        min_length = min(len(headlines), len(descriptions), 10)
        content = []
        for i in range(min_length):
            headline = headlines[i].get_text(strip=True)
            description = descriptions[i].get_text(strip=True)
            content.append(f"Headline: {headline}\nDescription: {description}")
        return "\n".join(content), source["url"]
    except Exception as error:
        logger.error(f"Content parsing failed for {source['name']}: {error}")
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

    return "\n".join(deduplicated)

def run_scraper() -> None:
    """Execute the web scraping pipeline for multiple sources."""
    logger.info("Initializing web scraper")

    try:
        with database_connection() as (conn, cursor):
            for source in SOURCES:
                logger.info(f"Scraping {source['name']}")
                content_data = fetch_content(source)
                if not content_data:
                    logger.info(f"Failed to fetch content for {source['name']}")
                    continue

                processed_content = process_content(*content_data)
                if not processed_content:
                    logger.info(f"No new content for {source['name']} after deduplication")
                    continue

                insert_news_to_db(
                    conn,
                    cursor,
                    processed_content,
                    content_data[1],
                    source["source_id"],
                    generate_unique_key(processed_content[:50], content_data[1])
                )
                logger.info(f"Inserted {len(processed_content.splitlines())} news items for {source['name']}")
                time.sleep(2)

            insert_crypto_data(conn, cursor)
            logger.info("Crypto data inserted")

        logger.info("Web scraping pipeline completed successfully.")
    except Exception as error:
        logger.error(f"Scraping pipeline failed: {error}", exc_info=True)
        raise

if __name__ == "__main__":
    run_scraper()
