import json
import os
import sys
from contextlib import closing
from datetime import datetime
from typing import Generator, Tuple, Optional
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)  

import psycopg2
from transformers import pipeline

# Configure absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Local imports
from src.db_connection import database_connection
from src.function.function import (
    process_headlines_with_descriptions,
    analyze_sentiment_individually,
    get_latest_raw_news_id,  # ✅ Fix: This function needs (connection, cursor)
    insert_analysis_to_db,
    summarize_news,
    generate_unique_key,
    insert_crypto_analysis_data
)
from src.cache.redis_bloom import check_duplicate_analysis, add_to_analysis_bloom
from src.web_scrapping.scrape_news import fetch_content, SCRAPE_URL

# Initialize models once
MODELS = {
    "financial": pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone"),
    "social": pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")
}

# Constants
ANALYSIS_VERSION = "1.0"
SOURCE_ID = 1  # BBC source identifier


def process_news_items(connection, cursor) -> Generator[Tuple, None, None]:
    """Process news items from the database."""
    
    # ✅ Pass the URL to fetch_content()
    raw_text, source_url = fetch_content(SCRAPE_URL)
    
    if not raw_text:
        logger.warning("No raw content available for processing")
        return

    keywords_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../utils/cleaned_coin_keywords.json"
    )

    try:
        processed_items = process_headlines_with_descriptions(raw_text, keywords_path)
    except (FileNotFoundError, json.JSONDecodeError) as error:
        logger.error(f"Keyword processing failed: {error}")
        return

    # ✅ Fix: Pass both `connection` and `cursor`
    rec_raw_news_id = get_latest_raw_news_id(connection, cursor)
    
    if not rec_raw_news_id:
        logger.error("No valid raw news ID found")
        return

    for headline, description, matched_keywords in processed_items:
        content_hash = generate_unique_key(headline, source_url)
        
        if check_duplicate_analysis(content_hash):
            logger.debug(f"Skipping duplicate: {content_hash}")
            continue
            
        add_to_analysis_bloom(content_hash)
        
        try:
            analysis = analyze_sentiment_individually(
                f"{headline}. {description}", 
                MODELS["financial"], 
                MODELS["social"]
            )
            summary = summarize_news(headline, description)
        except Exception as error:
            logger.error(f"Analysis failed for {content_hash}: {error}")
            continue

        yield (
            rec_raw_news_id,
            json.dumps(matched_keywords),
            datetime.utcnow().isoformat(),
            summary,
            ANALYSIS_VERSION,
            json.dumps({
                "raw_text": f"{headline}. {description}",
                "sentiment_analysis": analysis
            }),
            SOURCE_ID,
            content_hash
        )


def run_predict() -> None:
    """Execute the prediction pipeline."""
    logger.info("Initializing sentiment analysis pipeline")
    
    try:
        with database_connection() as (connection, cursor):
            for record in process_news_items(connection, cursor):  # ✅ Fix: Pass both `connection` and `cursor`
                try:
                    new_analysis_id = insert_analysis_to_db(connection, cursor, record)
                    logger.info(f"Inserted analysis: {record[-1]}")
                    matched_coins_str = record[1]
                    insert_crypto_analysis_data(connection, cursor, record, new_analysis_id, matched_coins_str)
                except psycopg2.Error as error:
                    logger.error(f"Database insertion failed: {error}")
                    connection.rollback()
        
                    
    except Exception as error:
        logger.error(f"Prediction pipeline failed: {error}", exc_info=True)
        raise


if __name__ == "__main__":
    run_predict()
