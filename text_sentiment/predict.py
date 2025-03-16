#!/usr/bin/env python3
"""
Sentiment prediction module for news articles.
This module fetches news from multiple sources (e.g., BBC, Global Times),
processes the content (either via headline/description splitting or full-content processing),
performs sentiment analysis using two models, and inserts the results into the database.
"""

import json
import os
import sys
from datetime import datetime
from typing import Generator, Tuple
import logging

import psycopg2
from transformers import pipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Adjust sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Local imports
from src.db_connection import database_connection
from src.function.function import (
    process_headlines_with_descriptions,
    analyze_sentiment_individually,
    get_latest_raw_news_id,
    insert_analysis_to_db,
    summarize_news,
    generate_unique_key,
    insert_crypto_analysis_data
)
from src.cache.redis_bloom import check_duplicate_analysis, add_to_analysis_bloom
from src.web_scrapping.scrape_news import SOURCES, fetch_content, process_content

# Initialize sentiment analysis models
MODELS = {
    "financial": pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone"),
    "social": pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")
}

# Constants
ANALYSIS_VERSION = "1.0"
DEFAULT_SOURCE_ID = 1  # Fallback source identifier if not provided in the source config

def process_news_items(connection, cursor) -> Generator[Tuple, None, None]:
    """
    Process news items from all defined sources for sentiment analysis.
    For each source, try to process using headlines and descriptions. If that fails,
    fall back to full-content processing.
    """
    for source in SOURCES:
        logger.info(f"Fetching news for source: {source['name']}")
        content_data = fetch_content(source)
        if not content_data:
            logger.warning(f"Failed to fetch content for {source['name']}")
            continue

        raw_text, source_url = content_data

        # Define path to keywords (if needed by process_headlines_with_descriptions)
        keywords_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../utils/cleaned_coin_keywords.json"
        )

        processed = False
        # Try processing headlines and descriptions (this may work well for structured sources like BBC)
        try:
            processed_items = process_headlines_with_descriptions(raw_text, keywords_path)
            if processed_items:
                rec_raw_news_id = get_latest_raw_news_id(connection, cursor)
                if not rec_raw_news_id:
                    logger.error("No valid raw news ID found")
                    continue
                for headline, description, matched_keywords in processed_items:
                    content_hash = generate_unique_key(headline, source_url)
                    if check_duplicate_analysis(content_hash):
                        logger.debug(f"Skipping duplicate: {content_hash}")
                        continue
                    add_to_analysis_bloom(content_hash)
                    try:
                        # Perform sentiment analysis on the combined headline and description
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
                        source.get("id", DEFAULT_SOURCE_ID),
                        content_hash
                    )
                processed = True
        except Exception as e:
            logger.error(f"Error processing headlines for {source['name']}: {e}")

        # If headlines processing did not return any results, fall back to full-content processing.
        if not processed:
            logger.info(f"Falling back to full-content processing for {source['name']}")
            processed_text = process_content(raw_text, source_url)
            if not processed_text:
                logger.info(f"No processed content available for {source['name']} after deduplication")
                continue
            rec_raw_news_id = get_latest_raw_news_id(connection, cursor)
            if not rec_raw_news_id:
                logger.error("No valid raw news ID found")
                continue

            content_hash = generate_unique_key(processed_text, source_url)
            if check_duplicate_analysis(content_hash):
                logger.debug(f"Skipping duplicate: {content_hash}")
                continue
            add_to_analysis_bloom(content_hash)
            try:
                analysis = analyze_sentiment_individually(
                    processed_text,
                    MODELS["financial"],
                    MODELS["social"]
                )
                # In fallback, we may only have one string so we pass an empty description
                summary = summarize_news(processed_text, "")
            except Exception as error:
                logger.error(f"Analysis failed for {content_hash}: {error}")
                continue

            yield (
                rec_raw_news_id,
                json.dumps([]),
                datetime.utcnow().isoformat(),
                summary,
                ANALYSIS_VERSION,
                json.dumps({
                    "raw_text": processed_text,
                    "sentiment_analysis": analysis
                }),
                source.get("id", DEFAULT_SOURCE_ID),
                content_hash
            )

def run_predict() -> None:
    """Execute the full prediction pipeline."""
    logger.info("Initializing sentiment analysis pipeline")
    try:
        with database_connection() as (connection, cursor):
            for record in process_news_items(connection, cursor):
                try:
                    logger.info(f"Processing record with hash: {record[-1]}")
                    new_analysis_id = insert_analysis_to_db(connection, cursor, record)
                    logger.info(f"Inserted analysis record: {new_analysis_id}")
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
