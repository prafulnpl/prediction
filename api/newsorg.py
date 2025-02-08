#!/usr/bin/env python3
"""
News API Fetching, Sentiment Analysis, and Database Insertion Script.
"""

import requests
import sys
import os
import json
import logging
from datetime import datetime
from transformers import pipeline
import time

# Append project root to import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)  # ✅ Define logger for this module

# Import database connection and utility functions
from src.db_connection import database_connection  # ✅ Corrected import
from src.function.function import (
    summarize_news,
    match_keywords_for_article,
    analyze_sentiment_individually,  
    insert_api_news_to_db,  
    generate_unique_key, 
    insert_crypto_analysis_data 
)
from src.cache.redis_bloom import check_duplicate_analysis, add_to_analysis_bloom

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define the NewsAPI endpoint and parameters
API_URL = "https://newsapi.org/v2/everything"
PARAMS = {
    "q": "finance OR business OR cryptocurrency OR economy OR culture OR technology OR science",
    "from": "2024-02-06",
    "sortBy": "popularity",
    "apiKey": "9a3c02b0a04c43409d379b41de50b3e9",  # Replace with your API key
}

# Initialize sentiment analysis models
logging.info("Loading sentiment analysis models...")
finbert_pipeline = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
twitter_pipeline = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")


def fetch_and_insert_news_with_sentiment_analysis():
    """
    Fetches news articles using the NewsAPI, performs sentiment analysis,
    generates summaries, matches keywords, and inserts relevant news into the database.
    """

    logging.info("Fetching news from NewsAPI...")
    response = requests.get(API_URL, params=PARAMS)

    if response.status_code != 200:
        logging.error(f"Failed to fetch news. Status Code: {response.status_code}")
        logging.error(f"Response: {response.text}")
        return

    data = response.json()
    articles = data.get("articles", [])
    logging.info(f"Total Results: {data.get('totalResults')}")

    # ✅ Use database_connection() correctly
    with database_connection() as (connection, cursor):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        keywords_file_path = os.path.join(current_dir, "../utils/cleaned_coin_keywords.json")

        for idx, article in enumerate(articles, start=1):
            title = article.get("title", "")
            description = article.get("description", "")
            source = article.get("source", {}).get("name", "")
            published_at = article.get("publishedAt", "")
            article_url = article.get("url", "")

            if not title or not description:
                logging.warning(f"Skipping Article {idx}: Missing title or description.")
                continue

            logging.info(f"Processing Article {idx}: {title}")

            try:
                matched_keywords = match_keywords_for_article(title, description, keywords_file_path)
                source_id = 2  # NewsAPI source identifier
            except Exception as e:
                logging.error(f"Error matching keywords for Article {idx}: {e}")
                matched_keywords = []

            if not matched_keywords:
                logging.info(f"Skipping Article {idx}: No relevant keywords matched.")
                continue

            try:
                summary = summarize_news(title, description)
            except Exception as e:
                logging.error(f"Error generating summary for Article {idx}: {e}")
                summary = f"{title}. {description}"  # Fallback summary

            try:
                combined_text = f"{title}. {description}"
                analysis_results = analyze_sentiment_individually(combined_text, finbert_pipeline, twitter_pipeline)
            except Exception as e:
                logging.error(f"Error performing sentiment analysis for Article {idx}: {e}")
                analysis_results = {}

            try:
                rec_content_hash = generate_unique_key(title, article_url)
            except Exception as e:
                logging.error(f"Error generating hash for Article {idx}: {e}")
                rec_content_hash = ""

            if not rec_content_hash:
                logging.warning(f"Skipping Article {idx}: Could not generate content hash.")
                continue

            try:
                if check_duplicate_analysis(rec_content_hash):
                    logging.info(f"Duplicate found (Bloom filter). Skipping Article {idx}: {title}")
                    continue  
                add_to_analysis_bloom(rec_content_hash)
            except Exception as e:
                logging.error(f"Error checking/inserting into Bloom filter for Article {idx}: {e}")
                continue  

            analysis_date_time = datetime.utcnow().isoformat()
            analysis_version = "1.0"
            news_metadata = {
                "title": title,
                "description": description,
                "source": source,
                "published_at": published_at,
                "url": article_url,
                "matched_keywords": matched_keywords,
                "sentiment_analysis": analysis_results,  
            }

            values = (
                json.dumps(matched_keywords),   
                analysis_date_time,              
                summary,                         
                analysis_version,                
                json.dumps(news_metadata),       
                source_id,                       
                rec_content_hash,                
            )

            try:
                new_analysis_id=insert_api_news_to_db(connection, cursor, values)
                logging.info(f"Article {idx} inserted successfully.")
                print(json.dumps(matched_keywords))
                print(values)
                insert_crypto_analysis_data(connection, cursor, values, new_analysis_id , json.dumps(matched_keywords))
            except Exception as e:
                logging.error(f"Error inserting Article {idx}: {e}")




if __name__ == "__main__":
    fetch_and_insert_news_with_sentiment_analysis()
