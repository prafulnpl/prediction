#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime
from transformers import pipeline

# Append project root to import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db_connection import create_connection, close_connection
from src.function.function import (
    process_headlines_with_descriptions,
    analyze_sentiment_individually,
    get_latest_raw_news_id,
    insert_analysis_to_db,
    summarize_news,
    generate_unique_key, 
)
from src.web_scrapping.scrape_news import fetch_raw_content

# Import Bloom filter functions:
from src.cache.redis_bloom import check_duplicate_analysis, add_to_analysis_bloom

# Initialize models
print("Loading models...")
finbert_pipeline = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
twitter_pipeline = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")


def process_and_insert(connection, cursor):
    """
    1) Fetch headlines/descriptions via fetch_raw_content().
    2) Parse them with process_headlines_with_descriptions().
    3) For each headline/description, do a Bloom filter check on content hash.
    4) If new, analyze sentiment and insert into rec_news_analysis, storing rec_content_hash.
    """
    print("Fetching raw news content...")
    combined_text, source_url = fetch_raw_content()  
    if not combined_text:
        print("No raw news content found. Exiting...")
        return

    current_dir = os.path.dirname(os.path.abspath(__file__))
    keywords_file_path = os.path.join(current_dir, "../utils", "cleaned_coin_keywords.json")

    print("Processing headlines and descriptions...")
    try:
        items = process_headlines_with_descriptions(combined_text, keywords_file_path)
    except Exception as e:
        print(f"Error processing headlines with descriptions: {e}")
        return

    if not items:
        print("No relevant news content found.")
        return

    # Get the most recently inserted raw_news_id from the rec_raw_news table
    rec_raw_news_id = get_latest_raw_news_id(connection, cursor)
    print(f"Latest rec_raw_news_id: {rec_raw_news_id}")

    if not rec_raw_news_id:
        print("Error: No rec_raw_news_id found, skipping processing.")
        return

    # Prepare some shared fields
    analysis_version = "1.0"
    source = 1  # e.g., Source ID for BBC or similar
    analysis_date_time = datetime.utcnow().isoformat()

    # Iterate through each (headline, description, matched_keywords) tuple
    for idx, (headline, description, matched_keywords) in enumerate(items, start=1):
        print(f"\nProcessing Pair {idx}...")
        text = f"{headline}. {description}"
        print(f"Headline: {headline}")
        print(f"Description: {description}")

        # 1) Generate a hash for dedup:
        rec_content_hash = generate_unique_key(headline, source_url)
        
        # 2) Check Bloom filter:
        if check_duplicate_analysis(rec_content_hash):
            print(f"Duplicate found for hash={rec_content_hash}. Skipping insert for: {headline}")
            continue
        else:
            add_to_analysis_bloom(rec_content_hash)
            print(f"New hash={rec_content_hash}. Proceeding with insert for: {headline}")

        # 3) Analyze sentiment
        try:
            analysis_results = analyze_sentiment_individually(text, finbert_pipeline, twitter_pipeline)
            summary = summarize_news(headline, description)
        except Exception as e:
            print(f"Error in Sentiment Analysis for {headline}: {e}")
            continue  # Skip this record if sentiment analysis fails

        # 4) Prepare JSON metadata
        news_metadata = {
            "raw_text": text,
            "sentiment_analysis": analysis_results,
        }

        # 5) Prepare the insertion tuple (note the 8th param: rec_content_hash)
        values = (
            rec_raw_news_id,
            json.dumps(matched_keywords),
            analysis_date_time,
            summary,
            analysis_version,
            json.dumps(news_metadata),
            source,
            rec_content_hash
        )

        # 6) Actually insert into DB
        try:
            insert_analysis_to_db(connection, cursor, values)
            print(f"Processed Pair {idx}. Inserted with hash={rec_content_hash}")
        except Exception as e:
            print(f"Error inserting record for Pair {idx}: {e}")


def run_predict():
    """
    Main entry point for prediction logic.
    """
    print("Running Prediction...")
    connection, cursor = create_connection()
    try:
        process_and_insert(connection, cursor)
    except Exception as e:
        print(f"An error occurred during processing: {e}")
    finally:
        if cursor:
            print("Closing the cursor...")
            cursor.close()
        close_connection(connection, cursor)
        print("Database connection closed.")


if __name__ == "__main__":
    run_predict()
