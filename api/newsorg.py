#!/usr/bin/env python3
import requests
import sys
import os
from datetime import datetime
import json
from transformers import pipeline

# Append project root to import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import database connection and utility functions
from src.db_connection import create_connection, close_connection
from src.function.function import (
    summarize_news,
    match_keywords_for_article,
    analyze_sentiment_individually,  # Add sentiment analysis function
    insert_api_news_to_db,  # New function for inserting API news
    generate_unique_key,  # Function to generate unique hash
)
from src.cache.redis_bloom import check_duplicate_analysis, add_to_analysis_bloom

# Define the NewsAPI endpoint and parameters
api_url = "https://newsapi.org/v2/everything"
params = {
    "q": "finance OR business OR cryptocurrency OR economy",  # Query terms
    "from": "2025-01-20",  # Date from which to retrieve articles
    "sortBy": "popularity",  # Sort articles by popularity
    "apiKey": "9a3c02b0a04c43409d379b41de50b3e9",  # Replace with your actual NewsAPI key
}

# Initialize sentiment analysis models
# print("Loading models...")
finbert_pipeline = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
twitter_pipeline = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")


def fetch_and_insert_news_with_sentiment_analysis():
    """
    Fetches news articles using the NewsAPI, performs sentiment analysis,
    generates summaries, matches keywords, and inserts relevant news into the database
    while ensuring deduplication using RedisBloom.
    """
    # Make the GET request to NewsAPI
    response = requests.get(api_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        articles = data.get("articles", [])
        print(f"Total Results: {data.get('totalResults')}\n")

        # Connect to the database
        connection, cursor = create_connection()

        try:
            # Load the keywords for matching
            current_dir = os.path.dirname(os.path.abspath(__file__))
            keywords_file_path = os.path.join(current_dir, "../utils/cleaned_coin_keywords.json")

            # Loop through the articles and process each one
            for idx, article in enumerate(articles, start=1):
                title = article.get("title", "")
                description = article.get("description", "")
                source = article.get("source", {}).get("name", "")
                published_at = article.get("publishedAt", "")
                article_url = article.get("url", "")

                if not title or not description:
                    print(f"Skipping Article {idx}: Missing title or description.")
                    continue

                print(f"\nProcessing Article {idx}:")
                print(f"Title: {title}")
                print(f"Description: {description}")

                # Match keywords using the provided function
                try:
                    matched_keywords = match_keywords_for_article(title, description, keywords_file_path)
                    source_id = 2  # This ID represents that the data source is from the API
                except Exception as e:
                    print(f"Error matching keywords for Article {idx}: {e}")
                    matched_keywords = []

                # Skip articles with no matched keywords
                if not matched_keywords:
                    print(f"Skipping Article {idx}: No relevant keywords matched.")
                    continue

                # Generate a summary for the title and description
                try:
                    summary = summarize_news(title, description)
                except Exception as e:
                    print(f"Error generating summary for Article {idx}: {e}")
                    summary = f"{title}. {description}"  # Fallback: Use title and description

                # Perform sentiment analysis on the combined text
                try:
                    combined_text = f"{title}. {description}"
                    analysis_results = analyze_sentiment_individually(combined_text, finbert_pipeline, twitter_pipeline)
                except Exception as e:
                    print(f"Error performing sentiment analysis for Article {idx}: {e}")
                    analysis_results = {}

                # Generate a unique hash for deduplication
                try:
                    rec_content_hash = generate_unique_key(title, article_url)
                except Exception as e:
                    print(f"Error generating hash for Article {idx}: {e}")
                    rec_content_hash = ""

                if not rec_content_hash:
                    print(f"Skipping Article {idx}: Could not generate content hash.")
                    continue

                # Check RedisBloom for duplication
                try:
                    if check_duplicate_analysis(rec_content_hash):
                        print(f"Duplicate found (Bloom filter). Skipping Article {idx}: {title}")
                        continue  # Skip insertion
                    else:
                        # If it's new, add to Bloom filter
                        add_to_analysis_bloom(rec_content_hash)
                except Exception as e:
                    print(f"Error checking/inserting into Bloom filter for Article {idx}: {e}")
                    continue  # Skip insertion if Bloom filter fails

                # Prepare metadata for insertion
                analysis_date_time = datetime.utcnow().isoformat()
                analysis_version = "1.0"
                news_metadata = {
                    "title": title,
                    "description": description,
                    "source": source,
                    "published_at": published_at,
                    "url": article_url,
                    "matched_keywords": matched_keywords,
                    "sentiment_analysis": analysis_results,  # Include sentiment analysis results
                }

                values = (
                    json.dumps(matched_keywords),    # Matched keywords (stored as JSON)
                    analysis_date_time,               # Current timestamp for the analysis
                    summary,                          # Generated summary
                    analysis_version,                 # Analysis version (e.g., "1.0")
                    json.dumps(news_metadata),        # Full news metadata (JSON)
                    source_id,                        # Data source identifier
                    rec_content_hash,                 # Unique content hash for deduplication
                )

                # Insert into the database using the new function
                try:
                    insert_api_news_to_db(connection, cursor, values)
                    print(f"Article {idx} inserted successfully.")
                except Exception as e:
                    print(f"Error inserting Article {idx}: {e}")

        finally:
            # Close the database connection
            if cursor:
                cursor.close()
            close_connection(connection, cursor)
            print("Database connection closed.")
    else:
        print("Failed to retrieve news articles.")
        print(f"Status Code: {response.status_code}")
        print(f"Error Response: {response.text}")


# Main function
if __name__ == "__main__":
    fetch_and_insert_news_with_sentiment_analysis()
