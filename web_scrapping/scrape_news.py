#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import os
import sys

# Make sure Python can find your "src" modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db_connection import create_connection, close_connection
from src.function.function import (
    process_headlines_with_descriptions,
    insert_news_to_db,
    generate_unique_key
)
from src.cache.redis_bloom import check_duplicate_scrape, add_to_scrape_bloom


def fetch_raw_content():
    """
    Fetches raw content from BBC Business by combining headlines and descriptions.
    Returns a single combined text block and the source URL.
    """
    url = "https://www.bbc.com/news/articles/cnvqe3le3z4o"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch the URL: {url}, Status Code: {response.status_code}")
        return None, None

    soup = BeautifulSoup(response.content, "html.parser")

    # Extract headlines and descriptions
    headlines = soup.find_all("h2")
    descriptions = soup.find_all("p")

    # Combine all headlines and descriptions into a single text block
    combined_text = "\n".join(
        [
            f"Headline: {h.get_text(strip=True)}\nDescription: {d.get_text(strip=True)}"
            for h, d in zip(headlines, descriptions)
        ]
    )
    return combined_text, url


def run_scraper():
    """
    1) Establish DB connection
    2) Fetch raw content from BBC Business
    3) Split into (headline, description) pairs
    4) Deduplicate using the Bloom filter
    5) Insert deduplicated text into the database
    6) Perform further "matching" logic (process_headlines_with_descriptions)
    """
    # 1) Connect to PostgreSQL
    connection, cursor = create_connection()
    if not connection:
        print("Database connection failed. Exiting...")
        return

    try:
        # 2) Fetch raw content
        print("Fetching raw news content...")
        raw_text, source_url = fetch_raw_content()
        raw_source_id = 1  # e.g. 1 => BBC

        if not raw_text:
            print("No content fetched to insert.")
            return

        # 3) Split lines into (headline, description) pairs
        lines = [line.strip() for line in raw_text.split("\n") if line.strip()]

        deduplicated_lines = []
        for i in range(0, len(lines), 2):
            if i + 1 >= len(lines):
                break

            headline_line = lines[i].replace("Headline:", "").strip()
            description_line = lines[i + 1].replace("Description:", "").strip()

            # 4) Generate hash + check Bloom
            rec_content_hash = generate_unique_key(headline_line, source_url)
            if check_duplicate_scrape(rec_content_hash):
                print(f"Duplicate found (Bloom filter). Skipping: {headline_line}")
                continue
            else:
                # If new, add to Bloom + store lines
                add_to_scrape_bloom(rec_content_hash)
                deduplicated_lines.append(
                    f"Headline: {headline_line}\nDescription: {description_line}"
                )

        # Combine deduplicated lines back into a single block
        deduplicated_text = "\n".join(deduplicated_lines)

        if deduplicated_text:
            print("Inserting deduplicated raw news content into the database...")
            insert_news_to_db(connection, cursor, deduplicated_text, source_url, raw_source_id,rec_content_hash)

            # 6) Existing logic: "matching" with keywords
            print("\nProcessing matching logic...")
            current_dir = os.path.dirname(os.path.abspath(__file__))
            keywords_file_path = os.path.join(current_dir, "..", "utils", "cleaned_coin_keywords.json")

            processed_data = process_headlines_with_descriptions(deduplicated_text, keywords_file_path)
            if processed_data:
                print("\nProcessed Data:")
                for headline, description, matches in processed_data:
                    print(f"- Headline: {headline}")
                    print(f"  Description: {description}")
                    print(f"  Matches: {matches}")
            else:
                print("\nNo matching data found.")
        else:
            print("No new (unique) lines to insert after deduplication.")

    except Exception as main_error:
        print(f"An error occurred: {main_error}")

    finally:
        close_connection(connection, cursor)


if __name__ == "__main__":
    run_scraper()
