import re
import json
from collections import defaultdict
import hashlib
from function.api import fetch_coingecko_markets_tor, fetch_coingecko_trending_tor,fetch_coingecko_keyword_data
import time
# from src.cache.redis_bloom import check_duplicate, add_to_bloom_filter






def generate_unique_key(headline: str, source_url: str) -> str:
    """
    Generates a hash key based on the headline + source_url, or any other unique combo you like.
    """
    unique_string = f"{headline}_{source_url}"
    return hashlib.sha256(unique_string.encode("utf-8")).hexdigest()


def extract_relevant_sentences(raw_text, keywords_file_path):
    """
    Loads coin keywords from a JSON file and matches them with the raw_text.
    Returns:
        - matching_sentences: List of (sentence, [category:keyword, ...]).
        - keywords_dict: Original dictionary loaded from JSON.
    """
# Load keywords from JSON
    try:
        with open(keywords_file_path, "r") as f:
            keywords_dict = json.load(f)
            if not isinstance(keywords_dict, dict):
                raise ValueError("Invalid JSON structure. Expected a dictionary of category: [keywords].")
    except Exception as e:
        raise ValueError(f"Error loading keywords from {keywords_file_path}: {e}")

    # Split raw_text into sentences
    sentences = [sentence.strip() for sentence in raw_text.split('.') if sentence.strip()]
    matching_sentences = []

    for sentence in sentences:
        category_matches = defaultdict(list)

        # Check each sentence against each keyword in every category
        for category, kw_list in keywords_dict.items():
            for kw in kw_list:
                pattern = re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
                if pattern.search(sentence):
                    category_matches[category].append(kw)

        if category_matches:
            # Sort matches by priority (frequency of keyword under each category)
            sorted_matches = []
            for category, keywords in category_matches.items():
                keyword_counts = defaultdict(int)
                for kw in keywords:
                    keyword_counts[kw] += 1
                sorted_matches.extend([f"{category}:{kw}" for kw in sorted(keyword_counts, key=keyword_counts.get, reverse=True)])

            matching_sentences.append((sentence, sorted_matches))

    return matching_sentences, keywords_dict


def process_headlines_with_descriptions(combined_text, keywords_file_path):
    """
    Processes raw text into a list of (headline, description, [category:keyword, ...]) tuples.
    """
    pairs = []
    # Split text into headline-description pairs
    lines = [line.strip() for line in combined_text.split("\n") if line.strip()]
    for i in range(0, len(lines), 2):
        if i + 1 < len(lines):
            headline = lines[i].replace("Headline:", "").strip()
            description = lines[i + 1].replace("Description:", "").strip()
            text = f"{headline}. {description}"
            matching_sentences, _ = extract_relevant_sentences(text, keywords_file_path)
            if matching_sentences:
                all_matches = []
                for _, category_keywords in matching_sentences:
                    all_matches.extend(category_keywords)
                pairs.append((headline, description, list(set(all_matches))))
    return pairs


def analyze_sentiment_individually(text, finbert_pipeline, twitter_pipeline):
    """
    Analyze text using FinBERT and Twitter-RoBERTa for sentiment.
    Returns a dict with sentiments and confidence scores.
    """
    # FinBERT sentiment analysis
    finbert_result = finbert_pipeline(text)[0]
    finbert_sentiment = finbert_result["label"]
    finbert_confidence = finbert_result["score"]

    # Twitter sentiment analysis
    twitter_result = twitter_pipeline(text)[0]
    twitter_label_map = {"LABEL_0": "Negative", "LABEL_1": "Neutral", "LABEL_2": "Positive"}
    twitter_sentiment = twitter_label_map.get(twitter_result["label"], "Unknown")
    twitter_confidence = twitter_result["score"]

    return {
        "finbert_sentiment": finbert_sentiment,
        "finbert_confidence": finbert_confidence,
        "twitter_sentiment": twitter_sentiment,
        "twitter_confidence": twitter_confidence,
    }


def summarize_news(headline, description):
    """
    Create a summary string based on the headline and description.
    """
    summary = (f"Headline: {headline}\n"
               f"Description: {description}")
    return summary


def insert_news_to_db(connection, cursor, rec_raw_text, rec_raw_source, rec_source_type, rec_content_hash):
    """
    Inserts the given raw text, source URL, source type, and content hash into rec_raw_news table,
    and returns the newly inserted row's ID.
    """
    try:
        query = """
            INSERT INTO rec_raw_news (
                rec_raw_text,
                rec_raw_source,
                rec_source_id,
                rec_raw_content_hash
            )
            VALUES (%s, %s, %s, %s)
            RETURNING rec_raw_id;
        """
        cursor.execute(query, (rec_raw_text, rec_raw_source, rec_source_type, rec_content_hash))
        
        # Fetch the newly inserted ID
        new_id = cursor.fetchone()[0]
        connection.commit()
        
        # Print/log success if desired
        print("Raw news content inserted successfully into the database.")
        print(f"New raw news ID: {new_id}")
        return new_id

    except Exception as error:
        connection.rollback()
        raise Exception(f"Error inserting data into the database: {error}")
    
def insert_crypto_data(connection, cursor):
    
    try:

        markets_data = fetch_coingecko_markets_tor()
        trending_data = fetch_coingecko_trending_tor()

        markets_data_json = json.dumps(markets_data)
        trending_data_json = json.dumps(trending_data)
        data_type = "marketcap_and_trending" 

        cursor.execute("SELECT rec_raw_id FROM rec_raw_news ORDER BY rec_raw_id  DESC LIMIT 1;")
        row = cursor.fetchone()
        if row:
            raw_rec_id =  row[0]
            rec_source_id = 2
        else:
            print("No raw news record found in rec_raw_news table.")
            return None

        query = """
            INSERT INTO rec_crypto_market_data (
                rec_raw_news_id,
                rec_source_id,
                rec_crypto_data,
                rec_cypto_data_type,
                rec_crypto_trending_data
            )
            VALUES ( %s, %s, %s, %s, %s)
            RETURNING rec_crypto_data_id;
        """
 
        cursor.execute(query, (
            raw_rec_id,               
            rec_source_id,                              
            markets_data_json, 
            data_type,               
            trending_data_json
        ))

        
        # Print/log success if desired
        print("crypto data content inserted successfully into the table.")

    except Exception as error:
        connection.rollback()
        raise Exception(f"Error inserting data into the database: {error}")
    
def insert_crypto_analysis_data(connection, cursor, record, new_analysis_id, matched_coins_str):
    """
    Inserts coin data (fetched from CoinGecko) into rec_crypto_analysis.

    For each coin in the matched_coins list (a JSON string like:
      '["Dogecoin:DOGE", "Dogecoin:Elon Musk"]'),
    the function:
      1. Extracts the coin identifier (using the last part after splitting on ":" if available).
      2. Fetches coin data from CoinGecko.
      3. If multiple coins are returned (i.e. a list), it inserts each coin data record separately.
      4. If a single coin data dict is returned, it inserts it as one record.
      5. Waits 20 seconds before processing the next insertion.
    """
    try:
        # Load the list of matched coins from the JSON string.
        matched_coins = json.loads(matched_coins_str)
        print("Matched coins:", matched_coins)
        
        # Process each coin string in the matched list.
        for item in matched_coins:
            # Split the string (e.g., "Dogecoin:DOGE" or "Dogecoin:Elon Musk")
            parts = item.split(":")
            if len(parts) >= 2:
                # Use the last part as the coin identifier.
                coinid = parts[0].strip()
            else:
                coinid = item.strip()
            print("\nProcessing coin:", coinid)
            
            try:
                # Fetch coin data using the coin identifier.
                coin_data = fetch_coingecko_keyword_data(coinid)
                print(f"Fetched coin data for keyword {coinid}: {coin_data}")
            except Exception as fetch_err:
                print(f"Error fetching coin data for keyword {coinid}: {fetch_err}")
                continue  # Skip this coin on error.
            
            # Check if the returned coin data is a list (multiple coins) or a dict (single coin)
            if isinstance(coin_data, list):
                # Insert each coin record individually.
                for cd in coin_data:
                    try:
                        coin_data_json = json.dumps(cd)
                        ind_coin_id = cd.get("id")
                        print(f"Inserting coin record for '{coinid}' with data: {coin_data_json}")
                        insert_sql = """
                            INSERT INTO rec_crypto_analysis (
                                rec_news_analysis_id,
                                rec_coingecko_coin_id,
                                rec_keyword_crypto_data
                            )
                            VALUES (%s, %s, %s)
                        """
                        cursor.execute(insert_sql, (new_analysis_id, ind_coin_id, coin_data_json))
                        connection.commit()
                        print(f"Successfully inserted coin record for '{coinid}' into rec_crypto_analysis with analysis ID={new_analysis_id}")
                    except Exception as ins_err:
                        connection.rollback()
                        print(f"Error inserting coin data for '{coinid}': {ins_err}")
                    # Enforce a delay between each insertion.
                    print("Waiting 20 seconds before next insertion...")
                    time.sleep(20)
            else:
                # If a single coin record was returned, insert it.
                try:
                    coin_data_json = json.dumps(coin_data)
                    coin_data_dict = json.loads(coin_data_json)
                    coin_id_single = coin_data_dict.get("id")
                    print(f"Inserting coin record for '{coinid}' with data: {coin_data_json}")
                    insert_sql = """
                        INSERT INTO rec_crypto_analysis (
                            rec_news_analysis_id,
                            rec_coingecko_coin_id,
                            rec_keyword_crypto_data
                        )
                        VALUES (%s, %s, %s)
                    """
                    cursor.execute(insert_sql, (new_analysis_id, coin_id_single, coin_data_json))
                    connection.commit()
                    print(f"Successfully inserted coin record for '{coin_id_single}' into rec_crypto_analysis with analysis ID={new_analysis_id}")
                except Exception as ins_err:
                    connection.rollback()
                    print(f"Error inserting coin data for '{coinid}': {ins_err}")
                print("Waiting 20 seconds before next insertion...")
                time.sleep(20)
                
    except Exception as error:
        connection.rollback()
        raise Exception(f"Error inserting crypto data for coins: {error}")

         

def get_latest_raw_news_id(connection, cursor):
    """
    Retrieve the most recently inserted raw news ID from the rec_raw_news table.
    This value will be used as a foreign key in the analysis table.
    """
    try:
        cursor.execute("SELECT rec_raw_id FROM rec_raw_news ORDER BY rec_raw_scrape_date DESC LIMIT 1;")
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            print("No raw news record found in rec_raw_news table.")
            return None
    except Exception as e:
        print("Error retrieving latest rec_raw_news_id:", e)
        return None


def insert_analysis_to_db(connection, cursor, values):
    """
    Inserts the analysis record into the rec_news_analysis table, including rec_content_hash.
    """
    try:
        insert_query = """
            INSERT INTO rec_news_analysis (
                rec_raw_news_id,
                rec_news_keyword_used,
                rec_news_analysis_date_time,
                rec_news_summary,
                rec_analysis_algorithm_version,
                rec_news_metadata,
                rec_source_id,
                rec_content_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING rec_analysis_id;
        """
        cursor.execute(insert_query, values)
        analysis_id= cursor.fetchone()[0]
        connection.commit()
        return analysis_id
        print("Inserted analysis record into the database.")
    except Exception as e:
        connection.rollback()
        raise Exception(f"Error inserting analysis record into the database: {e}")

    

def insert_api_news_to_db(connection, cursor, values):
    """
    Inserts API-based news records directly into the rec_news_analysis table.

    Args:
        connection: Database connection object.
        cursor: Database cursor object.
        values: Tuple containing:
            - Matched keywords (JSON)
            - Analysis timestamp
            - Summary of the news
            - Analysis algorithm version
            - News metadata (JSON)
    """
    try:
        insert_query = """
            INSERT INTO rec_news_analysis (
                rec_news_keyword_used,
                rec_news_analysis_date_time,
                rec_news_summary,
                rec_analysis_algorithm_version,
                rec_news_metadata,
                rec_source_id,
                rec_content_hash
            ) VALUES (%s, %s, %s, %s, %s, %s ,%s)
            RETURNING rec_analysis_id;
        """
        cursor.execute(insert_query, values)
        analysis_id= cursor.fetchone()[0]
        connection.commit()
        return analysis_id
        print("Inserted API news record into the database.")
    except Exception as e:
        connection.rollback()
        raise Exception(f"Error inserting API news record into the database: {e}")


def match_keywords_for_article(title, description, keywords_file_path):
  
    try:
        with open(keywords_file_path, "r") as f:
            keywords_dict = json.load(f)
            if not isinstance(keywords_dict, dict):
                raise ValueError("Invalid JSON structure. Expected a dictionary of category: [keywords].")
    except Exception as e:
        raise ValueError(f"Error loading keywords from {keywords_file_path}: {e}")

    # Initialize a dictionary to store matched keywords
    category_matches = defaultdict(list)

    # Match keywords in the title and description
    for text in [title, description]:
        if not text:
            continue
        for category, kw_list in keywords_dict.items():
            for kw in kw_list:
                pattern = re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
                if pattern.search(text):
                    category_matches[category].append(kw)

    # Combine matched keywords into a list
    matched_keywords = []
    for category, keywords in category_matches.items():
        # Remove duplicates and format as category:keyword
        unique_keywords = set(keywords)
        matched_keywords.extend([f"{category}:{kw}" for kw in unique_keywords])

    return matched_keywords

