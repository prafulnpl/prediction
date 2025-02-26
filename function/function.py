import re
import json
from collections import defaultdict
import hashlib
from function.api import fetch_coingecko_markets_tor, fetch_coingecko_trending_tor, fetch_coingecko_keyword_data
import time
import logging

logger = logging.getLogger(__name__)

def generate_unique_key(headline: str, source_url: str) -> str:
    """Generate a hash key based on the headline + source_url."""
    unique_string = f"{headline}_{source_url}"
    return hashlib.sha256(unique_string.encode("utf-8")).hexdigest()

def extract_relevant_sentences(raw_text, keywords_file_path):
    """Extract sentences matching keywords from a JSON file."""
    try:
        with open(keywords_file_path, "r") as f:
            keywords_dict = json.load(f)
            if not isinstance(keywords_dict, dict):
                raise ValueError("Invalid JSON structure. Expected a dictionary of category: [keywords].")
    except Exception as e:
        raise ValueError(f"Error loading keywords from {keywords_file_path}: {e}")

    sentences = [sentence.strip() for sentence in raw_text.split('.') if sentence.strip()]
    matching_sentences = []

    for sentence in sentences:
        category_matches = defaultdict(list)
        for category, kw_list in keywords_dict.items():
            for kw in kw_list:
                pattern = re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
                if pattern.search(sentence):
                    category_matches[category].append(kw)

        if category_matches:
            sorted_matches = []
            for category, keywords in category_matches.items():
                keyword_counts = defaultdict(int)
                for kw in keywords:
                    keyword_counts[kw] += 1
                sorted_matches.extend([f"{category}:{kw}" for kw in sorted(keyword_counts, key=keyword_counts.get, reverse=True)])
            matching_sentences.append((sentence, sorted_matches))

    return matching_sentences, keywords_dict

def process_headlines_with_descriptions(combined_text, keywords_file_path):
    """Process raw text into a list of (headline, description, [category:keyword, ...]) tuples."""
    pairs = []
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
    """Analyze text using FinBERT and Twitter-RoBERTa for sentiment."""
    finbert_result = finbert_pipeline(text)[0]
    finbert_sentiment = finbert_result["label"]
    finbert_confidence = finbert_result["score"]

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
    """Create a summary string based on the headline and description."""
    return f"Headline: {headline}\nDescription: {description}"

def insert_news_to_db(connection, cursor, rec_raw_text, rec_raw_source, rec_source_type, rec_content_hash):
    """Insert raw news into the database and return the new ID."""
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
        new_id = cursor.fetchone()[0]
        connection.commit()
        logger.info(f"Raw news content inserted successfully. New ID: {new_id}")
        return new_id
    except Exception as error:
        connection.rollback()
        logger.error(f"Error inserting data into the database: {error}")
        raise

def insert_crypto_data(connection, cursor):
    """Insert crypto market and trending data into the database."""
    try:
        markets_data = fetch_coingecko_markets_tor()
        trending_data = fetch_coingecko_trending_tor()

        markets_data_json = json.dumps(markets_data)
        trending_data_json = json.dumps(trending_data)
        data_type = "marketcap_and_trending"

        cursor.execute("SELECT rec_raw_id FROM rec_raw_news ORDER BY rec_raw_id DESC LIMIT 1;")
        row = cursor.fetchone()
        if row:
            raw_rec_id = row[0]
            rec_source_id = 2
        else:
            logger.warning("No raw news record found in rec_raw_news table.")
            return None

        query = """
            INSERT INTO rec_crypto_market_data (
                rec_raw_news_id,
                rec_source_id,
                rec_crypto_data,
                rec_crypto_data_type,
                rec_crypto_trending_data
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING rec_crypto_data_id;
        """
        cursor.execute(query, (raw_rec_id, rec_source_id, markets_data_json, data_type, trending_data_json))
        connection.commit()
        logger.info("Crypto data inserted successfully.")
    except Exception as error:
        connection.rollback()
        logger.error(f"Error inserting crypto data: {error}")
        raise

def insert_crypto_analysis_data(connection, cursor, record, new_analysis_id, matched_coins_str):
    """Insert crypto analysis data into the database."""
    try:
        matched_coins = json.loads(matched_coins_str)
        logger.info(f"Matched coins: {matched_coins}")

        for item in matched_coins:
            parts = item.split(":")
            coinid = parts[0].strip() if len(parts) >= 2 else item.strip()
            logger.info(f"Processing coin: {coinid}")

            try:
                coin_data = fetch_coingecko_keyword_data(coinid)
                logger.info(f"Fetched coin data for keyword {coinid}: {coin_data}")
            except Exception as fetch_err:
                logger.error(f"Error fetching coin data for keyword {coinid}: {fetch_err}")
                continue

            if isinstance(coin_data, list):
                for cd in coin_data:
                    try:
                        coin_data_json = json.dumps(cd)
                        ind_coin_id = cd.get("id")
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
                        logger.info(f"Inserted coin record for '{coinid}' with analysis ID={new_analysis_id}")
                    except Exception as ins_err:
                        connection.rollback()
                        logger.error(f"Error inserting coin data for '{coinid}': {ins_err}")
                    time.sleep(20)
            else:
                try:
                    coin_data_json = json.dumps(coin_data)
                    coin_id_single = coin_data.get("id")
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
                    logger.info(f"Inserted coin record for '{coin_id_single}' with analysis ID={new_analysis_id}")
                except Exception as ins_err:
                    connection.rollback()
                    logger.error(f"Error inserting coin data for '{coinid}': {ins_err}")
                time.sleep(20)
    except Exception as error:
        connection.rollback()
        logger.error(f"Error inserting crypto data for coins: {error}")
        raise

def get_latest_raw_news_id(connection, cursor):
    """Retrieve the most recently inserted raw news ID."""
    try:
        cursor.execute("SELECT rec_raw_id FROM rec_raw_news ORDER BY rec_raw_scrape_date DESC LIMIT 1;")
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            logger.warning("No raw news record found in rec_raw_news table.")
            return None
    except Exception as e:
        logger.error(f"Error retrieving latest rec_raw_news_id: {e}")
        return None

def insert_analysis_to_db(connection, cursor, values):
    """Insert analysis record into the database."""
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
        analysis_id = cursor.fetchone()[0]
        connection.commit()
        logger.info(f"Inserted analysis record. New ID: {analysis_id}")
        return analysis_id
    except Exception as e:
        connection.rollback()
        logger.error(f"Error inserting analysis record: {e}")
        raise

def insert_api_news_to_db(connection, cursor, values):
    """Insert API-based news records into the database."""
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
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING rec_analysis_id;
        """
        cursor.execute(insert_query, values)
        analysis_id = cursor.fetchone()[0]
        connection.commit()
        logger.info(f"Inserted API news record. New ID: {analysis_id}")
        return analysis_id
    except Exception as e:
        connection.rollback()
        logger.error(f"Error inserting API news record: {e}")
        raise

def match_keywords_for_article(title, description, keywords_file_path):
    """Match keywords in the title and description."""
    try:
        with open(keywords_file_path, "r") as f:
            keywords_dict = json.load(f)
            if not isinstance(keywords_dict, dict):
                raise ValueError("Invalid JSON structure. Expected a dictionary of category: [keywords].")
    except Exception as e:
        raise ValueError(f"Error loading keywords from {keywords_file_path}: {e}")

    category_matches = defaultdict(list)
    for text in [title, description]:
        if not text:
            continue
        for category, kw_list in keywords_dict.items():
            for kw in kw_list:
                pattern = re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
                if pattern.search(text):
                    category_matches[category].append(kw)

    matched_keywords = []
    for category, keywords in category_matches.items():
        unique_keywords = set(keywords)
        matched_keywords.extend([f"{category}:{kw}" for kw in unique_keywords])

    return matched_keywords