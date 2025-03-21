#!/usr/bin/env python3
"""
Fetch CoinGecko market, trending, and detailed coin data with caching and retry.
"""

import requests
import requests_cache
import time
import json
from urllib.parse import quote
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

def get_session(cache_name='default_cache', expire_after=300):
    """Create a session with caching and a custom User-Agent header."""
    session = requests_cache.CachedSession(cache_name, expire_after=expire_after)
    # Use a common browser User-Agent to mimic a real browser.
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36'
    })
    return session

@retry(stop=stop_after_attempt(5), wait=wait_fixed(60), reraise=True)
def fetch_coingecko_markets():
    """Fetch market data from CoinGecko with retry mechanism."""
    try:
        session = get_session(cache_name='coingecko_markets', expire_after=300)
        url = "https://api.coingecko.com/api/v3/coins/markets"
        # Include the API key as a query parameter for demo access
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 20,
            "page": 1,
            "sparkline": "false",
            "x_cg_demo_api_key": "CG-394G8mJadiJKBucFmqmLw7ZD"
        }
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        logger.info("Successfully fetched market data.")
        time.sleep(1)  # Reduced delay for demo purposes
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching market data: {e}")
        raise

@retry(stop=stop_after_attempt(5), wait=wait_fixed(60), reraise=True)
def fetch_coingecko_trending():
    """Fetch trending data from CoinGecko with retry mechanism."""
    try:
        session = get_session(cache_name='coingecko_trending', expire_after=300)
        url = "https://api.coingecko.com/api/v3/search/trending"
        response = session.get(url, timeout=10)
        response.raise_for_status()
        logger.info("Successfully fetched trending data.")
        time.sleep(1)  # Reduced delay for demo purposes
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching trending data: {e}")
        raise

def simplify_coin_data(raw_data: dict) -> dict:
    """Simplify CoinGecko API response."""
    simplified_data = {
        "id": raw_data.get("id"),
        "name": raw_data.get("name"),
        "genesis_date": raw_data.get("genesis_date"),
        "sentiment_votes_up_percentage": raw_data.get("sentiment_votes_up_percentage"),
        "sentiment_votes_down_percentage": raw_data.get("sentiment_votes_down_percentage"),
        "watchlist_portfolio_users": raw_data.get("watchlist_portfolio_users"),
        "market_cap_rank": raw_data.get("market_cap_rank"),
        "market_data": {
            "current_price": {"usd": raw_data.get("market_data", {}).get("current_price", {}).get("usd")},
            "high_24h": {"usd": raw_data.get("market_data", {}).get("high_24h", {}).get("usd")},
            "low_24h": {"usd": raw_data.get("market_data", {}).get("low_24h", {}).get("usd")},
            "price_change_24h_in_currency": {"usd": raw_data.get("market_data", {}).get("price_change_24h_in_currency", {}).get("usd")},
            "price_change_percentage_24h_in_currency": {"usd": raw_data.get("market_data", {}).get("price_change_percentage_24h_in_currency", {}).get("usd")},
            "market_cap": {"usd": raw_data.get("market_data", {}).get("market_cap", {}).get("usd")},
            "total_volume": {"usd": raw_data.get("market_data", {}).get("total_volume", {}).get("usd")},
            "total_supply": raw_data.get("market_data", {}).get("total_supply"),
            "max_supply": raw_data.get("market_data", {}).get("max_supply"),
            "circulating_supply": raw_data.get("market_data", {}).get("circulating_supply"),
            "last_updated": raw_data.get("market_data", {}).get("last_updated"),
            "ath_change_percentage": raw_data.get("market_data", {}).get("ath_change_percentage", {}).get("usd")
        },
        "developer_data": {
            "forks": raw_data.get("developer_data", {}).get("forks"),
            "stars": raw_data.get("developer_data", {}).get("stars"),
            "subscribers": raw_data.get("developer_data", {}).get("subscribers"),
            "total_issues": raw_data.get("developer_data", {}).get("total_issues"),
            "closed_issues": raw_data.get("developer_data", {}).get("closed_issues"),
            "pull_requests_merged": raw_data.get("developer_data", {}).get("pull_requests_merged"),
            "pull_request_contributors": raw_data.get("developer_data", {}).get("pull_request_contributors"),
            "code_additions_deletions_4_weeks": raw_data.get("developer_data", {}).get("code_additions_deletions_4_weeks")
        }
    }

    tickers = raw_data.get("tickers", [])
    if tickers:
        simplified_data["trust_score"] = tickers[0].get("trust_score")
        simplified_data["bid_ask_spread_percentage"] = tickers[0].get("bid_ask_spread_percentage")
    else:
        simplified_data["trust_score"] = None
        simplified_data["bid_ask_spread_percentage"] = None

    return simplified_data

@retry(stop=stop_after_attempt(5), wait=wait_fixed(60), reraise=True)
def fetch_coingecko_keyword_data(keyword: str):
    """Fetch detailed information about coin(s) matching the given keyword."""
    try:
        session_list = get_session(cache_name='coingecko_coin_list', expire_after=300)
        coin_list_url = "https://api.coingecko.com/api/v3/coins/list"
        response = session_list.get(coin_list_url, timeout=10)
        response.raise_for_status()
        coins = response.json()

        keyword_lower = keyword.lower()
        matching_coins = [
            coin for coin in coins
            if keyword_lower in coin["id"].lower() or keyword_lower in coin["name"].lower()
        ]

        if matching_coins:
            if len(matching_coins) > 1:
                simplified_data_list = []
                session_individual = get_session(cache_name='coingecko_individual', expire_after=300)
                for coin in matching_coins:
                    coin_identifier = coin["id"]
                    coin_identifier_encoded = quote(coin_identifier)
                    logger.info(f"Fetching coin id: {coin_identifier_encoded}")
                    coingecko_coin_url = f"https://api.coingecko.com/api/v3/coins/{coin_identifier_encoded}"
                    coin_response = session_individual.get(coingecko_coin_url, timeout=10)
                    coin_response.raise_for_status()
                    raw_data = coin_response.json()
                    simplified_data = simplify_coin_data(raw_data)
                    simplified_data_list.append(simplified_data)
                    logger.info(f"Fetched data for: {coin_identifier}")
                    time.sleep(2)  # Reduced delay
                return simplified_data_list
            else:
                coin_identifier = matching_coins[0]["id"]
        else:
            logger.warning("No matching coin found.")
            return None

        coin_identifier_encoded = quote(coin_identifier)
        logger.info(f"Using coin id: {coin_identifier_encoded}")

        session_individual = get_session(cache_name='coingecko_individual', expire_after=300)
        COINGECKO_API_URL = f"https://api.coingecko.com/api/v3/coins/{coin_identifier_encoded}"
        response = session_individual.get(COINGECKO_API_URL, timeout=10)
        response.raise_for_status()
        raw_data = response.json()

        time.sleep(2)  # Reduced delay
        return simplify_coin_data(raw_data)
    except Exception as e:
        logger.error(f"Error fetching keyword data: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        logger.info("Fetching markets data...")
        markets = fetch_coingecko_markets()
        logger.info(f"Markets data: {markets}")

        logger.info("Fetching trending data...")
        trending = fetch_coingecko_trending()
        logger.info(f"Trending data: {trending}")

        logger.info("Fetching Bitcoin data with caching...")
        bitcoin_data = fetch_coingecko_keyword_data("bitcoin")
        logger.info(f"Bitcoin data: {bitcoin_data}")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
