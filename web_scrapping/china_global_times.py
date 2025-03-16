#!/usr/bin/env python3
import requests
import time
import json
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# API Key (if needed for premium access)
API_KEY = "CG-394G8mJadiJKBucFmqmLw7ZD"

def get_vpn_session():
    """
    Create a requests session using the system's default network settings.
    The API key is added to the headers.
    """
    session = requests.Session()
    session.headers.update({'x_cg_pro_api_key': API_KEY})
    return session

@retry(stop=stop_after_attempt(5), wait=wait_fixed(60), reraise=True)
def fetch_coingecko_markets():
    """
    Fetch market data from CoinGecko using the active network connection.
    Retries up to 5 times with a 60-second delay on failure.
    """
    try:
        session = get_vpn_session()
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 20,
            "page": 1
        }

        logger.info("Fetching market data from CoinGecko...")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        logger.info("Successfully fetched market data.")

        # Delay to avoid hitting rate limits (adjust as needed)
        time.sleep(5)
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching market data: {e}")
        raise

if __name__ == "__main__":
    try:
        market_data = fetch_coingecko_markets()
        print(json.dumps(market_data, indent=4))
    except Exception as e:
        logger.error(f"Failed to fetch market data after multiple attempts: {e}")
