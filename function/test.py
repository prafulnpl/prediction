import requests
import requests_cache
import time
import json
import logging
from urllib.parse import quote
from tenacity import retry, stop_after_attempt, wait_fixed

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# API Key (If needed for premium access)
API_KEY = "CG-he5yWEp59V7X77tdtZ9FNBNy"

# Tor Proxy Settings
TOR_PROXY = "socks5h://127.0.0.1:9050"

def get_tor_session(cache_name='default_cache', expire_after=600):
    """Create a Tor session with caching."""
    session = requests_cache.CachedSession(cache_name, expire_after=expire_after)
    session.proxies = {
        'http': TOR_PROXY,
        'https': TOR_PROXY
    }
    session.headers.update({'x_cg_pro_api_key': API_KEY})  # Optional, for CoinGecko Pro users
    return session

def test_tor_connection():
    """Check if Tor is running by attempting to fetch an IP check service."""
    try:
        session = get_tor_session()
        response = session.get("https://check.torproject.org/api/ip", timeout=10)
        response.raise_for_status()
        ip_data = response.json()
        logger.info(f"Tor is working! Your IP: {ip_data.get('IP')}")
        return True
    except requests.RequestException as e:
        logger.error(f"Tor connection failed: {e}")
        return False

# Retry settings: Retry up to 5 times with a 60-second delay
@retry(stop=stop_after_attempt(5), wait=wait_fixed(60), reraise=True)
def fetch_coingecko_markets_tor():
    """Fetch market data using Tor with retry mechanism."""
    try:
        if not test_tor_connection():
            raise ConnectionError("Tor connection failed. Ensure Tor is running on port 9050.")

        session = get_tor_session(cache_name='coingecko_markets', expire_after=600)
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
        
        # Delay to avoid rate limits (adjust as needed)
        time.sleep(5)
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching market data: {e}")
        raise  # Re-raise exception for retry mechanism to handle it

if __name__ == "__main__":
    try:
        market_data = fetch_coingecko_markets_tor()
        print(json.dumps(market_data, indent=4))
    except Exception as e:
        logger.error(f"Failed to fetch market data after multiple attempts: {e}")
