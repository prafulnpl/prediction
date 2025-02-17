import requests
import requests_cache
import time
import json
from urllib.parse import quote

# Your CoinGecko API key.
API_KEY = "CG-he5yWEp59V7X77tdtZ9FNBNy"

# Define a function to create a TOR session with a specified cache.
def get_tor_session(cache_name='default_cache', expire_after=600):
    """
    Returns a CachedSession (using requests_cache) configured to use Tor via a SOCKS5 proxy,
    and sets the CoinGecko API key in the headers.
    
    Parameters:
      cache_name: Name of the cache file.
      expire_after: Expiration time (in seconds) for the cache.
    """
    session = requests_cache.CachedSession(cache_name, expire_after=expire_after)
    session.proxies = {
        'http': 'socks5h://127.0.0.1:9050',
        'https': 'socks5h://127.0.0.1:9050'
    }
    # Set your API key in the headers.
    session.headers.update({'x_cg_pro_api_key': API_KEY})
    return session

def fetch_coingecko_markets_tor():
    """
    Fetches market data (first 20 coins, sorted by market cap)
    from CoinGecko using a Tor SOCKS5 proxy.
    Uses a 10-minute cache.
    """
    session = get_tor_session(cache_name='coingecko_markets', expire_after=600)
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 20,
        "page": 1
    }
    response = session.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def fetch_coingecko_trending_tor():
    """
    Fetches the current 'trending' coins (up to 7 coins) from CoinGecko
    using a Tor SOCKS5 proxy.
    Uses a 10-minute cache.
    """
    session = get_tor_session(cache_name='coingecko_trending', expire_after=600)
    url = "https://api.coingecko.com/api/v3/search/trending"
    response = session.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def simplify_coin_data(raw_data: dict) -> dict:
    """
    Build a simplified version of the coin data from the raw CoinGecko API response.
    Includes coin id and coin name.
    """
    simplified_data = {}

    # Basic top-level fields
    simplified_data["id"] = raw_data.get("id")
    simplified_data["name"] = raw_data.get("name")
    simplified_data["genesis_date"] = raw_data.get("genesis_date")
    simplified_data["sentiment_votes_up_percentage"] = raw_data.get("sentiment_votes_up_percentage")
    simplified_data["sentiment_votes_down_percentage"] = raw_data.get("sentiment_votes_down_percentage")
    simplified_data["watchlist_portfolio_users"] = raw_data.get("watchlist_portfolio_users")
    simplified_data["market_cap_rank"] = raw_data.get("market_cap_rank")

    # Market data subtree
    market_data = raw_data.get("market_data", {})
    simplified_data["market_data"] = {
        "current_price": {"usd": market_data.get("current_price", {}).get("usd")},
        "high_24h": {"usd": market_data.get("high_24h", {}).get("usd")},
        "low_24h": {"usd": market_data.get("low_24h", {}).get("usd")},
        "price_change_24h_in_currency": {"usd": market_data.get("price_change_24h_in_currency", {}).get("usd")},
        "price_change_percentage_24h_in_currency": {"usd": market_data.get("price_change_percentage_24h_in_currency", {}).get("usd")},
        "market_cap": {"usd": market_data.get("market_cap", {}).get("usd")},
        "total_volume": {"usd": market_data.get("total_volume", {}).get("usd")},
        "total_supply": market_data.get("total_supply"),
        "max_supply": market_data.get("max_supply"),
        "circulating_supply": market_data.get("circulating_supply"),
        "last_updated": market_data.get("last_updated"),
        "ath_change_percentage": market_data.get("ath_change_percentage", {}).get("usd")
    }

    # Developer data subtree
    developer_data = raw_data.get("developer_data", {})
    simplified_data["developer_data"] = {
        "forks": developer_data.get("forks"),
        "stars": developer_data.get("stars"),
        "subscribers": developer_data.get("subscribers"),
        "total_issues": developer_data.get("total_issues"),
        "closed_issues": developer_data.get("closed_issues"),
        "pull_requests_merged": developer_data.get("pull_requests_merged"),
        "pull_request_contributors": developer_data.get("pull_request_contributors"),
        "code_additions_deletions_4_weeks": developer_data.get("code_additions_deletions_4_weeks")
    }

    # Tickers data (trust_score, bid_ask_spread_percentage)
    tickers = raw_data.get("tickers", [])
    if tickers:
        simplified_data["trust_score"] = tickers[0].get("trust_score")
        simplified_data["bid_ask_spread_percentage"] = tickers[0].get("bid_ask_spread_percentage")
    else:
        simplified_data["trust_score"] = None
        simplified_data["bid_ask_spread_percentage"] = None

    return simplified_data

def fetch_coingecko_keyword_data(keyword: str):
    """
    Fetch detailed information about coin(s) matching the given keyword from CoinGecko via a Tor proxy.

    The coin list is fetched using a session with a 3-day cache,
    while individual coin data is fetched using a session with a 10-minute cache.

    If multiple coins match the keyword, data for all matching coins is returned as a list of simplified dictionaries;
    otherwise, a single simplified dictionary is returned.
    """
    # Use a session for the coin list with a 3-day (2592000 seconds) cache.
    session_list = get_tor_session(cache_name='coingecko_coin_list', expire_after=2592000)
    coin_list_url = "https://api.coingecko.com/api/v3/coins/list"
    response = session_list.get(coin_list_url, timeout=10)
    response.raise_for_status()
    coins = response.json()

    # Search for coins that match the keyword (case-insensitive).
    keyword_lower = keyword.lower()
    matching_coins = [
        coin for coin in coins
        if keyword_lower in coin["id"].lower() or keyword_lower in coin["name"].lower()
    ]

    # Determine how to fetch data.
    if matching_coins:
        if len(matching_coins) > 1:
            # Batch: fetch data for all matching coins using a session with a 10-minute cache.
            simplified_data_list = []
            session_individual = get_tor_session(cache_name='coingecko_individual', expire_after=600)
            for coin in matching_coins:
                coin_identifier = coin["id"]
                coin_identifier_encoded = quote(coin_identifier)
                print("Fetching coin id:", coin_identifier_encoded)
                coingecko_coin_url = f"https://api.coingecko.com/api/v3/coins/{coin_identifier_encoded}"
                coin_response = session_individual.get(coingecko_coin_url, timeout=10)
                coin_response.raise_for_status()
                raw_data = coin_response.json()
                simplified_data = simplify_coin_data(raw_data)
                simplified_data_list.append(simplified_data)
                print("Fetched data for:", coin_identifier)
                time.sleep(120)  # Delay to help avoid rate limiting.
            return simplified_data_list
        else:
            coin_identifier = matching_coins[0]["id"]
    else:
        print("No matching coin found.")

    # URL-encode the coin identifier.
    coin_identifier_encoded = quote(coin_identifier)
    print("Using coin id:", coin_identifier_encoded)

    # Use the individual session (10-minute cache) to fetch coin data.
    session_individual = get_tor_session(cache_name='coingecko_individual', expire_after=600)
    COINGECKO_API_URL = f"https://api.coingecko.com/api/v3/coins/{coin_identifier_encoded}"
    response = session_individual.get(COINGECKO_API_URL, timeout=10)
    response.raise_for_status()
    raw_data = response.json()

    time.sleep(120)  # Delay to avoid rate limiting.
    print(simplify_coin_data(raw_data))
    return simplify_coin_data(raw_data)

if __name__ == "__main__":
    # Example usage:
    print("Fetching markets data via Tor...")
    markets = fetch_coingecko_markets_tor()
    print("Markets data:", markets)

    print("\nFetching trending data via Tor...")
    trending = fetch_coingecko_trending_tor()
    print("Trending data:", trending)

    print("\nFetching Bitcoin data via Tor with caching...")
    bitcoin_data = fetch_coingecko_keyword_data("bitcoin")
    print("Bitcoin data:", bitcoin_data)
