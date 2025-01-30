import requests
import json
import time
import re  # Import for keyword extraction
from collections import Counter

# Proxy configuration
proxies = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

# Base URLs for APIs
markets_url = "https://api.coingecko.com/api/v3/coins/markets"
details_url_template = "https://api.coingecko.com/api/v3/coins/{}"

# Fetch top 100 coins by market cap
def fetch_top_100_markets_coins():
    """
    Fetch and return the top 100 market coins data using proxies.
    """
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1
    }
    try:
        response = requests.get(markets_url, params=params, timeout=20, proxies=proxies)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching market data: {e}")
        return []

# Fetch detailed description for a specific coin by ID
def fetch_coin_description(coin_id):
    """
    Fetch and return the description of a coin using its ID through proxies.
    """
    try:
        response = requests.get(details_url_template.format(coin_id), timeout=20, proxies=proxies)
        response.raise_for_status()  # Raise an exception for HTTP errors
        coin_data = response.json()
        # Extract and return the English description if available
        return coin_data.get("description", {}).get("en", "Description not available")
    except requests.RequestException as e:
        print(f"Error fetching details for coin ID '{coin_id}': {e}")
        return "Error fetching description"

# Extract distinct keywords from a description
def extract_keywords(description):
    """
    Extract distinct, non-repetitive keywords from the description.
    """
    # Remove HTML tags and links
    clean_description = re.sub(r'<.*?>', '', description)
    clean_description = re.sub(r'https?://\S+|www\.\S+', '', clean_description)

    # Tokenize and extract words (alphanumeric, 3+ characters)
    words = re.findall(r'\b\w{3,}\b', clean_description.lower())
    
    # Remove common stopwords (can be customized further)
    stopwords = set(["the", "and", "of", "to", "in", "for", "a", "is", "it", "on", "by", "with", "as", "this", "an", "or"])
    keywords = [word for word in words if word not in stopwords]

    # Return unique keywords sorted alphabetically
    return sorted(set(keywords))

# Main function to fetch and save keywords
def save_coin_keywords_to_file(filename):
    """
    Fetch top 100 coins and save their keywords to a JSON file.
    """
    coins = fetch_top_100_markets_coins()
    if not coins:
        print("No coins data found.")
        return

    coin_keywords = {}

    for coin in coins:
        coin_id = coin["id"]
        coin_name = coin["name"]
        print(f"Fetching description for: {coin_name} ({coin_id})...")
        
        # Fetch the description
        description = fetch_coin_description(coin_id)
        if description.startswith("Error"):
            continue  # Skip if description could not be fetched

        # Extract distinct keywords
        keywords = extract_keywords(description)
        coin_keywords[coin_name] = keywords

        print(f"Extracted keywords for: {coin_name}")
        
        # Add a 20-second delay to avoid hitting the API rate limit
        time.sleep(20)

    # Save the keywords to a JSON file
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(coin_keywords, file, indent=4, ensure_ascii=False)

    print(f"\nKeywords saved to {filename}")

if __name__ == "__main__":
    # Save keywords to a JSON file
    output_file = "coin_keywords.json"
    save_coin_keywords_to_file(output_file)
    print(f"\nKeywords saved to {output_file}")
