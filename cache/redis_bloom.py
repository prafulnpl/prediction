import os
from redisbloom.client import Client as RedisBloom

# Configuration (use environment variables or default values)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB   = int(os.getenv("REDIS_DB", 0))

# Two separate Bloom filters
SCRAPE_BLOOM_FILTER = "news_scrape_bloom"
ANALYSIS_BLOOM_FILTER = "news_analysis_bloom"

ERROR_RATE = 0.001
INITIAL_CAPACITY = 2_000_000

# Initialize RedisBloom client
bloom_client = RedisBloom(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def init_bloom_filters():
    """
    Creates the Bloom filters if they don't exist.
    """
    try:
        bloom_client.bfCreate(SCRAPE_BLOOM_FILTER, errorRate=ERROR_RATE, capacity=INITIAL_CAPACITY)
        print(f"Bloom filter '{SCRAPE_BLOOM_FILTER}' created.")
    except Exception as e:
        if "ERR Bloom filter already exists" in str(e):
            print(f"Bloom filter '{SCRAPE_BLOOM_FILTER}' already exists.")
        else:
            print(f"Error initializing '{SCRAPE_BLOOM_FILTER}': {e}")

    try:
        bloom_client.bfCreate(ANALYSIS_BLOOM_FILTER, errorRate=ERROR_RATE, capacity=INITIAL_CAPACITY)
        print(f"Bloom filter '{ANALYSIS_BLOOM_FILTER}' created.")
    except Exception as e:
        if "ERR Bloom filter already exists" in str(e):
            print(f"Bloom filter '{ANALYSIS_BLOOM_FILTER}' already exists.")
        else:
            print(f"Error initializing '{ANALYSIS_BLOOM_FILTER}': {e}")

def check_duplicate_scrape(item: str) -> bool:
    """ Check if news article hash exists in Scrape Bloom filter """
    return bloom_client.bfExists(SCRAPE_BLOOM_FILTER, item)

def add_to_scrape_bloom(item: str) -> None:
    """ Add news article hash to Scrape Bloom filter """
    bloom_client.bfAdd(SCRAPE_BLOOM_FILTER, item)

def check_duplicate_analysis(item: str) -> bool:
    """ Check if news article hash exists in Analysis Bloom filter """
    return bloom_client.bfExists(ANALYSIS_BLOOM_FILTER, item)

def add_to_analysis_bloom(item: str) -> None:
    """ Add news article hash to Analysis Bloom filter """
    bloom_client.bfAdd(ANALYSIS_BLOOM_FILTER, item)
