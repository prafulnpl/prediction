# clear_bloom_filter.py

import sys
import os

# Ensure the project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "./")))

from cache.redis_bloom import clear_bloom_filter, init_bloom_filter, close_bloom_client

def main():
    """
    Clears the RedisBloom filter to reset the cache.
    """
    # Clear the Bloom filter
    clear_bloom_filter()
    
    # Optionally, re-initialize the Bloom filter after clearing
    # Uncomment the following line if you want to recreate the Bloom filter immediately
    # init_bloom_filter()
    
    # Close the RedisBloom client
    close_bloom_client()

if __name__ == "__main__":
    main()
