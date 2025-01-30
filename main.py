
import os
import sys

# Make sure Python can find our "src" modules.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR))

# Import the functions we created:
from web_scrapping.scrape_news import run_scraper
from text_sentiment.predict import run_predict
from api.newsorg import fetch_and_insert_news_with_sentiment_analysis
from transformers import pipeline



def main():



    print("Running Scraper...")
    run_scraper()  # This will do the scraping & insert raw text into the database.

    print("\nRunning Prediction...")
    run_predict()  # This will perform sentiment analysis & insert analysis results.

    print("\nAnalaysing api")
    fetch_and_insert_news_with_sentiment_analysis()  # This will fetch news from NewsAPI & insert into the database.

if __name__ == "__main__":
    main()
