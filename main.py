import logging
from typing import NoReturn
from time import sleep  # To keep the main program running

# Configure logging before other imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# Local imports
from web_scrapping.scrape_news import run_scraper
from text_sentiment.predict import run_predict
from api.newsorg import fetch_and_insert_news_with_sentiment_analysis


def main() -> NoReturn:
    """Orchestrate the data pipeline execution."""
    try:
        # Start the listener for new inserts in a background thread
        # logger.info("Starting background listener for new inserts...")
        # start_listener_thread()  # This starts the listener in a separate thread

        # Now run the main pipeline
        logger.info("Starting data pipeline...")

        logger.info("Running web scraper...")
        run_scraper()

        logger.info("Running sentiment analysis...")
        run_predict()

        logger.info("Fetching API news...")
        fetch_and_insert_news_with_sentiment_analysis()

        # Allow the main program to keep running while the listener is active
        logger.info("Pipeline completed successfully")

    except Exception as error:
        logger.error(f"Pipeline failed: {error}", exc_info=True)
        raise


def run_continuously() -> NoReturn:
    """Run the main pipeline continuously."""
    while True:
        try:
            main()
            logger.info("Restarting pipeline in 60 seconds...")
            sleep(870)  # Wait for 60 seconds before restarting
        except Exception as error:
            logger.error(f"Pipeline crashed: {error}", exc_info=True)
            logger.info("Restarting pipeline in 60 seconds after crash...")
            sleep(60)  # Wait for 60 seconds before restarting after a crash


if __name__ == "__main__":
    run_continuously()