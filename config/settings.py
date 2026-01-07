

import os
from dotenv import load_dotenv

load_dotenv()


GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


REVIEWS_FETCH_LANG = "en"
REVIEWS_FETCH_COUNTRY = "in"  # Can be overridden per app
SCRAPE_SLEEP_MS = 0  # Polite scraping delay
MAX_REVIEWS_PER_FETCH = 5000  # Maximum reviews to fetch per scrape


SIMILARITY_THRESHOLD = 0.75
MAX_RETRIES = 3
TEMPERATURE = 0.3
BATCH_SIZE = 20  # Reviews per Groq call


TREND_WINDOW_DAYS = 1  # T-30 to T

BASE_DATA_DIR = "data/apps"
BASE_OUTPUT_DIR = "output"
LOGS_DIR = "logs"
CACHE_DIR = "data/cache"

MIN_TOPIC_OCCURRENCES = 5  # Minimum mentions to add as new topic
CONFIDENCE_THRESHOLD = 0.70  # Below this, topic is considered unmapped


def get_app_data_dir(app_id: str) -> str:
    return os.path.join(BASE_DATA_DIR, app_id)


def get_app_output_dir(app_id: str) -> str:
    return os.path.join(BASE_OUTPUT_DIR, app_id)


def get_reviews_csv_path(app_id: str) -> str:
    return os.path.join(get_app_data_dir(app_id), "all_reviews.csv")


def get_processed_dir(app_id: str) -> str:
    return os.path.join(get_app_data_dir(app_id), "processed")


def get_taxonomy_path(app_id: str) -> str:
    return os.path.join(get_app_data_dir(app_id), "taxonomy", "master_taxonomy.json")


def get_batch_path(app_id: str, date_str: str) -> str:
    return os.path.join(get_processed_dir(app_id), f"batch_{date_str}.json")


def get_report_path(app_id: str, target_date: str) -> str:
    return os.path.join(get_app_output_dir(app_id), f"trend_report_{target_date}.csv")
