"""
Smart scraper for Google Play Store reviews.
Implements CSV caching logic with efficient date-range-based scraping.
Uses continuation tokens and stops when cutoff date is reached.
"""

import time
import random
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict
from tqdm import tqdm
from google_play_scraper import reviews, Sort

from config.settings import (
    REVIEWS_FETCH_LANG, REVIEWS_FETCH_COUNTRY, TREND_WINDOW_DAYS
)
from src.utils.logger import get_scraper_logger
from src.utils.date_utils import get_trend_date_range, format_date
from src.utils.storage import (
    reviews_csv_exists, load_reviews_csv, save_reviews_csv,
    initialize_app_directories
)

logger = get_scraper_logger()

# Sleep range between requests (seconds)
SLEEP_RANGE = (0.4, 1.2)


def fetch_reviews_by_date_range(
    app_id: str,
    reference_date: datetime,
    cutoff_date: datetime,
    country: str = REVIEWS_FETCH_COUNTRY,
    lang: str = REVIEWS_FETCH_LANG,
) -> pd.DataFrame:
    """
    Fetch Google Play Store reviews in the date range:
    [cutoff_date, reference_date]

    Reviews are fetched newest -> oldest.
    Scraping stops as soon as reviews go older than the cutoff date.
    
    Args:
        app_id: The app package identifier
        reference_date: End date (T)
        cutoff_date: Start date (T-N)
        country: Country code for reviews
        lang: Language code for reviews
        
    Returns:
        DataFrame: Reviews with standard columns
    """
    all_reviews: List[Dict] = []
    continuation_token: Optional[str] = None

    print(
        f"\nApp ID        : {app_id}\n"
        f"Country       : {country}\n"
        f"Language      : {lang}\n"
        f"Date range    : {cutoff_date.date()} → {reference_date.date()}\n"
    )
    
    logger.info(f"Fetching reviews from Play Store for: {app_id}")
    logger.info(f"Date range: {cutoff_date.date()} to {reference_date.date()}")

    with tqdm(desc="Reviews collected", unit="review") as pbar:
        while True:
            try:
                result, continuation_token = reviews(
                    app_id,
                    lang=lang,
                    country=country,
                    sort=Sort.NEWEST,
                    count=200,
                    continuation_token=continuation_token
                )
            except Exception as e:
                logger.error(f"Error fetching reviews: {e}")
                break

            if not result:
                break

            for r in result:
                review_date = r["at"]

                # Stop immediately once we cross the cutoff
                if review_date < cutoff_date:
                    print("Reached cutoff date. Stopping scrape.")
                    logger.info(f"Reached cutoff date. Total reviews: {len(all_reviews)}")
                    return _convert_to_dataframe(all_reviews)

                # Keep only reviews within the window
                if review_date <= reference_date:
                    all_reviews.append({
                        "reviewId": r["reviewId"],
                        "userName": r["userName"],
                        "score": r["score"],
                        "at": review_date,
                        "content": r["content"],
                        "thumbsUpCount": r["thumbsUpCount"],
                        "appVersion": r.get("reviewCreatedVersion"),
                        "replyContent": r["replyContent"],
                        "repliedAt": r["repliedAt"],
                    })
                    pbar.update(1)

            if continuation_token is None:
                break

            time.sleep(random.uniform(*SLEEP_RANGE))

    logger.info(f"Fetched {len(all_reviews)} reviews for {app_id}")
    return _convert_to_dataframe(all_reviews)


def _convert_to_dataframe(reviews_list: List[Dict]) -> pd.DataFrame:
    """Convert list of review dicts to DataFrame."""
    if not reviews_list:
        return pd.DataFrame(columns=[
            'reviewId', 'userName', 'score', 'at', 'content',
            'thumbsUpCount', 'appVersion', 'replyContent', 'repliedAt'
        ])
    
    df = pd.DataFrame(reviews_list)
    df['at'] = pd.to_datetime(df['at'])
    df = df.sort_values('at', ascending=False)
    return df


def filter_by_date_range(df: pd.DataFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Filter reviews DataFrame by date range.
    
    Args:
        df: Reviews DataFrame with 'at' column
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        DataFrame: Filtered reviews
    """
    if df.empty:
        return df
    
    df = df.copy()
    df['at'] = pd.to_datetime(df['at'])
    
    mask = (df['at'].dt.date >= start_date.date()) & (df['at'].dt.date <= end_date.date())
    filtered = df[mask].copy()
    
    logger.info(f"Filtered {len(df)} reviews to {len(filtered)} in date range "
                f"{format_date(start_date)} to {format_date(end_date)}")
    
    return filtered


def smart_scrape(app_id: str, target_date: str) -> pd.DataFrame:
    """
    Intelligently fetch reviews with CSV caching logic.
    
    This implements the smart scraping strategy:
    - Case A: CSV doesn't exist → Scrape for date range
    - Case B1: All dates present in CSV → Use existing data
    - Case B2: Need newer reviews → Scrape new, append, deduplicate
    - Case B3: Need older reviews → Just use existing data (may be incomplete)
    
    Args:
        app_id: The app package identifier
        target_date: Target date string in YYYY-MM-DD format
        
    Returns:
        DataFrame: Reviews for the date range [T-N, T]
    """
    # Initialize directories
    initialize_app_directories(app_id)
    
    # Calculate date range needed
    start_date, end_date = get_trend_date_range(target_date)
    logger.info(f"Date range needed: {format_date(start_date)} to {format_date(end_date)}")
    
    # CASE A: CSV doesn't exist
    if not reviews_csv_exists(app_id):
        logger.info(f"No CSV found for {app_id}. Running scrape for date range...")
        all_reviews = fetch_reviews_by_date_range(
            app_id=app_id,
            reference_date=end_date,
            cutoff_date=start_date
        )
        save_reviews_csv(app_id, all_reviews)
        return all_reviews
    
    # CASE B: CSV exists
    logger.info(f"CSV found for {app_id}. Checking date coverage...")
    existing_df = load_reviews_csv(app_id)
    
    if existing_df.empty:
        logger.warning("Existing CSV is empty. Running scrape...")
        all_reviews = fetch_reviews_by_date_range(
            app_id=app_id,
            reference_date=end_date,
            cutoff_date=start_date
        )
        save_reviews_csv(app_id, all_reviews)
        return all_reviews
    
    # Ensure datetime format
    existing_df['at'] = pd.to_datetime(existing_df['at'])
    
    # Get date range in existing CSV
    csv_min_date = existing_df['at'].min()
    csv_max_date = existing_df['at'].max()
    
    logger.info(f"Existing CSV date range: {csv_min_date.date()} to {csv_max_date.date()}")
    
    # CASE B1: All needed dates are within existing CSV
    if start_date.date() >= csv_min_date.date() and end_date.date() <= csv_max_date.date():
        logger.info("All needed dates present in CSV. No scraping needed.")
        return filter_by_date_range(existing_df, start_date, end_date)
    
    # CASE B2: Need newer reviews (target_date > csv_max_date)
    if end_date.date() > csv_max_date.date():
        logger.info(f"Need reviews after {csv_max_date.date()}. Scraping new reviews...")
        new_reviews = fetch_reviews_by_date_range(
            app_id=app_id,
            reference_date=end_date,
            cutoff_date=csv_max_date  # Only fetch from where we left off
        )
        
        # Merge with existing (deduplicate by reviewId)
        combined = pd.concat([existing_df, new_reviews], ignore_index=True)
        combined = combined.drop_duplicates(subset=['reviewId'], keep='last')
        combined = combined.sort_values('at', ascending=False)
        
        # Save updated CSV
        save_reviews_csv(app_id, combined)
        logger.info(f"Updated CSV with {len(combined)} total reviews (was {len(existing_df)})")
        
        return filter_by_date_range(combined, start_date, end_date)
    
    # CASE B3: Need older reviews (historical query)
    if start_date.date() < csv_min_date.date():
        logger.warning(f"Requested dates ({format_date(start_date)}) are older than CSV "
                      f"({csv_min_date.date()}). CSV may not have full history.")
        return filter_by_date_range(existing_df, start_date, end_date)
    
    # Default: return filtered existing data
    return filter_by_date_range(existing_df, start_date, end_date)


def get_reviews_for_date(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Get reviews for a specific date.
    
    Args:
        df: Reviews DataFrame
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        DataFrame: Reviews for that specific date
    """
    df = df.copy()
    df['at'] = pd.to_datetime(df['at'])
    
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    mask = df['at'].dt.date == target_date
    
    return df[mask].copy()
