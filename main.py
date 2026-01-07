"""
App Review Trend Analyzer - Main Entry Point

CLI tool to analyze Google Play Store app reviews and generate trend reports.

Usage:
    python main.py --app-id in.swiggy.android --target-date 2024-07-01
    python main.py --app-id com.application.zomato --target-date 2024-12-15
    python main.py --app-id com.netflix.mediaclient --target-date 2024-11-20
"""

import argparse
import sys
from datetime import datetime

from src.utils.logger import get_main_logger
from src.utils.storage import initialize_app_directories, load_app_taxonomy
from src.utils.date_utils import get_date_strings_in_range, format_date
from src.utils.groq_client import create_groq_client
from src.scraper import smart_scrape, get_reviews_for_date
from src.agents.topic_extractor import extract_topics_for_reviews
from src.agents.topic_mapper import map_topics_to_taxonomy
from src.agents.consolidator import consolidate_and_discover
from src.report_generator import generate_trend_report, get_report_summary, print_report_summary

logger = get_main_logger()


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Namespace with app_id and target_date
    """
    parser = argparse.ArgumentParser(
        description='App Review Trend Analyzer - Analyze Google Play Store reviews',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py --app-id in.swiggy.android --target-date 2024-07-01
    python main.py --app-id com.application.zomato --target-date 2024-12-15
    python main.py --app-id com.netflix.mediaclient --target-date 2024-11-20
        """
    )
    
    parser.add_argument(
        '--app-id',
        required=True,
        help='Google Play Store app package ID (e.g., in.swiggy.android)'
    )
    
    parser.add_argument(
        '--target-date',
        required=True,
        help='Target date for analysis in YYYY-MM-DD format'
    )
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        datetime.strptime(args.target_date, "%Y-%m-%d")
    except ValueError:
        parser.error(f"Invalid date format: {args.target_date}. Use YYYY-MM-DD format.")
    
    return args


def process_single_day(
    reviews_df,
    date_str: str,
    app_id: str,
    groq_client
) -> dict:
    """
    Process reviews for a single day through all three agents.
    
    Args:
        reviews_df: DataFrame with all reviews
        date_str: Date to process
        app_id: App identifier
        groq_client: Groq client instance
        
    Returns:
        Batch data dict
    """
    # Get reviews for this specific date
    day_reviews = get_reviews_for_date(reviews_df, date_str)
    
    if day_reviews.empty:
        logger.info(f"No reviews found for {date_str}")
        # Create empty batch
        return {
            "app_id": app_id,
            "date": date_str,
            "total_reviews": 0,
            "topic_frequencies": {},
            "new_topics_discovered": []
        }
    
    logger.info(f"Processing {len(day_reviews)} reviews for {date_str}")
    
    # Agent 1: Extract topics
    extraction_results = extract_topics_for_reviews(day_reviews, groq_client)
    
    # Agent 2: Map topics to taxonomy
    taxonomy = load_app_taxonomy(app_id)
    mappings = map_topics_to_taxonomy(extraction_results, taxonomy, groq_client)
    
    # Agent 3: Consolidate counts and discover new topics
    batch_data = consolidate_and_discover(
        extraction_results, 
        mappings, 
        app_id, 
        date_str,
        reviews_df=day_reviews,  # Pass reviews for detailed export
        groq_client=groq_client
    )
    
    return batch_data


def orchestrate_analysis(app_id: str, target_date: str):
    """
    Main workflow orchestration.
    
    1. Initialize directories
    2. Smart scrape (with CSV caching)
    3. Process each day through the 3 agents
    4. Generate trend report
    
    Args:
        app_id: App package identifier
        target_date: Target date string
    """
    logger.info("=" * 60)
    logger.info(f"Starting analysis for {app_id}")
    logger.info(f"Target date: {target_date}")
    logger.info("=" * 60)
    
    # Step 1: Initialize directories
    logger.info("Step 1: Initializing directories...")
    initialize_app_directories(app_id)
    
    # Step 2: Smart scraping
    logger.info("Step 2: Smart scraping with CSV caching...")
    reviews_df = smart_scrape(app_id, target_date)
    
    if reviews_df.empty:
        logger.error("No reviews found. Exiting.")
        return
    
    logger.info(f"Total reviews in date range: {len(reviews_df)}")
    
    # Step 3: Initialize Groq client
    logger.info("Step 3: Initializing AI agents...")
    groq_client = create_groq_client()
    
    # Step 4: Process each day
    logger.info("Step 4: Processing reviews day by day...")
    date_strings = get_date_strings_in_range(target_date)
    
    total_days = len(date_strings)
    for i, date_str in enumerate(date_strings, 1):
        logger.info(f"\n--- Day {i}/{total_days}: {date_str} ---")
        process_single_day(reviews_df, date_str, app_id, groq_client)
    
    # Step 5: Generate report
    logger.info("\nStep 5: Generating trend report...")
    report_path = generate_trend_report(app_id, target_date)
    
    if report_path:
        summary = get_report_summary(app_id, target_date)
        print_report_summary(summary)
    else:
        logger.error("Failed to generate report")
    
    logger.info("Analysis complete!")


def main():
    """Main entry point."""
    try:
        args = parse_arguments()
        orchestrate_analysis(args.app_id, args.target_date)
    except KeyboardInterrupt:
        logger.info("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise


if __name__ == "__main__":
    main()
