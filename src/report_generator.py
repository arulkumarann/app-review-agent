"""
Report Generator for App Review Trend Analyzer.
Creates 30-day trend CSV with topics as rows and dates as columns.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

from config.settings import get_report_path, TREND_WINDOW_DAYS
from src.utils.logger import get_main_logger
from src.utils.date_utils import get_date_strings_in_range
from src.utils.storage import (
    load_batch, load_app_taxonomy, ensure_dir_exists
)

logger = get_main_logger()


def generate_trend_report(app_id: str, target_date: str) -> str:
    """
    Generate a 30-day trend report CSV.
    
    The report has:
    - Rows: Topic names
    - Columns: Dates (T-30 to T)
    - Values: Frequency counts
    
    Args:
        app_id: App identifier
        target_date: Target date string in YYYY-MM-DD format
        
    Returns:
        str: Path to generated report
    """
    logger.info(f"Generating trend report for {app_id}, target date: {target_date}")
    
    # Get all dates in range
    date_strings = get_date_strings_in_range(target_date)
    
    # Load all batch files
    batches = []
    for date_str in date_strings:
        batch = load_batch(app_id, date_str)
        if batch:
            batches.append(batch)
    
    if not batches:
        logger.warning(f"No batch data found for {app_id}")
        return None
    
    logger.info(f"Loaded {len(batches)} batch files")
    
    # Get all unique topics across all batches
    all_topic_ids = set()
    for batch in batches:
        all_topic_ids.update(batch.get('topic_frequencies', {}).keys())
    
    all_topic_ids = sorted(all_topic_ids)
    
    if not all_topic_ids:
        logger.warning("No topics found in batches")
        return None
    
    # Load taxonomy for topic names
    taxonomy = load_app_taxonomy(app_id)
    topic_id_to_name = {
        t['topic_id']: t['topic_name'] 
        for t in taxonomy.get('topics', [])
    }
    
    # Create DataFrame
    # Index: topic_id, Columns: dates
    df = pd.DataFrame(index=all_topic_ids, columns=date_strings)
    df = df.fillna(0)
    
    # Fill frequencies
    for batch in batches:
        date_str = batch.get('date')
        if date_str in df.columns:
            for topic_id, count in batch.get('topic_frequencies', {}).items():
                if topic_id in df.index:
                    df.loc[topic_id, date_str] = count
    
    # Convert to int
    df = df.astype(int)
    
    # Add topic name as first column
    df.insert(0, 'Topic', df.index.map(lambda x: topic_id_to_name.get(x, x)))
    
    # Add total column
    date_cols = [c for c in df.columns if c != 'Topic']
    df['Total'] = df[date_cols].sum(axis=1)
    
    # Sort by total frequency (descending)
    df = df.sort_values('Total', ascending=False)
    
    # Remove total column from final output (optional - keeping for analysis)
    # df = df.drop('Total', axis=1)
    
    # Reset index to include topic_id
    df = df.reset_index()
    df = df.rename(columns={'index': 'topic_id'})
    
    # Reorder columns: topic_id, Topic, dates..., Total
    cols = ['topic_id', 'Topic'] + date_strings + ['Total']
    df = df[cols]
    
    # Save report
    report_path = get_report_path(app_id, target_date)
    ensure_dir_exists(report_path.rsplit('/', 1)[0] if '/' in report_path else report_path.rsplit('\\', 1)[0])
    
    df.to_csv(report_path, index=False)
    
    logger.info(f"Report saved: {report_path}")
    logger.info(f"Report contains {len(df)} topics across {len(date_strings)} days")
    
    return report_path


def get_report_summary(app_id: str, target_date: str) -> Dict[str, Any]:
    """
    Get a summary of the generated report.
    
    Args:
        app_id: App identifier
        target_date: Target date string
        
    Returns:
        Summary dictionary
    """
    report_path = get_report_path(app_id, target_date)
    
    try:
        df = pd.read_csv(report_path)
    except FileNotFoundError:
        return {"error": "Report not found"}
    
    # Get top topics by total
    top_topics = df.nlargest(10, 'Total')[['Topic', 'Total']].to_dict('records')
    
    # Calculate some stats
    date_cols = [c for c in df.columns if c not in ['topic_id', 'Topic', 'Total']]
    
    summary = {
        "app_id": app_id,
        "target_date": target_date,
        "total_topics": len(df),
        "total_mentions": int(df['Total'].sum()),
        "date_range": f"{date_cols[0]} to {date_cols[-1]}" if date_cols else "N/A",
        "top_10_topics": top_topics,
        "report_path": report_path
    }
    
    return summary


def print_report_summary(summary: Dict[str, Any]) -> None:
    """
    Print a formatted summary of the report.
    
    Args:
        summary: Summary dictionary from get_report_summary
    """
    print("\n" + "=" * 60)
    print("TREND REPORT SUMMARY")
    print("=" * 60)
    print(f"App ID: {summary.get('app_id')}")
    print(f"Target Date: {summary.get('target_date')}")
    print(f"Date Range: {summary.get('date_range')}")
    print(f"Total Topics: {summary.get('total_topics')}")
    print(f"Total Mentions: {summary.get('total_mentions')}")
    print("-" * 60)
    print("TOP 10 TOPICS:")
    print("-" * 60)
    
    for i, topic in enumerate(summary.get('top_10_topics', []), 1):
        print(f"  {i:2}. {topic['Topic']}: {topic['Total']} mentions")
    
    print("-" * 60)
    print(f"Report saved to: {summary.get('report_path')}")
    print("=" * 60 + "\n")
