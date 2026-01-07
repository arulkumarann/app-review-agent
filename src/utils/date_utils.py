"""
Date utility functions for App Review Trend Analyzer.
"""

from datetime import datetime, timedelta
from typing import List, Tuple
from config.settings import TREND_WINDOW_DAYS


def parse_iso_date(date_str: str) -> datetime:
    """
    Parse an ISO format date string.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        datetime: Parsed datetime object
    """
    return datetime.strptime(date_str, "%Y-%m-%d")


def format_date(dt: datetime) -> str:
    """
    Format a datetime object to ISO date string.
    
    Args:
        dt: Datetime object
        
    Returns:
        str: Date string in YYYY-MM-DD format
    """
    return dt.strftime("%Y-%m-%d")


def date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    Generate a list of dates between start and end (inclusive).
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        List[datetime]: List of dates
    """
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def get_trend_date_range(target_date: str) -> Tuple[datetime, datetime]:
    """
    Calculate the date range for trend analysis.
    Returns (start_date, end_date) where start_date = target_date - TREND_WINDOW_DAYS
    
    Args:
        target_date: Target date string in YYYY-MM-DD format
        
    Returns:
        Tuple[datetime, datetime]: (start_date, end_date)
    """
    end_date = parse_iso_date(target_date)
    start_date = end_date - timedelta(days=TREND_WINDOW_DAYS)
    return start_date, end_date


def get_date_strings_in_range(target_date: str) -> List[str]:
    """
    Get all date strings in the trend window.
    
    Args:
        target_date: Target date string in YYYY-MM-DD format
        
    Returns:
        List[str]: List of date strings in YYYY-MM-DD format
    """
    start_date, end_date = get_trend_date_range(target_date)
    dates = date_range(start_date, end_date)
    return [format_date(d) for d in dates]


def is_date_in_range(date: datetime, start: datetime, end: datetime) -> bool:
    """
    Check if a date is within a range (inclusive).
    
    Args:
        date: Date to check
        start: Start of range
        end: End of range
        
    Returns:
        bool: True if date is in range
    """
    return start <= date <= end
