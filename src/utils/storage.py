"""
Storage utility functions for App Review Trend Analyzer.
Handles file I/O for CSVs, JSONs, and directory management.
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, List

from config.settings import (
    get_app_data_dir, get_app_output_dir, get_reviews_csv_path,
    get_processed_dir, get_taxonomy_path, get_batch_path, get_report_path,
    LOGS_DIR, CACHE_DIR
)
from config.seed_topics import get_seed_topics_as_taxonomy


def ensure_dir_exists(path: str) -> None:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to ensure exists
    """
    if path and not os.path.exists(path):
        os.makedirs(path)


def initialize_app_directories(app_id: str) -> None:
    """
    Initialize all directories needed for an app.
    
    Args:
        app_id: The app identifier
    """
    # Create data directories
    ensure_dir_exists(get_app_data_dir(app_id))
    ensure_dir_exists(get_processed_dir(app_id))
    ensure_dir_exists(os.path.dirname(get_taxonomy_path(app_id)))
    
    # Create output directory
    ensure_dir_exists(get_app_output_dir(app_id))
    
    # Create logs and cache directories
    ensure_dir_exists(LOGS_DIR)
    ensure_dir_exists(CACHE_DIR)


def load_json(path: str) -> Optional[Dict[str, Any]]:
    """
    Load a JSON file.
    
    Args:
        path: Path to JSON file
        
    Returns:
        dict or None: Parsed JSON data or None if file doesn't exist
    """
    if not os.path.exists(path):
        return None
    
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(data: Dict[str, Any], path: str) -> None:
    """
    Save data to a JSON file.
    
    Args:
        data: Data to save
        path: Path to save to
    """
    ensure_dir_exists(os.path.dirname(path))
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_csv(path: str) -> Optional[pd.DataFrame]:
    """
    Load a CSV file into a DataFrame.
    
    Args:
        path: Path to CSV file
        
    Returns:
        DataFrame or None: Loaded data or None if file doesn't exist
    """
    if not os.path.exists(path):
        return None
    
    return pd.read_csv(path)


def save_csv(df: pd.DataFrame, path: str) -> None:
    """
    Save a DataFrame to CSV.
    
    Args:
        df: DataFrame to save
        path: Path to save to
    """
    ensure_dir_exists(os.path.dirname(path))
    df.to_csv(path, index=False)


def load_app_taxonomy(app_id: str) -> Dict[str, Any]:
    """
    Load the taxonomy for an app, creating from seeds if it doesn't exist.
    
    Args:
        app_id: The app identifier
        
    Returns:
        dict: The taxonomy data
    """
    taxonomy_path = get_taxonomy_path(app_id)
    taxonomy = load_json(taxonomy_path)
    
    if taxonomy is None:
        # Create from seed topics
        taxonomy = get_seed_topics_as_taxonomy(app_id)
        save_json(taxonomy, taxonomy_path)
    
    return taxonomy


def save_app_taxonomy(app_id: str, taxonomy: Dict[str, Any]) -> None:
    """
    Save the taxonomy for an app.
    
    Args:
        app_id: The app identifier
        taxonomy: The taxonomy data
    """
    taxonomy['last_updated'] = datetime.now().isoformat()
    save_json(taxonomy, get_taxonomy_path(app_id))


def add_topics_to_taxonomy(app_id: str, new_topics: List[Dict[str, Any]]) -> None:
    """
    Add new topics to an app's taxonomy.
    
    Args:
        app_id: The app identifier
        new_topics: List of new topic dictionaries
    """
    taxonomy = load_app_taxonomy(app_id)
    
    # Get existing topic IDs
    existing_ids = {t['topic_id'] for t in taxonomy['topics']}
    
    # Add only truly new topics
    for topic in new_topics:
        if topic['topic_id'] not in existing_ids:
            taxonomy['topics'].append(topic)
    
    save_app_taxonomy(app_id, taxonomy)


def load_batch(app_id: str, date_str: str) -> Optional[Dict[str, Any]]:
    """
    Load a processed batch file.
    
    Args:
        app_id: The app identifier
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        dict or None: Batch data or None if file doesn't exist
    """
    return load_json(get_batch_path(app_id, date_str))


def save_batch(app_id: str, date_str: str, batch_data: Dict[str, Any]) -> None:
    """
    Save a processed batch file.
    
    Args:
        app_id: The app identifier
        date_str: Date string in YYYY-MM-DD format
        batch_data: Batch data to save
    """
    save_json(batch_data, get_batch_path(app_id, date_str))


def reviews_csv_exists(app_id: str) -> bool:
    """
    Check if the reviews CSV exists for an app.
    
    Args:
        app_id: The app identifier
        
    Returns:
        bool: True if CSV exists
    """
    return os.path.exists(get_reviews_csv_path(app_id))


def load_reviews_csv(app_id: str) -> Optional[pd.DataFrame]:
    """
    Load the reviews CSV for an app.
    
    Args:
        app_id: The app identifier
        
    Returns:
        DataFrame or None: Reviews data or None if file doesn't exist
    """
    return load_csv(get_reviews_csv_path(app_id))


def save_reviews_csv(app_id: str, df: pd.DataFrame) -> None:
    """
    Save reviews to the master CSV for an app.
    
    Args:
        app_id: The app identifier
        df: Reviews DataFrame
    """
    save_csv(df, get_reviews_csv_path(app_id))
