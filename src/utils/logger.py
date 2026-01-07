"""
Logging configuration for App Review Trend Analyzer.
Provides file and console logging with rotation.
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from config.settings import LOGS_DIR


def ensure_logs_dir():
    """Ensure the logs directory exists."""
    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)


def get_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (usually module name)
        log_file: Optional log file name (without path)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    ensure_logs_dir()
    
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if specified)
        if log_file:
            file_path = os.path.join(LOGS_DIR, log_file)
            file_handler = RotatingFileHandler(
                file_path,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    
    return logger


# Pre-configured loggers for each component
def get_scraper_logger():
    return get_logger("scraper", "scraper.log")


def get_extractor_logger():
    return get_logger("agent_extractor", "agent_extractor.log")


def get_mapper_logger():
    return get_logger("agent_mapper", "agent_mapper.log")


def get_consolidator_logger():
    return get_logger("agent_consolidator", "agent_consolidator.log")


def get_main_logger():
    return get_logger("main", "main.log")
