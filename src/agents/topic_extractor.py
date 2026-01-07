

import pandas as pd
from typing import List, Dict, Any

from config.settings import BATCH_SIZE
from src.utils.logger import get_extractor_logger
from src.utils.groq_client import GroqClient, create_groq_client

logger = get_extractor_logger()

# System prompt for topic extraction (app-agnostic)
EXTRACTION_SYSTEM_PROMPT = """You are a topic extraction agent for mobile app reviews.

Your task:
1. Read user reviews for a mobile application
2. Extract ALL issues, requests, feedback, and complaints mentioned
3. Be specific but concise

Output format (JSON array):
[
  {
    "reviewId": "abc123",
    "extractedTopics": ["delivery was very late", "food arrived cold"]
  },
  {
    "reviewId": "xyz789", 
    "extractedTopics": ["app crashes frequently"]
  }
]

Rules:
- Extract actual problems/requests, NOT sentiment or general praise
- Keep extracted topics brief (3-7 words each)
- One review can have multiple topics
- If a review has no actionable topic (e.g., just "Great app!"), return empty array for extractedTopics
- Focus on specific issues: delays, quality problems, bugs, feature requests, etc.
- Do NOT include generic positive feedback like "good service" or "nice app"

Return ONLY the JSON array, no additional text."""


def create_extraction_prompt(reviews: List[Dict[str, Any]]) -> str:
    """
    Create the user prompt for topic extraction.
    
    Args:
        reviews: List of review dictionaries with 'reviewId' and 'content'
        
    Returns:
        str: Formatted prompt
    """
    reviews_text = []
    for r in reviews:
        review_id = r.get('reviewId', 'unknown')
        content = r.get('content', '')
        score = r.get('score', 'N/A')
        reviews_text.append(f"[ID: {review_id}] [Score: {score}/5]\n{content}")
    
    prompt = f"""Extract topics from these {len(reviews)} app reviews:

---
""" + "\n---\n".join(reviews_text) + """
---

Return JSON array with reviewId and extractedTopics for each review."""
    
    return prompt


def extract_topics_from_batch(
    reviews: List[Dict[str, Any]], 
    groq_client: GroqClient
) -> List[Dict[str, Any]]:
    """
    Extract topics from a batch of reviews.
    
    Args:
        reviews: List of review dictionaries
        groq_client: Groq client instance
        
    Returns:
        List of dicts with reviewId and extractedTopics
    """
    if not reviews:
        return []
    
    prompt = create_extraction_prompt(reviews)
    
    result = groq_client.send_message_json(
        EXTRACTION_SYSTEM_PROMPT,
        prompt
    )
    
    if result is None:
        logger.warning(f"Failed to extract topics from batch of {len(reviews)} reviews")
        # Return empty topics for all reviews
        return [{"reviewId": r.get('reviewId', 'unknown'), "extractedTopics": []} for r in reviews]
    
    # Validate and clean result
    if isinstance(result, list):
        return result
    
    logger.warning(f"Unexpected response format: {type(result)}")
    return [{"reviewId": r.get('reviewId', 'unknown'), "extractedTopics": []} for r in reviews]


def extract_topics_for_reviews(
    reviews_df: pd.DataFrame,
    groq_client: GroqClient = None
) -> List[Dict[str, Any]]:
    """
    Extract topics from all reviews in a DataFrame.
    Processes in batches to respect API limits.
    
    Args:
        reviews_df: DataFrame with reviews (must have 'reviewId' and 'content' columns)
        groq_client: Optional Groq client (creates one if not provided)
        
    Returns:
        List of dicts with reviewId and extractedTopics for all reviews
    """
    if reviews_df.empty:
        logger.info("No reviews to process")
        return []
    
    if groq_client is None:
        groq_client = create_groq_client()
    
    # Convert DataFrame to list of dicts
    reviews = reviews_df.to_dict('records')
    
    logger.info(f"Extracting topics from {len(reviews)} reviews in batches of {BATCH_SIZE}")
    
    all_results = []
    
    for i in range(0, len(reviews), BATCH_SIZE):
        batch = reviews[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(reviews) + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} reviews)")
        
        batch_results = extract_topics_from_batch(batch, groq_client)
        all_results.extend(batch_results)
    
    # Count extracted topics
    total_topics = sum(len(r.get('extractedTopics', [])) for r in all_results)
    logger.info(f"Extracted {total_topics} topics from {len(reviews)} reviews")
    
    return all_results


def get_all_extracted_topics(extraction_results: List[Dict[str, Any]]) -> List[str]:
    """
    Get a flat list of all extracted topics.
    
    Args:
        extraction_results: Results from extract_topics_for_reviews
        
    Returns:
        List of unique topic strings
    """
    all_topics = []
    for result in extraction_results:
        topics = result.get('extractedTopics', [])
        all_topics.extend(topics)
    
    return list(set(all_topics))
