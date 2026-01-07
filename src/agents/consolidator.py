

from datetime import datetime
from collections import Counter, defaultdict
from typing import List, Dict, Any, Tuple
import pandas as pd

from config.settings import MIN_TOPIC_OCCURRENCES, get_processed_dir
from src.utils.logger import get_consolidator_logger
from src.utils.storage import (
    load_app_taxonomy, add_topics_to_taxonomy, save_batch, save_json, ensure_dir_exists
)
from src.utils.groq_client import GroqClient, create_groq_client
import os

logger = get_consolidator_logger()

# System prompt for validating new topics
VALIDATION_SYSTEM_PROMPT = """You are a topic validation agent for app review analysis.

Your task is to determine if an extracted phrase represents a VALID, ACTIONABLE topic that should be added to a taxonomy.

A VALID topic is:
1. A specific issue, bug, complaint, or feature request
2. Something that can be tracked and addressed
3. Not generic sentiment (e.g., "bad app" is not a topic, but "app crashes on login" is)

An INVALID topic is:
1. Generic sentiment without specifics (e.g., "terrible", "worst app")
2. Too vague to be actionable (e.g., "doesn't work")
3. Actually a duplicate of existing topics (just worded differently)

Output format (JSON):
{
  "topic": "the original topic",
  "is_valid": true/false,
  "suggested_topic_id": "snake_case_id",
  "suggested_topic_name": "Human Readable Name",
  "suggested_category": "issue" or "request",
  "reasoning": "Why this is or isn't a valid new topic"
}

Return ONLY the JSON object."""


def generate_topic_id(topic_name: str) -> str:
    """
    Generate a snake_case topic ID from a topic name.
    
    Args:
        topic_name: Human-readable topic name
        
    Returns:
        str: Snake case ID
    """
    # Simple conversion to snake_case
    import re
    
    # Remove special characters, lowercase, replace spaces with underscores
    topic_id = topic_name.lower()
    topic_id = re.sub(r'[^a-z0-9\s]', '', topic_id)
    topic_id = re.sub(r'\s+', '_', topic_id.strip())
    
    # Limit length
    if len(topic_id) > 30:
        topic_id = topic_id[:30].rstrip('_')
    
    return topic_id


def validate_new_topic(
    topic: str, 
    existing_topics: List[str],
    groq_client: GroqClient
) -> Dict[str, Any]:
    """
    Validate if a topic should be added to the taxonomy.
    
    Args:
        topic: Topic text to validate
        existing_topics: List of existing topic names
        groq_client: Groq client instance
        
    Returns:
        Validation result dict
    """
    existing_str = ", ".join(existing_topics[:20])  # Limit for context
    
    prompt = f"""EXISTING TOPICS IN TAXONOMY:
{existing_str}

NEW TOPIC TO VALIDATE: "{topic}"

Determine if this should be added as a new topic."""

    result = groq_client.send_message_json(
        VALIDATION_SYSTEM_PROMPT,
        prompt
    )
    
    if result is None:
        return {
            "topic": topic,
            "is_valid": False,
            "reasoning": "Failed to validate with LLM"
        }
    
    return result


def consolidate_and_discover(
    extraction_results: List[Dict[str, Any]],
    mappings: Dict[str, Dict[str, Any]],
    app_id: str,
    date_str: str,
    reviews_df: pd.DataFrame = None,
    groq_client: GroqClient = None
) -> Dict[str, Any]:
    """
    Consolidate topic counts and discover new topics.
    
    This function:
    1. Counts frequency of each mapped topic
    2. Identifies unmapped topics (confidence < threshold)
    3. Validates frequently occurring unmapped topics as new topics
    4. Updates the app's taxonomy with valid new topics
    5. Saves the batch results
    6. Saves detailed reviews-per-topic JSON
    
    Args:
        extraction_results: Results from topic_extractor
        mappings: Results from topic_mapper
        app_id: App identifier
        date_str: Date string for this batch
        reviews_df: Original reviews DataFrame with full metadata
        groq_client: Optional Groq client
        
    Returns:
        Batch summary dict
    """
    if groq_client is None:
        groq_client = create_groq_client()
    
    logger.info(f"Consolidating topics for {app_id} on {date_str}")
    
    # Build review lookup from DataFrame
    reviews_lookup = {}
    if reviews_df is not None and not reviews_df.empty:
        for _, row in reviews_df.iterrows():
            reviews_lookup[row.get('reviewId', '')] = {
                "reviewId": row.get('reviewId', ''),
                "userName": row.get('userName', ''),
                "score": int(row.get('score', 0)),
                "at": str(row.get('at', '')),
                "content": row.get('content', ''),
                "thumbsUpCount": int(row.get('thumbsUpCount', 0)) if pd.notna(row.get('thumbsUpCount')) else 0,
                "appVersion": row.get('appVersion', ''),
                "replyContent": row.get('replyContent', '') if pd.notna(row.get('replyContent')) else None,
                "repliedAt": str(row.get('repliedAt', '')) if pd.notna(row.get('repliedAt')) else None,
            }
    
    # Count topic frequencies and build reviews-per-topic
    topic_counts = Counter()
    unmapped_topics = []
    
    # Detailed tracking
    reviews_by_topic = defaultdict(list)  # topic_id -> [reviews]
    reviews_with_no_topics = []  # reviews that had zero extracted topics
    unmapped_reviews = defaultdict(list)  # unmapped_topic_text -> [reviews]
    
    for result in extraction_results:
        review_id = result.get('reviewId', '')
        extracted_topics = result.get('extractedTopics', [])
        review_data = reviews_lookup.get(review_id, {"reviewId": review_id})
        
        # Track reviews with no topics
        if not extracted_topics:
            reviews_with_no_topics.append(review_data)
            continue
        
        for topic in extracted_topics:
            topic_lower = topic.strip().lower()
            mapping = mappings.get(topic_lower, {})
            
            mapped_id = mapping.get('mapped_topic_id')
            
            if mapped_id:
                topic_counts[mapped_id] += 1
                # Add review to this topic's list (avoid duplicates)
                review_entry = {
                    **review_data,
                    "extracted_topic": topic,
                    "confidence": mapping.get('confidence', 0)
                }
                reviews_by_topic[mapped_id].append(review_entry)
            else:
                unmapped_topics.append(topic)
                unmapped_reviews[topic_lower].append({
                    **review_data,
                    "extracted_topic": topic
                })
    
    # Count unmapped topics
    unmapped_counts = Counter(t.lower() for t in unmapped_topics)
    
    logger.info(f"Found {len(topic_counts)} mapped topics, {len(unmapped_counts)} unique unmapped topics")
    
    # Check for new topics to add (frequency >= threshold)
    new_topics_to_add = []
    
    # Get existing topic names for comparison
    taxonomy = load_app_taxonomy(app_id)
    existing_names = [t['topic_name'] for t in taxonomy.get('topics', [])]
    topic_id_to_name = {t['topic_id']: t['topic_name'] for t in taxonomy.get('topics', [])}
    
    for topic, count in unmapped_counts.items():
        if count >= MIN_TOPIC_OCCURRENCES:
            logger.info(f"Potential new topic: '{topic}' (mentioned {count} times)")
            
            # Validate with LLM
            validation = validate_new_topic(topic, existing_names, groq_client)
            
            if validation.get('is_valid', False):
                new_topic = {
                    "topic_id": validation.get('suggested_topic_id', generate_topic_id(topic)),
                    "topic_name": validation.get('suggested_topic_name', topic.title()),
                    "category": validation.get('suggested_category', 'issue'),
                    "variations": [topic],
                    "description": f"Auto-discovered topic: {topic}",
                    "added_date": date_str,
                    "is_seed": False,
                    "app_specific": True
                }
                new_topics_to_add.append(new_topic)
                
                # Add the count to topic_counts
                topic_counts[new_topic['topic_id']] = count
                
                # Move reviews from unmapped to this new topic
                reviews_by_topic[new_topic['topic_id']] = unmapped_reviews.get(topic, [])
                topic_id_to_name[new_topic['topic_id']] = new_topic['topic_name']
                
                logger.info(f"Validated new topic: {new_topic['topic_name']}")
            else:
                logger.info(f"Rejected potential topic: {topic} - {validation.get('reasoning', 'unknown')}")
    
    # Update taxonomy with new topics
    if new_topics_to_add:
        add_topics_to_taxonomy(app_id, new_topics_to_add)
        logger.info(f"Added {len(new_topics_to_add)} new topics to taxonomy")
    
    # Prepare batch data (summary)
    batch_data = {
        "app_id": app_id,
        "date": date_str,
        "total_reviews": len(extraction_results),
        "reviews_with_topics": len(extraction_results) - len(reviews_with_no_topics),
        "reviews_without_topics": len(reviews_with_no_topics),
        "total_topic_mentions": sum(topic_counts.values()),
        "topic_frequencies": dict(topic_counts),
        "new_topics_discovered": [t['topic_id'] for t in new_topics_to_add],
        "unmapped_count": sum(1 for t, c in unmapped_counts.items() if c < MIN_TOPIC_OCCURRENCES),
        "processed_at": datetime.now().isoformat()
    }
    
    # Save batch summary
    save_batch(app_id, date_str, batch_data)
    
    # Prepare and save detailed reviews-per-topic JSON
    detailed_data = {
        "app_id": app_id,
        "date": date_str,
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "total_reviews": len(extraction_results),
            "reviews_with_topics": len(extraction_results) - len(reviews_with_no_topics),
            "reviews_without_topics": len(reviews_with_no_topics),
            "total_topic_mentions": sum(topic_counts.values()),
            "unique_topics": len(topic_counts)
        },
        "topics": {},
        "unmapped_topics": {},
        "reviews_without_extractable_topics": reviews_with_no_topics
    }
    
    # Add reviews grouped by topic
    for topic_id, reviews_list in reviews_by_topic.items():
        detailed_data["topics"][topic_id] = {
            "topic_name": topic_id_to_name.get(topic_id, topic_id),
            "count": len(reviews_list),
            "reviews": reviews_list
        }
    
    # Add unmapped topics that weren't promoted to new topics
    for topic_text, reviews_list in unmapped_reviews.items():
        if not any(t['topic_id'] == generate_topic_id(topic_text) for t in new_topics_to_add):
            detailed_data["unmapped_topics"][topic_text] = {
                "count": len(reviews_list),
                "reviews": reviews_list
            }
    
    # Save detailed JSON
    detailed_path = os.path.join(get_processed_dir(app_id), f"details_{date_str}.json")
    ensure_dir_exists(os.path.dirname(detailed_path))
    save_json(detailed_data, detailed_path)
    
    logger.info(f"Batch saved for {date_str}: {sum(topic_counts.values())} total topic mentions")
    logger.info(f"Detailed reviews saved to: details_{date_str}.json")
    
    return batch_data


def get_topic_frequency_summary(batch_data: Dict[str, Any]) -> List[Tuple[str, int]]:
    """
    Get a sorted list of (topic_id, count) tuples from batch data.
    
    Args:
        batch_data: Batch data dictionary
        
    Returns:
        List of (topic_id, count) sorted by count descending
    """
    frequencies = batch_data.get('topic_frequencies', {})
    return sorted(frequencies.items(), key=lambda x: x[1], reverse=True)
