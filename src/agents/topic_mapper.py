

from typing import List, Dict, Any, Optional

from config.settings import CONFIDENCE_THRESHOLD
from src.utils.logger import get_mapper_logger
from src.utils.groq_client import GroqClient, create_groq_client

logger = get_mapper_logger()

# System prompt for topic mapping
MAPPING_SYSTEM_PROMPT = """You are a topic mapping agent for app review analysis.

Your task is to map extracted topics to predefined categories in a taxonomy.

Rules:
1. Be STRICT: Similar topics MUST map to the same category
   Example: "delivery late", "slow delivery", "took forever" → ALL map to "delivery_delay"

2. Consider semantic meaning, not just keywords
   Example: "driver was rude" should map to "staff_behavior", not "delivery_delay"

3. Output confidence score (0.0 to 1.0):
   - 1.0 = Perfect match
   - 0.7-0.9 = Good match with slight variation
   - 0.5-0.7 = Partial match, could be borderline
   - Below 0.5 = Poor match or new topic

4. If confidence < 0.70, set mapped_topic_id to null
   This indicates a potentially NEW topic that should be added to taxonomy

Output format (JSON):
{
  "extracted_topic": "the original topic",
  "mapped_topic_id": "delivery_delay",  // or null if no good match
  "confidence": 0.92,
  "reasoning": "Brief explanation of mapping decision"
}

Return ONLY the JSON object, no additional text."""


def create_mapping_prompt(extracted_topic: str, taxonomy: Dict[str, Any]) -> str:
    """
    Generate prompt to map an extracted topic to taxonomy.
    
    Args:
        extracted_topic: The topic text to map
        taxonomy: The app's taxonomy dictionary
        
    Returns:
        str: Formatted prompt
    """
    topics_list = []
    for t in taxonomy.get('topics', []):
        variations = ', '.join(t.get('variations', [])[:5])  # Limit variations shown
        topics_list.append(
            f"- {t['topic_id']}: {t['topic_name']} "
            f"(category: {t.get('category', 'unknown')}, variations: {variations})"
        )
    
    topics_str = "\n".join(topics_list)
    
    prompt = f"""PREDEFINED TOPICS IN TAXONOMY:
{topics_str}

EXTRACTED TOPIC TO MAP: "{extracted_topic}"

Map this extracted topic to the CLOSEST predefined topic, or indicate if it's a new topic."""
    
    return prompt


def map_single_topic(
    extracted_topic: str, 
    taxonomy: Dict[str, Any], 
    groq_client: GroqClient
) -> Dict[str, Any]:
    """
    Map a single extracted topic to the taxonomy.
    
    Args:
        extracted_topic: Topic text to map
        taxonomy: App's taxonomy
        groq_client: Groq client instance
        
    Returns:
        Mapping result dict
    """
    prompt = create_mapping_prompt(extracted_topic, taxonomy)
    
    result = groq_client.send_message_json(
        MAPPING_SYSTEM_PROMPT,
        prompt
    )
    
    if result is None:
        return {
            "extracted_topic": extracted_topic,
            "mapped_topic_id": None,
            "confidence": 0.0,
            "reasoning": "Failed to get mapping from LLM"
        }
    
    # Ensure result has required fields
    if isinstance(result, dict):
        result['extracted_topic'] = extracted_topic
        
        # Apply confidence threshold
        confidence = result.get('confidence', 0.0)
        if confidence < CONFIDENCE_THRESHOLD:
            result['mapped_topic_id'] = None
        
        return result
    
    return {
        "extracted_topic": extracted_topic,
        "mapped_topic_id": None,
        "confidence": 0.0,
        "reasoning": f"Unexpected response format: {type(result)}"
    }


def map_topics_batch(
    extracted_topics: List[str],
    taxonomy: Dict[str, Any],
    groq_client: GroqClient
) -> List[Dict[str, Any]]:
    """
    Map a batch of topics using a single API call for efficiency.
    
    Args:
        extracted_topics: List of topic strings
        taxonomy: App's taxonomy
        groq_client: Groq client instance
        
    Returns:
        List of mapping results
    """
    if not extracted_topics:
        return []
    
    # Create batch prompt
    topics_list = []
    for t in taxonomy.get('topics', []):
        variations = ', '.join(t.get('variations', [])[:3])
        topics_list.append(f"- {t['topic_id']}: {t['topic_name']} ({variations})")
    
    topics_str = "\n".join(topics_list)
    extracted_str = "\n".join([f"- \"{t}\"" for t in extracted_topics])
    
    batch_prompt = f"""PREDEFINED TOPICS:
{topics_str}

EXTRACTED TOPICS TO MAP:
{extracted_str}

For EACH extracted topic, provide a mapping. Return a JSON array:
[
  {{"extracted_topic": "...", "mapped_topic_id": "...", "confidence": 0.X, "reasoning": "..."}},
  ...
]

If confidence < 0.70, set mapped_topic_id to null."""

    result = groq_client.send_message_json(
        MAPPING_SYSTEM_PROMPT,
        batch_prompt
    )
    
    if result is None or not isinstance(result, list):
        # Fallback: return unmapped for all
        return [
            {
                "extracted_topic": t,
                "mapped_topic_id": None,
                "confidence": 0.0,
                "reasoning": "Batch mapping failed"
            }
            for t in extracted_topics
        ]
    
    # Apply confidence threshold
    for r in result:
        if r.get('confidence', 0.0) < CONFIDENCE_THRESHOLD:
            r['mapped_topic_id'] = None
    
    return result


def map_topics_to_taxonomy(
    extraction_results: List[Dict[str, Any]],
    taxonomy: Dict[str, Any],
    groq_client: GroqClient = None
) -> Dict[str, Dict[str, Any]]:
    """
    Map all extracted topics to taxonomy.
    
    Args:
        extraction_results: Results from topic_extractor (list of {reviewId, extractedTopics})
        taxonomy: App's taxonomy
        groq_client: Optional Groq client
        
    Returns:
        Dict mapping extracted topic strings to their mapping results
    """
    if groq_client is None:
        groq_client = create_groq_client()
    
    # Collect all unique extracted topics
    all_topics = set()
    for result in extraction_results:
        for topic in result.get('extractedTopics', []):
            if topic and topic.strip():
                all_topics.add(topic.strip().lower())
    
    all_topics = list(all_topics)
    
    if not all_topics:
        logger.info("No topics to map")
        return {}
    
    logger.info(f"Mapping {len(all_topics)} unique topics to taxonomy")
    
    # Process in batches
    MAPPING_BATCH_SIZE = 10
    all_mappings = {}
    
    for i in range(0, len(all_topics), MAPPING_BATCH_SIZE):
        batch = all_topics[i:i + MAPPING_BATCH_SIZE]
        batch_num = (i // MAPPING_BATCH_SIZE) + 1
        total_batches = (len(all_topics) + MAPPING_BATCH_SIZE - 1) // MAPPING_BATCH_SIZE
        
        logger.info(f"Mapping batch {batch_num}/{total_batches}")
        
        results = map_topics_batch(batch, taxonomy, groq_client)
        
        for result in results:
            topic = result.get('extracted_topic', '')
            all_mappings[topic.lower()] = result
    
    # Log summary
    mapped_count = sum(1 for m in all_mappings.values() if m.get('mapped_topic_id'))
    unmapped_count = len(all_mappings) - mapped_count
    
    logger.info(f"Mapping complete: {mapped_count} mapped, {unmapped_count} unmapped (potential new topics)")
    
    return all_mappings
