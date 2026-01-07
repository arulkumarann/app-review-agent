"""
Groq API client wrapper for App Review Trend Analyzer.
Provides retry logic and JSON parsing utilities.
"""

import json
import time
from typing import Optional, Dict, Any, List
from groq import Groq

from config.settings import GROQ_API_KEY, GROQ_MODEL, MAX_RETRIES, TEMPERATURE
from src.utils.logger import get_logger

logger = get_logger("groq_client")


class GroqClient:
    """Wrapper around Groq API with retry logic."""
    
    def __init__(self):
        """Initialize the Groq client."""
        if not GROQ_API_KEY:
            raise ValueError("GROQ_API_KEY not set in environment")
        
        self.client = Groq(api_key=GROQ_API_KEY)
        self.model = GROQ_MODEL
    
    def send_message(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = None,
        max_tokens: int = 4096
    ) -> Optional[str]:
        """
        Send a message to Groq and get a response.
        
        Args:
            system_prompt: System message for context
            user_prompt: User message/query
            temperature: Override default temperature
            max_tokens: Maximum tokens in response
            
        Returns:
            str or None: Response content or None on failure
        """
        temp = temperature if temperature is not None else TEMPERATURE
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=temp,
                    max_tokens=max_tokens
                )
                
                return response.choices[0].message.content
                
            except Exception as e:
                logger.warning(f"Groq API error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Groq API failed after {MAX_RETRIES} attempts")
                    return None
    
    def send_message_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = None
    ) -> Optional[Any]:
        """
        Send a message and parse the response as JSON.
        
        Args:
            system_prompt: System message for context
            user_prompt: User message/query
            temperature: Override default temperature
            
        Returns:
            Parsed JSON or None on failure
        """
        response = self.send_message(system_prompt, user_prompt, temperature)
        
        if response is None:
            return None
        
        return parse_json_response(response)


def parse_json_response(response: str) -> Optional[Any]:
    """
    Parse JSON from a Groq response, handling markdown code blocks.
    
    Args:
        response: Raw response string
        
    Returns:
        Parsed JSON or None on failure
    """
    if not response:
        return None
    
    # Clean up response
    text = response.strip()
    
    # Try to extract JSON from markdown code blocks
    if "```json" in text:
        start = text.find("```json") + 7
        end = text.find("```", start)
        if end > start:
            text = text[start:end].strip()
    elif "```" in text:
        start = text.find("```") + 3
        end = text.find("```", start)
        if end > start:
            text = text[start:end].strip()
    
    # Try to parse JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON array or object
        for start_char, end_char in [('[', ']'), ('{', '}')]:
            start = text.find(start_char)
            end = text.rfind(end_char)
            if start != -1 and end > start:
                try:
                    return json.loads(text[start:end + 1])
                except json.JSONDecodeError:
                    continue
        
        logger.error(f"Failed to parse JSON response: {text[:200]}...")
        return None


def create_groq_client() -> GroqClient:
    """
    Factory function to create a GroqClient instance.
    
    Returns:
        GroqClient: Configured client instance
    """
    return GroqClient()
