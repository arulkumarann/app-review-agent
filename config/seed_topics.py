

SEED_TOPICS = [
    {
        "topic_id": "delivery_delay",
        "topic_name": "Delivery/Service Delay",
        "category": "issue",
        "variations": [
            "late delivery", "slow service", "took too long",
            "delayed", "waiting time", "takes forever"
        ],
        "description": "Service or delivery slower than expected"
    },
    {
        "topic_id": "quality_issue",
        "topic_name": "Product/Service Quality Issue",
        "category": "issue",
        "variations": [
            "poor quality", "bad quality", "stale", "damaged",
            "not fresh", "low quality", "substandard"
        ],
        "description": "Problems with quality of product/service"
    },
    {
        "topic_id": "staff_behavior",
        "topic_name": "Staff/Representative Behavior Issue",
        "category": "issue",
        "variations": [
            "rude staff", "unprofessional", "bad behavior",
            "misbehaved", "impolite", "disrespectful"
        ],
        "description": "Negative interaction with staff/representatives"
    },
    {
        "topic_id": "order_accuracy",
        "topic_name": "Order/Request Incorrect",
        "category": "issue",
        "variations": [
            "wrong order", "missing items", "incorrect",
            "didn't receive", "wrong item", "incomplete order"
        ],
        "description": "Received something different than requested"
    },
    {
        "topic_id": "payment_issue",
        "topic_name": "Payment/Refund Issue",
        "category": "issue",
        "variations": [
            "payment failed", "refund pending", "charged extra",
            "billing problem", "money deducted", "double charged"
        ],
        "description": "Problems with payments or refunds"
    },
    {
        "topic_id": "app_technical",
        "topic_name": "App Technical Issue",
        "category": "issue",
        "variations": [
            "app crash", "not working", "login problem",
            "slow app", "freezing", "glitch", "bug", "error"
        ],
        "description": "Technical problems with the application"
    },
    {
        "topic_id": "customer_support",
        "topic_name": "Customer Support Issue",
        "category": "issue",
        "variations": [
            "no response", "support unhelpful", "can't reach support",
            "poor customer service", "no help", "ignored"
        ],
        "description": "Issues with customer service quality"
    },
    {
        "topic_id": "pricing",
        "topic_name": "Pricing/Charges Issue",
        "category": "issue",
        "variations": [
            "too expensive", "overpriced", "hidden charges",
            "high fees", "extra charges", "costly"
        ],
        "description": "Complaints about pricing or fees"
    },
    {
        "topic_id": "packaging",
        "topic_name": "Packaging/Presentation Issue",
        "category": "issue",
        "variations": [
            "poor packaging", "leaked", "damaged package",
            "messy", "spilled", "broken seal"
        ],
        "description": "Problems with packaging or presentation"
    },
    {
        "topic_id": "feature_request",
        "topic_name": "Feature Request/Suggestion",
        "category": "request",
        "variations": [
            "add feature", "bring back", "need option",
            "suggestion", "would be great if", "please add"
        ],
        "description": "User requests for features or improvements"
    }
]


def get_seed_topics_as_taxonomy(app_id: str) -> dict:
    """
    Convert seed topics to taxonomy format for initial app setup.
    
    Args:
        app_id: The app identifier
        
    Returns:
        dict: Taxonomy structure with seed topics
    """
    from datetime import datetime
    
    topics = []
    for seed in SEED_TOPICS:
        topic = {
            "topic_id": seed["topic_id"],
            "topic_name": seed["topic_name"],
            "category": seed["category"],
            "variations": seed["variations"].copy(),
            "description": seed["description"],
            "added_date": datetime.now().strftime("%Y-%m-%d"),
            "is_seed": True,
            "app_specific": False
        }
        topics.append(topic)
    
    return {
        "app_id": app_id,
        "topics": topics,
        "last_updated": datetime.now().isoformat()
    }
