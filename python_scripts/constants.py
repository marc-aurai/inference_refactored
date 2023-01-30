"""
TODO: The variables hard-coded in this file should instead be retrieved from some location (like the SQL database)
"""

FEATURE_TYPES = {
    "contains_email": "bool",
    "contains_phone_number": "bool",
    "contains_url": "bool",
    "is_body_in_english": "bool",
    "contains_non_eu": "bool",
    "body_cleaned": "O",
    "read_duration_logsec": "float64",
    "created_at_unix": "float64",
    "read_at_unix": "float64",
    "is_read": "bool",
    "created_at_time_sec": "int64",
    "read_at_time_sec": "float64",
    "amount_messages": "int64",
    "message_number": "int64",
    "n_word_in_message": "int64",
}

LABEL_LIST = [
    "label_agent",
    "label_contact_info",
    "label_fabric_service",
    "label_intro_message",
    "label_invoice",
    "label_meeting",
    "label_non_eu_country",
    "label_none",
    "label_product_details",
    "label_sampling_order",
]
