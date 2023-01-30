from fastapi import FastAPI
from pydantic import BaseModel
from uuid import UUID
from typing import Union
from datetime import datetime, timedelta
from requests import request

class incoming_model_request(BaseModel):
    "uuid": UUID
    "created_at": datetime
    "read_at": Union[datetime, None] = None
    "thread_uuid": UUID
    "sender_uuid": UUID
    "receiver_uuid": UUID
    "type": str
    "Body": str

class processed_message_response(BaseModel):
    "uuid": UUID
    "contains_email": bool
    "contains_phone_number": bool
    "contains_url": bool
    "is_body_in_english": bool
    "contains_non_eu": bool
    "body_cleaned": str
    "read_duration_logsec": Union[float, None]
    "created_at_unix": float
    "read_at_unix": Union[float, None]
    "is_read": bool
    "created_at_time_sec": int
    "read_at_time_sec": Union[float, None]
    "amount_messages": int
    "message_number": int
    "n_word_in_message": int


class outgoing_model_response(BaseModel):
    "uuid": UUID
    "label": Union[str, list[str]]


data_app = FastAPI(title="Data preprocessing pipeline.")

serving_app = FastAPI(title="Model serving app.")

@data_app.get("/process_request", response_model=processed_message_response)
async def preprocess_string(message_request: incoming_model_request):
    pass

@serving_app.get("/preprocess")
async def preprocess(message_request: incoming_model_request):
    pass

@serving_app.get("./predict", response_model=outgoing_model_response)
async def predict(message_request: incoming_model_request):
    pass