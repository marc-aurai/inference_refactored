FROM python:3.9-slim

COPY cosmosdb_insert.py cosmosdb_insert.py
COPY requirements.txt requirements.txt
COPY .env .env
COPY inference_pipe.py inference_pipe.py
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt 

CMD ["python", "-u","inference_pipe.py"]
