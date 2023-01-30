# -*- coding: utf-8 -*-

"""Script for preprocessing Manufy Chat messages.
   The script preprocesses chat messages from the incoming messages
   Azure Storage Queue.
   Preprocessed messages are then send to the Queue Storage for
   model inference.
"""

import os
import json
import pandas as pd
from time import sleep
import mlflow
from constants import FEATURE_TYPES, LABEL_LIST
from serving_gradio import format_as_string#, load_latest_model
from cosmosdb_insert import initialize_cosmos_session, insert_data

from azure.storage.queue import QueueClient, TextBase64DecodePolicy
from azure_blob_functions import downloadFromBlobStorage
# mlflow.set_tracking_uri(
#     "http://mlflow.gxc5gsczcrdpb8f4.westeurope.azurecontainer.io:5000"
# )
# model = load_latest_model()
model = downloadFromBlobStorage(BLOBNAME = "ml/0/3ee5e02750ea4063941acf493a4d4475/artifacts/model/model.pkl")

def get_queue_client(connect_str: str, q_name: str) -> QueueClient:
    """Returns an Azure Queue Storage connection object.

    Args:
        - connect_str: Connection string of the Azure storage account.
        - q_name: Name of the Storage Queue in the storage account.

    Returns:
        - QueueClient used for interacting with the message queue.
    """
    return QueueClient.from_connection_string(
        connect_str, q_name
    )


def process_message(queue_client: QueueClient) -> str:
    """Reads and processes messages in the Azure Storage Queue.
    Each message is expected to be of the following format:

    Args:
        - queue_client: QueueClient object of an Azure Storage Queue.
    Returns:
        - A json string containing the processed message.
    """
    messages = queue_client.receive_messages()
    for message in messages:
        #print(message.content)
        message_str = message.content[1:-2]
        message_str = json.loads(message.content)
        df = pd.DataFrame.from_dict(message_str, orient='index').T
        df.drop(
            columns=LABEL_LIST,
            inplace=True,
            errors="ignore",
        )
        uuid = df["uuid"].values
        # print(uuid)
        df.drop(columns=['uuid'], inplace=True)
        df = df.astype(FEATURE_TYPES)
        #prob_pred = model.predict_proba(df)
        #print(prob_pred)
        prediction_df = model.predict(df)
        return format_as_string(prediction_df), uuid[0], message


def insert_message_into_queue(queue_client: QueueClient, message: str, uuid: str, cosmos_session=None):
    dict_message = {"uuid":uuid, "Label":message}
    insert_data(cosmos_session, **dict_message)
    queue_client.send_message(dict_message)


if __name__ == "__main__":
    # Retrieve Azure credentials
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    inference_queue = os.getenv("INFERENCE_MESSAGES_QUEUE")
    prediction_queue = os.getenv("PREDICTION_MESSAGES_QUEUE")

    inference_queue_client = get_queue_client(conn_str, inference_queue)
    prediction_queue_client = get_queue_client(conn_str, prediction_queue)
    print("Loaded queues.")

    cosmos_cluster, cosmos_session = initialize_cosmos_session()

    # Continuously check for new messages in message queue
    while True:
        properties = inference_queue_client.get_queue_properties()
        count = properties.approximate_message_count
        if count > 0:
            prediction, uuid, message = process_message(inference_queue_client)
            print("prediction label: ",prediction)

            # Insert message into queue back to manufy
            insert_message_into_queue(prediction_queue_client, prediction, uuid, cosmos_session)

            # delete the message from the inference queue
            inference_queue_client.delete_message(message.id, message.pop_receipt)

        # cosmos_cluster.shutdown()
        sleep(int(os.getenv("SLEEP_TIME")))
