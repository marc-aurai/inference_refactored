""" This is the main script of the Inference container on Azure. 
The container reads the preprocessed messages inside the inference queue,
and sends a post request to the Azure ML studio endpoint. The endpoint will answer with 
a prediction, uuid, probabilities, prediction_list and timestamp -> created_at.
"""

import json
import os
from time import sleep

import requests
from azure.storage.queue import (QueueClient, TextBase64DecodePolicy,
                                 TextBase64EncodePolicy)

from cosmosdb_insert import initialize_cosmos_session, insert_data


def get_queue_client_inference(connect_str: str, q_name: str) -> QueueClient:
    """ Returns an Azure Queue Storage connection object, in this case the inference-messages-queue.

    Args:
        connect_str (str): The connection string to the storage account. 
                           The secret connection string is located in the .env file.
        q_name (str): The name of the Azure queue.

    Returns:
        QueueClient: This client represents interaction with the inference queue. 
                     It provides operations to create, delete, or configure a queue and includes operations to send, 
                     receive, peek, delete, and update messages within it.
    """
    return QueueClient.from_connection_string(
        connect_str, q_name
    )

def get_queue_client_prediction(connect_str: str, q_name: str) -> QueueClient:
    """ Returns an Azure Queue Storage connection object, in this case the predicted-labels-queue.

    Args:
        connect_str (str): The connection string to the storage account. 
                           The secret connection string is located in the .env file.
        q_name (str): The name of the Azure queue.

    Returns:
        QueueClient: This client represents interaction with the prediction queue. 
                     It provides operations to create, delete, or configure a queue and includes operations to send, 
                     receive, peek, delete, and update messages within it.
    """
    return QueueClient.from_connection_string(
        connect_str, q_name, message_encode_policy=TextBase64EncodePolicy()
    )

def endpoint_request(preprocessed_message: json) -> str:
    """ This function sends a post request to the Inference endpoint on Azure ML studio. And returns in total 5 objects.

    Args:
        preprocessed_message (json): The message from the inference Queue, this message was provided 
                                     by the preprocess container. Therefore this message is already pre-processed.

    Returns:
        str: Returns in total 5 objects: prediction, uuid, probabilities, prediction_list, created_at
    """
    api_key = '7cwnfH3DBQOU1IpZ4tgO2IOkeDNSBR21' 
    resp = requests.post(
        url="https://manufy-endpoint.westeurope.inference.ml.azure.com/score",
        json=preprocessed_message,
        headers={'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
    )
    resp_json = resp.json()
    prediction, uuid, probabilities, prediction_list, created_at = resp_json
    return prediction, uuid, probabilities, prediction_list, created_at

def process_message(message=None) -> str:
    """ Reads and processes messages in the Azure inference Queue.
        And sends the pre-processed message to the Azure ML studio endpoint.
    Args:
        message (dict): This is the preprocessed message that comes from the inference Queue.
    Returns:
        (str): Returns in total 6 objects: message, uuid, prediction, probabilities, prediction_list, created_at
    """
    message_str = message.content[1:-2]
    message_str = json.loads(message.content)
    prediction, uuid, probabilities, prediction_list, created_at = endpoint_request(message_str)
    return message, uuid, prediction, probabilities, prediction_list, created_at

def manufy_format(labels, probabilities):
    """ Manufy expects the following format inside the prediction Queue:
    [{'label': 'contact_info', 'probability': 94.06}, {'label': 'meeting', 'probability': 52.55}]


    Args:
        labels (list): Includes a list with 0's(False) and 1's(True) for each label as a list.
                       probabilities (list): List with all the probabilities, meaning that there are in total 7 probabilities in this list.
                       Since their are in total 7 labels.
    Returns:
        dict: Returns a dictionary with the following format: [{'label': 'contact_info', 'probability': 94.06}]
    """
    label_dict_totaal = {"Labels":[]}
    for index, label in enumerate(labels):
        label_dict = {"label":label, "probability":probabilities[index]}
        label_dict_totaal["Labels"].append(label_dict)
    return label_dict_totaal.get("Labels")

def cosmos_label_format(uuid, prediction, probabilities, created_at):
    """Create unique column for each label inside the cosmosdb for predictions

    Args:
        uuid (str): Unique user id.
        prediction (int): Predicted label for the message.
        probabilities (list): List with all the probabilities, meaning that there are in total 7 probabilities in this list.
        Since their are in total 7 labels.
        created_at (str): Timestamp of the message.

    Returns:
        dict: Dictionary with the following format: {"Label": boolean}
    """
    dict_message = {"uuid":uuid, "Label":prediction,"Probabilities":probabilities, "created_at": created_at}
    label_list = [
                "meeting",
                "fabric_service",
                "sampling_order",
                "contact_info",
                "intro_message",
                "non_eu_country",
                "invoice",
            ]
    dict_message = dict.fromkeys(dict_message, False) # Set all to false, default

    for label in label_list:
        for pred_label in prediction.split(","):
            if label == (pred_label.replace(" ", "")):
                dict_message[label] = True 
    return dict_message 
    
    
def insert_message_into_prediction_queue(
    queue_client: QueueClient, 
    uuid: str, 
    prediction: str, 
    probabilities: list, 
    prediction_list: list, 
    created_at=None,
    cosmos_session=None
):
    """ Insert the dictionary as a message inside the prediction Queue.
    with the following format: [{"label": "meeting", "probability":68.80}]

    Args:
        queue_client (QueueClient): This client represents interaction with the prediction queue. 
        uuid (str): Unique user id.
        prediction (int): Predicted label for the message.
        probabilities (list): List with all the probabilities, meaning that there are in total 7 probabilities in this list.
                              Since their are in total 7 labels.
        prediction_list (list): Includes a list with 0's(False) and 1's(True) for each label as a list.
        created_at (str): Timestamp of the message.
        cosmos_session (Session): The Cosmos Database session in Azure (manufy_db.predictions).
    """
    probabilities_formatted =  ", ".join(str(x) for x in probabilities) 
    
    dict_message = cosmos_label_format(uuid, prediction, probabilities_formatted, created_at)
    insert_data(cosmos_session, **dict_message)
    labels_and_probs = manufy_format(prediction_list, probabilities)

    if not labels_and_probs: # Check if list is empty
        labels_and_probs.append({"label":"unidentified", "probability":100.00})
    print(labels_and_probs,"\n")

    dict_messsage_manufy = {"uuid":uuid, "labels":labels_and_probs}
    queue_client.send_message(str(dict_messsage_manufy))


if __name__ == "__main__":
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    inference_queue = os.getenv("INFERENCE_MESSAGES_QUEUE")
    prediction_queue = os.getenv("PREDICTION_MESSAGES_QUEUE")

    inference_queue_client = get_queue_client_inference(conn_str, inference_queue)
    prediction_queue_client = get_queue_client_prediction(conn_str, prediction_queue)
    print("Loaded queues.")

    cosmos_cluster, cosmos_session = initialize_cosmos_session()

    # Continuously check for new messages in message queue
    while True:
        properties = inference_queue_client.get_queue_properties()
        count = properties.approximate_message_count
        if count > 0:
            messages = inference_queue_client.receive_messages()
            for message in messages:
                message, uuid, prediction, probabilities, prediction_list, created_at = process_message(message)
                print("prediction label: ",prediction)

                insert_message_into_prediction_queue(prediction_queue_client, uuid, prediction, probabilities, prediction_list, created_at, cosmos_session)
                inference_queue_client.delete_message(message.id, message.pop_receipt)

        sleep(int(os.getenv("SLEEP_TIME")))
