# -*- coding: utf-8 -*-

"""Script for preprocessing Manufy Chat messages.
   The script preprocesses chat messages from the incoming messages
   Azure Storage Queue.
   Preprocessed messages are then send to the Queue Storage for
   model inference.
"""

import os
import json
from time import sleep
from cosmosdb_insert import initialize_cosmos_session, insert_data
from azure.storage.queue import QueueClient, TextBase64DecodePolicy, TextBase64EncodePolicy
import requests

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

def get_queue_client_prediction_queue(connect_str: str, q_name: str) -> QueueClient:
    """Returns an Azure Queue Storage connection object.

    Args:
        - connect_str: Connection string of the Azure storage account.
        - q_name: Name of the Storage Queue in the storage account.

    Returns:
        - QueueClient used for interacting with the message queue.
    """
    return QueueClient.from_connection_string(
        connect_str, q_name, message_encode_policy=TextBase64EncodePolicy()
    )

def enpoint_request(preprocessed_message):
    # Green-Blue deployment
    scoring_uri_GB = "https://manufy-endpoint.westeurope.inference.ml.azure.com/score" # green blue deployement
    api_key = '7cwnfH3DBQOU1IpZ4tgO2IOkeDNSBR21' # Replace this with the API key for the web service
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
    resp = requests.post(scoring_uri_GB, json=preprocessed_message, headers=headers)
    resp_json = resp.json()
    #print("Response:", resp.text)
    return resp_json[1], resp_json[0], resp_json[2], resp_json[3], resp_json[4] # UUID, PREDICTION, PROBABILITIES, prediction_list, created_at

def process_message(queue_client: QueueClient, message=None) -> str:
    """Reads and processes messages in the Azure Storage Queue.
    Each message is expected to be of the following format:

    Args:
        - queue_client: QueueClient object of an Azure Storage Queue.
    Returns:
        - A json string containing the processed message.
    """
    
    message_str = message.content[1:-2]
    message_str = json.loads(message.content)
    uuid, prediction, probabilities, prediction_list, created_at = enpoint_request(message_str)
    return message, uuid, prediction, probabilities, prediction_list, created_at

def manufy_format(labels, probabilities):
    label_dict_totaal = {"Labels":[]}
    for index, label in enumerate(labels):
        label_dict = {"label":labels[index], "probability":probabilities[index]}
        label_dict_totaal["Labels"].append(label_dict)
    return label_dict_totaal.get("Labels")

def cosmos_label_format(uuid, prediction, probabilities, created_at):
    """create unique column for each label inside the cosmosdb for predictions

    Args:
        uuid (_type_): _description_
        prediction (_type_): _description_
        probabilities (_type_): _description_
        created_at (_type_): _description_

    Returns:
        _type_: _description_
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
    for label in label_list:
        dict_message[label] = False
    for label in label_list:
        for pred_label in prediction.split(","):
            if label == (pred_label.replace(" ", "")):
                dict_message[label] = True 
    return dict_message 
    

    
def insert_message_into_queue(queue_client: QueueClient, uuid: str, prediction: str, probabilities: list, prediction_list: list, created_at=None,cosmos_session=None):
    prob_list = probabilities
    probabilities =  ", ".join(str(x) for x in probabilities) # cosmos does not accept list, so convert to string
    #dict_message = {"uuid":uuid, "Label":prediction, "Probabilities":probabilities, "created_at": created_at}
    
    dict_message = cosmos_label_format(uuid, prediction, probabilities, created_at)
    insert_data(cosmos_session, **dict_message)

    labels_and_probs = manufy_format(prediction_list, prob_list)
    if not labels_and_probs: # Check if list is empty
        labels_and_probs.append({"label":"unidentified", "probability":100.00})
    print(labels_and_probs,"\n")
    dict_messsage_manufy = {"uuid":uuid, "labels":labels_and_probs}
    queue_client.send_message(str(dict_messsage_manufy))


if __name__ == "__main__":
    # Retrieve Azure credentials
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    inference_queue = os.getenv("INFERENCE_MESSAGES_QUEUE")
    prediction_queue = os.getenv("PREDICTION_MESSAGES_QUEUE")

    inference_queue_client = get_queue_client(conn_str, inference_queue)
    prediction_queue_client = get_queue_client_prediction_queue(conn_str, prediction_queue)
    print("Loaded queues.")

    cosmos_cluster, cosmos_session = initialize_cosmos_session()

    # Continuously check for new messages in message queue
    while True:
        properties = inference_queue_client.get_queue_properties()
        count = properties.approximate_message_count
        if count > 0:
            messages = inference_queue_client.receive_messages()
            for message in messages:
                message, uuid, prediction, probabilities, prediction_list, created_at = process_message(inference_queue_client, message)
                print("prediction label: ",prediction)

                # Insert message into queue back to manufy
                insert_message_into_queue(prediction_queue_client, uuid, prediction, probabilities, prediction_list, created_at, cosmos_session)

                # delete the message from the inference queue
                inference_queue_client.delete_message(message.id, message.pop_receipt)

        # cosmos_cluster.shutdown()
        sleep(int(os.getenv("SLEEP_TIME")))
