import pandas as pd
import mlflow
from gradio import Slider, Interface
from constants import FEATURE_TYPES, LABEL_LIST
import numpy as np
import os
from dotenv import load_dotenv
from preprocess import preprocess_df
load_dotenv()

"""
Run this script and visit the URL:port that is prompted to the terminal
"""

def try_predict(message):
    try:
        return predict(message)
    except:
        return 'none.'

def predict(message):
    print('\n'*10)

    print('Received message:\n'+message)

    # format input as a pandas dataframe with the correct types
    print('\nconverting input to pd dataframe...')
    df = pd.DataFrame({
        'uuid': ['3bddfbda-623f-4316-a233-f011e5991743'],
        'created_at': ['2022-09-06 9:00:51'],
        'read_at': [None],
        'thread_uuid': ['000738a3-59ad-4436-a841-ba13fb325e45'],
        'sender_uuid': ['cedd1995-2cb5-4a33-b63c-fd98f9b1a086'],
        'receiver_uuid': ['881ca708-8fe4-4789-bc30-6a346a3c90b1'],
        'type': 'text',
        'Label 1': '',
        'Label 2': '',
        'Label 3': '',
        'body': message,
    })
    print(df.T)

    # preprocess pandas DtaFrame
    print('\npreprocessing...')
    df = preprocess_df(df)
    print(df.T)

    print('\ndataframe types:')
    print(df.dtypes)

    # use the model to create a prediction
    print('\ncreating prediction...')
    prediction_df = model.predict(df)

    print('\nprediction:')
    print(prediction_df)

    print('\nformatting prediction as string...')
    prediction_str = format_as_string(prediction_df)
    print(prediction_str)

    return prediction_str


def format_as_string(model_prediction: np.ndarray) -> str:
    # format the prediction to a list of strings
    #   also remove the 'label_' prefix
    #   and replace underscores (_) by spaces
    prediction_string_list = [
        name.replace("label_", "").replace("_", " ")
        for (name, prediction) in zip(LABEL_LIST, model_prediction[0])
        if prediction == 1
    ]

    # remove prediction 'none' from the list, except if it is the only prediction
    #   add none to the list if it is empty
    if "none" in prediction_string_list and len(prediction_string_list) > 1:
        prediction_string_list.remove("none")
    if len(prediction_string_list) == 0:
        prediction_string_list = ['none']

    # transform the string list as a single string
    prediction_string = ", ".join(prediction_string_list)

    return prediction_string


demo = Interface(
    fn=predict,
    inputs=["text",],
    outputs=["text"],
)

def load_latest_model():
    last_run = mlflow.search_runs(max_results=1, order_by=['attribute.end_time'])#.iloc[0]
    print(last_run)
    model_uri = fr"wasbs://mlflow@manufysmartchat.blob.core.windows.net/ml/{last_run.experiment_id}/{last_run.run_id}/artifacts/model"
    return mlflow.sklearn.load_model(model_uri=model_uri)


# mlflow.set_tracking_uri('http://mlflow.gxc5gsczcrdpb8f4.westeurope.azurecontainer.io:5000')
# model = load_latest_model()
# demo.launch(server_name='0.0.0.0')
