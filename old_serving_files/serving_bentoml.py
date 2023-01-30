import pandas as pd
import bentoml
from bentoml.io import JSON
from constants import FEATURE_TYPES, LABEL_LIST
from dotenv import load_dotenv
import mlflow 
load_dotenv()  # load Azure login environment-variables which are used by mlflow

"""
run the following command in a terminal, then visit the link:port that is prompted
    bentoml serve serving_bentoml.py:service

Example json to try:
    {"13e0d559-662d-4121-879d-1c015f09956d":{"contains_email":false,"contains_phone_number":false,"contains_url":false,"is_body_in_english":true,"contains_non_eu":false,"body_cleaned":"hey there we \ufffd d love to work with you on this project our facility is stationed in croatia and we \ufffd re highly experienced and specialized in manufacturing basic wear fashion wear and workwear our in house team includes seamstresses digital designer embroidery specialist garment constructor and backoffice management so you can have it all in one place when needed we can also help with sourcing the fabrics printing and packaging production before further info we \ufffd ve had a few questions come to mind do you have some kind of visuals or inspo pictures of what you would like to do we can help you with every aspect of your project from the sewing patterns fabric sourcing producing and shipping to you after receiving all the info about your project we \ufffd re more than happy to share the estimated prices and lead times eagerly awaiting your reply of course we are open to any questions you may have regarding our services if you want to see our previous work and the services we provide as well as our point of view on sustainability check out our profile gallery on manufy team lupr \ufffd","read_duration_logsec":10.7992489932,"created_at_unix":1661409783.0,"read_at_unix":1661458766.0,"is_read":false,"created_at_time_sec":24183,"read_at_time_sec":73166.0,"amount_messages":5,"message_number":1,"n_word_in_message":188},"0303ef7b-bcc4-4a86-8e8e-393db31c8b67":{"contains_email":false,"contains_phone_number":false,"contains_url":false,"is_body_in_english":false,"contains_non_eu":false,"body_cleaned":"wir gehen von einer anfrage f \ufffd r die fertigung aus schnitte stoffe usw sind vorhanden \n der   angegebene fertigungspreis ist ausschlie \ufffd lich grob nach den bildern gecsh \ufffd tzt bitte gerne weitere infos zur verarbeitung schicken \n viele gr \ufffd \ufffd e \n niklas","read_duration_logsec":0.0,"created_at_unix":1660053159.0,"read_at_unix":-1.0,"is_read":true,"created_at_time_sec":49959,"read_at_time_sec":43287.8899709302,"amount_messages":2,"message_number":2,"n_word_in_message":32}}

"""


def get_latest_model_uri():
    mlflow.set_tracking_uri('http://mlflow.gxc5gsczcrdpb8f4.westeurope.azurecontainer.io:5000')
    last_run = mlflow.search_runs(max_results=1, order_by=['attribute.end_time']).iloc[0]
    return fr"wasbs://mlflow@manufysmartchat.blob.core.windows.net/ml/{last_run.experiment_id}/{last_run.run_id}/artifacts/model"


runner = bentoml.mlflow.import_model(
    name="model",
    model_uri=get_latest_model_uri(),
).to_runner()

service = bentoml.Service("model", runners=[runner])


@service.api(input=JSON(), output=JSON())
def classify(input_json):

    # format the json input as a pandas DataFrame
    df = pd.DataFrame(input_json).T.astype(FEATURE_TYPES)

    # predict the labels and format the numpy ndarray as a pandas dataframe
    prediction_np = runner.predict.run(df)
    prediction_df = pd.DataFrame(prediction_np, columns=LABEL_LIST, index=df.index)

    # return the prediction as a json string
    return prediction_df.T.to_json()
