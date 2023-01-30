import os
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from dotenv import load_dotenv
load_dotenv()
"""
The authenticate keys are accessed through the connection string of the cosmos db:
Navigate to the cosmosdb storage account, and access the Connection String settings on the left panel.
Username is the USERNAME key, password is the PRIMARY PASSWORD key, the first input for the Cluster
command is the CONTACT POINT key, and port is the PORT key.
"""


def initialize_cosmos_session():
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.verify_mode = CERT_NONE
    auth_provider = PlainTextAuthProvider(
        username="manufycosmos", password=os.getenv("COSMOSDB_PASSWORD")
    )

    cluster = Cluster(
        ["manufycosmos.cassandra.cosmos.azure.com"],
        port=10350,
        auth_provider=auth_provider,
        ssl_context=ssl_context,
    )

    session = cluster.connect()

    print("\nCreating Keyspace")
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS manufy_db WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter' : '1' }"
    )

    print("\nCreating Table")
    session.execute(
        "CREATE TABLE IF NOT EXISTS manufy_db.predictions (uuid varchar PRIMARY KEY, all_Labels varchar, Probabilities varchar, created_at varchar, label_meeting boolean, label_fabric_service boolean, label_sampling_order boolean, label_contact_info boolean, label_intro_message boolean, label_non_eu_country boolean, label_invoice boolean)"
    )
    return cluster, session

def insert_data(
    session,
    uuid=None,
    Label=None,
    Probabilities=None,
    created_at=None,
    meeting=None,
    fabric_service=None,
    sampling_order=None,
    contact_info=None,
    intro_message=None,
    non_eu_country=None,
    invoice=None
):
    session.execute(
        "INSERT INTO  manufy_db.predictions (uuid, all_Labels, Probabilities, created_at, label_meeting, label_fabric_service, label_sampling_order, label_contact_info, label_intro_message, label_non_eu_country, label_invoice) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        [
            uuid,
            Label,
            Probabilities,
            created_at,
            meeting,
            fabric_service,
            sampling_order,
            contact_info,
            intro_message,
            non_eu_country,
            invoice,
        ],
    )


"""
The inputs for insertdata function refer to the columns to give as input for the table.
A row to insert should be a list of the values to insert into the table i.e. a list of length 24 with a value for each of the columns.
"""
