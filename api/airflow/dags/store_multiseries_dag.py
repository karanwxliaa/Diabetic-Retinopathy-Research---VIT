from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from store_multiseries_data import getMoodysData
import pymongo
import json


# Replace with your actual API credentials and mnemonics list
ACC_KEY = "BF9706F2-610B-4907-8045-A1AF2E664A60"
ENC_KEY = "20B68D6B-CEB7-4885-BF55-293254D08E9F"
#WRITE CODE TO GENERATE MNEMONICS HERE (FOR NOW IM WRITING A TEMPORARY CODE)
states="SD CSOR TN TX UT VT VA WA CWNC WER CWSC WV WI WY AL AK AZ AR CA CO CT DE DC CENC CESC FL GA HI ID IL IN IA KS KY LA ME MD MA MI CMAC CNCR MN MS MO MT CMTN NE NV CNEC NH NJ NM NY NC ND CNER OH OK OR CPAC PA PR RI CSAC"
states=states.split()
mn_list=[]

codes=['FE22.','FYPCDPI$Q.']
index=0

for code in codes:
    for i in range(0,len(states)):
        mn_list.append(code+states[i])



# Define the DAG
dag = DAG(
    "store_multiseries_dag",
    description="DAG to retrieve and store multiseries data",
    schedule="0 0 * * *",  # Run the DAG daily at midnight
    start_date=datetime(2023, 6, 8),  # Replace with the appropriate start date
    catchup=False
)

# Function to retrieve and store multiseries data
def retrieve_and_store_multiseries():
    constructor = getMoodysData(accKey=ACC_KEY, encKey=ENC_KEY)  # Create an instance of the getMoodysData class

    data = constructor.retrieveMultiSeries(mn_list)

    # To check for series that don't exist and count the total retrieved series
    count = 0
    for i in data:
        if i.get('error') == 'Series Not Found':
            print('Series not found for the mnemonic:', i.get('mnemonic'))
        else:
            count += 1
    print('Total mnemonics retrieved:', count)

    # Assuming you have the retrieved series data stored in the 'data' variable
    series_data = data  # Your retrieved series data

    # Establish a connection to the MongoDB server
    client = pymongo.MongoClient('mongodb://localhost:27017')

    # Access or create a database
    database = client['your-db-name']

    # Access or create a collection
    collection = database['your-collection-name']

    # Loop through the series data and save each series as a document in the collection
    for series in series_data:
        mnemonic = series['mnemonic']
        series_json = series  # Assuming the entire series output is already in JSON format

        # Create the document to be inserted
        document = {'mnemonic': mnemonic, 'series_data': series_json}

        # Insert the document into the collection
        collection.insert_one(document)

    # Close the MongoDB connection
    client.close()

# Define the task
retrieve_and_store_task = PythonOperator(
    task_id="retrieve_and_store_task",
    python_callable=retrieve_and_store_multiseries,
    dag=dag
)

# Set task dependencies
retrieve_and_store_task

