import requests
import pymongo
import json
import hmac  # Library for HMAC-based authentication
import datetime  # Library for creating delays
from time import sleep  # Function for adding delays between requests
import hashlib  # Library for hashing
from pandas import read_csv

#getting the mnemonic list ready to call the API
df = read_csv('Assurant_fullMnemonicList_20220510.csv',header=0, sep="|")
mn_list=df['mnemonic'].tolist()
mn_list[0]=mn_list[0][1:]


mn_list = mn_list[0:10] ##reducing the list from ~1.6 million to 10 mnemonics


class getMoodysData(object):
    def __init__(self, accKey, encKey, BASKET_NAME=None):
        self.accKey = accKey  # Access key for API authentication
        self.encKey = encKey  # Encryption key for API authentication
        

    # Function for making API requests
    def api_call(self, apiCommand, accKey, encKey, call_type="GET", params=None):
        url = "https://api.economy.com/data/v1/" + apiCommand
        timeStamp = datetime.datetime.strftime(
            datetime.datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"
        )
        payload = bytes(self.accKey + timeStamp, "utf-8")
        signature = hmac.new(
            bytes(self.encKey, "utf-8"), payload, digestmod=hashlib.sha256
        )
        head = {
            "AccessKeyId": self.accKey,
            "Signature": signature.hexdigest(),
            "TimeStamp": timeStamp,
        }
        sleep(1)  # Delay to avoid making too many requests too quickly

        if call_type == "POST":
            response = requests.post(url, headers=head, params=params)
        elif call_type == "DELETE":
            response = requests.delete(url, headers=head, params=params)
        else:
            response = requests.get(url, headers=head, params=params)

        return response

    # Function to retrieve multi-series data
    def retrieveMultiSeries(self, mnemonic_list, freq=None, trans=None, conv=None, startDate=None, endDate=None, vintage=None, vintageVersion=None):
        mnemonic_count = len(mnemonic_list)
        results = []

        # Define the batch size for each request (25 mnemonics per request)
        batch_size = 25

        # Calculate the number of requests needed based on the mnemonic count
        num_requests = (mnemonic_count + batch_size - 1) // batch_size
        total=0
        for i in range(num_requests):
            # Calculate the start and end index for the current batch
            start_index = i * batch_size
            end_index = min(start_index + batch_size, mnemonic_count)
            batch_mnemonics = mnemonic_list[start_index:end_index]

            # Adding time delay
            sleep(1)
            total+=1
            print(total)
            # Make the API call for the current batch of mnemonics
            data = self.retrieveMultiSeriesBatch(batch_mnemonics, freq, trans, conv, startDate, endDate, vintage, vintageVersion)

            # Append the 'data' from each response to the results list
            results.extend(data['data'])

        return results

    # Function to retrieve multi-series data for a batch of mnemonics
    def retrieveMultiSeriesBatch(self, mnemonic_list, freq=None, trans=None, conv=None, startDate=None, endDate=None, vintage=None, vintageVersion=None):
        mnemonic_param = ';'.join(mnemonic_list)
        params = {'m': mnemonic_param}

        if freq:
            params['freq'] = freq
        if trans:
            params['trans'] = trans
        if conv:
            params['conv'] = conv
        if startDate:
            params['startDate'] = startDate
        if endDate:
            params['endDate'] = endDate
        if vintage:
            params['vintage'] = vintage
        if vintageVersion:
            params['vintageVersion'] = vintageVersion

        response = self.api_call('multi-series', self.accKey, self.encKey, params=params)
        data = response.json()
        return data

if __name__ == "__main__":

    # API credentials 
    ACC_KEY = "BF9706F2-610B-4907-8045-A1AF2E664A60"
    ENC_KEY = "20B68D6B-CEB7-4885-BF55-293254D08E9F"

    constructor = getMoodysData(accKey=ACC_KEY, encKey=ENC_KEY)  # Create an instance of the getMoodysData class

    
    data = constructor.retrieveMultiSeries(mn_list)

    # To check for series that don't exist and count the total retrieved series
    count, count_n = 0, 0
    for i in data:
        if i.get('error') == 'Series Not Found':
            count_n += 1
        else:
            count += 1
    print('Total mnemonics retrieved:', count)
    print('Total mnemonics not found:', count_n)

    # Establish a connection to the MongoDB server
    client = pymongo.MongoClient('mongodb://localhost:27017')

    # Access or create a database
    database = client['testing']

    # Access or create a collection
    collection = database['test']

    series_data = data

    # Loop through the series data and save each series as a document in the collection
    for series in series_data:
        mnemonic = series['mnemonic']
        series_json = series

        # Create the document to be inserted
        document = {'id': mnemonic, 'series_data': series_json}

        
        collection.insert_one(document)
        

    # Close the MongoDB connection
    client.close()


    
