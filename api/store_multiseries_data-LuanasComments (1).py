import requests
import pymongo
import json
import hmac  # Library for HMAC-based authentication
import datetime  # Library for creating delays
from time import sleep  # Function for adding delays between requests
import hashlib  # Library for hashing
from pandas import read_csv


""" Luana'a comments
 Class name starts with Uppercase, most appropriate in this case to call MoodysData,
 get is an action, which means that is more related to a function/method
 Also is good to have a documentation pattern, I will show you my style you can look for a different one
"""
"""#################################################################################"""
"""                            MoodysData Class                                   """
"""#################################################################################"""
"""
@desc <description of the class>
"""
class getMoodysData(object):
    """
    @desc constructor initiate values for variables/attributes.
    @param accKey{string}: Access key for API authentication.
    @param encKey{string}: Encryption key for API authentication.
    @param BASKET_NAME{<type>}: <decription of BASKET_NAME>. Default is None.
    """
    def __init__(self, accKey, encKey, BASKET_NAME=None):
        self.accKey = accKey  # Access key for API authentication
        self.encKey = encKey  # Encryption key for API authentication        

    """
    @desc Function for making API requests.
    @param apiCommand{string}: <decription of apiCommand>.
    @param accKey{string}: Access key for API authentication. [Is NOT being used]
    @param encKey{string}: Encryption key for API authentication. [Is NOT being used]
    @param call_type{string}: <decription of call_type>. Default is "GET".
    @param params{<type>}: <decription of params>. Default is None.
    @return response{request}: <description response>.
    """
    """If you are not gonna used accKey and encKey I would remove it"""
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

    """
    @desc retrieve multi-series data for a batch of mnemonics
    @param mnemonic_list{list}: <decription of mnemonic_list>.
    @param freq{<type>}: <decription of freq>. Default is None.
    @param trans{<type>}: <decription of trans>. Default is None.
    @param conv{<type>}: <decription of conv>. Default is None.
    @param startDate{<type>}: <decription of startDate>. Default is None.
    @param endDate{<type>}: <decription of endDate>. Default is None.
    @param vintage{<type>}: <decription of vintage>. Default is None.
    @param vintageVersion{<type>}: <decription of vintageVersion>. Default is None.
    @return data{json}: <description data>.
    """
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

    """
    @desc retrieve multi-series data
    @param mnemonic_list{list}: <decription of mnemonic_list>.
    @param freq{<type>}: <decription of freq>. Default is None.
    @param trans{<type>}: <decription of trans>. Default is None.
    @param conv{<type>}: <decription of conv>. Default is None.
    @param startDate{<type>}: <decription of startDate>. Default is None.
    @param endDate{<type>}: <decription of endDate>. Default is None.
    @param vintage{<type>}: <decription of vintage>. Default is None.
    @param vintageVersion{<type>}: <decription of vintageVersion>. Default is None.
    @return results{list}: <description results>.
    """
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

if __name__ == "__main__":

    #getting the mnemonic list ready to call the API
    df = read_csv('Assurant_fullMnemonicList_20220510.csv',header=0, sep="|")
    mn_list=df['mnemonic'].tolist()
    mn_list[0]=mn_list[0][1:]

    mn_list = mn_list[0:10] ##reducing the list from ~1.6 million to 10 mnemonics

    # API credentials 
    ACC_KEY = "BF9706F2-610B-4907-8045-A1AF2E664A60"
    ENC_KEY = "20B68D6B-CEB7-4885-BF55-293254D08E9F"

    constructor = getMoodysData(accKey=ACC_KEY, encKey=ENC_KEY)  # Create an instance of the getMoodysData class

    
    data = constructor.retrieveMultiSeries(mn_list)

    """ if it is interesting for you to keep which ones retrieved and which not,
    you can save the index of one of them in a list """
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

    """ Why are you copying the data?
    if you need original and if you are going to make changes on seres_data sure,
    but if not than you are just duplicating data and using more memory. """
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


    
