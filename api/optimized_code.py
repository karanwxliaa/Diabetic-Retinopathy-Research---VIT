import requests
import pymongo
import json
import hmac
import datetime
from time import sleep
import hashlib
from pandas import read_csv

# Getting the mnemonic list ready to call the API
df = read_csv('Assurant_fullMnemonicList_20220510.csv', header=0, sep="|")
mn_list = df['mnemonic'].tolist()
mn_list[0] = mn_list[0][1:]
mn_list = mn_list[0:10000]  # Reducing the list to 10 mnemonics for testing

class GetMoodysData(object):
    def __init__(self, accKey, encKey, BASKET_NAME=None):
        self.accKey = accKey
        self.encKey = encKey

    def api_call(self, apiCommand, accKey, encKey, call_type="GET", params=None):
        url = "https://api.economy.com/data/v1/" + apiCommand
        timeStamp = datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        payload = bytes(self.accKey + timeStamp, "utf-8")
        signature = hmac.new(bytes(self.encKey, "utf-8"), payload, digestmod=hashlib.sha256)
        head = {
            "AccessKeyId": self.accKey,
            "Signature": signature.hexdigest(),
            "TimeStamp": timeStamp,
        }
        sleep(1)
        if call_type == "POST":
            response = requests.post(url, headers=head, params=params)
        elif call_type == "DELETE":
            response = requests.delete(url, headers=head, params=params)
        else:
            response = requests.get(url, headers=head, params=params)
        return response

    def retrieveMultiSeries(self, mnemonic_list, freq=None, trans=None, conv=None, startDate=None, endDate=None, vintage=None, vintageVersion=None):
        mnemonic_count = len(mnemonic_list)
        results = []

        batch_size = 25  # Adjust the batch size as per your requirements

        for i in range(0, mnemonic_count, batch_size):
            batch_mnemonics = mnemonic_list[i:i + batch_size]
            data = self.retrieveMultiSeriesBatch(batch_mnemonics, freq, trans, conv, startDate, endDate, vintage, vintageVersion)
            results.extend(data['data'])

        return results

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
    ACC_KEY = "BF9706F2-610B-4907-8045-A1AF2E664A60"
    ENC_KEY = "20B68D6B-CEB7-4885-BF55-293254D08E9F"

    constructor = GetMoodysData(accKey=ACC_KEY, encKey=ENC_KEY)

    data = constructor.retrieveMultiSeries(mn_list)
    '''
    count, count_n = 0, 0
    for i in data:
        if i.get('error') == 'Series Not Found':
            count_n += 1
        else:
            count += 1
    print('Total mnemonics retrieved:', count)
    print('Total mnemonics not found:', count_n)
    '''
    client = pymongo.MongoClient('mongodb://localhost:27017')
    database = client['testDB']
    collection = database['2nd_test_collection_10k']

    documents = []
    for series in data:
        mnemonic = series['mnemonic']
        series_json = series
        document = {'id': mnemonic, 'series_data': series_json}
        documents.append(document)

    if documents:
        collection.insert_many(documents)

    client.close()
