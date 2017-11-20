#!/usr/bin/env python
import sys
import pandas as pd
import pymongo
import json
import os
from bson import ObjectId

def import_content(filepath):
    mng_client = pymongo.MongoClient('localhost', 27017)
    mng_db = mng_client['newsfeed-discover'] # Replace mongo db name
    collection_name = 'articles' # Replace mongo db collection name
    db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)

    data = pd.read_csv(file_res)
    data.dropna(inplace=True)
    data.columns=['author_hide', 'description', 'createdAt', 'source', 'title', 'url', 'urlToImage', 'category', 'updatedAt', 'body']
    data_json = json.loads(data.to_json(orient='records'))
    db_cm.remove()
    
    for doc in data_json:
        doc['author'] = ObjectId("5a1260055ccfdc03aab88a42") # publisher id
        doc['tagList'] = [doc['category'],doc['source']]
        doc['slug'] = '-'.join([doc['url'].split('/')[-2], doc['url'].split('/')[-1]])
        doc['comments'] = []
        doc['favoritesCount'] = 0
        doc['_v'] = 0

    db_cm.insert(data_json)

if __name__ == "__main__":
  filepath = './news_full.csv'  # pass csv file path
  import_content(filepath)