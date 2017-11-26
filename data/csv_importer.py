#!/usr/bin/env python
import sys
import pandas as pd
import pymongo
import json
import os
from bson import ObjectId

CLEAR_DB = True

def import_content(filepath):
    mng_client = pymongo.MongoClient('localhost', 27017)
    mng_db = mng_client['newsfeed-discover'] # Replace mongo db name
    collection_name = 'articles' # Replace mongo db collection name
    db_cm = mng_db[collection_name]
    file_res = os.path.join(os.path.dirname(__file__), filepath)

    data = pd.read_csv(file_res)
    data.dropna(inplace=True)
    data.columns=['author_hide', 'description', 'createdAt', 'source', 
    'title', 'url', 'urlToImage', 'category', 'updatedAt', 'body',
    'keywords', 'named_entities','keywords_named_entities', 'doc_id', 'topic_id']
    data_json = json.loads(data.to_json(orient='records'))
    if CLEAR_DB:
        db_cm.remove()

    for doc in data_json:
        doc['author'] = ObjectId("5a1260055ccfdc03aab88a42") # publisher id

        tag_list = []
        tokens = doc['keywords_named_entities'].split('\n')
        for token in tokens:
            if len(token.split(' ')) < 4:
                tag_list.append(token)
        tag_list.append(doc['category'])
        tag_list.append(doc['source'])
        tag_list.append('Topic' + str(doc['topic_id']))
        doc['tagList'] = list(set(tag_list))

        doc['slug'] = '-'.join([doc['url'].split('/')[-2], doc['url'].split('/')[-1]])
        doc['comments'] = []
        doc['favoritesCount'] = 0
        doc['_v'] = 0

    db_cm.insert(data_json)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("csv_importer.py csvFileName")
    else:
        import_content(sys.argv[1])
