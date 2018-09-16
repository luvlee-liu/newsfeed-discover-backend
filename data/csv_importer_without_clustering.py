#!/usr/bin/env python
import sys
import pandas as pd
import pymongo
import json
import os
from bson import ObjectId

CLEAR_DB = True

def import_content(filepath):
    uri = "mongodb://heroku_bmtx8t7m:e9av1siaavqhf03keho74phv5i@ds133856.mlab.com:33856/heroku_bmtx8t7m"
    mng_client = pymongo.MongoClient(uri)
    mng_db = mng_client['heroku_bmtx8t7m'] # Replace mongo db name
    collection_name = 'articles' # Replace mongo db collection name
    db_cm = mng_db[collection_name]
    file_res = os.path.join(os.path.dirname(__file__), filepath)

    data = pd.read_csv(file_res)
    data.dropna(inplace=True)
    data.columns=['author_hide', 'content', 'description', 'createdAt', 'source', 
    'title', 'url', 'urlToImage', 'category', 'updatedAt', 'body',
    'keywords', 'named_entities']
    data_json = json.loads(data.to_json(orient='records'))
    if CLEAR_DB:
        db_cm.remove()

    known_slug = []
    for doc in data_json:
        doc['author'] = ObjectId("5a2a15d1642a501400ccce1d") # publisher id

        tag_list = []
        tokens = (doc['keywords'] + "\n" + doc["named_entities"]).split('\n')
        # tokens = doc['keywords_named_entities'].split('\n')
        for token in tokens:
            if len(token.split(' ')) < 4:
                tag_list.append(token)
        tag_list.append(doc['category'])
        tag_list.append(doc['source'])
        # tag_list.append('Topic' + str(doc['topic_id']))
        doc['tagList'] = list(set(tag_list))
        url_tokens = list(filter(None, doc['url'].split('/')))
        doc['slug'] = '-'.join([url_tokens[-2], url_tokens[-1]])
        if doc['slug'] in known_slug:
            doc['slug'] = doc['slug'] + "_"
            print('duplicate slug ' + doc['slug'])
        known_slug.append(doc['slug'])
        doc['comments'] = []
        doc['favoritesCount'] = 0
        doc['_v'] = 0

    db_cm.insert(data_json)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("csv_importer.py csvFileName")
    else:
        import_content(sys.argv[1])
