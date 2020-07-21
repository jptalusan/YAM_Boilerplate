from pprint import pprint
import pymongo
import os
import pickle
import pandas as pd

class MyMongoDBClass():
    def __init__(self, host="localhost", port=27017, db="admin", collection="tasks"):
        self._mongoc = pymongo.MongoClient(host, port)

        self._db = self._mongoc[db]
        # print(self._db)
        pass

    def insert(self, collection, payload):
        # print("Insert -> {}".format(payload))
        c = self._db[collection]
        t_id = c.insert_one(payload).inserted_id
        return t_id

    def find_one(self, collection, query_dict=None):
        # 5e4cd5d3a68938cdc24707d6
        res = self._db[collection].find_one(query_dict)
        # pprint(res)
        return res

    def find(self, collection, query_dict):
        # 5e4cd5d3a68938cdc24707d6
        res = self._db[collection].find(query_dict).sort([('inquiry_time', pymongo.ASCENDING)])
        # pprint("Found: {}".format(self._db[collection].count_documents(query_dict)))
        return res

    def count(self, collection):
    # 5e4cd5d3a68938cdc24707d6
        res = self._db[collection].count()

        # pprint("Found: {}".format(self._db[collection].count()))
        return len(list(res))

    def find_unsent_tasks(self):
        res = self._db["tasks"].find({'processed':None})
        return res

    def update_one(self, collection, id, change_dict):
        res = self._db[collection].find_one_and_update({"_id": id}, {'$set': change_dict})
        return res

    def update_many(self, collection, query_dict, change_dict):
        res = self._db[collection].update_many(query_dict, {'$set': change_dict})
        return res

    def delete_all(self, collection):
        return self._db[collection].delete_many({})

    def get_db(self, db):
        return self._mongoc[db]

    def get_client(self):
        return self._mongoc

    def save_collection_to_json(self, collection):
        if not os.path.exists(os.path.join(os.getcwd(), 'data')):
            raise OSError("Must first download data, see README.md")
        data_dir = os.path.join(os.getcwd(), 'data')

        file_path = os.path.join(data_dir, '{}.pkl'.format(collection))
        with open(file_path, 'wb') as handle:
            df = pd.DataFrame(list(self._db[collection].find()))
            df.to_pickle(handle)