#used https://github.com/Wilpapa/mongdb-faker-sample/blob/master/fakerFR.py as main source.
from pymongo import MongoClient
from faker import Factory
import time
from random import randint
import csv
import json
import uuid
from bson.objectid import ObjectId
import copy

batchSize = 50000
bulkSize=5000
fake = Factory.create()

client = MongoClient()
db = client['ci_311db']
incidentCollection = db['ci_311_incident']
userCollection = db['users']


bulk = userCollection.initialize_unordered_bulk_op()


for i in range(batchSize):
    if (i % bulkSize== 0): #print every bulk writes
        print('%s - records %s '% (time.strftime("%H:%M:%S"),i))

    if (i % bulkSize == (bulkSize - 1)): #bulk write
        try:
            bulk.execute()
        except BulkWriteError as bwe:
            pprint(bwe.details)
        bulk = userCollection.initialize_unordered_bulk_op()

    try:
        result=bulk.insert({
                            "name" : fake.name(),
                            "phone": fake.phone_number(),
                            "address": fake.address()
        })
    except Exception as e:
        print("insert failed:", i, " error : ", e)



