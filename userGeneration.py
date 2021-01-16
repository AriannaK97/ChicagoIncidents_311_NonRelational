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

batchSize = 3000
bulkSize=1000
fake = Factory.create()

client = MongoClient()
db = client['ci_311db']
incidentCollection = db['incident']
userCollection = db['citizens']


bulkUsers = userCollection.initialize_unordered_bulk_op()
bulkIncidents = incidentCollection.initialize_unordered_bulk_op()


for i in range(batchSize):
    if (i % bulkSize== 0): #print every bulk writes
        print('%s - records %s '% (time.strftime("%H:%M:%S"),i))

    if (i % bulkSize == (bulkSize - 1)): #bulk write
        try:
            bulkUsers.execute()
            bulkIncidents.execute()
        except BulkWriteError as bwe:
            pprint(bwe.details)
        bulkUsers = userCollection.initialize_unordered_bulk_op()
        bulkIncidents = incidentCollection.initialize_unordered_bulk_op()

    try:
        #upvotes = list(incidentCollection.aggregate([{"$sample": {"size": randint(1, 1000)}}]))
        upvotes = []
        incidents = incidentCollection.aggregate([{"$sample": {"size": randint(1, 1000)}}])
        name = fake.name()
        phone = fake.phone_number()
        address = fake.address()

        for incident in incidents:
        	upvotes.append(incident.get('_id'))
        	incidentResult = bulkIncidents.find({ '_id': incident.get('_id') }).update( { "$addToSet": { "voters" : {"name": name, "phone":phone} } })


        result=bulkUsers.insert({
                            "name" : name,
                            "phone": phone,
                            "address": address,
                            "upvotes": upvotes
        })
    except Exception as e:
        print("insert failed:", i, " error : ", e)



