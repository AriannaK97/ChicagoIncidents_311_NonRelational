import datetime
import re

from django.http import JsonResponse
from rest_framework import status

from ci_311.models import *
from django.core import serializers
from pymongo import MongoClient
from bson.objectid import ObjectId
from rest_framework.decorators import api_view
import json


@api_view(['GET'])
def incident_view(request):
    id = '5ff8ae49de4f5fd78275d71c'
    incidents = Incident.objects.filter(censusTracts=779)
    data = serializers.serialize("json", incidents)
    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query1_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    start_date_str = request.GET.get('startDate')
    end_date_str = request.GET.get('endDate')

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match": {"creationDate":
            {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        },
        {"$group": {"_id": "$requestType", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$project": {"_id": 0, "requestType": "$_id", "total": 1}}
    ])
    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query2_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    start_date_str = request.GET.get('startDate')
    end_date_str = request.GET.get('endDate')
    requestType_str = request.GET.get('requestType')

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match":
            {
                "creationDate": {"$gt": start_date, "$lt": end_date},
                "requestType": requestType_str}},
        {"$group": {
            "_id": "$creationDate",
            "Total": {"$sum": 1}
        }
        }],
        allowDiskUse=True
    )
    data = []
    for i in query_raw_data:
        data.append([str(i["_id"]), i["Total"]])

    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query3_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    date = request.GET.get('Date')
    date, time = date.split('T')
    start_date_str = date + 'T00:00:00Z'
    end_date_str = date + 'T23:59:59Z'

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match": {"creationDate": {
            "$gte": start_date,
            "$lt": end_date
        }}
        },
        {"$group": {
            "_id": {
                "zipcode": "$zipcode",
                "requestType": "$requestType",
            },
            "requestCount": {"$sum": 1}
        }},
        {"$sort": {"requestCount": -1}},
        {"$group": {
            "_id": "$_id.zipcode",
            "requestTypes": {
                "$push": {
                    "requestType": "$_id.requestType",
                    "count": "$requestCount"
                },
            }
        }},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "zipcode": "$_id", "RequestTypes": {"$slice": ["$requestTypes", 3]}}}
    ])
    data = []
    for i in query_raw_data:
        data.append(i)

    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query4_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    request_type = request.GET.get('requestType')

    query_raw_data = incident_collection.aggregate([
        {"$match": {"requestType": request_type}},
        {"$group": {
            "_id": "$ward",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": 1}},
        {"$limit": 3},
        {"$project": {"ward": 1, "count": 1}}
    ])

    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query5_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    start_date_str = request.GET.get('startDate')
    end_date_str = request.GET.get('endDate')

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match": {"$expr": {"$and": [
            {"$gt": ["$completionDate", "$creationDate"]},
            {"$gte": ["$creationDate", start_date]},
            {"$lte": ["$creationDate", end_date]}]}
        }},
        {"$group": {
            "_id": None,
            "averageTime": {"$avg": {"$divide": [{"$subtract": ["$completionDate", "$creationDate"]}, 3600000 * 24]}}
        }},
        {"$project": {"_id": 0, "Average Request Time": "$averageTime"}}
    ])
    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


# http://127.0.0.1:8000/query6/?Date=2015-06-04T21:00:00Z&latitude_1=41.80550003051758&latitude_2=41.80963897705078
# &longitude_1=-87.70037841796875&longitude_2=-87.62371063232422

@api_view(['GET'])
def query6_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    date = request.GET.get('Date')
    date, time = date.split('T')
    start_date_str = date + 'T00:00:00Z'
    end_date_str = date + 'T23:59:59Z'

    latitude_1 = float(request.GET.get('latitude_1'))
    latitude_2 = float(request.GET.get('latitude_2'))
    longitude_1 = float(request.GET.get('longitude_1'))
    longitude_2 = float(request.GET.get('longitude_2'))

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match": {
            "creationDate": {"$gte": start_date, "$lt": end_date},
            "latitude": {"$gt": latitude_1, "$lt": latitude_2},
            "longitude": {"$gt": longitude_1, "$lt": longitude_2}
        }},
        {"$group": {
            "_id": "$requestType",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ],
        allowDiskUse=True
    )

    data = []
    for i in query_raw_data:
        data.append([i["_id"], i["count"]])

    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query7_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    date = request.GET.get('Date')
    date, time = date.split('T')
    start_date_str = date + 'T00:00:00Z'
    end_date_str = date + 'T23:59:59Z'

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate([
        {"$match": {"creationDate": {
            "$gte": start_date,
            "$lt": end_date
        },
            "names": {"$exists": True}
        }},
        {"$unwind": "$names"},
        {"$group": {"_id": '$_id', 'count': {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 50}
    ],
        allowDiskUse=True
    )
    data = []
    for i in query_raw_data:
        data.append([str(i["_id"]), i["count"]])
    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query8_view(request):
    client = MongoClient()
    db = client['ci_311db']
    users_collection = db['ci_311_users']

    query_raw_data = users_collection.aggregate([
        {"$project": {
            "_id": 0,
            "name": 1,
            "numberOfIncidents": {
                "$cond": {"if": {"$isArray": "$upvotes"}, "then": {"$size": "$upvotes"}, "else": "NA"}}
        }},
        {"$sort": {"numberOfIncidents": -1}},
        {"$limit": 50}
    ],
        allowDiskUse=True
    )

    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query9_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']
    query_raw_data = incident_collection.aggregate([
        {"$unwind": "$names"},
        {"$group": {
            "_id": {
                "upvotes": "$names",
                "ward": "$ward",
            }
        }},
        {"$group": {"_id": "$_id.upvotes", "wardCount": {"$sum": 1}}},
        {"$sort": {"wardCount": -1}},
        {"$limit": 50},
        {"$project": {"_id": 0, "Name": "$_id", "totalWards": "$wardCount"}}

    ], allowDiskUse=True)

    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query10_view(request):
    client = MongoClient()
    db = client['ci_311db']
    users_collection = db['ci_311_users']

    raw_query_data = users_collection.aggregate([

        {"$unwind": "$upvotes"},
        {"$group": {
            "_id": {
                "phone": "$phone",
                "id": "$upvotes"
            },
            "uniqueIds": {"$addToSet": "$_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {
            "count": {"$gt": 1}
        }},
        {"$group": {"_id": "$_id.id"}},
        {"$project": {"_id": 0, "IncidentId": "$_id"}}
    ], allowDiskUse=True)

    data = []
    for i in raw_query_data:
        data.append([str(i["IncidentId"])])
    print(data)
    return JsonResponse(data, safe=False)


@api_view(['GET'])
def query11_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']
    name = request.GET.get('name')
    query_raw_data = incident_collection.aggregate([
        {"$unwind": "$names"},
        {"$match": {"names": name}},
        {"$group": {"_id": "$ward"}},
        {"$project": {"_id": 0, "WardIds": "$_id"}}
    ],
        allowDiskUse=True
    )

    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


def query12_view(request):
    data = None
    return JsonResponse(data, safe=False)


@api_view(['POST'])
def insert_new_incident(request):
    received_json_data = json.loads(request.body.decode("utf-8"))

    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    valid_request_types = incident_collection.distinct("requestType")

    if received_json_data['requestType'] in valid_request_types:
        print("Ok " + received_json_data['requestType'])

        creation_date = datetime.datetime.now()
        service_request_number = str(creation_date.year) + "-" + re.sub('[^A-Za-z0-9,-]', '', str(creation_date.time()))
        # tutorial_data = JSONParser().parse(request)
        received_json_data.update({"creationDate": creation_date})
        received_json_data.update({"serviceRequestNumber": service_request_number})
        received_json_data.update({"completionDate": None})

        query = incident_collection.insert_one(received_json_data)
        print(query)
        print(creation_date, " ", service_request_number)

        message = "New Incident successfully Inserted\nService Request Number: " + service_request_number
        return JsonResponse(message, safe=False, status=status.HTTP_201_CREATED)

    return JsonResponse("Error", safe=False, status=status.HTTP_501_NOT_IMPLEMENTED)


@api_view(['PUT'])
def upvote_view(request):
    client = MongoClient()
    db = client['ci_311db']
    testsCollection = db['mytests']
    users_collection = db['ci_311_users']
    incident_collection = db['ci_311_incident']

    parameters_dict = json.loads(request.body)
    result = users_collection.update_one({"name": parameters_dict['name']},
                                         {"$addToSet": {"upvotes": ObjectId(parameters_dict['id'])}})

    if result.modified_count != 0:
        incident_result = incident_collection.update_one({"_id": ObjectId(parameters_dict['id'])},
                                                         {"$addToSet": {"names": parameters_dict['name']}})
        print("Incident fields modified")
        print(incident_result.modified_count)

    print("User Fields modified:")
    print(result.modified_count)
    data = result.modified_count
    message = "Updated: " + str(data)
    return JsonResponse(message, safe=False)
