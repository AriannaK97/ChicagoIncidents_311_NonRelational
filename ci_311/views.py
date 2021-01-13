import datetime
import json

from django.core.serializers.json import DjangoJSONEncoder
from django.http import JsonResponse
from ci_311.models import *
from django.core import serializers
from pymongo import MongoClient


def incident_view(request):
    id = '5ff8ae49de4f5fd78275d71c'
    incidents = Incident.objects.filter(censusTracts=779)
    data = serializers.serialize("json", incidents)
    return JsonResponse(data, safe=False)


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
        {"$group": { "_id": "$requestType", "total": { "$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$project": {"_id": 0, "requestType": "$_id", "total": 1}}
    ])
    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


'''
db.ci_311_incident.aggregate([
    {$match:
            {creationDate: {$gt: new ISODate("2014-06-22T21:00:00Z"), $lt: new ISODate("2015-06-22T21:00:00Z")},
                requestType: "Street Light Out"}},
    {$group: {
        _id: "$creationDate", Total: { $sum: 1 }}
}])
'''


def query2_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    start_date_str = request.GET.get('startDate')
    end_date_str = request.GET.get('endDate')

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match":
            {
                "creationDate": {"$gt": start_date, "$lt": end_date},
                "requestType": "Street Light Out"}},
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


def query3_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    start_date_str = request.GET.get('startDate')
    end_date_str = request.GET.get('endDate')

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        { "$match": {"creationDate": {
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


'''
db.ci_311_incident.aggregate([
     {$match: {requestType: "Abandoned Vehicle Complaint"}},
     {$group: {
         _id: "$ward",
         count: {$sum:1}}
     },
     {$sort:{count:1}},
     {$limit:3},
     {$project: {ward: 1, count: 1}}
])
'''


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

#TODO: CHECK HOW TO PASS NULL....
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


'''
db.ci_311_incident.aggregate([
    {$match:    
            {
                creationDate: new ISODate("2015-06-04T21:00:00.000Z"),
                latitude: {$gt: 41.80550003051758, $lt: 41.80963897705078},
                longitude: {$gt: -87.70037841796875, $lt: -87.62371063232422}
            }
    },
    {$group: {
        _id: "$requestType",
        count: {$sum:1}}
    },
    {$sort:{count:-1}},
    {$limit: 1}
])
'''


def query6_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    date_str = request.GET.get('Date')
    latitude_1 = float(request.GET.get('latitude_1'))
    latitude_2 = float(request.GET.get('latitude_2'))
    longitude_1 = float(request.GET.get('longitude_1'))
    longitude_2 = float(request.GET.get('longitude_2'))

    date = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match":
            {
                "creationDate": date,
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


def query7_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']

    start_date_str = request.GET.get('startDate')
    end_date_str = request.GET.get('endDate')

    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$match": {"creationDate": {
            "$gte": start_date,
            "$lt": end_date
        },
            "names": {"$exists": True}
        }},
        {"$unwind": "$names"},
        {"$group": {"_id": '$_id', 'count': { "$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 50}
    ],
        allowDiskUse=True
    )
    data = []
    for i in query_raw_data:
        data.append([i["_id"], i["count"]])
    return JsonResponse(data, safe=False)


'''
db.ci_311_users.aggregate([
    {$project: {
           _id: 0,
           name: 1,
           numberOfIncidents: {$cond: {if: {$isArray: "$upvotes"}, then: {$size: "$upvotes"}, else: "NA"}}
       }
    },
    {$sort:{numberOfIncidents:-1}},
    {$limit:50}
])
'''


def query8_view(request):
    client = MongoClient()
    db = client['ci_311db']
    users_collection = db['ci_311_users']

    query_raw_data = users_collection.aggregate([
        {"$project": {
           "_id": 0,
           "name": 1,
           "numberOfIncidents": {"$cond": {"if": {"$isArray": "$upvotes"}, "then": {"$size": "$upvotes"}, "else": "NA"}}
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


def query9_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']
    query_raw_data = incident_collection.aggregate(pipeline=[
        {"$unwind": "$names"},
        {"$group": {
            "_id": {
                "upvotes": "$names",
                "ward": "$ward",
            },
            "wardCount": {"$sum": 1}
        }},
        {"$sort": {"wardCount": -1}},
        {"$limit": 50},
        {"$project": {"_id": 0, "Name": "$_id.upvotes", "totalWards": "$wardCount"}}

    ], allowDiskUse=True)

    data = []
    for i in query_raw_data:
        data.append(i)
    return JsonResponse(data, safe=False)


'''
db.ci_311_users.aggregate([
    {$group:
            {
                _id: "$phone",
                incidentsForUniquePhones: {$addToSet: "$_id"},
                count: {$sum: 1}
            }
    },
    {$match:
            {count: {"$gt": 1}}
    },
    {$project:
            {
                _id: 1,
                incidentsForUniquePhones: 1,
                count: 1
            }
    }
])
'''


def query10_view(request):
    client = MongoClient()
    db = client['ci_311db']
    users_collection = db['ci_311_users']

    raw_query_data = users_collection.aggregate([
        {"$group": {
                    "_id": "$phone",
                    "incidentsForUniquePhones": {"$addToSet": "$_id"},
                    "count": {"$sum": 1}

        }},
        {"$match": {
            "count": {"$gt": 1}
        }},
        {"$project": {
                    "_id": 1,
                    "incidentsForUniquePhones": 1,
                    "count": 1
        }}
    ],
        allowDiskUse=True
    )

    data = []
    for i in raw_query_data:
        data.append([i["_id"], str(i["incidentsForUniquePhones"]), i["count"]])
    print(data)
    return JsonResponse(data, safe=False)


def query11_view(request):
    client = MongoClient()
    db = client['ci_311db']
    incident_collection = db['ci_311_incident']
    name = request.GET.get('name')
    query_raw_data = incident_collection.aggregate(pipeline=[
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