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
    data = None
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
    data = None
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
    data = None
    return JsonResponse(data, safe=False)


def query5_view(request):
    data = None
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
    data = None
    return JsonResponse(data, safe=False)


def query7_view(request):
    data = None
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
    data = None
    return JsonResponse(data, safe=False)


def query9_view(request):
    data = None
    return JsonResponse(data, safe=False)


'''
db.ci_311_users.aggregate([
    {$group:
            {
                _id: {phone: "$phone"},
                uniquePhones: {$addToSet: "$_id"},
                count: {$sum: 1}
            }
    },
    {$match:
            {count: {"$gt": 1}}
    },
    {$project:
            {
                _id: 1,
                uniquePhones: 1,
                count: 1
            }
    }
])
'''


def query10_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query11_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query12_view(request):
    data = None
    return JsonResponse(data, safe=False)
