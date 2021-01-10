from django.http import JsonResponse
from ci_311.models import *
from django.core import serializers


def incident_view(request):

    incidents = Incident.objects.filter(_id="5ffa3fe449ba80f666934888")
    data = serializers.serialize("json", incidents)
    return JsonResponse(data, safe=False)


def query1_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query2_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query3_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query4_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query5_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query6_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query7_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query8_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query9_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query10_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query11_view(request):
    data = None
    return JsonResponse(data, safe=False)


def query12_view(request):
    data = None
    return JsonResponse(data, safe=False)