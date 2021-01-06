from rest_framework import viewsets, status, permissions
from rest_framework.decorators import api_view
from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from rest_framework.response import Response
from ci_311.models import *
from ci_311.serializers import *


class IncidentViewSet(APIView):
    '''
    Contains information about inputs/outputs of a single program
    that may be used in Universe workflows.
    '''
    permission_classes = [AllowAny]

    def post(self, request, format=None):

        if request.method == 'GET':
            incidents = Incident.objects.all()
            serializer = IncidentSerializer(incidents, many=True)
            return Response(IncidentSerializer.data)

        elif request.method == 'POST':
            serializer = IncidentSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

