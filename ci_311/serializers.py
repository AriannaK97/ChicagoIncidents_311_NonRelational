from rest_framework import serializers
from ci_311.models import *


class IncidentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Incident
        fields = ['__all__']