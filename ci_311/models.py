from djongo import models
import uuid


# class SubIncident(models.Model):
#     id = models.UUIDField(null=False, primary_key=True)
#     sub_incident_name = models.CharField(null=True, default=None, max_length=255)


class Incident(models.Model):
    id = models.ObjectIdField()
    incident_bla_name = models.CharField(null=True, default=None, max_length=255)
