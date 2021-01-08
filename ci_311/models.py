from djongo import models


class Incident(models.Model):
    # Common fields for all incidents
    name = models.CharField(null=True, default=None, max_length=255)
    creationDate = models.DateTimeField(null=False)
    completionDate = models.DateTimeField(null=False)
    serviceRequestNumber = models.CharField(null=True, default=None, max_length=255)
    streetAddress = models.CharField(null=True, default=None, max_length=255)
    longitude = models.FloatField()
    latitude = models.FloatField()
    y_coordinate = models.FloatField()
    x_coordinate = models.FloatField()
    requestType = models.CharField(null=True, default=None, max_length=255)
    statusType = models.CharField(null=True, default=None, max_length=255)
    historical_wards_2003_2015 = models.IntegerField()
    zipcodes = models.IntegerField()
    communityAreas = models.IntegerField()
    censusTracts = models.IntegerField()
    zipcode = models.IntegerField()
    ward = models.IntegerField()
    policeDistrict = models.IntegerField()
    communityArea = models.IntegerField()

    currentActivity = models.CharField(null=True, default=None, max_length=255)
    mostRecentAction = models.CharField(null=True, default=None, max_length=255)
    SSA = models.IntegerField()

    # For abandoned vehicles
    licensePlate = models.CharField(null=True, default=None, max_length=255)
    vehicleMakeModel = models.CharField(null=True, default=None, max_length=255)
    vehicleColor = models.CharField(null=True, default=None, max_length=255)
    daysReportedParked = models.CharField(null=True, default=None, max_length=255)

    # For garbage carts incidents
    deliveredBlackCartsNum = models.CharField(null=True, default=None, max_length=255)

    # For graffiti removal incidents
    surfaceType = models.CharField(null=True, default=None, max_length=255)
    graffitiLocation = models.CharField(null=True, default=None, max_length=255)

    # For pot holes reported incidents
    filledBlockPotholesNum = models.CharField(null=True, default=None, max_length=255)

    # For rodent baiting incidents
    baitedPremisesNum = models.IntegerField()
    premisesWithGarbageNum = models.IntegerField()
    premisesWithRatsNum = models.IntegerField()

    # For sanitation code complaint incidents
    violationNature = models.CharField(null=True, default=None, max_length=255)

    # For tree debris incidents
    debrisLocation = models.CharField(null=True, default=None, max_length=255)

    # For tree trims incidents
    treeLocation = models.CharField(null=True, default=None, max_length=255)
