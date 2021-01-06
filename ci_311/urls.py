from django.urls import path
from rest_framework import routers
from ci_311.views import *

router = routers.DefaultRouter()

urlpatterns = router.urls

urlpatterns += [
    path('incident/', IncidentViewSet.as_view(), name='incident'),
]