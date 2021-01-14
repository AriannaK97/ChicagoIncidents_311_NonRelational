from django.urls import path
from rest_framework import routers
from ci_311.views import *

router = routers.DefaultRouter()

urlpatterns = router.urls

urlpatterns += [
    path('incident/', incident_view, name='incident'),
    path('query1/', query1_view, name='query1'),
    path('query2/', query2_view, name='query2'),
    path('query3/', query3_view, name='query3'),
    path('query4/', query4_view, name='query4'),
    path('query5/', query5_view, name='query5'),
    path('query6/', query6_view, name='query6'),
    path('query7/', query7_view, name='query7'),
    path('query8/', query8_view, name='query8'),
    path('query9/', query9_view, name='query9'),
    path('query10/', query10_view, name='query10'),
    path('query11/', query11_view, name='query11'),
    path('query12/', query12_view, name='query12'),
    path('insert/', insert_new_incident, name='insert'),
    path('upvote/', upvote_view, name='upvote'),
]
