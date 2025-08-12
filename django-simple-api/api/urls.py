from django.urls import path
from .views import kafka_search

urlpatterns = [
    path('search/', kafka_search, name='kafka_search'),
]
