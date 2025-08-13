from django.urls import path
from .views import kafka_search
from . import views, metrics_view

urlpatterns = [
    path('search/', kafka_search, name='kafka_search'),
    path('metrics/', metrics_view.metrics, name='metrics')
]
