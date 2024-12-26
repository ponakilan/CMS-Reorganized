from django.urls import path
from visualize import views 

urlpatterns = [
    path("", views.index),
]
