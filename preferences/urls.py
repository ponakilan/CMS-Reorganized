from django.urls import path
from . import views

urlpatterns = [
    path("", views.index),
    path("cms-b/", views.cms_b),
    path("cms-d/", views.cms_d),
    path("nucc/", views.nucc),
    path("openpay-cat/", views.openpay_cat),
    path("openpay-cat-page/", views.openpay_cat_html),
    path("openpay-data/", views.openpay_data),
    path("cms-all-data/", views.cms_data),
]