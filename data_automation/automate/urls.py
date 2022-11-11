
from django.contrib import admin
from django.urls import path,include
from . import views

urlpatterns = [
    path('', views.index, name = 'index'),
    path('home/', views.home, name = 'home'),
    path('get_sftp_cred/', views.get_sftp_cred, name = 'get_sftp_cred'),
]
