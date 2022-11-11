from django.shortcuts import render
from get_category import latest_run
# Create your views here.

def index(request):
    return render (request, 'index.html')

def upload_data(request):
    