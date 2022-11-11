from django.shortcuts import render
from .models import LastRun, SftpCred, AwsCred
from datetime import datetime
from automate.get_category import *
from automate import env
from django.contrib import messages
from django.shortcuts import render, HttpResponse,redirect
# Create your views here.

def index(request):
    return render (request, 'home.html')

def home(request):
    return render (request, 'home.html')


def get_sftp_cred(request):
    if request.method =="POST":
        print('post')
        host = request.POST.get('hostname')
        username = request.POST.get('username')
        password = request.POST.get('password')
        port = int(request.POST.get('port'))
        sftp_cred = SftpCred(user_name=username, host=host, password=password,port=port)
        sftp_cred.save()
        status = check_FTP_connection(host,username,password)
        if not status == False:
            messages.success(request, 'connection established successfully.')
            return redirect('get_sftp_cred')
    elif request.method =="GET":
        return render (request, 'get_sftp_cred.html')
    else:
        return HttpResponse("Exception occured!")

