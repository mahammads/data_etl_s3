from django.db import models

# Create your models here.
class LastRun(models.Model):
    file_name =  models.CharField(max_length=100, null = False)
    last_modified_date =  models.DateTimeField()
    last_run_date = models.DateTimeField()
    is_complete = models.BooleanField()

class SftpCred(models.Model):
    user_name =  models.CharField(max_length=100, primary_key=True)
    host =  models.CharField(max_length=100, null = False)
    password =  models.CharField(max_length=100, null = False)
    port = models.IntegerField(null = True)
  
class AwsCred(models.Model):
    user_name =  models.CharField(max_length=100, primary_key=True)
    password =  models.CharField(max_length=100, null = False)
    access_key =  models.CharField(max_length=100, null = False)
    secreat_key = models.CharField(max_length=100, null = False)
    aws_s3 = models.CharField(max_length=100, null = False)