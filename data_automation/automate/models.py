from django.db import models

# Create your models here.
class Last_run(models.Model):
    file_name =  models.CharField(max_length=100, null = False)
    last_modified_date =  models.DateTimeField()
    last_run_date = models.DateTimeField()
    is_complete = models.BooleanField()


    def __str__(self):
        return "%s %s %s %s" %(self.file_name, self.last_modified_date, self.status, self.last_run_date)