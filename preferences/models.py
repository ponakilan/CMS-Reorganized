from django.db import models


class Job(models.Model):
    job_id = models.CharField(max_length=36)
    username = models.CharField(max_length=50)
    in_time = models.DateTimeField()
    out_time = models.DateTimeField(null=True)
    input_link = models.CharField(max_length=250)
    output_link = models.CharField(max_length=250, null=True)
