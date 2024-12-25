from django.contrib import admin
from preferences.models import Job


class JobAdmin(admin.ModelAdmin):
    list_display = ["job_id", "username", "in_time", "out_time", "input_link", "output_link"]

admin.site.register(Job, JobAdmin)
