# Generated by Django 4.2.17 on 2024-12-25 14:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('preferences', '0003_remove_job_visualize_link_job_output_link_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='job',
            name='out_time',
            field=models.DateTimeField(null=True),
        ),
        migrations.AlterField(
            model_name='job',
            name='output_link',
            field=models.CharField(max_length=250, null=True),
        ),
    ]