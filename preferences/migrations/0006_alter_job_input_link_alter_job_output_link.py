# Generated by Django 4.2.17 on 2024-12-26 08:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('preferences', '0005_alter_job_input_link_alter_job_output_link'),
    ]

    operations = [
        migrations.AlterField(
            model_name='job',
            name='input_link',
            field=models.CharField(max_length=250),
        ),
        migrations.AlterField(
            model_name='job',
            name='output_link',
            field=models.CharField(max_length=250, null=True),
        ),
    ]