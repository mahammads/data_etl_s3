# Generated by Django 4.1 on 2022-11-11 19:18

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("automate", "0001_initial"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="awscred",
            name="id",
        ),
        migrations.AlterField(
            model_name="awscred",
            name="user_name",
            field=models.CharField(max_length=100, primary_key=True, serialize=False),
        ),
    ]