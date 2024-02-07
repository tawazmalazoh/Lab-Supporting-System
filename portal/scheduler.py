from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from django.conf import settings

# Import the function you want to run as a scheduled job
from .tasks import my_scheduled_task  # adjust this to your module

def start():
    scheduler = BackgroundScheduler()
    scheduler.configure(settings.SCHEDULER_CONFIG)  # Use settings from Django settings file

    # Schedule the function "my_scheduled_task" to run every day at 21:00
    # take note we are an hr ahead 
    #trigger = CronTrigger(hour=21, minute=0)  # 9 PM daily
    trigger = CronTrigger(hour=11, minute=30)  # 9 PM daily
    scheduler.add_job(my_scheduled_task, trigger)

    # Add more jobs here if needed

    scheduler.start()
