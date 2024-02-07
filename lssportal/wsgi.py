"""
WSGI config for lssportal project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

# If scheduler.py is inside an app (let's say, 'myapp'), you need to adjust the import accordingly
from portal.scheduler import start as scheduler_start

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "lssportal.settings")

application = get_wsgi_application()



# Start the scheduler
scheduler_start()

