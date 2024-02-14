

from pathlib import Path
#import pyodbc
from django.conf.global_settings import MEDIA_URL
from django.contrib.messages import constants as messages

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "django-insecure-5-$lue!68dx!0c(3l*ql2rr-=t0q*auhr9%*188)*i=dnat7fa"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['pythonclusters-133579-0.cloudclusters.net','www.brtilssme.net', 'brtilssme.net','127.0.0.1','localhost']

# Crispy forms settings
CRISPY_TEMPLATE_PACK = 'bootstrap4'

# Application definition

INSTALLED_APPS = [
    "portal",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "crispy_forms",


        
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    
]

ROOT_URLCONF = "lssportal.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"], #we add this so that it will be recognised
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                # "portal.context_processors.load_data",
                
            ],
        },
    },
]

WSGI_APPLICATION = "lssportal.wsgi.application"


# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'mssql',
        'NAME': '#',
        'USER': '#',
        'PASSWORD': '#',
        'HOST': '#',
        "PORT":"17983",
        'OPTIONS': {
            'driver': 'ODBC Driver 17 for SQL Server',
        },
    },
    'auth_db': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'auth_db.sqlite3',
    },
}



# DATABASES = {
    
#     "default":{},

#     "auth_db": {
#         "ENGINE": "django.db.backends.sqlite3",
#         "NAME": BASE_DIR / "db.sqlite3",
#     },

#     "mssql": {
#         "ENGINE": "sql_server.pyodbc",
#         "NAME": "#",
#         "USER": "#",
#         "PASSWORD": "#",
#         "HOST": "#",
#         "PORT":"17983",

#         'OPTIONS': {
#             'driver': 'ODBC Driver 17 for SQL Server',
#                 },

#             }     
# }    

# DATABASES = {
    
#     "default":{},

#     "auth_db": {
#         "ENGINE": "django.db.backends.sqlite3",
#         "NAME": BASE_DIR / "db.sqlite3",
#     },

#     "mssql": {
#         "ENGINE": "sql_server.pyodbc",
#         "NAME": "#",
#         "USER": "#",
#         "PASSWORD": "#",
#         "HOST": ".\SQLEXPRESS",
#         "PORT":"",

#         'OPTIONS': {
#             'driver': 'ODBC Driver 17 for SQL Server',
#                 },

#             }     
# }       

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': '/static/django_cache',
    }
}

DATABASE_ROUTERS = ['routers.db_routers.AuthRouter',
                     'routers.db_routers.mssqlRouter'
                     ]


#AUTH_USER_MODEL = 'portal.CustomUser'
# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = "/static/"
MEDIA_URL = "/images/"

STATICFILES_DIRS = [
    BASE_DIR / "static"
]

STATIC_ROOT = BASE_DIR / "staticfiles"
MEDIA_ROOT = BASE_DIR / "images"

LOGIN_URL = '/login/'

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

MESSAGE_TAGS = {
    messages.DEBUG: 'alert-info',
    messages.INFO: 'alert-info',
    messages.SUCCESS: 'alert-success',
    messages.WARNING: 'alert-warning',
    messages.ERROR: 'alert-danger',
}



# SCHEDULING JOBS

# APScheduler configuration
SCHEDULER_CONFIG = {
    'apscheduler.job_defaults.max_instances': '1',
    'apscheduler.executors.default': {
        'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
        'max_workers': '20'
    },
    'apscheduler.jobstore.default': {
        'class': 'apscheduler.jobstores.memory:MemoryJobStore',
    },
}
