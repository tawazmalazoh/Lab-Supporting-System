# context_processors.py
from django.contrib import messages
from django.core.mail import message
from django.core.files.storage import default_storage
from django.core.files.storage import FileSystemStorage
from django.shortcuts import render,redirect, Http404
from django.contrib.auth import authenticate,login,logout
from django.contrib import messages
from django.contrib.auth.forms import UserCreationForm
from .forms import RegisterUserForm
from django.http import HttpResponseBadRequest
from django.core.mail import send_mail
from django.http import HttpResponse
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
# from matplotlib import pyplot
import seaborn as sns
# import matplotlib as mpl
# import seaborn as sns  
from scipy.stats import norm
from scipy.stats import ttest_ind
import statistics
from IPython.display import Image
import plotly.graph_objects as go
import json
from datetime import timedelta
from datetime import datetime, timedelta

import shutil


import pyodbc
import matplotlib.patches as mpatches
import datetime
import datetime as dt
from PIL import Image
from django.conf import settings
import io
from io import StringIO
import gspread
import os
from sqlalchemy import create_engine, text,inspect

from .forms import IstForm
from .forms import RtcqiForm
from .forms import WeeklyForm
from django.db import connections, transaction

#----importing functions for odk
import pysurveycto
from .cleanODK import pull_data
from .cleanODK import save_media_file
from .cleanODK import construct_url
from .cleanODK import sanitize_data
from .cleanODK import loading_ODK_to_Dbase


from .customised import reading_dash
from .upload_vls import reading_vls
from .upload_vls import reading_Creation_reports

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import FileResponse
from django.core import serializers
#importing SQL from sqlfile
from .SqlQueries import allqueries
from .SqlQueries import istqueries
from .SqlQueries import TATSQLQueries
from .SqlQueries import weeklyDashboardPull
from .SqlQueries import get_update_query_and_params
from .SqlQueries import get_update_carryoversamples_and_params
from .SqlQueries import get_update_thisweek_and_params
from .SqlQueries import delete_creation_reports
from .SqlQueries import delete_dashboard_entry
from .SqlQueries import files_uploaded_in_database
from .SqlQueries import Dashboard_uploaded_in_database
from .SqlQueries import pull_last_day
from .SqlQueries import odkScripts
from django.urls import reverse
import time
from django.http import  HttpResponseRedirect
from pandas.io.excel._base import read_excel
from django.core.cache import cache
import urllib.parse
import traceback

import tempfile
from django.contrib.auth.models import User
MEDIA_ROOT = settings.MEDIA_ROOT



def load_data(request):
    startdate = '2020-01-01'
    enddate = datetime.datetime.now().strftime('%Y-%m-%d')
    selected_provinces = None
    pepfar_support = None
    SQLqueries = allqueries(startdate, enddate, selected_provinces, pepfar_support)
    data = []
    for query in SQLqueries:
        try:
            with connections['default'].cursor() as cursor:
                cursor.execute(query['sql'], query['parameters'])
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': query['query_name'], 'query_data': query_data})
        except Exception as e:
            print(f"An error occurred for {query['query_name']}: {e}")

    return {'data': data}
