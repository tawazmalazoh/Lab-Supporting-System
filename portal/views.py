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
from dateutil.relativedelta import relativedelta
import re
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
import traceback

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
import collections

#----importing functions for odk
import pysurveycto
from .cleanODK import pull_data
from .cleanODK import save_media_file
from .cleanODK import construct_url
from .cleanODK import sanitize_data
from .cleanODK import loading_ODK_to_Dbase

from .Upload_SMS import readingSMS_data


from .generate_word_narrative import write_word_document_narrative

from .customised import reading_dash
from .write_to_excel_cdc import  create_and_copy_excel_template
from .IST_write_to_excel_cdc import IST_create_and_copy_excel_template
from .write_to_excel_cdc import write_data_to_excel
from .IST_write_to_excel_cdc   import IST_write_data_to_excel

from .write_to_excel_cdc import download_excel

from .readingLABdash import readingLabdashboard
from .readingHUBdash import readingHUBdashboard
from .readingIST import reading_National_IST_Rider_Driver_Weekly
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
from .SqlQueries import vl_received_uploaded_in_database
from .SqlQueries import sms_uploaded_in_database
from .SqlQueries import delete_vl_received_reports
from .SqlQueries import pull_last_day
from .SqlQueries import odkScripts
from .SqlQueries import odkScripts_province_district
from .SqlQueries import odkScripts_province
from .SqlQueries import odkScripts_granular
from .SqlQueries import odkScripts_province_totals
from .SqlQueries import weekly_narrative_queries
from .SqlQueries import bike_fxnality_prov
from .SqlQueries import pull_last_day_ist
from .SqlQueries import ISTSQLQueries
from .SqlQueries import ISTSpecimenQueries

from .SqlQueries import update_savingdashboard_referred_samples
from .SqlQueries import update_savingdashboard_sample_run
from .SqlQueries  import update_savingdashboard_testing_capacity
from .SqlQueries  import update_savingdashboard_operational_matrix
from .SqlQueries  import update_savingdashboard_lims

from .SqlQueries  import update_savingdashboard_specimentransport
from .SqlQueries import update_savingdashboard_power_outage

from .SqlQueries import update_savingdashboard_CLI

from .SqlQueries import cdcweeklyreport
from .SqlQueries import ISTMOdelSQLQueries

from openpyxl import load_workbook

from django.urls import reverse
import time
from django.http import  HttpResponseRedirect
from pandas.io.excel._base import read_excel
from django.core.cache import cache
import urllib.parse
import traceback

import tempfile

from django.template.loader import render_to_string
from selenium import webdriver
import time
from io import BytesIO
from django.contrib.auth.models import User
MEDIA_ROOT = settings.MEDIA_ROOT



import warnings
warnings.filterwarnings("ignore", category=UserWarning)






@login_required
def BASE(request):  
    
    labs_list = ["Beitbridge - 100052 - District Hospital", 
                 "Bindura - 100070 - Provincial Hospital",
                 "Beatrice Infectious - 100050 - Hospital", 
                 "Chinhoyi - 100235 - Provincial Hospital",
                 "Gwanda - 100561 - Provincial Hospital",
                 "Gweru - 100572 - Provincial Hospital", 
                 "Kadoma - 100681 - District Hosp",
                 "Marondera - 100903 - Provincial Hospital", 
                 "Masvingo - 100937 - General Hospital", 
                 "Mpilo - 101041 - Central Hospital", 
                 "Mutare - 101165 - Provincial Hospital",
                 "National Reference Laboratory - 101206 - Laboratory", 
                 "St. Lukes - 101645 - Mission Hospital", 
                 "Victoria Falls - 101739 - District Hospital"]
    
    # Corrected Alias List (Dictionary)
    alias_dict = {
                        "Beitbridge - 100052 - District Hospital": "Beitbridge", 
                        "Bindura - 100070 - Provincial Hospital": "Bindura",
                        "Beatrice Infectious - 100050 - Hospital": "BRIDH", 
                        "Chinhoyi - 100235 - Provincial Hospital": "Chinhoyi",
                        "Gwanda - 100561 - Provincial Hospital": "Gwanda",
                        "Gweru - 100572 - Provincial Hospital": "Gweru", 
                        "Kadoma - 100681 - District Hosp": "Kadoma",
                        "Marondera - 100903 - Provincial Hospital": "Marondera", 
                        "Masvingo - 100937 - General Hospital": "Masvingo", 
                        "Mpilo - 101041 - Central Hospital": "Mpilo", 
                        "Mutare - 101165 - Provincial Hospital": "Mutare",
                        "National Reference Laboratory - 101206 - Laboratory": "NMRL", 
                        "St. Lukes - 101645 - Mission Hospital": "St Lukes", 
                        "Victoria Falls - 101739 - District Hospital": "Vic Falls"
                    }
    
    processed_labs = []
    
    
    ist_provinces=['Bulawayo','Harare','Manicaland','Mash Central','Mash East','Mash West',
                     'Masvingo','Mat North','Mat South','Midlands']
    processed_province=[]

    def append_value_to_name(name, value):
                    
        """Helper function to append a value to a given name in the format 'name (value)'"""
        return f"{name} ({value})"
    user = request.user  # Get the authenticated user
    #laboratory = user.laboratory
    
    for user in User.objects.all():
        groups = ', '.join([group.name for group in user.groups.all()])
        #print(f"{user.username}: {groups}")

    startdate = None
    enddate = None
    selected_provinces = None
    pepfar_support = None
    
    SQLqueries = allqueries(startdate, enddate, selected_provinces, pepfar_support)
    data = []

    # Initialize your lists
    RESOLVED_Specimens_rejected = []
    UNRESOLVED_Investigation_in_progress = []
    Specimens_not_received = []
    not_documented = []
    UNRESOLVED_Pending_testing = []
    pending_publishing = []
    not_yet_dispatched_printed = []
    Results_wrong_facility = []
    UNRESOLVED_Referred_awaiting_results = []

    # Process the SQL queries
    for query in SQLqueries:
        try:
            with connections['default'].cursor() as cursor:
                cursor.execute(query['sql'], query['parameters'])
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': query['query_name'], 'query_data': query_data})

                if query['query_name'] == 'CLIanalysis':
                    for row in query_data:
                        RESOLVED_Specimens_rejected.append(row['RESOLVED_Specimens_rejected'])
                        UNRESOLVED_Investigation_in_progress.append(row['UNRESOLVED_Investigation_in_progress'])
                        Specimens_not_received.append(row['Specimens_not_received'])
                        not_documented.append(row['not_documented'])
                        UNRESOLVED_Pending_testing.append(row['UNRESOLVED_Pending_testing'])
                        pending_publishing.append(row['pending_publishing'])
                        not_yet_dispatched_printed.append(row['not_yet_dispatched_printed'])
                        Results_wrong_facility.append(row['Results_wrong_facility'])
                        UNRESOLVED_Referred_awaiting_results.append(row['UNRESOLVED_Referred_awaiting_results'])
        except Exception as e:
            print(f"An error occurred for {query['query_name']}: {e}")
    
    lab_status = {}  # Step 1: Create an empty dictionary     

    for lab in labs_list:
        lab_exists = False
        for result in data:
            if 'query_name' in result and result['query_name'] == 'weekly_Dashboard':
                for row in result['query_data']:
                    if row['LAB'] == lab:
                        lab_exists = True
                        break
        alias = alias_dict.get(lab, "Unknown")  # Retrieve alias, default to "Unknown" if not found
        processed_labs.append({
            'name': lab,
            'alias': alias,
            'exists': lab_exists
        })
        lab_status[lab] = lab_exists    
        
        
        
        
    #--looking at who submit ist
    ist_prov_status = {}  # Step 1: Create an empty dictionary      
    for prov in ist_provinces:
        
        prov_exists = False 
        for result in data:
            if 'query_name' in result and result['query_name'] == 'ist_submission':
                for row in result['query_data']:
                    if row['Province'] == prov:
                        prov_exists = True
                        break
        processed_province.append({
            'name': prov,
            'exists': prov_exists
        })
        ist_prov_status[prov] = prov_exists  # Step 2: Update the dictionary      
        
 
        

    # Define sankey nodes
    sankey_data = [
                    { "name": 'Resolved' },
                    { "name": 'UnResolved' },
                    { "name": 'Pending Investigation' },
                    { "name": 'LIMS Failed' },
                    { "name": 'Rejected' },
                    { "name": 'Waiting' },
                    { "name": 'Pending Testing' },
                    { "name": 'Not Received' },
                    { "name": 'Not Documented' },
                    { "name": 'Pending Published' },
                    { "name": 'Not Dispatched' },
                    { "name": 'Wrong Publishing' }
                ]

    # Define sankey links
    # Prepare data for the sankey chart
    sankey_links= [
        { "source": 'Resolved', "target": 'LIMS Failed', "value": RESOLVED_Specimens_rejected[0] if RESOLVED_Specimens_rejected else 0 },
        { "source": 'UnResolved',"target": 'Pending Investigation', "value": UNRESOLVED_Investigation_in_progress[0] if UNRESOLVED_Investigation_in_progress else 0 },
        { "source": 'Resolved', "target": 'Not Received', "value": Specimens_not_received[0] if Specimens_not_received else 0 },
        { "source": 'Resolved', "target": 'Not Documented', "value": not_documented[0] if not_documented else 0 },
        { "source": 'UnResolved',"target": 'Pending Testing', "value": UNRESOLVED_Pending_testing[0] if UNRESOLVED_Pending_testing else 0 },
        { "source": 'Resolved', "target": 'Pending Published', "value": pending_publishing[0] if pending_publishing else 0 },
        { "source": 'Resolved', "target": 'Not Dispatched', "value": not_yet_dispatched_printed[0] if not_yet_dispatched_printed else 0 },
        { "source": 'Resolved', "target": 'Wrong Publishing', "value": Results_wrong_facility[0] if Results_wrong_facility else 0 },
        { "source": 'UnResolved',"target": 'Waiting', "value": UNRESOLVED_Referred_awaiting_results[0] if UNRESOLVED_Referred_awaiting_results else 0 }
    ]
    # Calculate node values
    node_values = {}
    for link in sankey_links:
        value_source = link["value"] if link["value"] is not None else 0
        node_values[link["source"]] = node_values.get(link["source"], 0) + value_source
        
        value_target = link["value"] if link["value"] is not None else 0
        node_values[link["target"]] = node_values.get(link["target"], 0) + value_target

    # Modify node names to include values
    for node in sankey_data:
        node_value = node_values.get(node["name"], 0)
        node["name"] = append_value_to_name(node["name"], node_value)

    # Adjust link names to match modified node names
    for link in sankey_links:
        link["source"] = append_value_to_name(link["source"], node_values[link["source"]])
        link["target"] = append_value_to_name(link["target"], node_values[link["target"]])
        
        
        
        
    # Extract unique platforms and dates
    uniquePlatforms = set()
    uniqueDates = set()

    for result in data:
        if result['query_name'] == 'failure_rate_monthly':
            for row in result['query_data']:
                # Add the platform to the unique platforms set
                uniquePlatforms.add(row['Platform'])
                
                # Add the date columns to the unique dates set
                for key in row.keys():
                    if key != 'Platform' and '-' in key:  # assuming dates are the only keys with '-' character
                        uniqueDates.add(key)

    uniquePlatforms = list(uniquePlatforms)
    uniqueDates = sorted(list(uniqueDates))   
    


    # Check if uniqueDates is not empty before accessing the last element
    if uniqueDates:
        most_recent_date = uniqueDates[-1]
    else:
        # Handle the case where uniqueDates is empty
        # You might want to set a default value or handle this situation appropriately
        most_recent_date = None  # Or some default value
   
    # print(uniquePlatforms)
    # print(uniqueDates)
    # print(most_recent_date)
    
    headers_unresolved = []
    for result in data:
        if result['query_name'] == 'lab_unresolved':
            headers_unresolved = result['query_data'][0].keys() if result['query_data'] else []
            break
    
    context = {
        'data': data,
        'user': user,
        
        'labs_list':labs_list,
        'lab_status':lab_status,
        'processed_labs':processed_labs,
        
         'ist_prov_status':ist_prov_status,
         'processed_province':processed_province,
         
        'uniquePlatforms': uniquePlatforms,
        'uniqueDates': uniqueDates,
        'most_recent_date':most_recent_date,
        'now': datetime.datetime.now,
        
        'sankey_data': sankey_data,
        'sankey_links': sankey_links,
        'headers_unresolved':headers_unresolved,
    }
    return render(request, 'index.html', context)


#============================IST SECTION================================================

def ist_view(request):
    
    if request.method == 'POST':
        defaultStart_date = '2022-01-01'
        default_end_date = datetime.datetime.now().strftime('%Y-%m-%d')

        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')
                
        selected_provinces = request.POST.getlist('province') 
        selected_provinces = tuple(selected_provinces)  # Convert list to tuple

        # selected_provinces = request.POST.get('province') 
        # selected_provinces = json.loads(selected_provinces)  # Convert JSON string to Python list
        # selected_provinces = tuple(selected_provinces)  # Convert list to tuple

        dsd_checked = request.POST.get('dsd',True)
        ta_sdi_checked = request.POST.get('ta_sdi',True)

        pepfar_support = ()
        if (dsd_checked == 'true' and ta_sdi_checked == 'false'):
            pepfar_support = ('DSD',)
        elif (ta_sdi_checked == 'true' and dsd_checked == 'false'):
            pepfar_support = ('TA-SDI',)
        elif (ta_sdi_checked == 'true' and dsd_checked == 'true'):
            pepfar_support = ('DSD', 'TA-SDI')

        # Check if the values are being passed correctly
        # print(startdate)
        # print(enddate)
        # print(selected_provinces)
        # print( dsd_checked)
        # print(ta_sdi_checked)
        # print(pepfar_support)

        # Your processing logic here...

       
        
        # Get the queries with dynamic filters
        queries = istqueries(startdate, enddate, selected_provinces, pepfar_support)

        data = []
        
        for query in queries:
            try:
                with connections['default'].cursor() as cursor:
                    # No need to define parameters here, as they are now included in the istqueries function
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                
         # Return a JSON response (optional)
        msg = f"The filters successfully applied on the Page and  Charts: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate}:<br> <strong>Provinces :</strong> {selected_provinces} "
        messages.error(request, msg)
        
        context = {
            'data': data,
            'msg': msg,
            
        }
        return render(request, 'ist.html', context)
        #return JsonResponse(response_data)

    #------------------Lets handle the default settings ---------------------------------------------->
    else:
        # Handle the case when the request method is not POST
        defaultStart_date = '2022-01-01'
        default_end_date = datetime.datetime.now().strftime('%Y-%m-%d')

        startdate = defaultStart_date
        enddate = default_end_date

        selected_provinces = ['Mashonaland West','Masvingo','Matebeleland North','Metebeleland South','Mashonaland Central','Midlands,Harare','Manicaland','Bulawayo','Mashonaland East']
        
        # Prepare the tuple for SQL IN clause
        selected_provinces = tuple(selected_provinces)

        dsd_checked = 'true'
        ta_sdi_checked = 'true'

        pepfar_support = ('DSD', 'TA-SDI')

        # Get the queries with dynamic filters using default parameters
        queries = istqueries(startdate, enddate, selected_provinces, pepfar_support)

        data = []
        for query in queries:
            try:
                with connections['default'].cursor() as cursor:
                    # No need to define parameters here, as they are now included in the istqueries function
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")

        context = {
            'data': data,
        }
        return render(request, 'ist.html', context)



#-- For the Provincial SIEs ----------------------------------->
@login_required
def index_sie(request):

    error_messages=None
    user = request.user  # Get the authenticated user
  
    # msg = "Welcome User."
    # messages.info(request, msg)
    
    if request.method == 'POST':
        form = WeeklyForm(request.POST, request.FILES)
        
        if form.is_valid():
            file = request.FILES['upload_file']
            # msg = file
            # messages.success(request, msg)            
            
            # Save the uploaded file
            # fs = FileSystemStorage()
            # filename = fs.save(file.name, file)
            # file_path = fs.path(filename)

            # Check if the uploaded document is an Excel file
            if not file.name.endswith('.xlsx'):
                msg = "Uploaded file is not an Excel file. Please upload a .xlsx file."
                messages.error(request, msg)
            else:
                try:
                    # Check the value in cell B7 of the first sheet
                    df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
                    #print(df.iloc[5, 1])
                    # print(df.shape[0])
                    # msg = df.iloc[5, 1]
                    # messages.error(request, msg)
                    
                    # Check if the DataFrame has enough rows
                    if df.shape[0] < 7:
                        msg = "The file does not suit the format of the weekly Dashboard , Make sure you are using the correct Version."
                        messages.error(request, msg)
                    elif (df.iloc[2, 1] != 'Compiled by') and  (df.iloc[2, 1] != 'Select District'):
                        msg = "The file is not a Weekly Dashboard. Please download and use the latest Dashboard."
                        messages.error(request, msg)
                    else:
                        if df.iloc[2, 1]=='Compiled by':
                            error_messages=readingLabdashboard(file)
                        elif df.iloc[2, 1]=='Select District':
                            error_messages=readingHUBdashboard(file)
                        #error_messages  = reading_dash(file)  # Pass the 'file_path' argument to the function
                        # print(error_messages)
                        # messages.error(request, error_messages)
                        if error_messages :
                            if 'successfully' in error_messages.lower():
                                # 1. Extract current month and year
                                current_date = datetime.datetime.now()
                                month_year = current_date.strftime('%B %Y')

                                # # 2. Determine the date of the last Sunday of the month
                                # last_day_of_month = (current_date.replace(month=current_date.month%12+1, day=1) - timedelta(days=1))
                                # while last_day_of_month.weekday() != 6:  # 6 represents Sunday
                                #     last_day_of_month -= timedelta(days=1)
                                
                                # Find the previous Sunday from the current date
                                while current_date.weekday() != 6:  # 6 represents Sunday
                                    current_date -= timedelta(days=1)

                                previous_sunday = current_date.date()

                                # 3. Create directory structure
                                base_folder = os.path.join(MEDIA_ROOT, 'Weekly_Dashboard', month_year, previous_sunday.strftime('%Y-%m-%d'))
                                #base_folder = os.path.join(month_year, last_day_of_month.strftime('%Y-%m-%d'))
                                if not os.path.exists(base_folder):
                                    os.makedirs(base_folder)

                                # 4. Move the uploaded file to the appropriate directory
                                fs = FileSystemStorage(location=base_folder)  # set the location to your base_folder
                                filename = fs.save(file.name, file)
                                file_path = fs.path(filename)
                                
                                # print(base_folder)
                                # print(filename)
                                # print(file_path)
                                # print(fs)

                                msg = error_messages
                                messages.success(request, msg)
                            else:
                                msg = error_messages
                                messages.error(request, msg)                                                                   


                        else:
                            # Handle if reading_dash() returns None
                            # ...
                            msg = f"Ra faila kuonekwa "
                            messages.error(request, msg)

                        
                except Exception as e:
                    msg = f"Failed to detect the File as : Weekly dashboard : it seems its not the correct version: {str(e)}"
                    #print(e)
                    messages.error(request, msg)

    else:
        form = WeeklyForm()
    
    files_uploaded = get_files_uploaded(user.username)  
    dash_uploaded = get_dashboad_uploaded(user.username)
    vl_received_uploaded=get_vl_received_uploaded(user.username)
    sms_uploaded=get_sms_uploaded(user.username)
    
    #print(sms_uploaded)
    
 
    context = {
        'form': form,
        'error_messages':error_messages,
        'user': user,
        'now': datetime.datetime.now,
        'files_uploaded': files_uploaded,
        'dash_uploaded': dash_uploaded,
        'vl_received_uploaded':vl_received_uploaded,
        'sms_uploaded':'sms_uploaded'
        
    }
    return render(request, 'index_sie.html', context)




def get_vl_received_uploaded(user):
    query, params = vl_received_uploaded_in_database(user)
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()    
    
    return results

def get_sms_uploaded(user):
    query, params = sms_uploaded_in_database(user)
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()    
    
    return results

def get_dashboad_uploaded(user):
    query, params = Dashboard_uploaded_in_database(user)
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()    
    
    return results


def get_files_uploaded(user):
    query, params = files_uploaded_in_database(user)
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()    
    
    return results

def delete_record_sie(request):
    if request.method == "POST":
        update_date = request.POST.get('update_date')
        sourcefile = request.POST.get('sourcefile')
        user = request.POST.get('user')
        status = request.POST.get('status')   
    
        
        connection_default = connections['default']
        try:
            with transaction.atomic(using='default'):  # Specify the database for the transaction
                with connection_default.cursor() as cursor:
                    # Get the query and parameters from the function
                    query, params = delete_creation_reports(update_date, sourcefile, user, status)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

            return JsonResponse({"status": "success", "msg": "File Successfully Deleted"})

        except Exception as e:
            # Handle exceptions and errors as necessary. 
            # It's advisable to log the actual error for debugging while sending a generic message to the client.
            return JsonResponse({"status": "error", "msg": str(e)}) 

    return JsonResponse({'status': 'error', 'msg': 'Invalid request method.'})



#deleting the vl_received
def delete_record_vl_received(request):
    if request.method == "POST":
        update_date = request.POST.get('update_date')
        sourcefile = request.POST.get('sourcefile')
        user = request.POST.get('user')
      
    
        
        connection_default = connections['default']
        try:
            with transaction.atomic(using='default'):  # Specify the database for the transaction
                with connection_default.cursor() as cursor:
                    # Get the query and parameters from the function
                    query, params = delete_vl_received_reports(update_date, sourcefile, user)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

            return JsonResponse({"status": "success", "msg": "File Successfully Deleted"})

        except Exception as e:
            # Handle exceptions and errors as necessary. 
            # It's advisable to log the actual error for debugging while sending a generic message to the client.
            return JsonResponse({"status": "error", "msg": str(e)}) 

    return JsonResponse({'status': 'error', 'msg': 'Invalid request method.'})


def upload_files(request):
    df = None
    read_response = None
    data_frames = {}
    user = request.user  # Get the authenticated user
    
    if request.method == "GET":
        return render(request, 'index_sie.html',)
    elif request.method == 'POST':
        files = request.FILES.getlist('files[]', None)
        number_of_files=len(files)
        #print(files)
        for file in files:
            #print(file)
            try:
                if os.path.splitext(file.name)[1].lower() in ['.csv']:
                    df = pd.read_csv(file)
                    data_frames[file.name] = df
                    handle_uploaded_file(file,user)
                    msg = read_response
                    messages.error(request, msg)
                elif os.path.splitext(file.name)[1].lower() in ['.xls', '.xlsx', '.xlsm']:
                    read_response = reading_vls(file,user.username)
                    msg = read_response
                    messages.error(request, msg)
                    handle_uploaded_file(file,user)
                else:
                    handle_uploaded_file(file,user)
                    msg = read_response
                    messages.error(request, msg)

                # Reset read_response and msg variables for the next file
                read_response = None
                msg = None

            except Exception as e:
                return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{str(e)}</div>'})

        # Return JsonResponse after processing all files
        #return JsonResponse({'msg': '<div class="alert alert-success" role="alert">Files successfully uploadedxxx</div>'})
        return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  reports successfully uploaded</div>', 'files': [file.name for file in files]})
    else:
        return render(request, 'index_sie.html',)
    
    
#Lets Handle the creation reports here
@login_required  
def HUBupload_files(request):    
    user = request.user  # Get the authenticated user
    messages_list = []  # To store all messages (errors or success)

    if request.method == "GET":
        return render(request, 'index_sie.html')
    
    elif request.method == 'POST':
        status = request.POST.get('status')
        if status == 'Standalone Hubs':
            files = request.FILES.getlist('HUBfiles[]', None)
            number_of_files = len(files)
            #print (number_of_files)   
            #print (files)   
                
            

            for file in files:
                #print(f"Processing file: {file.name}")
                try:
                    file_extension = os.path.splitext(file.name)[1].lower()
                    
                    # Check for valid Excel formats
                    if file_extension in ['.xls', '.xlsx', '.xlsm']:
                        df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
                        print(f"Successfully read {file.name} as an Excel file.")
                        
                    # Check for valid CSV format
                    elif file_extension == '.csv':
                        df = pd.read_csv(file)
                        #print(df.columns)
                        
                    else:
                        messages_list.append(f'🔬The file: - > {file.name} <-  is not a valid Creation report file. Please upload a valid one📔 in excel or CSV format❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})

                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Sample ID': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid Creation report file. Please upload a valid one❗❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:
                        #lets process a valid excel file
                        df['SourceFile'] =file.name
                        df['Status']=status
                        read_response = reading_Creation_reports(df,user.username)
                        if 'successfully' in  read_response: 
                            handle_uploaded_file(file, user)
                            
                            

                        else: 
                            messages_list.append(read_response)
                            return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                            

                except Exception as e:
                    messages_list.append(f'Error processing file: - > {file.name} <- : {str(e)}')

            # Check if there were any error messages
            if any("error" in msg.lower() for msg in messages_list):  # Here, we assume any message containing "error" is an error message
                return JsonResponse({'msg': '<br>'.join(messages_list)})

            # If no errors, return the success message

            #return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  Creation report(s) successfully uploaded {files} </div>'})
            return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  Creation report(s) successfully uploaded</div>', 'files': [file.name for file in files]})

        
        else: 
            files = request.FILES.getlist('LABfiles[]', None)
            number_of_files = len(files)
            #print (files)       
            

            for file in files:
                #print(f"Processing file: {file.name}")
                try:
                    file_extension = os.path.splitext(file.name)[1].lower()
                    
                    # Check for valid Excel formats
                    if file_extension in ['.xls', '.xlsx', '.xlsm']:
                        df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
                        #print(f"Successfully read {file.name} as an Excel file.")
                        
                    # Check for valid CSV format
                    elif file_extension == '.csv':
                        df = pd.read_csv(file)
                        #print(df.columns)
                        
                    else:
                        messages_list.append(f'🔬The file: - > {file.name} <-  is not a valid Creation report file. Please upload a valid one📔 in excel or CSV format❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})

                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Sample ID': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid Creation report file. Please upload a valid one❗❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:
                        #lets process a valid excel file
                        df['SourceFile'] =file.name
                        df['Status']=status
                        read_response = reading_Creation_reports(df,user.username)
                        if 'successfully' in  read_response: 
                            handle_uploaded_file(file, user)
                            

                        else: 
                            messages_list.append(read_response)
                            return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                            

                except Exception as e:
                    messages_list.append(f'Error processing file: - > {file.name} <- : {str(e)}')

            # Check if there were any error messages
            if any("error" in msg.lower() for msg in messages_list):  # Here, we assume any message containing "error" is an error message
                return JsonResponse({'msg': '<br>'.join(messages_list)})

            # If no errors, return the success message

            #return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  Creation report(s) successfully uploaded</div>'})
            return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  Creation report(s) successfully uploaded</div>', 'files': [file.name for file in files]})


    
    else:
        return render(request, 'index_sie.html')



#-----Loading IST files --------------------------------
 
#Lets Handle the creation reports here
@login_required  
def ISTupload_files(request):    
    user = request.user  # Get the authenticated user
    user=user.username
    messages_list = []  # To store all messages (errors or success)

    if request.method == "GET":
        return render(request, 'index_sie.html')
    
    elif request.method == 'POST':
        
        files = request.FILES.getlist('ISTfiles[]', None)
        number_of_files = len(files)
        #print (number_of_files)   
        #print (files)   
                
            

        for file in files:
            #print(f"Processing file: {file.name}")
            try:
                file_extension = os.path.splitext(file.name)[1].lower()
                
                # Check for valid Excel formats
                if file_extension in ['.xls', '.xlsx', '.xlsm']:                 
                    
                    
                    df = pd.read_excel(file, sheet_name=0,skiprows=1, engine='openpyxl')
                    df_relief = pd.read_excel(file, sheet_name=1,skiprows=1, engine='openpyxl')
                    df_drivers = pd.read_excel(file, sheet_name=2, skiprows=1, engine='openpyxl')
                    #print(df.columns)
                    
                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Name of Rider': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid IST Weekly report file. Please upload a valid one❗❗ <br> HINT: Delete Unnecessary (hidden) sheets, or download the template and work on it ')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:
                        
                    
                        #print(f"Successfully read {file.name} as an Excel file.")
                        response=reading_National_IST_Rider_Driver_Weekly(df,df_relief,df_drivers,file.name,user)
                        
                        if 'successfully' in response.lower():
                            
                            # 1. Extract current month and year
                            
                            current_date = datetime.datetime.now()
                            month_year = current_date.strftime('%B %Y')

                            
                            # Find the previous Sunday from the current date
                            while current_date.weekday() != 6:  # 6 represents Sunday
                                current_date -= timedelta(days=1)

                            previous_sunday = current_date.date()

                            # 3. Create directory structure
                            base_folder = os.path.join(MEDIA_ROOT, 'IST_WEEKLY', month_year, previous_sunday.strftime('%Y-%m-%d'))
                            #base_folder = os.path.join(month_year, last_day_of_month.strftime('%Y-%m-%d'))
                            if not os.path.exists(base_folder):
                                os.makedirs(base_folder)

                            # 4. Move the uploaded file to the appropriate directory
                            fs = FileSystemStorage(location=base_folder)  # set the location to your base_folder
                            filename = fs.save(file.name, file)
                            file_path = fs.path(filename)
                            
                            # print(base_folder)
                            # print(filename)
                            # print(file_path)
                            # print(fs)

                            msg = response
                            #messages.success(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{msg}  </div>'})
                        else:
                            msg = response
                            #messages.error(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{msg}  </div>'})
                        
                    
                else:
                    messages_list.append(f'🔬The file: - > {file.name} <-  is not a valid IST WEEKLY report file. Please upload a valid one📔 in excel format❗')
                    return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})

                
                

            except Exception as e:
                messages_list.append(f'Error processing file: - > {file.name} <- : {str(e)}')

        # Check if there were any error messages
        if any("error" in msg.lower() for msg in messages_list):  # Here, we assume any message containing "error" is an error message
            return JsonResponse({'msg': '<br>'.join(messages_list)})

        # If no errors, return the success message

        #return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  Creation report(s) successfully uploaded {files} </div>'})
        # return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  ISTx Weekly report(s) successfully uploaded</div>', 'files': [file.name for file in files]})

        
    else:
            
        files = request.FILES.getlist('ISTfiles[]', None)
        number_of_files = len(files)
        #print (number_of_files)   
        #print (files)   
                
            

        for file in files:
            print(f"Processing file: {file.name}")
            try:
                file_extension = os.path.splitext(file.name)[1].lower()
                
                # Check for valid Excel formats
                if file_extension in ['.xls', '.xlsx', '.xlsm']:                 
                    
                    
                    df = pd.read_excel(file, sheet_name=0,skiprows=1, engine='openpyxl')
                    df_relief = pd.read_excel(file, sheet_name=1,skiprows=1, engine='openpyxl')
                    df_drivers = pd.read_excel(file, sheet_name=2, skiprows=1, engine='openpyxl')
                    
                    
                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Name_of_Rider_': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid IST Weekly report file. Please upload a valid one❗❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:
                        
                    
                        #print(f"Successfully read {file.name} as an Excel file.")
                        response=reading_National_IST_Rider_Driver_Weekly(df,df_relief,df_drivers,file.name,user)
                        
                        if 'successfully' in response.lower():
                            
                            # 1. Extract current month and year
                            current_date = datetime.datetime.now()
                            month_year = current_date.strftime('%B %Y')

                            
                            # Find the previous Sunday from the current date
                            while current_date.weekday() != 6:  # 6 represents Sunday
                                current_date -= timedelta(days=1)

                            previous_sunday = current_date.date()

                            # 3. Create directory structure
                            base_folder = os.path.join(MEDIA_ROOT, 'IST_WEEKLY', month_year, previous_sunday.strftime('%Y-%m-%d'))
                            #base_folder = os.path.join(month_year, last_day_of_month.strftime('%Y-%m-%d'))
                            if not os.path.exists(base_folder):
                                os.makedirs(base_folder)

                            # 4. Move the uploaded file to the appropriate directory
                            fs = FileSystemStorage(location=base_folder)  # set the location to your base_folder
                            filename = fs.save(file.name, file)
                            file_path = fs.path(filename)
                            
                            # print(base_folder)
                            # print(filename)
                            # print(file_path)
                            # print(fs)

                            msg = response
                            #messages.success(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{msg}  </div>'})
                        else:
                            msg = response
                            #messages.error(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{msg}  </div>'})
                        
                    
                else:
                    messages_list.append(f'🔬The file: - > {file.name} <-  is not a valid IST WEEKLY report file. Please upload a valid one📔 in excel format❗')
                    return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})

                
                

            except Exception as e:
                messages_list.append(f'Error processing file: - > {file.name} <- : {str(e)}')

        # Check if there were any error messages
        if any("error" in msg.lower() for msg in messages_list):  # Here, we assume any message containing "error" is an error message
            return JsonResponse({'msg': '<br>'.join(messages_list)})

        # If no errors, return the success message

        #return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  Creation report(s) successfully uploaded {files} </div>'})
        # return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  ISTx Weekly report(s) successfully uploaded</div>', 'files': [file.name for file in files]})






#--------------------------------------------------------

def handle_uploaded_file(file, username):
    now = datetime.datetime.now()

    offset = (now.weekday() - 6) % 7
    last_sunday = now - datetime.timedelta(days=offset)
    last_sunday = last_sunday.strftime('%Y-%m-%d')

    # formatting the folder and file names
    folder_name = now.strftime('%B-%Y')
    
    # user_folder_name = f"{username}_{now.strftime('%Y%m%d_%H%M%S')}"
    # file_name = f"{os.path.splitext(file.name)[0]}_{now.strftime('%Y%m%d_%H%M%S')}{os.path.splitext(file.name)[1]}"
    
    user_folder_name = f"{username}_{now.strftime('%Y%m%d')}"
    file_name = f"{os.path.splitext(file.name)[0]}_{now.strftime('%Y%m%d')}{os.path.splitext(file.name)[1]}"

    # creating the folders if they don't exist
    save_path = os.path.join(settings.MEDIA_ROOT, 'SIE_Reports', folder_name,last_sunday, user_folder_name)
    os.makedirs(save_path, exist_ok=True)

    # saving the file
    full_file_path = os.path.join(save_path, file_name)
    with default_storage.open(full_file_path, 'wb+') as destination:
        for chunk in file.chunks():
            destination.write(chunk)
            
            





def explore_folder(request, folder_name="MandE_Tools"):
    folder_path = os.path.join(settings.MEDIA_ROOT, folder_name)

    if os.path.isfile(folder_path):
        return FileResponse(open(folder_path, 'rb'), content_type='application/octet-stream')

    files_and_folders = []
    for item in os.listdir(folder_path):
        path = os.path.join(folder_path, item)
        item_dict = {
            "name": item,
            "is_file": os.path.isfile(path),
            "date_modified": datetime.datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')
        }
        files_and_folders.append(item_dict)

    return render(request, "explore.html", {"files_and_folders": files_and_folders, "current_folder": folder_name})


@login_required
def explore_folder_Admin(request, folder_name=""):
    folder_name = folder_name.replace("//", "/")
    # Construct the folder_path using MEDIA_ROOT and folder_name
    folder_path = os.path.join(settings.MEDIA_ROOT, folder_name)

    # Check if the path is a file
    if os.path.isfile(folder_path):
        return FileResponse(open(folder_path, 'rb'), content_type='application/octet-stream')

    # Check if the folder exists
    if not os.path.exists(folder_path):
        raise Http404("The specified folder does not exist.")

    files_and_folders = []
    for item in os.listdir(folder_path):
        path = os.path.join(folder_path, item)
        item_dict = {
            "name": item,
            "is_file": os.path.isfile(path),
            "date_modified": datetime.datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')
        }
        files_and_folders.append(item_dict)
        
        # Sort the list by date_modified in descending order
        files_and_folders = sorted(files_and_folders, key=lambda x: x['date_modified'], reverse=True)
        

    return render(request, "explore_Admin.html", {"files_and_folders": files_and_folders, "current_folder": folder_name})



    
def viral_load_received(request):     
    

    # Get today's date
    current_date = datetime.datetime.now().date()
    # Get the start date as the first day of the month 3 months ago
    startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
    # Convert dates to string format
    startdate = startdate_srt.strftime('%Y-%m-%d')
    enddate = datetime.datetime.now().strftime('%Y-%m-%d')
 
    
    SQLqueries = odkScripts(startdate, enddate)
    
    
    data = []
    for query in SQLqueries:
        try:
            with connections['default'].cursor() as cursor:
                cursor.execute(query['sql'])
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': query['query_name'], 'query_data': query_data})
        except Exception as e:
            print(f"An error occurred for {query['query_name']}: {e}")
            
            
            
    headers_vl_received = []
    for result in data:
        if result['query_name'] == 'lab_unresolved':
            headers_vl_received = result['query_data'][0].keys() if result['query_data'] else []
            break
    
    context = {    
        'data': data,
        'headers_vl_received':headers_vl_received,
    }
    return render(request, 'vl_received.html', context)




def transported(request):     
    

    # Get today's date
    current_date = datetime.datetime.now().date()
    # Get the start date as the first day of the month 3 months ago
    startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
    # Convert dates to string format
    startdate = startdate_srt.strftime('%Y-%m-%d')
    enddate = datetime.datetime.now().strftime('%Y-%m-%d')
 
    
    SQLqueries = odkScripts(startdate, enddate)
    
    
    data = []
    for query in SQLqueries:
        try:
            with connections['default'].cursor() as cursor:
                cursor.execute(query['sql'])
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': query['query_name'], 'query_data': query_data})
        except Exception as e:
            print(f"An error occurred for {query['query_name']}: {e}")
            
            
            
    headers_vl_received = []
    for result in data:
        if result['query_name'] == 'lab_unresolved':
            headers_vl_received = result['query_data'][0].keys() if result['query_data'] else []
            break
    
    context = {    
        'data': data,
        'headers_vl_received':headers_vl_received,
    }
    return render(request, 'transported.html', context)





    
def bike_function_view(request):
    
    if request.method == 'POST':        
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')
    
        results = pull_last_day_fxn_IST()   
        #The [SubmissionDate] is the second column in your query
        submission_date = results[0][0] if results else None

        SQLqueries=bike_fxnality_prov(startdate,enddate)
        data = []
        
        
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                
    
            
        headers = []
        for result in data:
            if result['query_name'] == 'bike_fxn_prov':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
            
        msg = f"The filters successfully applied on the Page and  Charts: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} "
        messages.error(request, msg)
        
        context = {
            'data': data,
            'headers':headers,
            'submission_date':submission_date,
            'msg': msg,
     
                }
        return render(request, 'bike_functionality.html', context)
    
    else:        
    
        
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')       
        # print(startdate)
        # print(enddate)
    
        results = pull_last_day_fxn_IST()   
        #The [SubmissionDate] is the second column in your query
        submission_date = results[0][0] if results else None

        SQLqueries=bike_fxnality_prov(startdate,enddate)
        data = []
        
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                
      
            
        headers = []
        for result in data:
            if result['query_name'] == 'bike_fxn_prov':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
             

        context = {
            'data': data,
            'headers':headers,
            'submission_date':submission_date,
     
                }
        return render(request, 'bike_functionality.html', context)




# def weeklyNarrative_view(request):   

    
#     if request.method == 'POST':
#         startdate = request.POST.get('startdate')
#         enddate = request.POST.get('enddate')
#         selected_provinces=None
#         pepfar_support=None
  

            
#         SQLqueries = allqueries(startdate, enddate, selected_provinces, pepfar_support)
#         data = []
#         for query in SQLqueries:
#             try:
#                 with connections['default'].cursor() as cursor:
#                     cursor.execute(query['sql'], query['parameters'])
#                     columns = [col[0] for col in cursor.description]
#                     query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
#                     data.append({'query_name': query['query_name'], 'query_data': query_data})
#             except Exception as e:
#                 print(f"An error occurred for {query['query_name']}: {e}")
                
#         headers = []
#         for result in data:
#             if result['query_name'] == 'failed_Samples':
#                 headers = result['query_data'][0].keys() if result['query_data'] else []
#                 break
             
#         msg = f"The filters successfully applied on the Page and  Charts: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} "
#         messages.error(request, msg)
            
#         context = {
#                 'data': data,
#                 'msg': msg,
#                 'headers':headers,
                
#             }


#         return render(request, 'weekly_narrative.html', context)
#     else:
#         # Calculate the start and end dates based on the current date
#         startdate = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
#         enddate = datetime.datetime.now().strftime('%Y-%m-%d')
#         selected_provinces=None
#         pepfar_support=None

#         print(startdate)
#         # print(enddate)
#         SQLqueries = allqueries(startdate, enddate, selected_provinces, pepfar_support)
#         data = []
#         for query in SQLqueries:
#             try:
#                 with connections['default'].cursor() as cursor:
#                     cursor.execute(query['sql'], query['parameters'])
#                     columns = [col[0] for col in cursor.description]
#                     query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
#                     data.append({'query_name': query['query_name'], 'query_data': query_data})
#             except Exception as e:
#                 print(f"An error occurred for {query['query_name']}: {e}")
                
                
#         headers = []
#         for result in data:
#             if result['query_name'] == 'failed_Samples':
#                 headers = result['query_data'][0].keys() if result['query_data'] else []
#                 break
#         print(headers)
#         context = {
#             'headers':headers,
#             'data': data,
#         }    
#         return render(request, 'weekly_narrative.html',context)


def weeklyNarrative_view(request):  

    
    if request.method == 'POST':
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')
        selected_provinces=None
        pepfar_support=None
  

            
        SQLqueries = weekly_narrative_queries(startdate, enddate)
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
                
        headers = []
        for result in data:
            if result['query_name'] == 'failed_Samples':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
             
        msg = f"The filters successfully applied on the Page and  Charts: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} "
        messages.error(request, msg)
            
        context = {
                'data': data,
                # 'msg': msg,
                # 'headers':headers,
                
            }


        return render(request, 'weekly_narrative.html', context)
    else:
        # Calculate the start and end dates based on the current date
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        selected_provinces=None
        pepfar_support=None

        # print(startdate)
        # print(enddate)
        SQLqueries = weekly_narrative_queries(startdate, enddate)
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
                
                
        headers = []
        for result in data:
            if result['query_name'] == 'failed_Samples':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
        context = {
            'headers':headers,
            'data': data,
        }    
        return render(request, 'weekly_narrative.html',context)














    
def specimens_results(request):
    if request.method == 'POST':        
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')                
        selected_provinces = request.POST.getlist('province') 
        selected_provinces = tuple(selected_provinces)  # Convert list to tuple
        dsd_checked = request.POST.get('dsd',True)
        ta_sdi_checked = request.POST.get('ta_sdi',True)

        pepfar_support = ()
        if (dsd_checked == 'true' and ta_sdi_checked == 'false'):
            pepfar_support = ('DSD',)
        elif (ta_sdi_checked == 'true' and dsd_checked == 'false'):
            pepfar_support = ('TA-SDI',)
        elif (ta_sdi_checked == 'true' and dsd_checked == 'true'):
            pepfar_support = ('DSD', 'TA-SDI')

        print(pepfar_support)
        # Get the queries with dynamic filters
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
        
        # Return a JSON response (optional)
        msg = f"The filters successfully applied on the Page and  Charts: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate}:<br> <strong>Provinces :</strong> {selected_provinces} "
        messages.error(request, msg)
        
        context = {
            'data': data,
            'msg': msg,
            
        }
        return render(request, 'result.html',context)
    
    else:
        startdate = '2020-01-01'
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        selected_provinces = ['Mashonaland West','Masvingo','Matebeleland North','Metebeleland South','Mashonaland Central','Midlands,Harare','Manicaland','Bulawayo','Mashonaland East']
        selected_provinces = tuple(selected_provinces)
        pepfar_support = ('DSD', 'TA-SDI')
        print(startdate, enddate)
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

        context = {
        
            'data': data,
        }    
        return render(request, 'result.html',context)
        


#---------------------------------------------------------------------->

# This function will generate all months between two dates
def list_months(start_date, end_date):
    # List to store result
    result = []

    # Current date to be used for iteration, starting from the start_date
    current_date = start_date

    while current_date <= end_date:
        # Append the current month and year in the format 'MM-YYYY'
        result.append(current_date.strftime('%m-%Y'))

        # Move to the next month. We set the day to 1 to handle month length differences
        current_month = current_date.month
        current_date = current_date.replace(day=1)

        # Increase the month by one, and if it's December, reset to January and increase the year
        if current_month == 12:
            current_date = current_date.replace(month=1, year=current_date.year + 1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

    return result




#View for the relief riders
def relief_spec_and_res(request):
    if request.method == 'POST':
        results = pull_last_day_fxn_IST()   
        #The [SubmissionDate] is the second column in your query
        last_Uploaded_date = results[0][0] if results else None
         
        start_date = request.POST.get('startdate')
        end_date = request.POST.get('enddate') 
        
        start_date_r = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"
        #print(datelist)
        
        SQLqueries = ISTSQLQueries(datelist,start_date,end_date)
        
        data = []
   
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    
                    
  

                 
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                    
 
            
               
        headers = []
        for result in data:
            if result['query_name'] == 'ist_reliefSamples_transported':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
    
        headers_ref = []
        for result in data:
            if result['query_name'] == 'ist_relief_riders_per_province':
                headers_ref = result['query_data'][0].keys() if result['query_data'] else []
                break 
    
        msg = f"The filters successfully applied:  <br> <strong>Start Date: </strong>  {start_date} <br> <strong> End Date:</strong>   {end_date}  "
        messages.error(request, msg)  

        context = {
            'data': data,               
            'headers': headers,
            'msg': msg,
            'last_Uploaded_date':last_Uploaded_date,
            'headers_ref':headers_ref,
        }
    
        # Check if the request is AJAX
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse(context)  # Return JSON data if it's an AJAX request
        else:
            return render(request, 'relief_specimen_and_results.html', context)  # Else, render the template as usual
        
    else:
        
        results = pull_last_day_fxn_IST()   
        #The [SubmissionDate] is the second column in your query
        last_Uploaded_date = results[0][0] if results else None
        start_date = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        
     
        start_date_r = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"
        #print(datelist)
        
        SQLqueries = ISTSQLQueries(datelist,start_date,end_date)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                    

                
        headers = []
        for result in data:
            if result['query_name'] == 'ist_reliefSamples_transported':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break 
              
        headers_ref = []
        for result in data:
            if result['query_name'] == 'ist_relief_riders_per_province':
                headers_ref = result['query_data'][0].keys() if result['query_data'] else []
                break 
      
        context = {
            'data': data,               
            'headers': headers,
            'last_Uploaded_date':last_Uploaded_date,
            'headers_ref':headers_ref,
          
        }
    
        # Check if the request is AJAX
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse(context)  # Return JSON data if it's an AJAX request
        else:
            return render(request, 'relief_specimen_and_results.html', context)  # Else, render the template as usual

    #--------------------------------------------------------------------------->


# Function to convert any non-serializable types like np.int64
def convert_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (list, tuple, set)):
        return [convert_types(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_types(val) for key, val in obj.items()}
    else:
        return obj
def shipments(request):
    messages_list = []
    result_data=None
    data_by_lab=None
    if request.method == "GET":
        return render(request, 'shipment.html')
    
    elif request.method == 'POST':        
        files = request.FILES.getlist('files[]')
        print(files)

        for file in files:
            print(f"Processing file: {file.name}")
            try:
                file_extension = os.path.splitext(file.name)[1].lower()
                
                # Check for valid Excel formats
                if file_extension in ['.xls', '.xlsx', '.xlsm']:               
                    
                    
                    df = pd.read_excel(file, sheet_name=1, engine='openpyxl')
                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Lab': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid Shipment report file. Please upload a valid one❗❗ <br> HINT:  it must be in sheet 2 ')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:                       
                        # Converting 'Date Registered' to datetime
                        df['Date Registered'] = pd.to_datetime(df['Date Registered'])
                        df['Date_reg'] = pd.to_datetime(df['Date Registered']).dt.date

                        # Grouping by 'Lab' and 'Shipment' and performing the necessary calculations
                        result_df = df.groupby(['Lab', 'Shipment','Creator','Date_reg']).agg(
                            count=('Sample ID', 'size'),
                            time_taken=('Date Registered', lambda x: (x.max() - x.min()).seconds / 60)  # in minutes
                        ).reset_index()

                        # Converting 'time_taken' to a string representation
                        result_df['time_taken'] = result_df['time_taken'].apply(lambda x: f"{int(x)}")  # in minutes

                        # A dictionary to hold all data by lab
                        data_by_lab = {}
                  
                   

                        # Iterate over each lab
                        for lab in result_df['Lab'].unique():
                            lab_df = result_df[result_df['Lab'] == lab]
                            
                            # Get distinct 'count' values and sort
                            counts = lab_df['count'].unique()
                            counts.sort()
                            
                  
                            # Prepare data for plotting
                            plot_data = []
                            for count in counts:
                                max_time = lab_df[lab_df['count'] == count]['time_taken'].max()
                                # Convert 'max_time' from string to integer before appending it to 'plot_data'
                                try:
                                    max_time_int = int(max_time)  # convert to integer
                                except ValueError:
                                    # handle the case where conversion fails, for example, when 'max_time' is not a valid integer string.
                                    max_time_int = 0  # or some other appropriate default value
                                
                                plot_data.append([count, max_time_int])
                            
                            data_by_lab[lab] = plot_data
                            
                            
                            
                            
                            
                        result_data = result_df.to_dict(orient='records') 
                        # Ensure all data within your structures are JSON serializable
                        result_data = [convert_types(record) for record in result_data]
                        data_by_lab = {key: convert_types(val) for key, val in data_by_lab.items()}
                    




                else:
                        messages_list=0
                        messages_list.append(f'🔬The file: - > {file.name} <-  is not a valid IST WEEKLY report file. Please upload a valid one📔 in excel format❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})

            except Exception as e:
                messages_list.append(f'Error processing file: - > {file.name} <- : {str(e)}')


        max_values_by_lab = {}  # This dictionary will store the results.

        # Iterate through each lab in the data_by_lab dictionary.
        for lab, data in data_by_lab.items():
            # Initialize variables to hold the maximum values and their corresponding values.
            max_count = None
            max_time = None
            time_for_max_count = None
            count_for_max_time = None

            # Iterate through each record in the lab's data.
            for record in data:
                count, time = record  # Unpack the record into count and time.

                # Update max_count and time_for_max_count if the current count is higher than the previous maximum.
                if max_count is None or count > max_count:
                    max_count = count
                    time_for_max_count = time

                # Update max_time and count_for_max_time if the current time is higher than the previous maximum.
                if max_time is None or time > max_time:
                    max_time = time
                    count_for_max_time = count

            # Store the results in the max_values_by_lab dictionary.
            max_values_by_lab[lab] = {
                'max_count': max_count,
                'time_for_max_count': time_for_max_count,
                'max_time': max_time,
                'count_for_max_time': count_for_max_time,
            }



            
        context = {
            'result_data': result_data,   
            'data_by_lab':data_by_lab, 
            'data_by_lab_json': json.dumps(data_by_lab), 
            'max_values_by_lab_json': json.dumps(max_values_by_lab), 
                  

        }
    
        # Check if the request is AJAX
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse(context)  # Return JSON data if it's an AJAX request
        else:
            return render(request, 'shipment.html', context)  # Else, render the template as usual
    
   
#extracting Other samples and assign them in their columns
def extract_and_assign(row, prefix):
    """
    Process and extract values from a row based on a regex pattern.
    """
    # Handle non-string and NaN values
    if pd.isnull(row) or not isinstance(row, str):
        return {}

    # Existing processing logic
    row = row.replace(" ", "").lower()  # Remove spaces and convert to lowercase
    items = re.split(r',', row)  # Split by comma
    
    result = {}
    for item in items:
        match = re.match(r'(\d*\.?\d+)([a-zA-Z0-9]+)', item)
        if match:
            number, label = match.groups()
            clean_label = prefix + "_" + re.sub(r'[^a-zA-Z0-9]', '', label).lower()
            result[clean_label] = result.get(clean_label, 0) + float(number)
    
    return result


# Function to rename duplicate columns
def rename_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))
        ]
    df.columns = cols

def IST_specimens_and_results(request):
    if request.method == 'POST':
        results = pull_last_day_fxn_IST()   
        #The [SubmissionDate] is the second column in your query
        last_Uploaded_date = results[0][0] if results else None
         
        start_date = request.POST.get('startdate')
        end_date = request.POST.get('enddate') 
        
        start_date_r = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        # Generate month list
        # months_list = list_months(start_date_r, end_date_r)

        # # The result can be joined into a single string if needed
        # months_str = ','.join(months_list)
        # datelist = f"'{months_str}'"
        #print(datelist)
        datelist=None
        SQLqueries = ISTSQLQueries(datelist,start_date,end_date)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                    
        headers = []
        for result in data:
            if result['query_name'] == 'ist_getspecimen_results':            
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break   
            
        headersGF = []
        for result in data:
            if result['query_name'] == 'ist_GFgetspecimen_results':            
                headersGF = result['query_data'][0].keys() if result['query_data'] else []
                break 
            
        headersPEPFAR = []
        for result in data:
            if result['query_name'] == 'ist_PEPFARgetspecimen_results':            
                headersPEPFAR = result['query_data'][0].keys() if result['query_data'] else []
                break 
            
        headersWK = []
        for result in data:
            if result['query_name'] == 'weekly_ist_transported':            
                headersWK = result['query_data'][0].keys() if result['query_data'] else []
                break 
        
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'ist_other_samples_and_results':
                ist_other_samples_and_results_df = pd.DataFrame(result['query_data'])
                #print(ist_other_samples_and_results_df.columns)
       
        # Check if the dataframe is empty or if expected columns are missing
        if 'ist_other_samples_and_results_df' not in locals() or ist_other_samples_and_results_df.empty:
            print("No data returned from the database.")
            ist_other_samples_and_results_df = pd.DataFrame({'other_sam': [0], 'other_res': [0]})
        else:
            # Check if the expected columns exist, if not add them
            if 'other_sam' not in ist_other_samples_and_results_df.columns:
                ist_other_samples_and_results_df['other_sam'] = 0
            if 'other_res' not in ist_other_samples_and_results_df.columns:
                ist_other_samples_and_results_df['other_res'] = 0
        
        
        # Define columns of interest excluding 'Province' as it doesn't need processing
        columns_of_interest = [
            'other_sam',
            'other_res',
        ]
        prefixes = ["samples", "results"]  # Define the prefixes for your new columns
        aggregated_data = []
        # Apply the function to each cell in the columns of interest
        for col, prefix in zip(columns_of_interest, prefixes):
            col_data = []
            for _, row_value in ist_other_samples_and_results_df[col].items():
                col_data.append(extract_and_assign(row_value, prefix))
            aggregated_data.append(pd.DataFrame(col_data))
            
        
        # Merge the aggregated data columns into the main dataframe
        for element_in_aggregated_data, prefix in zip(aggregated_data, prefixes):
            ist_other_samples_and_results_df = pd.concat([ist_other_samples_and_results_df, element_in_aggregated_data], axis=1)
        ist_other_samples_and_results_df.fillna(0, inplace=True)   
        
        # Rename duplicate columns to ensure uniqueness
        rename_duplicate_columns(ist_other_samples_and_results_df)

        # Identify non-numeric columns and convert them
        non_numeric_cols = ist_other_samples_and_results_df.select_dtypes(include=['object', 'string']).columns
        for col in non_numeric_cols:
            ist_other_samples_and_results_df[col] = pd.to_numeric(ist_other_samples_and_results_df[col], errors='coerce')

        # Proceed with combining 'samples_measels' and 'samples_measles', if they exist
        if 'samples_measels' in ist_other_samples_and_results_df.columns:
            if 'samples_measles' not in ist_other_samples_and_results_df.columns:
                ist_other_samples_and_results_df['samples_measles'] = 0
            ist_other_samples_and_results_df['samples_measles'] += ist_other_samples_and_results_df.pop('samples_measels')

        # After correcting the DataFrame, proceed with the summation
        column_sums = ist_other_samples_and_results_df.sum()

        # Prepare lists of column names for samples and results
        samples_columns = [col for col in column_sums.index if col.startswith('samples_')]
        results_columns = [col for col in column_sums.index if col.startswith('results_')]

        # Define major columns
        major_columns_samples = ['samples_fbc', 'samples_cholera', 'samples_cd4', 'samples_malaria', 'samples_measles']
        major_columns_results = ['results_fbc', 'results_cholera', 'results_cd4', 'results_malaria', 'results_measles']

        # Calculate sums for non-major columns
        samples_other = column_sums[[col for col in samples_columns if col not in major_columns_samples]].sum()
        results_other = column_sums[[col for col in results_columns if col not in major_columns_results]].sum()

        # Create a new summary DataFrame
        summary_df = pd.DataFrame({
            'sample_other': [samples_other],
            'result_other': [results_other]
        })

        # Adding the sums of major columns, ensuring they exist in the DataFrame
        for col in major_columns_samples + major_columns_results:
            if col in column_sums:
                summary_df[col] = column_sums[col]
            else:
                summary_df[col] = 0  # If a major column doesn't exist, we assign a sum of 0
        
        
        #print(summary_df)
        summary_list = summary_df.to_dict(orient='records')

    
        msg = f"The filters successfully applied:  <br> <strong>Start Date: </strong>  {start_date} <br> <strong> End Date:</strong>   {end_date}  "
        messages.error(request, msg)  

        context = {
            'data': data,               
            'headers': headers,
            'msg': msg,
            'last_Uploaded_date':last_Uploaded_date,
            'summary_list':summary_list,
            'headersGF':headersGF,
            'headersWK':headersWK,
            'headersPEPFAR':headersPEPFAR,
        }
    
        # Check if the request is AJAX
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse(context)  # Return JSON data if it's an AJAX request
        else:
            return render(request, 'specimen_and_results.html', context)  # Else, render the template as usual
        
    else:
        
        results = pull_last_day_fxn_IST()   
        #The [SubmissionDate] is the second column in your query
        last_Uploaded_date = results[0][0] if results else None
        # start_date = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        # end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # start_date_r = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        # end_date_r = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        end_date = datetime.datetime.now()
        start_date = datetime.datetime.now() - timedelta(days=6)
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        # Generate month list
        # months_list = list_months(start_date_str, end_date_str)

        # # The result can be joined into a single string if needed
        # months_str = ','.join(months_list)
        # datelist = f"'{months_str}'"
        #print(datelist)
        datelist=None
        SQLqueries = ISTSQLQueries(datelist,start_date_str,end_date_str)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                    

        headersGF = []
        for result in data:
            if result['query_name'] == 'ist_GFgetspecimen_results':            
                headersGF = result['query_data'][0].keys() if result['query_data'] else []
                break 
            
        headersPEPFAR = []
        for result in data:
            if result['query_name'] == 'ist_PEPFARgetspecimen_results':            
                headersPEPFAR = result['query_data'][0].keys() if result['query_data'] else []
                break
            
        headersWK = []
        for result in data:
            if result['query_name'] == 'weekly_ist_transported':            
                headersWK = result['query_data'][0].keys() if result['query_data'] else []
                break    
                   
        headers = []
        for result in data:
            if result['query_name'] == 'ist_getspecimen_results':            
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break   
        
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'ist_other_samples_and_results':
                ist_other_samples_and_results_df = pd.DataFrame(result['query_data'])
                print(ist_other_samples_and_results_df.columns)
       
        # Check if the dataframe is empty or if expected columns are missing
        if 'ist_other_samples_and_results_df' not in locals() or ist_other_samples_and_results_df.empty:
            print("No data returned from the database.")
            ist_other_samples_and_results_df = pd.DataFrame({'other_sam': [0], 'other_res': [0]})
        else:
            # Check if the expected columns exist, if not add them
            if 'other_sam' not in ist_other_samples_and_results_df.columns:
                ist_other_samples_and_results_df['other_sam'] = 0
            if 'other_res' not in ist_other_samples_and_results_df.columns:
                ist_other_samples_and_results_df['other_res'] = 0
        
        
        # Define columns of interest excluding 'Province' as it doesn't need processing
        columns_of_interest = [
            'other_sam',
            'other_res',
        ]
        prefixes = ["samples", "results"]  # Define the prefixes for your new columns
        aggregated_data = []
        # Apply the function to each cell in the columns of interest
        for col, prefix in zip(columns_of_interest, prefixes):
            col_data = []
            for _, row_value in ist_other_samples_and_results_df[col].items():
                col_data.append(extract_and_assign(row_value, prefix))
            aggregated_data.append(pd.DataFrame(col_data))
        
            
        
        
            
        
        # Merge the aggregated data columns into the main dataframe
        for element_in_aggregated_data, prefix in zip(aggregated_data, prefixes):
            ist_other_samples_and_results_df = pd.concat([ist_other_samples_and_results_df, element_in_aggregated_data], axis=1)
        ist_other_samples_and_results_df.fillna(0, inplace=True)   
        
        #print(ist_other_samples_and_results_df.columns)
        # Rename duplicate columns to ensure uniqueness
        rename_duplicate_columns(ist_other_samples_and_results_df)

        # Identify non-numeric columns and convert them
        non_numeric_cols = ist_other_samples_and_results_df.select_dtypes(include=['object', 'string']).columns
        for col in non_numeric_cols:
            ist_other_samples_and_results_df[col] = pd.to_numeric(ist_other_samples_and_results_df[col], errors='coerce')

        # Proceed with combining 'samples_measels' and 'samples_measles', if they exist
        if 'samples_measels' in ist_other_samples_and_results_df.columns:
            if 'samples_measles' not in ist_other_samples_and_results_df.columns:
                ist_other_samples_and_results_df['samples_measles'] = 0
            ist_other_samples_and_results_df['samples_measles'] += ist_other_samples_and_results_df.pop('samples_measels')

        # After correcting the DataFrame, proceed with the summation
        column_sums = ist_other_samples_and_results_df.sum()

        # Prepare lists of column names for samples and results
        samples_columns = [col for col in column_sums.index if col.startswith('samples_')]
        results_columns = [col for col in column_sums.index if col.startswith('results_')]

        # Define major columns
        major_columns_samples = ['samples_fbc', 'samples_cholera', 'samples_cd4', 'samples_malaria', 'samples_measles']
        major_columns_results = ['results_fbc', 'results_cholera', 'results_cd4', 'results_malaria', 'results_measles']

        # Calculate sums for non-major columns
        samples_other = column_sums[[col for col in samples_columns if col not in major_columns_samples]].sum()
        results_other = column_sums[[col for col in results_columns if col not in major_columns_results]].sum()

        # Create a new summary DataFrame
        summary_df = pd.DataFrame({
            'sample_other': [samples_other],
            'result_other': [results_other]
        })

        # Adding the sums of major columns, ensuring they exist in the DataFrame
        for col in major_columns_samples + major_columns_results:
            if col in column_sums:
                summary_df[col] = column_sums[col]
            else:
                summary_df[col] = 0  # If a major column doesn't exist, we assign a sum of 0
        
        
        #print(summary_df)
        summary_list = summary_df.to_dict(orient='records')
       

        
        context = {
            'data': data,               
            'headers': headers,
            'headersGF': headersGF,
            'last_Uploaded_date':last_Uploaded_date,
            'summary_list':summary_list,
            'headersWK':headersWK,
            'headersPEPFAR':headersPEPFAR,
          
        }
    
        # Check if the request is AJAX
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse(context)  # Return JSON data if it's an AJAX request
        else:
            return render(request, 'specimen_and_results.html', context)  # Else, render the template as usual

    #--------------------------------------------------------------------------->


def overalTAT(request):
    if request.method == 'POST':
      
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')
        
        print(startdate)
        print(enddate)
        
        
        SQLqueries = TATSQLQueries(startdate, enddate)
        # Construct a unique cache key based on startdate and enddate
        cache_key = f"tat_data_{startdate}_{enddate}"        
        # Try to get data from cache first
        data = cache.get(cache_key)
        print(cache_key)
        
        # If data is not in cache, fetch from database
        if data is None:
            data = []
            for query in SQLqueries:
                try:
                    with connections['default'].cursor() as cursor:
                        print('start')
                        cursor.execute(query['sql'], query['parameters'])
                        print('end')
                        columns = [col[0] for col in cursor.description]
                        query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                        data.append({'query_name': query['query_name'], 'query_data': query_data})
                        print(data)
                except Exception as e:
                    print(f"An error occurred for {query['query_name']}: {e}")
                    
            # Store data in cache for next time. The timeout here is 1 hour (3600 seconds).
            cache.set(cache_key, data, 3600)
                
        headers = []
        for result in data:
            if result['query_name'] == 'ist_sample_tat_sql':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
            
        client_start_time = request.POST.get('startTime')
        if client_start_time:
            client_start_time = int(client_start_time) / 1000  # Convert from milliseconds to seconds
        else:
            client_start_time = time.time()  # Use the current time as a fallback, though this won't give the desired result

        server_end_time = time.time()
        time_taken_seconds = server_end_time - client_start_time
        time_taken_minutes = time_taken_seconds / 60  # Convert time to minutes
        
        print(time_taken_minutes)
        print(headers) 
        msg = f"The filters successfully applied: it takes  {time_taken_minutes:.2f} minutes. <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate}: "
        messages.error(request, msg)  

        context = {
            'data': data,               
            'headers': headers,
            'msg': msg,
        }
        print('we are now here')
        # Check if the request is AJAX
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse(context)  # Return JSON data if it's an AJAX request
        else:
            return render(request, 'overalTAT.html', context)  # Else, render the template as usual
    
    else:
        # Current date
        now = datetime.datetime.now()
        startdate = now.strftime('%Y-%m-%d')
        enddate = now.strftime('%Y-%m-%d')
        
      
                
        SQLqueries = TATSQLQueries(startdate, enddate)
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
                
        headers = []
        for result in data:
            if result['query_name'] == 'ist_sample_tat_sql':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break           

        context = {
            'data': data,               
            'headers': headers,           
        }
        return render(request, 'overalTAT.html', context)

    
    
    
    # Province Level TAT Provider
    
def provincelevelTAT(request):
    if request.method == 'POST':
          
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')
        
        print(startdate)
        
        
        SQLqueries = TATSQLQueries(startdate,enddate)
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
     
            
        headers_IST_TAT_FACILITY_SAMPLE_TYPE_province = []
        for result in data:
            if result['query_name'] == 'IST_TAT_FACILITY_SAMPLE_TYPE_province':
                headers_IST_TAT_FACILITY_SAMPLE_TYPE_province = result['query_data'][0].keys() if result['query_data'] else []
                break  

      
        client_start_time = request.POST.get('startTime')
        if client_start_time:
            client_start_time = int(client_start_time) / 1000  # Convert from milliseconds to seconds
        else:
            client_start_time = time.time()  # Use the current time as a fallback, though this won't give the desired result

        server_end_time = time.time()

        time_taken_seconds = server_end_time - client_start_time
        time_taken_minutes = time_taken_seconds / 60  # Convert time to minutes
        
        print(time_taken_minutes)
        
        msg = f"The filters successfully applied {time_taken_minutes:.2f} minutes. <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate}: "
        messages.error(request, msg)  
        context = {
                'data': data,        
                 'msg': msg,
                'headers_IST_TAT_FACILITY_SAMPLE_TYPE_province':headers_IST_TAT_FACILITY_SAMPLE_TYPE_province,
  
            }
            
      
        return render(request, 'provincelevelTAT.html', context)
    
    else:
        # Current date
# Current date
        now = datetime.datetime.now()
        # Calculate the start of the month 3 months ago
        #start_time = (now - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Format the dates
        #startdate = start_time.strftime('%Y-%m-%d')
        startdate = now.strftime('%Y-%m-%d')
        enddate = now.strftime('%Y-%m-%d')
        print(startdate)
        print(enddate)
                
        SQLqueries = TATSQLQueries(startdate,enddate)
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
  
        headers_IST_TAT_FACILITY_SAMPLE_TYPE_province = []
        for result in data:
            if result['query_name'] == 'IST_TAT_FACILITY_SAMPLE_TYPE_province':
                headers_IST_TAT_FACILITY_SAMPLE_TYPE_province = result['query_data'][0].keys() if result['query_data'] else []
                break  

        context = {
                'data': data,               
                'headers_IST_TAT_FACILITY_SAMPLE_TYPE_province':headers_IST_TAT_FACILITY_SAMPLE_TYPE_province,
       
                
            }
        return render(request, 'provincelevelTAT.html', context)
    
    
    
        
    
    
    # Return the data as a JSON response
    #return JsonResponse({'data': data})

    

#==============LOGGING============SYSTEM============



def login_user(request):
    if request.method == "POST":
        username = request.POST['username']
        password = request.POST['password']
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            if user.is_superuser:  # Check if the user is an admin
                return redirect('BASE')
            elif user.groups.filter(name='sie_role').exists():  # Check if the user is in the "sie_role" group
                return redirect('index_SIE')
            else:
                # Add a default redirection or return an error message here if needed
                messages.error(request, ("Username or Password incorrect"))
                return redirect('login')  
        else:
            messages.error(request, ("Username or Password incorrect"))
            return redirect('login')  # Redirect to the login page if authentication failed

    else:
        return render(request, 'authenticate/login.html', {})




def logout_user(request):
	logout(request)
	
	return redirect('login')


def register_user(request):
	if request.method == "POST":
		form = RegisterUserForm(request.POST)
		if form.is_valid():
			form.save()
			username = form.cleaned_data['username']
			password = form.cleaned_data['password1']
			user = authenticate(username=username, password=password)
			login(request, user)
			messages.success(request, ("Registration Successful!"))
			return redirect('home')
	else:
		form = RegisterUserForm()

	return render(request, 'authenticate/register_user.html', {
		'form':form,
		})





#=========================END IST SECTION=======================================




  
  

                  
def get_default_data():
    # Retrieve default data from the database and return it as a list of dictionaries
    data = []
    for query in SQLqueries:           
        try:
            with connections['default'].cursor() as cursor:
                cursor.execute(query['sql'])
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': 'Default Data', 'query_data': query_data})
        except Exception as e:
            print(f"An error occurred while retrieving default data: {e}")
            return []  # Return an empty list when there's an error
    return data


def get_filtered_data(start_date, end_date):
    
    # Retrieve filtered data from the database based on start date and end date, and return it as a list of dictionaries
    data = []
    for query in SQLqueries:        
        try:
            with connections['default'].cursor() as cursor:
                # Pass the start_date and end_date values to the execute() method
                cursor.execute(query['sql'], [start_date, end_date])
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': 'Filtered Data', 'query_data': query_data})
        except Exception as e:
            print(f"An error occurred while retrieving filtered data: {e}")
    return data 
    







def download_csv(request):
    # Check if the file exists
    if not os.path.exists('rtcqm.csv'):
        return HttpResponse("No data available", status=404)

    # Read DataFrame from CSV
    with open('rtcqm.csv', 'r', encoding='utf-8') as file:
        data = file.read()

    response = HttpResponse(data, content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="rtcqm.csv"'

    return response



def handle_uploaded_file_Tools(file, username):
    now = datetime.datetime.now()

    file_name = f"{os.path.splitext(file.name)[0]}_{now.strftime('%Y%m%d')}{os.path.splitext(file.name)[1]}"
    # creating the folders if they don't exist
    save_path = os.path.join(settings.MEDIA_ROOT, 'MandE_Tools')
    os.makedirs(save_path, exist_ok=True)

    # saving the file
    full_file_path = os.path.join(save_path, file_name)
    with default_storage.open(full_file_path, 'wb+') as destination:
        for chunk in file.chunks():
            destination.write(chunk)



@login_required
def adminMETools_view(request):
    
    df = None
    read_response = None
    data_frames = {}
    user = request.user  # Get the authenticated user
    if request.method == "GET":
        return render(request, 'MandE_tool.html',)
    elif request.method == 'POST':
        files = request.FILES.getlist('files[]', None)
        #print(files)
        for file in files:
           # print(file)handle_uploaded_file
            try:
                if os.path.splitext(file.name)[1].lower() in ['.doc', '.docx', '.pdf', '.csv', '.xls', '.xlsx', '.xlsm','.pptx']:
                    handle_uploaded_file_Tools(file,user)
                    msg = 'Sucessfully uploaded file(s)'
                    messages.error(request, msg)
             
                else:
                    
                    msg = 'The file format is not permited by site admin'
                    messages.error(request, msg)

            except Exception as e:
                return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{str(e)}</div>'})

        # Return JsonResponse after processing all files
        return JsonResponse({'msg': '<div class="alert alert-success" role="alert">Files successfully uploadedxxx</div>'})
    else:
        return render(request, 'MandE_tool.html',)
  
    



from django.http import HttpResponse, HttpResponseRedirect
from django.urls import reverse
import os

def delete_file(request, file_path):
    if not file_path:
        # Handle empty file_path
        return HttpResponse("File path is empty!")

    # Assuming you have the base directory where files are stored
    base_dir = settings.MEDIA_ROOT
    full_path = os.path.join(base_dir, file_path)

    if os.path.exists(full_path) and os.path.isfile(full_path):
        os.remove(full_path)
        # Redirect to a success page or back to the main page
        return HttpResponseRedirect(reverse('explore_Admin'))
    else:
        return HttpResponse("File not found!")


def delete_folder(request, folder_path):
    if not folder_path:
        # Handle empty folder_path
        return HttpResponse("Folder path is empty!")

    base_dir = settings.MEDIA_ROOT
    full_path = os.path.join(base_dir, folder_path)

    if os.path.exists(full_path) and os.path.isdir(full_path):
        # Delete contents of the directory without removing the directory itself
        for root, dirs, files in os.walk(full_path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        
        # Redirect to a success page or back to the main page
        return HttpResponseRedirect(reverse('explore_Admin'))
    else:
        return HttpResponse("Folder not found!")




def cli_view(request):
  
    return render(request, 'cli.html')




@csrf_exempt
def verify_admin_password(request):
    # Check for POST method
    if request.method == "POST":
        password = request.POST['password']

        # print(password)  # print the password for debugging
        admin_password = "brtimne"
        
        if password == admin_password:
            return JsonResponse({'is_valid': True})
        else:
            return JsonResponse({'is_valid': False})
    return JsonResponse({'error': 'Invalid request'})





# @ensure_csrf_cookie
# def delete_weekly_dashboard(request):
#     if request.method == 'POST':
#         try:
#             unique_key = request.POST.get('unique_key')
#             print (unique_key)

#             if unique_key:
#                 # Your SQL queries here
#                 queries = [
#                     # "DELETE FROM [LSS].[dbo].[Dash_This_week_Rec_Samples] WHERE unique_key=%s",
#                     # "DELETE FROM [LSS].[dbo].Dash_Testing_Capacity WHERE unique_key=%s",
#                     # ... add the rest of your DELETE queries
#                 ]

#                 with connections['default'].cursor() as cursor:
#                     for query in queries:
#                         cursor.execute(query, [unique_key])
                
#                 msg = "Data deleted successfully."            
#                 return JsonResponse({'message': msg})
#             else:
#                 msg = "Invalid request data." 
#                 return JsonResponse({'error': msg})
#         except Exception as e:
#             return JsonResponse({'error': str(e)}, status=500)
#     else:
#         return JsonResponse({'error': 'Invalid request method'}, status=405)







def rtcql_view(request):
  
    return render(request, 'login.html')


# Downloading the data from the Query set
def download_query_output(request):

    startdate = request.GET.get('startdate', '2022-01-01')
    enddate = request.GET.get('enddate', datetime.datetime.now().strftime('%Y-%m-%d'))

    selected_provinces = request.GET.get('province') 
    if selected_provinces:
        selected_provinces = tuple(selected_provinces.split(',')) 

    dsd_checked = request.GET.get('dsd', 'true')
    ta_sdi_checked = request.GET.get('ta_sdi', 'true')

    pepfar_support = ()
    if (dsd_checked == 'true' and ta_sdi_checked == 'false'):
        pepfar_support = ('DSD',)
    elif (ta_sdi_checked == 'true' and dsd_checked == 'false'):
        pepfar_support = ('TA-SDI',)
    elif (ta_sdi_checked == 'true' and dsd_checked == 'true'):
        pepfar_support = ('DSD', 'TA-SDI')

    queries = istqueries(startdate, enddate, selected_provinces, pepfar_support)

    data = []
    for query in queries:
        if query['query_name'] == 'Query 1':
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")

    if data:
        df = pd.DataFrame(data)
        csv = df.to_csv(index=False)
       
    else:
        csv = "No data available"

    response = HttpResponse(csv, content_type='text/csv')
    filename = "query_output_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".csv"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response







def admin_manage(request):
    users_with_groups = []
    for user in User.objects.all():
        groups = ', '.join([group.name for group in user.groups.all()])
        users_with_groups.append({
            'username': user.username,
            'groups': groups
        })
    if 'button' in request.POST:
            
        if request.method == 'POST':
            buttonname = request.POST.get('button')
            lab = request.POST.get('lab')
            #print(buttonname)
            
            if buttonname=='pullDashboards':         
                # startdate = request.POST.get('startdate')
                # enddate = request.POST.get('enddate')
                startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
                enddate = datetime.datetime.now().strftime('%Y-%m-%d')
                
                selected_provinces=None
                pepfar_support=None
                tab_state1='false'
                tab_state2= 'True'
                tab_state3='False'
                #.print(lab)
        

                    
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
                        
                        
                print(startdate, enddate, lab)     
                SQLi = weeklyDashboardPull(startdate, enddate, lab)
                data_dash = []
                for query in SQLi:
                    try:
                        with connections['default'].cursor() as cursor:
                            cursor.execute(query['sql'], query['parameters'])
                            columns = [col[0] for col in cursor.description]
                            query_data_dash = [dict(zip(columns, row)) for row in cursor.fetchall()]
                            data_dash.append({'query_name_dash': query['query_name'], 'query_data_dash': query_data_dash})
                    except Exception as e:
                        print(f"An error occurred for {query['query_name']}: {e}")
                        
                
                # msg = f"The filters successfully applied on the Page <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate}  <br> <strong> Lab: </strong> {lab}"
                # messages.error(request, msg)
                        
                
                # msg = f"The filters successfully applied on the Page and  Charts: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} "
                # messages.error(request, msg)
                msg=f"{lab} Data   successfully loaded"
                return JsonResponse({
                                'data': data,
                                'data_dash':data_dash,
                                "message": msg,
                                'tab_state1': tab_state1,
                                'tab_state2': tab_state2,
                                'tab_state3': tab_state3,
                                'users_with_groups': users_with_groups,
                            })
                    
            # context = {
            #         'data': data,
            #         'msg': msg,   
            #         'tab_state1':tab_state1,
            #         'tab_state2':tab_state2,
            #         'tab_state3':tab_state3,                           
            #     }

            # return render(request, 'admin_manage.html', context)
        
        # Add this to handle other POST requests
    
        return HttpResponse("Invalid POST request or button not handled.")
        
       

    else:
        # Calculate the start and end dates based on the current date
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        selected_provinces=None
        pepfar_support=None
        lab = request.POST.get('lab')
        tab_state1='true'
        tab_state2= 'false'
        tab_state3='False'

        # print(startdate)
        # print(enddate)
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
                
                
        SQLi = weeklyDashboardPull(startdate, enddate, lab)
        data_dash = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data_dash = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data_dash.append({'query_name_dash': query['query_name'], 'query_data_dash': query_data_dash})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                
        
 
                        
        
        context = {
       
            'data': data,
            'tab_state1':tab_state1,
            'tab_state2':tab_state2,
            'tab_state3':tab_state3,
            'users_with_groups': users_with_groups,
        }    
        return render(request, 'admin_manage.html',context)
    
    
    
    #--------------------------------views to save/UPDATE the dashboard --------------------------------

def update_dashboard_indicators(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    print(row)
                    print(row_idx)
                    # Get the query and parameters from the function
                    query, params = get_update_query_and_params(row, row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
      
        return JsonResponse({"status": "error", "message": str(e)})
    
    
    

def update_dashboard_carryover_sample(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = get_update_carryoversamples_and_params(row, row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
      
        return JsonResponse({"status": "error", "message": str(e)})
    
 #-----------------------------------------------------------------------------   
 
def update_dashboard_referred_samples(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_referred_samples(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
        
        return JsonResponse({"status": "error", "message": str(e)}) 
    #---------------------------------------------------------------------

def update_dashboard_sample_run(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_sample_run(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
        
        return JsonResponse({"status": "error", "message": str(e)}) 
    #---------------------------------------------------------------------
    
    
def update_dashboard_testing_capasity(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_testing_capacity(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
        
        return JsonResponse({"status": "error", "message": str(e)}) 
    #---------------------------------------------------------------------

    
def update_dashboard_operational_matrix(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_operational_matrix(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
        
        return JsonResponse({"status": "error", "message": str(e)})

#-------------------------------------------------------------------------


   
def update_dashboard_lims(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_lims(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
        
        return JsonResponse({"status": "error", "message": str(e)})

#-------------------------------------------------------------------------


   
def update_dashboard_Specimen_Transport(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_specimentransport(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
        
        return JsonResponse({"status": "error", "message": str(e)})
    
    
  
def update_dashboard_CLI(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_CLI(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})
        
    except Exception as e:
        
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()

        error_messages = f"An error occurred :\n\nType: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
        return JsonResponse({"status": "error", "message": error_messages})


   
   
def update_dashboard_power_outage(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)

    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = update_savingdashboard_power_outage(row,row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
        
        return JsonResponse({"status": "error", "message": str(e)})


def update_dashboard_thisweek_sample(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Invalid method."})

    table_data = json.loads(request.POST.get('tableData'))
    #print(table_data)
    
    # Explicitly get the 'default' database connection
    connection_default = connections['default']

    try:
        with transaction.atomic(using='default'):  # Specify the database for the transaction
            with connection_default.cursor() as cursor:
                for row_idx, row in enumerate(table_data):
                    #print(row)
                    #print(row_idx)
                    # Get the query and parameters from the function
                    query, params = get_update_thisweek_and_params(row, row_idx)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

        return JsonResponse({"status": "success", "message": "Data Saved successfully!"})

    except Exception as e:
        # Logging the exception can be helpful for debugging
      
        return JsonResponse({"status": "error", "message": str(e)})
    
    
#---------------------------------------------------------------->



@csrf_exempt
def delete_Dashboard_Entry(request):
    if request.method == 'POST':
        unique_key = request.POST.get('unique_key')
        print(unique_key)

        # Explicitly get the 'default' database connection
        connection_default = connections['default']

        try:
            with transaction.atomic(using='default'):  # Specify the database for the transaction
                with connection_default.cursor() as cursor:
                    # Get the query and parameters from the function
                    query, params = delete_dashboard_entry(unique_key)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

            return JsonResponse({"status": "success", "message": "Dashboard Successfully Deleted"})

        except Exception as e:
            # Handle exceptions and errors as necessary. 
            # It's advisable to log the actual error for debugging while sending a generic message to the client.
            return JsonResponse({"status": "error", "message": str(e)}) 

    return JsonResponse({'status': 'error', 'message': 'Invalid request method.'})



@csrf_exempt
def delete_ist_Entry(request):
    if request.method == 'POST':
        unique_key = request.POST.get('unique_key')
        print(unique_key)

        # Explicitly get the 'default' database connection
        connection_default = connections['default']

        try:
            with transaction.atomic(using='default'):  # Specify the database for the transaction
                with connection_default.cursor() as cursor:
                    # Get the query and parameters from the function
                    query, params = delete_IST_Entry(unique_key)
                    
                    # Execute the query with the parameters
                    cursor.execute(query, params)

            return JsonResponse({"status": "success", "message": "IST Excel Records Successfully Deleted"})

        except Exception as e:
            # Handle exceptions and errors as necessary. 
            # It's advisable to log the actual error for debugging while sending a generic message to the client.
            return JsonResponse({"status": "error", "message": str(e)}) 

    return JsonResponse({'status': 'error', 'message': 'Invalid request method.'})





def pull_last_day_fxn():
    query, params = pull_last_day()
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()    
    
    return results


def pull_last_day_fxn_IST():
    query, params = pull_last_day_ist()
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()    
    
    return results


def parse_date(date_string):
    try:
        # Try parsing with fractions of a second
        return datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        # If that fails, try parsing without fractions of a second
        return datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')


def odk_specimen_and_results_sync(request):
    if request.method == 'POST':
        
        results = pull_last_day_fxn()   
        #The [SubmissionDate] is the second column in your query
        submission_date = results[0][1] if results else None
               
     
        # Details for accessing the SurveyCTO API.
        form_config = [
            {
                'form_id': 'ist11092020',
                'servername': 'cch',
                'username': 'tdadirai@brti.co.zw',
                'password': 'Adm!n123',
                'initial_date': 'Oct%2001%2C%202022%2000%3A00%3A00%20AM'   #Oct 01 2022
            }
        ]

        form = form_config[0]

      

        if submission_date:
                # Trim the string if it ends with "0"
            if submission_date.endswith("0"):
                submission_date = submission_date[:-1]

            # Convert the string date to a datetime object
            try:
                
                # Try parsing the string with the format that includes microseconds
                submission_date_obj = datetime.datetime.strptime(submission_date, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                # If the above fails, it means the datetime string did not have microseconds.
                # Try parsing it with a format string that doesn't expect microseconds.
                submission_date_obj = datetime.datetime.strptime(submission_date, '%Y-%m-%d %H:%M:%S')


            # Add a day to the last submission date
            #new_date_obj = submission_date_obj + datetime.timedelta(days=1)
            
            # Format the date to the desired format
            date_str = submission_date_obj.strftime("%b %d, %Y %H:%M:%S %p")
            
            # URL-encode the date string to match the API format
            encoded_date_str = urllib.parse.quote(date_str)
            
            # Update form's initial_date
            form_config[0]['initial_date'] = encoded_date_str

            print(form['initial_date'] )
        
        print(form['initial_date'] )
        # Now construct the URL with the appropriate date range.
        url = construct_url(form['form_id'], form['servername'], form['initial_date'])
        response = pull_data(url, form['username'], form['password'])
        data = response.json()

        # Process the data
        df = pd.DataFrame(data)
        df = sanitize_data(df)
        #df.to_csv('tosee.csv')
        df['SubmissionDate'] = pd.to_datetime(df['SubmissionDate'])
        df['today'] = pd.to_datetime(df['today'])
        df['month-year'] = df['today'].dt.to_period('M')
        df['month-year'] = df['month-year'].astype(str)
        latest_date = pd.to_datetime(df['SubmissionDate']).max()

        # df_grouped = df.groupby(['prov', 'dis', 'month-year'])['rider'].nunique().reset_index(name='count')
        # df_pivot = df_grouped.pivot_table(index=['prov', 'dis'], columns='month-year', values='count', aggfunc='sum')
        df = df.fillna(0)
        response= loading_ODK_to_Dbase(df)
        print(response)
       
     
        
        
        return JsonResponse({"status": "success", "msg": response,"latest_date":latest_date})  
     




def odk_fxn_view(request):
    
    if request.method == 'POST':
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')
        selected_provinces = request.POST.getlist('province') 
        selected_provinces = tuple(selected_provinces)  # Convert list to tuple
        
        print(startdate)   
        print(enddate)
        print(selected_provinces)
        user = request.user  
        lab=user.username
      
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
    
        
        SQLi = odkScripts_province_district(startdate, enddate,selected_provinces)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
                
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_fxn':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
        msg = f"The filters successfully applied: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} <br> <strong>Provinces :</strong> {selected_provinces} "
        messages.error(request, msg)                
        context = {
            'data': data,
            'submission_date':submission_date,
            'headers':headers,
    
                }
        
        return render(request, 'odk_fxn.html', context)

        
        
        
    else:
         
        user = request.user  
        lab=user.username
        # Calculate the start and end dates based on the current date
        startdate = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        # startdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
        
        selected_provinces=('Manicaland Province')
      
    
        
        SQLi = odkScripts_province_district(startdate, enddate,selected_provinces)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
                
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_fxn':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
                        
        context = {
            'data': data,
            'submission_date':submission_date,
            'headers':headers,
    
                }
        
        return render(request, 'odk_fxn.html', context)

#-----------------------FOR THE SIEs-----------------------------------------


    
    
def ridersgranular (request):
    if request.method == 'POST':
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')  
        selected_provinces = request.POST.getlist('province') 
        selected_provinces = tuple(selected_provinces)  # Convert list to tuple
        
        print (selected_provinces)
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
             
        SQLi = odkScripts_granular(startdate, enddate,selected_provinces)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_granular':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
            
        msg = f"The filters successfully applied: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} <br> <strong>Provinces :</strong> {selected_provinces}  "
        messages.error(request, msg)   
            
        context = {
                'data': data,
                'headers':headers,
                'submission_date':submission_date,
                #'chart_data': formatted_chart_data,  # Add this to the context
                    }
        return render(request, 'ridersgranular.html', context) 
    
    else:

        user = request.user  

        # Calculate the start and end dates based on the current date
        startdate = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        # startdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        selected_provinces=('Bulawayo Province','Harare Province','Manicaland Province','Mashonaland Central Province','Mashonaland East Province','Mashonaland West Province','Masvingo Province','Matebeleland North Province','Matebeleland South Province','Midlands Province')
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
              
        SQLi = odkScripts_granular(startdate, enddate,selected_provinces)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
                
        
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_granular':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
  
        context = {
                'data': data,
                'headers':headers,
                'submission_date':submission_date,
                #'chart_data': formatted_chart_data,  # Add this to the context
                    }
        return render(request, 'ridersgranular.html', context)  
         


#-------------------------------------------------------------------
    
    
def odk_summaries (request):
    if request.method == 'POST':
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')  
        selected_provinces = request.POST.getlist('province') 
        selected_provinces = tuple(selected_provinces)  # Convert list to tuple
        
        print (selected_provinces)
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
             
        SQLi = odkScripts_province_totals(startdate, enddate,selected_provinces)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_totals':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
            
        msg = f"The filters successfully applied: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} <br> <strong>Provinces :</strong> {selected_provinces}  "
        messages.error(request, msg)   
            
        context = {
                'data': data,
                'headers':headers,
                'submission_date':submission_date,
                #'chart_data': formatted_chart_data,  # Add this to the context
                    }
        return render(request, 'ODK_totals.html', context) 
    
    else:

        user = request.user  

        # Calculate the start and end dates based on the current date
        startdate = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        # startdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        selected_provinces=('Bulawayo Province','Harare Province','Manicaland Province','Mashonaland Central Province','Mashonaland East Province','Mashonaland West Province','Masvingo Province','Matebeleland North Province','Matebeleland South Province','Midlands Province')
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
          
        #print(startdate, enddate, selected_provinces)    
        SQLi = odkScripts_province_totals(startdate, enddate,selected_provinces)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
                
        
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_totals':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
  
        context = {
                'data': data,
                'headers':headers,
                'submission_date':submission_date,
                #'chart_data': formatted_chart_data,  # Add this to the context
                    }
        return render(request, 'ODK_totals.html', context)  



#---------------------------End of SIE ODK-------------------------------------


def format_data_for_apex_chart(data):
    # Get the list of categories (months) from the first item's keys, excluding 'Province'
    categories = [key for key in data[0]['query_data'][0].keys() if key != 'Province']

    # Construct the series data
    series = []
    for province_data in data[0]['query_data']:
        province_name = province_data['Province']
        province_values = [province_data[month] if month in province_data else 0 for month in categories]
        series.append({
            'name': province_name,
            'data': province_values
        })

    return {
        'categories': categories,
        'series': series
    }





def fetch_province_data(request):
    province_name = request.GET.get('province', None)
    startdate = request.GET.get('startdate', None)
    enddate = request.GET.get('enddate', None)

    # Validation for the required parameters
    if not province_name or not startdate or not enddate:
        return JsonResponse({'error': 'Missing required parameters'}, status=400)
    
    SQLi = odkScripts_province_detailed(province_name, startdate, enddate)
    
    data = []
    for query in SQLi:
        try:
            with connections['default'].cursor() as cursor:
                cursor.execute(query['sql'], query['parameters'])
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': query['query_name'], 'query_data': query_data})
        except Exception as e:
            return JsonResponse({'error': f"An error occurred for {query['query_name']}: {e}"}, status=500)
    
    # Extracting the relevant data for the response.
    # Assuming you want the 'query_data' of the first query.
    values_list = data[0]['query_data'] if data else []
    
    return JsonResponse({'data': values_list})

def odkScripts_province_detailed(province_name, startdate, enddate):
    parameters = (province_name, startdate, enddate)

    SQLqueries = [
        {
            'query_name': 'odk_province_granular',
            'sql': """
                   select * from ODK_Specimen_and_Results where prov = %s and cast(today as date) between %s and %s
                   """,
            'parameters': parameters
        },
    ]
    return SQLqueries


def odk_fxnProvince(request):
    
    if request.method == 'POST':
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')
        
        print(startdate)   
        print(enddate)
        user = request.user  
        lab=user.username
      
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
    
        
        SQLi = odkScripts_province(startdate, enddate)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
                
        # Format the data for the Apex chart
        formatted_chart_data = format_data_for_apex_chart(data)
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_fxn_provincial':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
        msg = f"The filters successfully applied: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} "
        messages.error(request, msg)                
        context = {
            'data': data,
            'submission_date':submission_date,
            'headers':headers,
            'chart_data': formatted_chart_data,  # Add this to the context
    
                }
        
        return render(request, 'odk_fxn_provincial.html', context)

        
        
        
    else:
         
        user = request.user  
        lab=user.username
        # Calculate the start and end dates based on the current date
        startdate = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        # startdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        
        results = pull_last_day_fxn()  
        submission_date = results[0][1] if results else None
    
        
        SQLi = odkScripts_province(startdate, enddate)
        data = []
        for query in SQLi:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}") 
                
                # Format the data for the Apex chart
        formatted_chart_data = format_data_for_apex_chart(data)
        
        
        headers = []
        for result in data:
            if result['query_name'] == 'odk_bike_fxn_provincial':
                headers = result['query_data'][0].keys() if result['query_data'] else []
                break
        
                        
        context = {
            'data': data,
            'submission_date':submission_date,
            'headers':headers,
            'chart_data': formatted_chart_data,  # Add this to the context
    
                }
        
        return render(request, 'odk_fxn_provincial.html', context)




def download_folder(request, folder_path):
    # Create a unique temporary zip file
    temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
    zip_file_name = temp_zip.name

    # Zip the folder
    shutil.make_archive(zip_file_name.replace('.zip', ''), 'zip', folder_path)

    # Serve the file
    response = FileResponse(open(zip_file_name, 'rb'))
    response['Content-Type'] = 'application/zip'
    response['Content-Disposition'] = f'attachment; filename="{os.path.basename(folder_path)}.zip"'

    # Attach a callback to delete the file after sending the response
    def delete_file(response):
        os.remove(zip_file_name)
    
    response.closed = delete_file

    return response




def sample_rejections(request):
    if request.method == 'POST':
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')  
        # Get today's date
        current_date = datetime.datetime.now().date()
        # Get the start date as the first day of the month 3 months ago
        startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
        # Convert dates to string format
        
        start_date_r = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"
    
        
        SQLqueries = ISTSpecimenQueries(startdate, enddate)
        print(startdate, enddate)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                    
                
        msg = f"The filters successfully applied on the Page and  Charts: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} "
        messages.error(request, msg)    
                
        headers_vl_rejected = []
        for result in data:
            if result['query_name'] == 'specimen_rejection_weekly':
                headers_vl_rejected = result['query_data'][0].keys() if result['query_data'] else []
                break
        
        context = {    
            'data': data,
            'headers_vl_rejected':headers_vl_rejected,
            'msg':msg,
        }
        return render(request, 'sample_rejected.html', context)
    else:
        
        # Get today's date
        current_date = datetime.datetime.now().date()
        # Get the start date as the first day of the month 3 months ago
        startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
        # Convert dates to string format
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date_r = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"
    
        
        SQLqueries = ISTSpecimenQueries(startdate, enddate)
        print(startdate, enddate)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")
                    
                
            
                
        headers_vl_rejected = []
        for result in data:
            if result['query_name'] == 'specimen_rejection_weekly':
                headers_vl_rejected = result['query_data'][0].keys() if result['query_data'] else []
                break
        
        context = {    
            'data': data,
            'headers_vl_rejected':headers_vl_rejected,
        }
        return render(request, 'sample_rejected.html', context)




###----------------- generating the Weekly report-----------

def Home_generate_excel(request):
        
     # Get today's date
    current_date = datetime.datetime.now().date()
    # Get the start date as the first day of the month 3 months ago
    startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
    # Convert dates to string format
    startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
    enddate = datetime.datetime.now().strftime('%Y-%m-%d')
    start_date_r = datetime.datetime.strptime(startdate, '%Y-%m-%d')
    end_date_r = datetime.datetime.strptime(enddate, '%Y-%m-%d')
    
    # Generate month list
    months_list = list_months(start_date_r, end_date_r)

    # The result can be joined into a single string if needed
    months_str = ','.join(months_list)
    datelist = f"'{months_str}'"


    SQLqueries = cdcweeklyreport(startdate, enddate)
    print(startdate, enddate)
    
    data = []
    for query in SQLqueries:
        try:
            with connections['default'].cursor() as cursor:
                #print('start')
                cursor.execute(query['sql'], query['parameters'])
                #print('end')
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                data.append({'query_name': query['query_name'], 'query_data': query_data})
                #print(data)
        except Exception as e:
            print(f"An error occurred for {query['query_name']}: {e}")
            
            
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().strftime('%B')  # Month as full name

    directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")

    file_list = []
    file_details = []  # List to store file details including modified date
    if os.path.exists(directory_path):
        
        for file in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file)
            if os.path.isfile(file_path):
                modified_time = os.path.getmtime(file_path)
                date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                file_details.append({'name': file, 'date_modified': date_modified})
            else:
                file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
        file_list = [file['name'] for file in file_details]
        
    # Sort the file details by 'date_modified', handling None values appropriately
    file_details.sort(key=lambda x: x['date_modified'] or '', reverse=True)
    
    IST_SQLqueries = ISTMOdelSQLQueries(start_date_r,end_date_r) 
    
    IST_data = []
    for query in IST_SQLqueries:
        try:
            with connections['default'].cursor() as cursor:
    
                cursor.execute(query['sql'], query['parameters'])
            
                columns = [col[0] for col in cursor.description]
                query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                IST_data.append({'query_name': query['query_name'], 'query_data': query_data})
                
        except Exception as e:
            print(f"An error occurred for {query['query_name']}: {e}")


    IST_directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")
    IST_file_list = []
    IST_file_details = []  # List to store file details including modified date
    if os.path.exists(IST_directory_path):
        
        for file in os.listdir(IST_directory_path):
            file_path = os.path.join(IST_directory_path, file)
            if os.path.isfile(file_path):
                modified_time = os.path.getmtime(file_path)
                date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                IST_file_details.append({'name': file, 'date_modified': date_modified})
            else:
                IST_file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
        IST_file_list = [file['name'] for file in IST_file_details]
 


        
    narr_directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")
    narr_file_list = []
    narr_file_details = []  # List to store file details including modified date
    if os.path.exists(narr_directory_path):
        
        for file in os.listdir(narr_directory_path):
            file_path = os.path.join(narr_directory_path, file)
            if os.path.isfile(file_path):
                modified_time = os.path.getmtime(file_path)
                date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                narr_file_details.append({'name': file, 'date_modified': date_modified})
            else:
                narr_file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
        narr_file_list = [file['name'] for file in narr_file_details]
        
           
        

   

    context = {
            'data': data, 
            'file_list': file_list,
            'file_details': file_details,
            'directory_path': directory_path,
            'current_year':current_year,
            'current_month':current_month, 
            
            'IST_data': IST_data,      
            'IST_excel_file_path': request.session.get('IST_excel_file_path'), 
            'IST_file_list': IST_file_list,
            'IST_file_details': IST_file_details,
            'IST_directory_path': IST_directory_path,  
            
            
             'narr_file_list': narr_file_list,
            'narr_file_details': narr_file_details,
            'narr_directory_path': narr_directory_path,       
             
                }

    # Render the template with the file name in the context
    return render(request, 'Generate_Weekly.html',context )



    

# View for handling the Excel file creation and download
def excel_file_handler(request):

    if request.method == 'POST':
               
        # Get today's date
        current_date = datetime.datetime.now().date()
        # Get the start date as the first day of the month 3 months ago
        startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
        # Convert dates to string format
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date_r = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"


        SQLqueries = cdcweeklyreport(startdate, enddate)
        #print(startdate, enddate)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")

        #print('the issues are detected ------------')

        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'vlreceived_model':
                df = pd.DataFrame(result['query_data'])
        if len(df)>0:
            df['Lab'] = df['Lab'].replace('Vic Falls', 'Victoria Falls')
            df['Lab'] = df['Lab'].replace('Masvingo', 'Masvingo ')

                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'EIDreceived_model':
                df_eid_rec = pd.DataFrame(result['query_data']) 
        if len(df_eid_rec)>0:
            df_eid_rec['Lab'] = df_eid_rec['Lab'].replace('Vic Falls', 'Victoria Falls')
            df_eid_rec['Lab'] = df_eid_rec['Lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'Downtime_model':
                df_downtime = pd.DataFrame(result['query_data'])
        if len(df_downtime)>0:           
            df_downtime['name_of_lab'] = df_downtime['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_downtime['name_of_lab'] = df_downtime['name_of_lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'Referal_reasons_model':
                df_ref_reasons = pd.DataFrame(result['query_data']) 
        if len(df_ref_reasons)>0: 
            df_ref_reasons['lab'] = df_ref_reasons['lab'].replace('Vic Falls', 'Victoria Falls')
            df_ref_reasons['lab'] = df_ref_reasons['lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'Referred_model':
                df_referred = pd.DataFrame(result['query_data'])  
        if len(df_referred)>0:         
            df_referred['name_of_lab'] = df_referred['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_referred['name_of_lab'] = df_referred['name_of_lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'lab_dispatch_model':
                df_dispatch = pd.DataFrame(result['query_data'])
        if len(df_dispatch)>0:  
            df_dispatch['lab'] = df_dispatch['lab'].replace('Vic Falls', 'Victoria Falls')
            df_dispatch['lab'] = df_dispatch['lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'sample_runVL_model':
                df_VL_run = pd.DataFrame(result['query_data']) 
        if len(df_VL_run)>0:
            
            df_VL_run['name_of_lab'] = df_VL_run['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_VL_run['name_of_lab'] = df_VL_run['name_of_lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'sample_runEID_model':
                df_EID_run = pd.DataFrame(result['query_data']) 
        if len(df_EID_run)>0:
            
            df_EID_run['name_of_lab'] = df_EID_run['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_EID_run['name_of_lab'] = df_EID_run['name_of_lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'VL_run_reasons_model':
                df_vl_run_reasons = pd.DataFrame(result['query_data']) 
        if len(df_vl_run_reasons)>0:       
            df_vl_run_reasons['name_of_lab'] = df_vl_run_reasons['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_vl_run_reasons['name_of_lab'] = df_vl_run_reasons['name_of_lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'EID_run_reasons_model':
                df_eid_run_reasons = pd.DataFrame(result['query_data']) 
        if len(df_eid_run_reasons)>0: 
            df_eid_run_reasons['name_of_lab'] = df_eid_run_reasons['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_eid_run_reasons['name_of_lab'] = df_eid_run_reasons['name_of_lab'].replace('Masvingo', 'Masvingo ') 
            
            
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'power_outage_model':
                df_power_outage = pd.DataFrame(result['query_data']) 
        if len(df_power_outage)>0: 
            df_power_outage['Laboratory'] = df_power_outage['Laboratory'].replace('Vic Falls', 'Victoria Falls')
            df_power_outage['Laboratory'] = df_power_outage['Laboratory'].replace('Masvingo', 'Masvingo ') 


                 
        template_path = os.path.join(MEDIA_ROOT, 'template_weekly', 'TEMPLATE_CDC.xlsx')
        destination_folder_base = os.path.join(MEDIA_ROOT,'CDC Reports')
        
        # if len(df['Lab'])==14:             
        new_excel_file_path = create_and_copy_excel_template(template_path, destination_folder_base)
        write_data_to_excel(new_excel_file_path, df, df_eid_rec, df_downtime, df_ref_reasons, df_referred, df_dispatch, df_VL_run, df_EID_run, df_vl_run_reasons, df_eid_run_reasons,df_power_outage)

        #print(new_excel_file_path, df, df_eid_rec, df_downtime, df_ref_reasons, df_referred, df_dispatch, df_VL_run, df_EID_run, df_vl_run_reasons, df_eid_run_reasons)
    # print('We are here ')
        request.session['excel_file_path'] = new_excel_file_path

        file_name_new = os.path.basename(new_excel_file_path)
    
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().strftime('%B')  # Month as full name

        directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")

        file_list = []
        file_details = []  # List to store file details including modified date
        if os.path.exists(directory_path):
            
            for file in os.listdir(directory_path):
                file_path = os.path.join(directory_path, file)
                if os.path.isfile(file_path):
                    modified_time = os.path.getmtime(file_path)
                    date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                    file_details.append({'name': file, 'date_modified': date_modified})
                else:
                    file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
            file_list = [file['name'] for file in file_details]
            
            # Sort the file details by 'date_modified', handling None values appropriately
        file_details.sort(key=lambda x: x['date_modified'] or '', reverse=True)

        context = {
                'data': data,      
                'file_name': file_name_new,
                'excel_file_path': request.session.get('excel_file_path'), 
                'file_list': file_list,
                'file_details': file_details,
                'directory_path': directory_path,
                'current_year':current_year,
                'current_month':current_month,
        
                    }

        # Render the template with the file name in the context
        return render(request, 'Generate_Weekly.html',context )

    else:
        
            # Get today's date
        current_date = datetime.datetime.now().date()
        # Get the start date as the first day of the month 3 months ago
        startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
        # Convert dates to string format
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date_r = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"


        SQLqueries = cdcweeklyreport(startdate, enddate)
        #print(startdate, enddate)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")

        print('the issues are detected ------------')

        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'vlreceived_model':
                df = pd.DataFrame(result['query_data'])
        if len(df)>0:
            df['Lab'] = df['Lab'].replace('Vic Falls', 'Victoria Falls')
            df['Lab'] = df['Lab'].replace('Masvingo', 'Masvingo ')

                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'EIDreceived_model':
                df_eid_rec = pd.DataFrame(result['query_data']) 
        if len(df_eid_rec)>0:
            df_eid_rec['Lab'] = df_eid_rec['Lab'].replace('Vic Falls', 'Victoria Falls')
            df_eid_rec['Lab'] = df_eid_rec['Lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'Downtime_model':
                df_downtime = pd.DataFrame(result['query_data'])
        if len(df_downtime)>0:           
            df_downtime['name_of_lab'] = df_downtime['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_downtime['name_of_lab'] = df_downtime['name_of_lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'Referal_reasons_model':
                df_ref_reasons = pd.DataFrame(result['query_data']) 
        if len(df_ref_reasons)>0: 
            df_ref_reasons['lab'] = df_ref_reasons['lab'].replace('Vic Falls', 'Victoria Falls')
            df_ref_reasons['lab'] = df_ref_reasons['lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'Referred_model':
                df_referred = pd.DataFrame(result['query_data'])  
        if len(df_referred)>0:         
            df_referred['name_of_lab'] = df_referred['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_referred['name_of_lab'] = df_referred['name_of_lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'lab_dispatch_model':
                df_dispatch = pd.DataFrame(result['query_data'])
        if len(df_dispatch)>0:  
            df_dispatch['lab'] = df_dispatch['lab'].replace('Vic Falls', 'Victoria Falls')
            df_dispatch['lab'] = df_dispatch['lab'].replace('Masvingo', 'Masvingo ')


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'sample_runVL_model':
                df_VL_run = pd.DataFrame(result['query_data']) 
        if len(df_VL_run)>0:
            
            df_VL_run['name_of_lab'] = df_VL_run['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_VL_run['name_of_lab'] = df_VL_run['name_of_lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'sample_runEID_model':
                df_EID_run = pd.DataFrame(result['query_data']) 
        if len(df_EID_run)>0:
            
            df_EID_run['name_of_lab'] = df_EID_run['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_EID_run['name_of_lab'] = df_EID_run['name_of_lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'VL_run_reasons_model':
                df_vl_run_reasons = pd.DataFrame(result['query_data']) 
        if len(df_vl_run_reasons)>0:       
            df_vl_run_reasons['name_of_lab'] = df_vl_run_reasons['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_vl_run_reasons['name_of_lab'] = df_vl_run_reasons['name_of_lab'].replace('Masvingo', 'Masvingo ') 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'EID_run_reasons_model':
                df_eid_run_reasons = pd.DataFrame(result['query_data']) 
        if len(df_eid_run_reasons)>0: 
            df_eid_run_reasons['name_of_lab'] = df_eid_run_reasons['name_of_lab'].replace('Vic Falls', 'Victoria Falls')
            df_eid_run_reasons['name_of_lab'] = df_eid_run_reasons['name_of_lab'].replace('Masvingo', 'Masvingo ') 


        template_path = os.path.join(MEDIA_ROOT, 'template_weekly', 'TEMPLATE_CDC.xlsx')
        destination_folder_base = os.path.join(MEDIA_ROOT,'CDC Reports')
        
        #if len(df['Lab'])==14:            
        new_excel_file_path = create_and_copy_excel_template(template_path, destination_folder_base)
        write_data_to_excel(new_excel_file_path, df, df_eid_rec, df_downtime, df_ref_reasons, df_referred, df_dispatch, df_VL_run, df_EID_run, df_vl_run_reasons, df_eid_run_reasons)

        #print(new_excel_file_path, df, df_eid_rec, df_downtime, df_ref_reasons, df_referred, df_dispatch, df_VL_run, df_EID_run, df_vl_run_reasons, df_eid_run_reasons)
        #print('We are here ')
        request.session['excel_file_path'] = new_excel_file_path

        file_name_new = os.path.basename(new_excel_file_path)
            
        
        
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().strftime('%B')  # Month as full name

        directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")

        file_list = []
        file_details = []  # List to store file details including modified date

        if os.path.exists(directory_path):
            for file in os.listdir(directory_path):
                file_path = os.path.join(directory_path, file)
                if os.path.isfile(file_path):
                    modified_time = os.path.getmtime(file_path)
                    date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                    file_details.append({'name': file, 'date_modified': date_modified})
                else:
                    file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
            file_list = [file['name'] for file in file_details]
            
            # Sort the file details by 'date_modified', handling None values appropriately
        file_details.sort(key=lambda x: x['date_modified'] or '', reverse=True)

            
        

        context = {
                'data': data,      
                'file_name': file_name_new,
                'excel_file_path': request.session.get('excel_file_path'), 
                'file_list': file_list,
                'file_details': file_details,
                'directory_path': directory_path,
                'current_year':current_year,
                'current_month':current_month,
        
                    }

        # Render the template with the file name in the context
        return render(request, 'Generate_Weekly.html',context )








    
def download_file(request, file_name):
    file_path = os.path.join(settings.MEDIA_ROOT, 'CDC Reports', str(datetime.datetime.now().year), datetime.datetime.now().strftime('%B'), file_name)
    if os.path.exists(file_path):
        with open(file_path, 'rb') as fh:
            response = HttpResponse(fh.read(), content_type="application/vnd.ms-excel")
            response['Content-Disposition'] = 'inline; filename=' + os.path.basename(file_path)
            return response
    raise Http404

def download_ist_file(request, file_name):
    file_path = os.path.join(settings.MEDIA_ROOT, 'CDC Reports', str(datetime.datetime.now().year), datetime.datetime.now().strftime('%B'), file_name)
    if os.path.exists(file_path):
        with open(file_path, 'rb') as fh:
            response = HttpResponse(fh.read(), content_type="application/vnd.ms-excel")
            response['Content-Disposition'] = 'inline; filename=' + os.path.basename(file_path)
            return response
    raise Http404




def cdc_list_files(request):
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().strftime('%B')  # Month as full name

    directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")

    if os.path.exists(directory_path):
        file_list = os.listdir(directory_path)
    else:
        file_list = []

    context = {
        'file_list': file_list,
        'directory_path': directory_path,
        'current_year':current_year,
        'current_month':current_month,
        
    }
    return render(request, 'Generate_Weekly.html', context)





def IST_excel_file_view(request):

    if request.method == 'POST':
               
        # Get today's date
        current_date = datetime.datetime.now().date()
        # Get the start date as the first day of the month 3 months ago
        startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
        # Convert dates to string format
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date_r = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"


        SQLqueries = ISTMOdelSQLQueries(start_date_r,end_date_r)
        #print(startdate, enddate)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")

        #print('the issues are detected ------------')
    
        
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'rider_model':
                df_rider = pd.DataFrame(result['query_data']) 
        


                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'reliefrider_model':
                df_relief = pd.DataFrame(result['query_data']) 

    
        
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'driver_model':
                df_driver = pd.DataFrame(result['query_data'])

       
                 
        template_path = os.path.join(MEDIA_ROOT, 'template_weekly', 'National IST Rider and Vehicle Weekly Report TEMPLATE.xlsx')
        destination_folder_base = os.path.join(MEDIA_ROOT,'CDC Reports')
        
        # if len(df['Lab'])==14:             
        new_IST_excel_file_path = IST_create_and_copy_excel_template(template_path, destination_folder_base)
        IST_write_data_to_excel(new_IST_excel_file_path, df_rider, df_relief,df_driver)

        request.session['IST_excel_file_path'] = new_IST_excel_file_path

        ist_file_name_new = os.path.basename(new_IST_excel_file_path)
    
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().strftime('%B')  # Month as full name

        IST_directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")

        IST_file_list = [] 
        IST_file_details = []  # List to store file details including modified date
        if os.path.exists(IST_directory_path):
            
            for file in os.listdir(IST_directory_path):
                file_path = os.path.join(IST_directory_path, file)
                if os.path.isfile(file_path):
                    modified_time = os.path.getmtime(file_path)
                    date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                    IST_file_details.append({'name': file, 'date_modified': date_modified})
                else:
                    IST_file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
            IST_file_list = [file['name'] for file in IST_file_details]

        context = {
                'data': data,      
                'IST_file_name': ist_file_name_new,
                'IST_excel_file_path': request.session.get('IST_excel_file_path'), 
                'IST_file_list': IST_file_list,
                'IST_file_details': IST_file_details,
                'IST_directory_path': IST_directory_path,
                'current_year':current_year,
                'current_month':current_month,
        
                    }

        # Render the template with the file name in the context
        return render(request, 'Generate_Weekly.html',context )
    
    else:
        
        # Get today's date
        current_date = datetime.datetime.now().date()
        # Get the start date as the first day of the month 3 months ago
        startdate_srt = (current_date - relativedelta(months=3)).replace(day=1)
        # Convert dates to string format
        startdate = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date_r = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        end_date_r = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        
        # Generate month list
        months_list = list_months(start_date_r, end_date_r)

        # The result can be joined into a single string if needed
        months_str = ','.join(months_list)
        datelist = f"'{months_str}'"


        SQLqueries =  ISTMOdelSQLQueries(start_date_r,end_date_r)
        #print(startdate, enddate)
        
        data = []
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    #print('start')
                    cursor.execute(query['sql'], query['parameters'])
                    #print('end')
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                    #print(data)
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")

        #print('the issues are detected ------------')

        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'rider_model':
                df_rider = pd.DataFrame(result['query_data']) 

                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'reliefrider_model':
                df_relief = pd.DataFrame(result['query_data']) 


        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'driver_model':
                df_driver = pd.DataFrame(result['query_data'])


                 
        template_path = os.path.join(MEDIA_ROOT, 'template_weekly', 'National IST Rider and Vehicle Weekly Report TEMPLATE.xlsx')
        destination_folder_base = os.path.join(MEDIA_ROOT,'CDC Reports')
        
        # if len(df['Lab'])==14:             
        new_IST_excel_file_path = IST_create_and_copy_excel_template(template_path, destination_folder_base)
        IST_write_data_to_excel(new_IST_excel_file_path, df_rider, df_relief,df_driver)

        request.session['IST_excel_file_path'] = new_IST_excel_file_path

        ist_file_name_new = os.path.basename(new_IST_excel_file_path)
    
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().strftime('%B')  # Month as full name

        IST_directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")

        IST_file_list = [] 
        IST_file_details = []  # List to store file details including modified date
        if os.path.exists(IST_directory_path):
            
            for file in os.listdir(IST_directory_path):
                file_path = os.path.join(IST_directory_path, file)
                if os.path.isfile(file_path):
                    modified_time = os.path.getmtime(file_path)
                    date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                    IST_file_details.append({'name': file, 'date_modified': date_modified})
                else:
                    IST_file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
            IST_file_list = [file['name'] for file in IST_file_details]

        context = {
                'data': data,      
                'IST_file_name': ist_file_name_new,
                'IST_excel_file_path': request.session.get('IST_excel_file_path'), 
                'IST_file_list': IST_file_list,
                'IST_file_details': IST_file_details,
                'IST_directory_path': IST_directory_path,
                'current_year':current_year,
                'current_month':current_month,
        
                    }

        # Render the template with the file name in the context
        return render(request, 'Generate_Weekly.html',context )


def CDC_narrative_word(request):
      
    if request.method == 'POST':        
        datelist=0       
        end_date = datetime.datetime.now()
        start_date = datetime.datetime.now() - timedelta(days=11)
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().strftime('%B')  # Month as full name

                     
                
        template_path = os.path.join(MEDIA_ROOT, 'template_weekly', 'BRTI November  VL-EID Report Narrative.docx')
        destination_folder_base = os.path.join(MEDIA_ROOT,'CDC Reports')  
        
   
        response=write_word_document_narrative(template_path, destination_folder_base)
  
        
        
        
        
        narr_directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")
        narr_file_list = []
        narr_file_details = []  # List to store file details including modified date
        if os.path.exists(narr_directory_path):
            
            for file in os.listdir(narr_directory_path):
                file_path = os.path.join(narr_directory_path, file)
                if os.path.isfile(file_path):
                    modified_time = os.path.getmtime(file_path)
                    date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                    narr_file_details.append({'name': file, 'date_modified': date_modified})
                else:
                    narr_file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
            narr_file_list = [file['name'] for file in narr_file_details]
        
            context = {
         
            'narr_file_list': narr_file_list,
            'narr_file_details': narr_file_details,
            'narr_directory_path': narr_directory_path,     
    
                }

        # Render the template with the file name in the context
        return render(request, 'Generate_Weekly.html',context )
    else:
        
        
        datelist=0       
        end_date = datetime.datetime.now()
        start_date = datetime.datetime.now() - timedelta(days=11)
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
       
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().strftime('%B')  # Month as full name      
                
        template_path = os.path.join(MEDIA_ROOT, 'template_weekly', 'BRTI November  VL-EID Report Narrative.docx')
        destination_folder_base = os.path.join(MEDIA_ROOT,'CDC Reports')       
        response=write_word_document_narrative(template_path, destination_folder_base)
        
        
        narr_directory_path = os.path.join(settings.MEDIA_ROOT, f"CDC Reports/{current_year}/{current_month}")
        narr_file_list = []
        narr_file_details = []  # List to store file details including modified date
        if os.path.exists(narr_directory_path):
            
            for file in os.listdir(narr_directory_path):
                file_path = os.path.join(narr_directory_path, file)
                if os.path.isfile(file_path):
                    modified_time = os.path.getmtime(file_path)
                    date_modified = datetime.datetime.fromtimestamp(modified_time).strftime('%Y-%m-%d %H:%M:%S')
                    narr_file_details.append({'name': file, 'date_modified': date_modified})
                else:
                    narr_file_details.append({'name': file, 'date_modified': None})  # Assuming non-files (like directories) also exist
            narr_file_list = [file['name'] for file in narr_file_details]
        
            context = {
    
            'narr_file_list': narr_file_list,
            'narr_file_details': narr_file_details,
            'narr_directory_path': narr_directory_path,     
    
                }

        # Render the template with the file name in the context
        return render(request, 'Generate_Weekly.html',context )
    
    
    

def Supply_chain_view(request):
        
    if request.method == 'POST':
        startdate = request.POST.get('startdate')
        enddate = request.POST.get('enddate')       

        user = request.user  
        lab=user.username
      
       
        SQLqueries = cdcweeklyreport(startdate, enddate)
        data=[]

        # Process the SQL queries
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")    
                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'SUPPLY_CHAIN_Ref':
                df_sql_ref = pd.DataFrame(result['query_data'])  
                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'reagent_expiary':
                df_reagent = pd.DataFrame(result['query_data']) 
          
            #-------------------CREATING DATA FOR SANKLEY
        # print(df_sql_ref)           
        # # Filter the DataFrame where Plasma_Samples_reffered_Out > 0
        # filtered_df = df_sql_ref[df_sql_ref['Plasma_Samples_reffered_Out'] > 0]
        # # Selecting the relevant columns
        # selected_columns = filtered_df[['name_of_lab', 'Plasma_refered_to_lab', 'Plasma_Samples_reffered_Out']]
        # # Transforming the data for Highcharts
        # chart_data = selected_columns.apply(lambda x: [x['name_of_lab'], x['Plasma_refered_to_lab'], x['Plasma_Samples_reffered_Out']], axis=1).tolist()

        # # Filter the DataFrame where Plasma_Samples_reffered_Out > 0
        # dbs_filtered_df = df_sql_ref[df_sql_ref['DBS_Samples_reffered_Out'] > 0]
        # # Selecting the relevant columns
        # dbs_selected_columns = dbs_filtered_df[['name_of_lab', 'DBS_refered_to_lab', 'DBS_Samples_reffered_Out']]
        # # Transforming the data for Highcharts
        # dbs_chart_data = dbs_selected_columns.apply(lambda x: [x['name_of_lab'], x['DBS_refered_to_lab'], x['DBS_Samples_reffered_Out']], axis=1).tolist()

            # Filter the DataFrame where Plasma_Samples_reffered_Out > 0
        filtered_df = df_sql_ref[df_sql_ref['Plasma_Samples_reffered_Out'] > 0]
        # Selecting the relevant columns
        selected_columns = filtered_df[['name_of_lab', 'Plasma_refered_to_lab', 'Plasma_Samples_reffered_Out']]
        # Transforming the data for Highcharts using list comprehension
        chart_data = [row.tolist() for index, row in selected_columns.iterrows()]

        # Do the same for DBS_Samples_reffered_Out
        dbs_filtered_df = df_sql_ref[df_sql_ref['DBS_Samples_reffered_Out'] > 0]
        dbs_selected_columns = dbs_filtered_df[['name_of_lab', 'DBS_refered_to_lab', 'DBS_Samples_reffered_Out']]
        # Transforming the data for Highcharts
        dbs_chart_data = [row.tolist() for index, row in dbs_selected_columns.iterrows()]
        
    
        #------------------Creating the nodes
        # Extracting unique lab names from both columns
        df_sql_ref = df_sql_ref.replace('0', '')
        unique_from_labs = df_sql_ref['name_of_lab'].unique()
        unique_to_labs = df_sql_ref['Plasma_refered_to_lab'].unique()

        # Combining and deduplicating
        all_unique_labs = list(set(unique_from_labs) | set(unique_to_labs))
        # Predefined list of colors
        colors = ['#ffa500', '#74ffe7', '#8cff74', '#ff8da1', '#f4c0ff', '#e6e6e6', '#F9E79F', 
                '#9c007d', '#1a8dff', '#2c1fa7', '#4896a0', '#9c2a00', '#1a8dff', 'rgb(225, 169, 203)', 
                '#989898', '#bed817']

        # Assigning unique colors to each lab
        # If there are more labs than colors, it will cycle through the colors again
        nodes = [{'id': lab, 'color': colors[i % len(colors)]} for i, lab in enumerate(all_unique_labs)]

            #------------------------For BArchart-------------------------------------->
            
            
        # For Plasma:
        df_filtered_plasma = df_sql_ref[df_sql_ref['Plasma_Samples_reffered_Out'] > 0]
        df_barcharts_plasma = df_filtered_plasma.groupby('Plasma_refered_to_lab').agg(
            referred=('Plasma_Samples_reffered_Out', 'sum')
        ).reset_index()

        df_barcharts_plasma = df_barcharts_plasma.merge(
            df_sql_ref[['name_of_lab', 'Plasma_VL_Total_samples_received', 'Plasma_VL_RECEIVED_TOTAL_Sample_RUN', 'Plasma_VL_Carryover_Samples_in_the_lab']],
            left_on='Plasma_refered_to_lab', right_on='name_of_lab'
        )

        df_barcharts_plasma.rename(columns={
            'Plasma_VL_Total_samples_received': 'samples_received',
            'Plasma_VL_RECEIVED_TOTAL_Sample_RUN': 'samples_run',
            'Plasma_refered_to_lab': 'lab',
            'Plasma_VL_Carryover_Samples_in_the_lab':'carryover'
        }, inplace=True)

        df_barcharts_plasma.drop('name_of_lab', axis=1, inplace=True) # new dataframePLASMA

        # For DBS:
        df_filtered_dbs = df_sql_ref[df_sql_ref['DBS_Samples_reffered_Out'] > 0]
        df_barcharts_dbs = df_filtered_dbs.groupby('DBS_refered_to_lab').agg(
            referred=('DBS_Samples_reffered_Out', 'sum')
        ).reset_index()

        df_barcharts_dbs = df_barcharts_dbs.merge(
            df_sql_ref[['name_of_lab', 'DBS_VL_Total_samples_received', 'DBS_VL_RECEIVED_TOTAL_Sample_RUN', 'DBS_VL_Carryover_Samples_in_the_lab']],
            left_on='DBS_refered_to_lab', right_on='name_of_lab'
        )

        df_barcharts_dbs.rename(columns={
            'DBS_VL_Total_samples_received': 'samples_received',
            'DBS_VL_RECEIVED_TOTAL_Sample_RUN': 'samples_run',
            'DBS_refered_to_lab': 'lab',
            'DBS_VL_Carryover_Samples_in_the_lab':'carryover',
        }, inplace=True)

        df_barcharts_dbs.drop('name_of_lab', axis=1, inplace=True) # new dataframeDBS
        
        
        

        # Initialize the min and max variables with 'inf' and '-inf'
        min_value = float('inf')
        max_value = float('-inf')

        # List of columns to check
        columns_to_check = ['referred', 'samples_received', 'samples_run','carryover']

        # Loop through each column and update min and max values
        for column in columns_to_check:
            # Check if the column exists in the DataFrame
            if column in df_barcharts_plasma.columns:
                current_min = df_barcharts_plasma[column].min()
                current_max = df_barcharts_plasma[column].max()

                # Update the min and max values if needed
                min_value = min(min_value, current_min)
                max_value = max(max_value, current_max)
                
                
        #DBS     # Initialize the min and max variables with 'inf' and '-inf'
        dbs_min_value = float('inf')
        dbs_max_value = float('-inf') 

        # Loop through each column and update min and max values
        for column in columns_to_check:
            # Check if the column exists in the DataFrame
            if column in df_barcharts_dbs.columns:
                current_min = df_barcharts_dbs[column].min()
                current_max = df_barcharts_dbs[column].max()

                # Update the min and max values if needed
                dbs_min_value = min(min_value, current_min)
                dbs_max_value = max(max_value, current_max)




        msg = f"The filters successfully applied: <br> <strong>Start Date: </strong> {startdate} <br> <strong> End Date: </strong> {enddate} "
        messages.error(request, msg)    
                    
        dbs_lab_series_list = df_barcharts_dbs['lab'].tolist()
        dbs_reffered_list  = df_barcharts_dbs['referred'].tolist()
        dbs_received_list   = df_barcharts_dbs['samples_received'].tolist()
        dbs_run_list  = df_barcharts_dbs['samples_run'].tolist()
        dbs_carryover_list  = df_barcharts_dbs['carryover'].tolist()
        
        plasma_lab_series_list = df_barcharts_plasma['lab'].tolist()
        plasma_reffered_list  = df_barcharts_plasma['referred'].tolist()
        plasma_received_list   = df_barcharts_plasma['samples_received'].tolist()
        plasam_run_list  = df_barcharts_plasma['samples_run'].tolist()
        plasam_carryover_list  = df_barcharts_plasma['carryover'].tolist()
       
       
       #----------------------------------GANTHH CHART
               # Filter DataFrame
        df_reagent['Reagent_tests_kits_Stock_on_hand'] = pd.to_numeric(df_reagent['Reagent_tests_kits_Stock_on_hand'], errors='coerce')
        df_reagent = df_reagent[df_reagent['Reagent_tests_kits_Stock_on_hand'] > 0]
         # Convert 'Reagent_tests_kits_available_Expiry_Date' to datetime
        df_reagent['Reagent_tests_kits_available_Expiry_Date'] = pd.to_datetime(df_reagent['Reagent_tests_kits_available_Expiry_Date'], errors='coerce')
        df_reagent = df_reagent.dropna(subset=['Reagent_tests_kits_available_Expiry_Date'])

        # Preparing JavaScript date format
        today_js = "new Date().getTime()"

        # Convert DataFrame to Highcharts series format
        series_data = []
        labs = df_reagent['Lab'].unique()

        for lab in labs:
            lab_entry = {
                'name': f"'{lab}'",
                'id': f"'{lab}'",
                'start': today_js,
                'end': f"new Date().getTime() + (365 * 24 * 60 * 60 * 1000)"  # One year from today
            }
            series_data.append(lab_entry)

            lab_df = df_reagent[df_reagent['Lab'] == lab]
            for _, row in lab_df.iterrows():
                expiry_date_js = f"new Date('{row['Reagent_tests_kits_available_Expiry_Date'].strftime('%Y-%m-%d')}').getTime()"
                platform_entry = {
                    'name': f"'{row['platform']} - {row['Reagent_tests_kits_Stock_on_hand']} in stock'",
                    'id': f"'{row['platform']}'",
                    'parent': f"'{lab}'",
                    'start': today_js,
                    'end': expiry_date_js
                }
                series_data.append(platform_entry)

        sanitized_data = []

        for entry in series_data:
            sanitized_entry = "{\n"
            for key, value in entry.items():
                if key in ['name', 'id', 'parent']:
                    # Remove single quotes from the string value
                    value = value.strip("'")
                    sanitized_entry += f"    {key}: '{value}',\n"
                elif key in ['start', 'end']:
                    # Directly append value without quotes
                    sanitized_entry += f"    {key}: {value},\n"
                else:
                    sanitized_entry += f"    {key}: {value},\n"
            sanitized_entry = sanitized_entry.rstrip(',\n') + "\n},"
            sanitized_data.append(sanitized_entry)

        sanitized_data_string = "[\n" + "\n".join(sanitized_data) + "\n]"
        
        #-------------------------------------------------------------------------
        #chart to display failures
        # Expanded list of color codes
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'supplu_chain_failures':
                df_run_failures = pd.DataFrame(result['query_data'])  
                
        colors = [
            "#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF", "#00FFFF",
            "#800000", "#008000", "#000080", "#808000", "#800080", "#008080",
            "#416047", "#808080", "#FF8000", "#800080", "#FF8080", "#80FF80",
            "#8080FF", "#FFFF80", "#FF80FF", "#80FFFF", "#804040", "#408040",
            "#404080", "#808040", "#804080", "#408080", "#FF4040", "#40FF40",
            "#4040FF", "#FFFF40", "#FF40FF", "#40FFFF", "#400040", "#404000",
            "#004040", "#400000", "#004000", "#000040", "#400040", "#004040"
        ]

        labs = df_run_failures['name_of_lab'].unique()
        series = []
        color_index = 0

        for lab in labs:
            lab_data = df_run_failures[df_run_failures['name_of_lab'] == lab]
            lab_series = {'name': lab, 'data': [], 'color': colors[color_index % len(colors)]}  # Assign a color

            for column in df_run_failures.columns:
                if column != 'name_of_lab' and column != 'Comments':  # Exclude non-failure columns
                    failure_count = lab_data.iloc[0][column]
                    if failure_count > 0:
                        # Convert numpy data types to Python native data types
                        lab_series['data'].append({'name': column, 'value': int(failure_count)})

            if len(lab_series['data']) > 0:
                series.append(lab_series)

            color_index += 1  # Move to the next color

        # Convert to JSON string
        series_json = json.dumps(series)

        # Remove quotes from keys
        formatted_series_json = re.sub(r'"(\w+)":', r'\1:', series_json)

        #-----------------------------
        
        
        
        
        
        
        
       
        context = {
            'data': data,
             'chart_data': chart_data,
            'dbs_chart_data': dbs_chart_data,
            'nodes': nodes,
             'df_sql_ref':df_sql_ref,  
              
             'dbs_lab_series_list':dbs_lab_series_list,
             'dbs_reffered_list':dbs_reffered_list,
             'dbs_received_list':dbs_received_list,
             'dbs_run_list':dbs_run_list,
             'dbs_carryover_list':dbs_carryover_list,
             
             'plasam_carryover_list':plasam_carryover_list,
             'plasma_lab_series_list':plasma_lab_series_list,
             'plasma_reffered_list':plasma_reffered_list,
             'plasma_received_list':plasma_received_list,
             'plasam_run_list':plasam_run_list,
             'dbs_max_value':dbs_max_value,
             'max_value':max_value,
             'sanitized_data_string':sanitized_data_string,
             'formatted_series_json':formatted_series_json,
             

        
                }
        
        return render(request, 'supply_chain.html', context) 
    
    
    else:
         # Get today's date
        current_date = datetime.datetime.now().date()
        end_date = datetime.datetime.now()
        start_date = datetime.datetime.now() - timedelta(days=6)
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')


        data=[]
        SQLqueries = cdcweeklyreport(start_date_str, end_date_str)
        # Process the SQL queries
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})
                 
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")    
                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'SUPPLY_CHAIN_Ref':
                df_sql_ref = pd.DataFrame(result['query_data'])  
                
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'reagent_expiary':
                df_reagent = pd.DataFrame(result['query_data'])  
                
        #-------------------CREATING DATA FOR SANKLEY
           
    #    # Filter the DataFrame where Plasma_Samples_reffered_Out > 0
    #     filtered_df = df_sql_ref[df_sql_ref['Plasma_Samples_reffered_Out'] > 0]
    #     # Selecting the relevant columns
    #     selected_columns = filtered_df[['name_of_lab', 'Plasma_refered_to_lab', 'Plasma_Samples_reffered_Out']]
    #     # Transforming the data for Highcharts
    #     chart_data = selected_columns.apply(lambda x: [x['name_of_lab'], x['Plasma_refered_to_lab'], x['Plasma_Samples_reffered_Out']], axis=1).tolist()

    #     # Filter the DataFrame where Plasma_Samples_reffered_Out > 0
    #     dbs_filtered_df = df_sql_ref[df_sql_ref['DBS_Samples_reffered_Out'] > 0]
    #     # Selecting the relevant columns
    #     dbs_selected_columns = dbs_filtered_df[['name_of_lab', 'DBS_refered_to_lab', 'DBS_Samples_reffered_Out']]
    #     # Transforming the data for Highcharts
    #     dbs_chart_data = dbs_selected_columns.apply(lambda x: [x['name_of_lab'], x['DBS_refered_to_lab'], x['DBS_Samples_reffered_Out']], axis=1).tolist()

       
        # Filter the DataFrame where Plasma_Samples_reffered_Out > 0
        filtered_df = df_sql_ref[df_sql_ref['Plasma_Samples_reffered_Out'] > 0]
        # Selecting the relevant columns
        selected_columns = filtered_df[['name_of_lab', 'Plasma_refered_to_lab', 'Plasma_Samples_reffered_Out']]
        # Transforming the data for Highcharts using list comprehension
        chart_data = [row.tolist() for index, row in selected_columns.iterrows()]

        # Do the same for DBS_Samples_reffered_Out
        dbs_filtered_df = df_sql_ref[df_sql_ref['DBS_Samples_reffered_Out'] > 0]
        dbs_selected_columns = dbs_filtered_df[['name_of_lab', 'DBS_refered_to_lab', 'DBS_Samples_reffered_Out']]
        # Transforming the data for Highcharts
        dbs_chart_data = [row.tolist() for index, row in dbs_selected_columns.iterrows()]
        


       
       #------------------Creating the nodes
       # Extracting unique lab names from both columns
        df_sql_ref = df_sql_ref.replace('0', '')
        unique_from_labs = df_sql_ref['name_of_lab'].unique()
        unique_to_labs = df_sql_ref['Plasma_refered_to_lab'].unique()

        # Combining and deduplicating
        all_unique_labs = list(set(unique_from_labs) | set(unique_to_labs))
        # Predefined list of colors
        colors = ['#ffa500', '#74ffe7', '#8cff74', '#ff8da1', '#f4c0ff', '#e6e6e6', '#F9E79F', 
                '#9c007d', '#1a8dff', '#2c1fa7', '#4896a0', '#9c2a00', '#1a8dff', 'rgb(225, 169, 203)', 
                '#989898', '#bed817']

        # Assigning unique colors to each lab
        # If there are more labs than colors, it will cycle through the colors again
        nodes = [{'id': lab, 'color': colors[i % len(colors)]} for i, lab in enumerate(all_unique_labs)]

        #-------------------------------------------------------------->
        
 


        #------------------------For BArchart-------------------------------------->
            
            
        # For Plasma:
        df_filtered_plasma = df_sql_ref[df_sql_ref['Plasma_Samples_reffered_Out'] > 0]
        df_barcharts_plasma = df_filtered_plasma.groupby('Plasma_refered_to_lab').agg(
            referred=('Plasma_Samples_reffered_Out', 'sum')
        ).reset_index()

        df_barcharts_plasma = df_barcharts_plasma.merge(
            df_sql_ref[['name_of_lab', 'Plasma_VL_Total_samples_received', 'Plasma_VL_RECEIVED_TOTAL_Sample_RUN', 'Plasma_VL_Carryover_Samples_in_the_lab']],
            left_on='Plasma_refered_to_lab', right_on='name_of_lab'
        )

        df_barcharts_plasma.rename(columns={
            'Plasma_VL_Total_samples_received': 'samples_received',
            'Plasma_VL_RECEIVED_TOTAL_Sample_RUN': 'samples_run',
            'Plasma_refered_to_lab': 'lab',
            'Plasma_VL_Carryover_Samples_in_the_lab':'carryover'
        }, inplace=True)

        df_barcharts_plasma.drop('name_of_lab', axis=1, inplace=True) # new dataframePLASMA

        # For DBS:
        df_filtered_dbs = df_sql_ref[df_sql_ref['DBS_Samples_reffered_Out'] > 0]
        df_barcharts_dbs = df_filtered_dbs.groupby('DBS_refered_to_lab').agg(
            referred=('DBS_Samples_reffered_Out', 'sum')
        ).reset_index()

        df_barcharts_dbs = df_barcharts_dbs.merge(
            df_sql_ref[['name_of_lab', 'DBS_VL_Total_samples_received', 'DBS_VL_RECEIVED_TOTAL_Sample_RUN', 'DBS_VL_Carryover_Samples_in_the_lab']],
            left_on='DBS_refered_to_lab', right_on='name_of_lab'
        )

        df_barcharts_dbs.rename(columns={
            'DBS_VL_Total_samples_received': 'samples_received',
            'DBS_VL_RECEIVED_TOTAL_Sample_RUN': 'samples_run',
            'DBS_refered_to_lab': 'lab',
            'DBS_VL_Carryover_Samples_in_the_lab':'carryover',
        }, inplace=True)

        df_barcharts_dbs.drop('name_of_lab', axis=1, inplace=True) # new dataframeDBS
        
        
        

        # Initialize the min and max variables with 'inf' and '-inf'
        min_value = float('inf')
        max_value = float('-inf')

        # List of columns to check
        columns_to_check = ['referred', 'samples_received', 'samples_run','carryover']

        # Loop through each column and update min and max values
        for column in columns_to_check:
            # Check if the column exists in the DataFrame
            if column in df_barcharts_plasma.columns:
                current_min = df_barcharts_plasma[column].min()
                current_max = df_barcharts_plasma[column].max()

                # Update the min and max values if needed
                min_value = min(min_value, current_min)
                max_value = max(max_value, current_max)
                
                
        #DBS     # Initialize the min and max variables with 'inf' and '-inf'
        dbs_min_value = float('inf')
        dbs_max_value = float('-inf') 

        # Loop through each column and update min and max values
        for column in columns_to_check:
            # Check if the column exists in the DataFrame
            if column in df_barcharts_dbs.columns:
                current_min = df_barcharts_dbs[column].min()
                current_max = df_barcharts_dbs[column].max()

                # Update the min and max values if needed
                dbs_min_value = min(min_value, current_min)
                dbs_max_value = max(max_value, current_max)


  
                    
        dbs_lab_series_list = df_barcharts_dbs['lab'].tolist()
        dbs_reffered_list  = df_barcharts_dbs['referred'].tolist()
        dbs_received_list   = df_barcharts_dbs['samples_received'].tolist()
        dbs_run_list  = df_barcharts_dbs['samples_run'].tolist()
        dbs_carryover_list  = df_barcharts_dbs['carryover'].tolist()
        print(dbs_carryover_list)
        
        plasma_lab_series_list = df_barcharts_plasma['lab'].tolist()
        plasma_reffered_list  = df_barcharts_plasma['referred'].tolist()
        plasma_received_list   = df_barcharts_plasma['samples_received'].tolist()
        plasam_run_list  = df_barcharts_plasma['samples_run'].tolist()
        plasam_carryover_list  = df_barcharts_plasma['carryover'].tolist()
        
        #---------------------------------------------------------------------------------------->
        #Ganntt CHart
        
        # Filter DataFrame
        df_reagent['Reagent_tests_kits_Stock_on_hand'] = pd.to_numeric(df_reagent['Reagent_tests_kits_Stock_on_hand'], errors='coerce')
        df_reagent = df_reagent[df_reagent['Reagent_tests_kits_Stock_on_hand'] > 0]
        # Convert 'Reagent_tests_kits_available_Expiry_Date' to datetime
        df_reagent['Reagent_tests_kits_available_Expiry_Date'] = pd.to_datetime(df_reagent['Reagent_tests_kits_available_Expiry_Date'], errors='coerce')
        df_reagent = df_reagent.dropna(subset=['Reagent_tests_kits_available_Expiry_Date'])

        # Preparing JavaScript date format
        today_js = "new Date().getTime()"

        # Convert DataFrame to Highcharts series format
        series_data = []
        labs = df_reagent['Lab'].unique()

        for lab in labs:
            lab_entry = {
                'name': f"'{lab}'",
                'id': f"'{lab}'",
                'start': today_js,
                'end':today_js  #f"new Date().getTime() + (365 * 24 * 60 * 60 * 1000)"  # One year from today
            }
            series_data.append(lab_entry)

            lab_df = df_reagent[df_reagent['Lab'] == lab]
            for _, row in lab_df.iterrows():
                expiry_date_js = f"new Date('{row['Reagent_tests_kits_available_Expiry_Date'].strftime('%Y-%m-%d')}').getTime()"
                platform_entry = {
                    'name': f"'{row['platform']} - {row['Reagent_tests_kits_Stock_on_hand']} in stock'",
                    'id': f"'{row['platform']}'",
                    'parent': f"'{lab}'",
                    'start': today_js,
                    'end': expiry_date_js
                }
                series_data.append(platform_entry)

        sanitized_data = []

        for entry in series_data:
            sanitized_entry = "{\n"
            for key, value in entry.items():
                if key in ['name', 'id', 'parent']:
                    # Remove single quotes from the string value
                    value = value.strip("'")
                    sanitized_entry += f"    {key}: '{value}',\n"
                elif key in ['start', 'end']:
                    # Directly append value without quotes
                    sanitized_entry += f"    {key}: {value},\n"
                else:
                    sanitized_entry += f"    {key}: {value},\n"
            sanitized_entry = sanitized_entry.rstrip(',\n') + "\n},"
            sanitized_data.append(sanitized_entry)

        sanitized_data_string = "[\n" + "\n".join(sanitized_data) + "\n]"
        
        #-------------------------------------------------------------------------
        #chart to display failures
        # Expanded list of color codes
        for result in data:
            if isinstance(result, dict) and result.get('query_name') == 'supplu_chain_failures':
                df_run_failures = pd.DataFrame(result['query_data'])  
                
        colors = [
            "#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF", "#00FFFF",
            "#800000", "#008000", "#000080", "#808000", "#800080", "#008080",
            "#416047", "#808080", "#FF8000", "#800080", "#FF8080", "#80FF80",
            "#8080FF", "#FFFF80", "#FF80FF", "#80FFFF", "#804040", "#408040",
            "#404080", "#808040", "#804080", "#408080", "#FF4040", "#40FF40",
            "#4040FF", "#FFFF40", "#FF40FF", "#40FFFF", "#400040", "#404000",
            "#004040", "#400000", "#004000", "#000040", "#400040", "#004040"
        ]

        labs = df_run_failures['name_of_lab'].unique()
        series = []
        color_index = 0

        for lab in labs:
            lab_data = df_run_failures[df_run_failures['name_of_lab'] == lab]
            lab_series = {'name': lab, 'data': [], 'color': colors[color_index % len(colors)]}  # Assign a color

            for column in df_run_failures.columns:
                if column != 'name_of_lab' and column != 'Comments':  # Exclude non-failure columns
                    failure_count = lab_data.iloc[0][column]
                    if failure_count > 0:
                        # Convert numpy data types to Python native data types
                        lab_series['data'].append({'name': column, 'value': int(failure_count)})

            if len(lab_series['data']) > 0:
                series.append(lab_series)

            color_index += 1  # Move to the next color

        # Convert to JSON string
        series_json = json.dumps(series)

        # Remove quotes from keys
        formatted_series_json = re.sub(r'"(\w+)":', r'\1:', series_json)

        #-----------------------------
       
       
        context = {
            'data': data,
             'chart_data': chart_data,
            'dbs_chart_data': dbs_chart_data,
            'nodes': nodes,
             'df_sql_ref':df_sql_ref,  
              
             'dbs_lab_series_list':dbs_lab_series_list,
             'dbs_reffered_list':dbs_reffered_list,
             'dbs_received_list':dbs_received_list,
             'dbs_run_list':dbs_run_list,
             'dbs_carryover_list':dbs_carryover_list,
             
             'plasam_carryover_list':plasam_carryover_list,
             'plasma_lab_series_list':plasma_lab_series_list,
             'plasma_reffered_list':plasma_reffered_list,
             'plasma_received_list':plasma_received_list,
             'plasam_run_list':plasam_run_list,
             'dbs_max_value':dbs_max_value,
             'max_value':max_value,
             'sanitized_data_string':sanitized_data_string,
             'formatted_series_json':formatted_series_json,
             

        
                }
        
        return render(request, 'supply_chain.html', context) 
        
          
   
   
#---------------FOr the SMS --------------------------------
 
#Lets Handle the creation reports here
@login_required  
def SMSupload_files(request):    
    user = request.user  # Get the authenticated user
    user=user.username
    messages_list = []  # To store all messages (errors or success)

    if request.method == "GET":
        return render(request, 'index_sie.html')
    
    elif request.method == 'POST':
        
        files = request.FILES.getlist('smsfiles[]', None)
        number_of_files = len(files)
        # print (number_of_files)   
        # print (files)   
                
            

        for file in files:
            print(f"Processing file: {file.name}")
            try:
                file_extension = os.path.splitext(file.name)[1].lower()
                
                # Check for valid Excel formats
                if  file_extension in ['.csv' ]:
                    df = pd.read_csv(file,  engine='openpyxl')
                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Job #': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid SMS report file. Please upload a valid one❗❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:
                        response=readingSMS_data(df,file,user)  
                        
                        print(f'response= :{response}')
                        if 'successfully uploaded' in response.lower():                            
                            # 1. Extract current month and year                            
                            current_date = datetime.datetime.now()
                            month_year = current_date.strftime('%B %Y')                            
                            # Find the previous Sunday from the current date
                            while current_date.weekday() != 6:  # 6 represents Sunday
                                current_date -= timedelta(days=1)

                            previous_sunday = current_date.date()

                            # 3. Create directory structure
                            base_folder = os.path.join(MEDIA_ROOT, 'SMS_REPORTS', month_year, previous_sunday.strftime('%Y-%m-%d'))
                            #base_folder = os.path.join(month_year, last_day_of_month.strftime('%Y-%m-%d'))
                            if not os.path.exists(base_folder):
                                os.makedirs(base_folder)

                            # 4. Move the uploaded file to the appropriate directory
                            fs = FileSystemStorage(location=base_folder)  # set the location to your base_folder
                            filename = fs.save(file.name, file)
                            file_path = fs.path(filename)
                            
                            # print(base_folder)
                            # print(filename)
                            # print(file_path)
                            # print(fs)

                            msg = response
                            #messages.success(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{msg}  </div>'})
                        else:
                            msg = response
                            #messages.error(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{msg}  </div>'})
                            
                        
                    
                elif file_extension in ['.xls', '.xlsx', '.xlsm']:      
                    df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
                                        
                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Job #': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid SMS report file. Please upload a valid one❗❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:
                        
                    
                        #print(f"Successfully read {file.name} as an Excel file.")
                        response=readingSMS_data(df,file,user) 
                        print(f'response= :{response}')                       
                        if 'successfully uploaded' in response.lower():                            
                            # 1. Extract current month and year                            
                            current_date = datetime.datetime.now()
                            month_year = current_date.strftime('%B %Y')                            
                            # Find the previous Sunday from the current date
                            while current_date.weekday() != 6:  # 6 represents Sunday
                                current_date -= timedelta(days=1)
                            previous_sunday = current_date.date()
                            # 3. Create directory structure
                            base_folder = os.path.join(MEDIA_ROOT, 'SMS_REPORTS', month_year, previous_sunday.strftime('%Y-%m-%d'))
                            #base_folder = os.path.join(month_year, last_day_of_month.strftime('%Y-%m-%d'))
                            if not os.path.exists(base_folder):
                                os.makedirs(base_folder)
                            # 4. Move the uploaded file to the appropriate directory
                            fs = FileSystemStorage(location=base_folder)  # set the location to your base_folder
                            filename = fs.save(file.name, file)
                            file_path = fs.path(filename)
                            
                            # print(base_folder)
                            # print(filename)
                            # print(file_path)
                            # print(fs)

                           
                            #messages.success(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{response}  </div>'})
                        else:
                            #messages.error(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{response}  </div>'})
                        
                    
                else:
                    messages_list.append(f'🔬The file: - > {file.name} <-  is not a valid SMS  file. Please upload a valid one📔 in excel format❗')
                    return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})

                
                

            except Exception as e:
                messages_list.append(f'Error processing file: - > {file.name} <- : {str(e)}')

        # Check if there were any error messages
        if any("error" in msg.lower() for msg in messages_list):  # Here, we assume any message containing "error" is an error message
            return JsonResponse({'msg': '<br>'.join(messages_list)})

        # If no errors, return the success message

        #return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  Creation report(s) successfully uploaded {files} </div>'})
        # return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{number_of_files}  ISTx Weekly report(s) successfully uploaded</div>', 'files': [file.name for file in files]})

        
    else:
            
        files = request.FILES.getlist('smsfiles[]', None)
        number_of_files = len(files)

        for file in files:
            print(f"Processing file: {file.name}")
            try:
                file_extension = os.path.splitext(file.name)[1].lower()
                
                # Check for valid Excel formats
                if file_extension in ['.xls', '.xlsx', '.xlsm']:              

                    df = pd.read_excel(file, sheet_name=0, engine='openpyxl')                   
                    
                    validate=df.columns[0]
                    # Validate the content of the DataFrame
                    if validate != 'Job #': 
                        messages_list.append(f'⚠️The file: - > {file.name} <- is not a valid SMS   file. Please upload a valid one❗❗')
                        return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})
                    else:
                        response=readingSMS_data(df,file,user)
                        print(f'response= :{response}')                     
             
                        if 'successfully uploaded' in response.lower():
                            
                            # 1. Extract current month and year
                            current_date = datetime.datetime.now()
                            month_year = current_date.strftime('%B %Y')

                            
                            # Find the previous Sunday from the current date
                            while current_date.weekday() != 6:  # 6 represents Sunday
                                current_date -= timedelta(days=1)

                            previous_sunday = current_date.date()

                            # 3. Create directory structure
                            base_folder = os.path.join(MEDIA_ROOT, 'SMS_REPORTS', month_year, previous_sunday.strftime('%Y-%m-%d'))
                            #base_folder = os.path.join(month_year, last_day_of_month.strftime('%Y-%m-%d'))
                            if not os.path.exists(base_folder):
                                os.makedirs(base_folder)

                            # 4. Move the uploaded file to the appropriate directory
                            fs = FileSystemStorage(location=base_folder)  # set the location to your base_folder
                            filename = fs.save(file.name, file)
                            file_path = fs.path(filename)
                            
                            # print(base_folder)
                            # print(filename)
                            # print(file_path)
                            # print(fs)

                     
                            #messages.success(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-success" role="alert">{response}  </div>'})
                        else:
                         
                            #messages.error(request, msg)
                            return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{response}  </div>'})
                        
                    
                else:
                    messages_list.append(f'🔬The file: - > {file.name} <-  is not a valid SMS file. Please upload a valid one📔 in excel format❗')
                    return JsonResponse({'msg': f'<div class="alert alert-danger" role="alert">{"<br>".join(messages_list)}</div>'})

                
                

            except Exception as e:
                messages_list.append(f'Error processing file: - > {file.name} <- : {str(e)}')

        # Check if there were any error messages
        if any("error" in msg.lower() for msg in messages_list):  # Here, we assume any message containing "error" is an error message
            return JsonResponse({'msg': '<br>'.join(messages_list)})