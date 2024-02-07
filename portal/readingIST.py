import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import pyplot
import seaborn as sns
import matplotlib as mpl
import seaborn as sns  
from scipy.stats import norm
from scipy.stats import ttest_ind
import statistics
from IPython.display import Image
import io
import requests
from openpyxl import load_workbook
import os
import glob
from openpyxl import load_workbook
import shutil
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pyodbc
import urllib
import smtplib
from email.mime.text import MIMEText
import warnings
import time
import random
from django.contrib import messages
from django.core.mail import message
from django.core.files.storage import FileSystemStorage
from django.shortcuts import render,redirect
from django.contrib.auth import authenticate,login,logout
from django.core.mail import send_mail
from django.http import HttpResponse
from django.conf import settings
from django.core.files.uploadedfile import InMemoryUploadedFile
from sqlalchemy.pool import QueuePool
import traceback
import re
import warnings
import openpyxl
import traceback
from datetime import datetime, timedelta

# Suppress the SettingWithCopyWarning
# warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)
# warnings.simplefilter(action='ignore')
pd.set_option('mode.chained_assignment', None)





    # Load Data for dashboard in database

server = 'mssql-133539-0.cloudclusters.net,17983'
database = 'LSS'
username = 'admin'
password = 'Adm!n123'
driver = 'ODBC Driver 17 for SQL Server'


# server ='.\SQLEXPRESS'
# database = 'LSS'
# username = 'sa'
# password = 'Adm!n123'
# driver = 'ODBC Driver 17 for SQL Server'




# server = '.\SQLEXPRESS'
# database = 'LSS'
# username = 'sa'
# password = 'Adm!n123'
# driver = settings.DATABASES['default']['OPTIONS']['driver']
# engine_insert = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}', echo=False)

# Create the engine with connection pooling
engine_insert = create_engine(
    f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&timeout=5380',
    echo=False,
    poolclass=QueuePool,
    pool_size=10,           # Number of connections to keep open in the pool. Adjust as needed.
    max_overflow=20,        # Number of connections to create if pool is exhausted. Adjust as needed.
    pool_timeout=30         # Time to wait before getting a connection from the pool. Adjust as needed.
)


params = urllib.parse.quote_plus('DRIVER=' + driver + ';SERVER=' + server +
                                    ';DATABASE=' + database + ';UID=' + username +
                                    ';PWD=' + password)

# Create the database engine
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)



def replace_characters(df):
    new_columns = [re.sub("[<>'=.:]", "_", col) for col in df.columns]
    df.columns = new_columns
    return df

def generate_unique_key(user,date):
    # Generate a random number between 1000 and 9999
    random_number = random.randint(1000, 9999)
    formated_time=datetime.now().strftime("%H:%M:%S")
    # Get the current date
    current_date = date

    # Combine the random number, lab name, and date
    unique_key = f"{random_number}_{user}_{current_date}_{formated_time}"

    return unique_key

from datetime import datetime, timedelta
import pandas as pd

def convert_excel_date(df, date_column):
    def convert_to_date(x):
        if pd.isnull(x) or x == 'NaT':
            return x  # Return the original value if it's null or 'NaT'
        elif isinstance(x, datetime):
            return x.strftime('%Y-%m-%d')  # Convert to 'yyyy-mm-dd' format
        elif isinstance(x, str):
            # If it's a string, we assume it's already in the desired format (or you could add extra checks here)
            return x  
        else:
            try:
                # For string dates in a different format like '%m/%d/%Y'
                date = pd.to_datetime(x, format='%m/%d/%Y')
                return date.strftime('%Y-%m-%d')  # Convert to 'yyyy-mm-dd' format
            except ValueError:
                try:
                    # For Excel float dates
                    days = float(x)
                    date = datetime(1899, 12, 30) + timedelta(days=days)
                    return date.strftime('%Y-%m-%d')  # Convert to 'yyyy-mm-dd' format
                except ValueError:
                    return x  # Return the original value if it's not convertible
    
    df[date_column] = df[date_column].apply(convert_to_date)
    return df


def convert_columns_to_int(df):
    for column in df.columns:
        # Skip columns with null values
        if df[column].isnull().any():
            continue
        
        try:
            # Try converting the column to integers
            df[column] = df[column].astype(int)
        except ValueError:
            # If conversion fails, leave the column as is (likely a string)
            continue
    return df

def is_last_sunday(input_date):
    # Convert the input_date string to a date object
    input_date = datetime.strptime(input_date, '%Y-%m-%d %H:%M:%S').date()

    # Find the first day of the next month
    first_day_of_next_month = date(input_date.year, input_date.month + 1, 1)
    
    # Calculate the last day of the current month by subtracting one day from the first day of the next month
    last_day_of_current_month = first_day_of_next_month - timedelta(days=1)
    
    # Check if the last day of the current month is a Sunday (0 corresponds to Sunday)
    return last_day_of_current_month.weekday() == 6  # Sunday is represented by 6


# Step 1: Sanitize the Province_ column
def sanitize_province(province):
    mappings = {
        'Masonaland West': 'Mash West', 'Mashonaland West': 'Mash West', 'MASHWEST': 'Mash West','Mashonaland West ':'Mash West',
        'Matebeleland North': 'Mat North',
        'Mashonaland Central': 'Mash Central','Mashonaland Central ':'Mash Central','Mashonaland Central  ':'Mash Central',
        'Matebeleland South': 'Mat South',
        'Mashonaland East': 'Mash East','Mashonaland East ':'Mash East',
        'Masvingo ':'Masvingo',
        'Manicaland ':'Manicaland',
        'Midlands ':'Midlands'
        
    }
    return mappings.get(province, province)


def reading_National_IST_Rider_Driver_Weekly(df,df_relief,df_drivers,file,user):


      # Get the current date and time
    current_date = datetime.now()

    # Check if today is Sunday
    if current_date.weekday() == 6:  # In Python, Monday is 0, so Sunday is 6
        recent_sunday = current_date
    else:
        # Calculate the most recent Sunday
        recent_sunday = current_date - timedelta(days=current_date.weekday() + 1)

    # Format the date in a readable format (e.g., YYYY-MM-DD)
    last_sunday_str = recent_sunday.strftime("%Y-%m-%d")

        
    try:
        

        #2: FORMATING THIS WEEKS SAMPLE RECEIVED
        ist_new_columns = {
                    'Name of Rider': 'Name_of_Rider_',
                    'BikeRegistrationNumber': 'Bike_Registration_Number',
                    'Province ':'Province_' ,
                    'District ':'District_' ,
                    'Types of PEPFAR Support':'Type_of_PEPFAR_Support' ,
                    'V L Blood/Plasma ':'vl_plasma_sam'  ,
                    'VL DBS ':'vl_dbs_sam' ,
                    'EID Blood':'eid_sam' ,
                    'EID DBS': 'eid_dbs',
                    'Sputum':'sputum_sam' ,
                    'Sputum Culture DR (NTBRL)':'Sputum_Culture_DR_NTBRL',
                     'HPV':'HPV',
                    'Other [Specify e.g: 3 CD4, 17 COVID-19, 10 FBC, 15 Measles, 20 Cholera, 30 Stool]':'other_sam',
                    'V L Blood/Plasma .1':'vl_plasma_res', 
                    'VL DBS .1':'vl_dbs_res', 
                    'EID Blood.1':'eid_res',
                    'EID DBS.1':'eid_dbs_res',
                    'Sputum.1':'sputum_res',
                    'Sputum Culture DR (NTBRL).1':'Sputum_Culture_DR_NTBRL_res',
                    'HPV.1':'HPV_res',
                    'Other [Specify e.g: 3 CD4, 17 COVID-19, 10 FBC, 15 Measles, 20 Cholera, 30 Stool].1':'other_res',
                    'Fuel allocated to rider this week':'Fuel_allocated_to_rider_per_week', 
                    'Fuel used by rider this  week':'Fuel_used_by_rider_per_week',
                    'Distance travelled by this week':'Distance_travelled_by_rider_per_week',
                    'Number of days  bike was functional':'Number_of_days_bike_was_functional',
                    'Number of scheduled visits':'Number_of__Scheduled_Visits_to_Clinic_per__Week',
                    'Number of actual visits':'Number_of_Visits_to_Clinic_per_week',
                    'Bike breakdown':'Bike_breakdown_',
                    'Bike on routine service/ maintenance':'Bike_on_routine_service_and_mainte0nce', 
                    'Bike had no fuel':'Bike_had_no_fuel',
                    'Rider on sick leave':'Rider_on_Sick_Leave',
                    'Rider on annual leave':'Rider_on_Leave',
                    'Inclement weather':'Inclement_weather',
                    'Bike accident damaged':'Accident_damaged_bike_vehicle', 
                    'Clinical IPs related issues':'Clinical_IPs_related_issues',
                    'Other Reasons [Specify e.g: 1 Vacant, 15 Rider reassigned, 20 Suspension]':'Other_Reasons__Specify_',
                    'Mitigation Measures':'Mitigation_measures_',
                    'Comments':'Comments',
                    'date':'date',
                    'Week':'week'          
                }

        df.rename(columns=ist_new_columns, inplace=True) 
   

        riders_colums=['Bike_Registration_Number', 'Province_', 'District_',
       'Type_of_PEPFAR_Support', 'vl_plasma_sam', 'vl_dbs_sam', 'eid_sam',
       'eid_dbs', 'sputum_sam', 'Sputum_Culture_DR_NTBRL', 'HPV', 'other_sam',
       'vl_plasma_res', 'vl_dbs_res', 'eid_res', 'eid_dbs_res', 'sputum_res',
       'Sputum_Culture_DR_NTBRL_res', 'HPV_res', 'other_res',
       'Fuel_allocated_to_rider_per_week', 'Fuel_used_by_rider_per_week',
       'Distance_travelled_by_rider_per_week',
       'Number_of_days_bike_was_functional',
       'Number_of__Scheduled_Visits_to_Clinic_per__Week',
       'Number_of_Visits_to_Clinic_per_week', 'Bike_breakdown_',
       'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
       'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
       'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues',
       'Other_Reasons__Specify_', 'Mitigation_measures_']

        df = df.dropna(subset=riders_colums, how='all')
        df['date'] = last_sunday_str       
        df['SourceFile']=file
        unique_key = generate_unique_key(user, last_sunday_str)
        df['unique_key']=unique_key
        df['status']='rider'
        df.replace('', pd.NA, inplace=True)







        ist_new_columns['SourceFile'] = 'SourceFile'
        ist_new_columns['unique_key'] = 'unique_key'
        ist_new_columns['status'] = 'status' 
        df = df[list(ist_new_columns.values())]
        
        df=df.replace([np.nan, 'nan', 'Nan', 'NaN', 'NAN','NaT', pd.NaT], np.nan)
        df=df.dropna(how='all')
        
        
        df['Province_'] = df['Province_'].apply(sanitize_province)

        # Step 2: Count distinct bikes per province
        bike_counts = df.groupby('Province_')['Bike_Registration_Number'].nunique()

        # Step 3: Check for correct bike counts
        expected_counts = {
            'Bulawayo': 4, 'Harare': 8, 'Manicaland': 45, 'Mash Central': 30,
            'Mash East': 38, 'Mash West': 34, 'Masvingo': 33, 'Mat North': 26,
            'Mat South': 27, 'Midlands': 40
        }

        Bike_Shortage = 1
        for province, expected_count in expected_counts.items():
            
            if province in bike_counts and bike_counts[province] != expected_count:
                Bike_Shortage = 1
                break
        else:
            Bike_Shortage = 0
            
            
            
        #--------checks dups-------------------------------------------------------------------------->
        
        # Assuming df is your DataFrame
        columns_to_check = [
                        'Name_of_Rider_', 'Bike_Registration_Number', 'Province_', 'District_',
                    'Type_of_PEPFAR_Support', 'vl_plasma_sam', 'vl_dbs_sam', 'eid_sam',
                    'eid_dbs', 'sputum_sam', 'Sputum_Culture_DR_NTBRL', 'HPV', 'other_sam',
                    'vl_plasma_res', 'vl_dbs_res', 'eid_res', 'eid_dbs_res', 'sputum_res',
                    'Sputum_Culture_DR_NTBRL_res', 'HPV_res', 'other_res',
                    'Fuel_allocated_to_rider_per_week', 'Fuel_used_by_rider_per_week',
                    'Distance_travelled_by_rider_per_week',
                    'Number_of_days_bike_was_functional',
                    'Number_of__Scheduled_Visits_to_Clinic_per__Week',
                    'Number_of_Visits_to_Clinic_per_week', 'Bike_breakdown_',
                    'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
                    'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
                    'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues',
                    'Other_Reasons__Specify_', 'Mitigation_measures_', 'Comments', 'status'
                        ]

        # Finding duplicates
        duplicates = df.duplicated(subset=columns_to_check, keep=False)

        # You can then use this to filter your DataFrame
        #df_duplicates = df[duplicates]
        df_duplicates = df[duplicates][['Name_of_Rider_', 'Bike_Registration_Number']]



        # Check if there are any duplicate entries
        if duplicates.any():
            duplicate_entries = 1
        else:
            duplicate_entries = 0
        #------------------------------------------------------------------------------------------->

        # Remove spaces before, after, and between words for the 'Bike_Registration_Number' column
        #df['Bike_Registration_Number'] = df['Bike_Registration_Number'].str.replace(' ', '')
        df['Bike_Registration_Number'] = df['Bike_Registration_Number'].astype(str).str.replace(' ', '')


        df['other_sam'] = df['other_sam'].astype(str).str.replace(" ", "").str.lower()
        df['other_res'] = df['other_res'].astype(str).str.replace(" ", "").str.lower()

        # Remove spaces only before and after words for 'Province_' and 'District_' columns
        # df['Province_'] = df['Province_'].str.strip()
        #df['District_'] = df['District_'].str.strip()
        
        # Only applying string strip on non-NaN values
        mask = df['Province_'].notna()
        df.loc[mask, 'Province_'] = df.loc[mask, 'Province_'].astype(str).str.strip()

        mask1 = df['District_'].notna()
        df.loc[mask1, 'District_'] = df.loc[mask1, 'District_'].astype(str).str.strip()

        #----------------relief rider
        relief_new_columns={
            'Name of Rider':'Name_of_Rider_',
             'BikeRegistrationNumber':'Bike_Registration_Number',
             'Province ':'Province_',
              'District ':'District_',
             'Types of PEPFAR Support':'Type_of_PEPFAR_Support',
             'V L Blood/Plasma ':'vl_plasma_sam',
             'VL DBS ':'vl_dbs_sam',
             'EID Blood':'eid_sam',
               'EID DBS':'eid_dbs',
             'Sputum':'sputum_sam',
             'Sputum Culture DR (NTBRL)':'Sputum_Culture_DR_NTBRL',
             'HPV':'HPV',
               'Other [Specify e.g: 3 CD4, 17 COVID-19, 10 FBC, 15 Measles, 20 Cholera, 30 Stool]':'other_sam',
               'V L Blood/Plasma .1':'vl_plasma_res',
             'VL DBS .1':'vl_dbs_res', 
             'EID Blood.1':'eid_res',
             'EID DBS.1':'eid_dbs_res',
               'Sputum.1':'sputum_res',
             'Sputum Culture DR (NTBRL).1':'Sputum_Culture_DR_NTBRL_res', 
             'HPV.1':'HPV_res',
               'Other [Specify e.g: 3 CD4, 17 COVID-19, 10 FBC, 15 Measles, 20 Cholera, 30 Stool].1':'other_res',
               'Fuel allocated to rider this week ':'Fuel_allocated_to_rider_per_week',
             'Fuel used by rider this week':'Fuel_used_by_rider_per_week',
               'Distance travelled by rider this  week':'Distance_travelled_by_rider_per_week',
               'Number of days  bike was functional':'Number_of_days_bike_was_functional', 
             'Number of scheduled visits':'Number_of__Scheduled_Visits_to_Clinic_per__Week',
               'Number of actual visits':'Number_of_Visits_to_Clinic_per_week', 
             'Bike breakdown':'Bike_breakdown_',
               'Bike on routine service/ maintenance':'Bike_on_routine_service_and_mainte0nce', 
             'Bike had no fuel':'Bike_had_no_fuel',
               'Rider on sick leave':'Rider_on_Sick_Leave',
             'Rider on annual leave':'Rider_on_Leave', 
             'Inclement weather':'Inclement_weather',
               'Bike accident damaged':'Accident_damaged_bike_vehicle',
             'Clinical IPs related issues':'Clinical_IPs_related_issues',
               'Other Reasons [Specify e.g: 1 Vacant, 15 Rider reassigned, 20 Suspension]':'Other_Reasons__Specify_',
               'Mitigation Measures':'Mitigation_measures_',
             'Comments':'Comments', 
             'date':'date',
             'Week':'Week'
        }

        df_relief.rename(columns=relief_new_columns, inplace=True) 
      

        relief_riders_colums=['Name_of_Rider_', 'Bike_Registration_Number', 'Province_', 'District_',
                               'Type_of_PEPFAR_Support', 'vl_plasma_sam', 'vl_dbs_sam', 'eid_sam',
                               'eid_dbs', 'sputum_sam', 'Sputum_Culture_DR_NTBRL', 'HPV', 'other_sam',
                               'vl_plasma_res', 'vl_dbs_res', 'eid_res', 'eid_dbs_res', 'sputum_res',
                               'Sputum_Culture_DR_NTBRL_res', 'HPV_res', 'other_res',
                               'Fuel_allocated_to_rider_per_week', 'Fuel_used_by_rider_per_week',
                               'Distance_travelled_by_rider_per_week',
                               'Number_of_days_bike_was_functional',
                               'Number_of__Scheduled_Visits_to_Clinic_per__Week',
                               'Number_of_Visits_to_Clinic_per_week', 'Bike_breakdown_',
                               'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
                               'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
                               'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues',
                               'Other_Reasons__Specify_', 'Mitigation_measures_']

        df_relief = df_relief.dropna(subset=relief_riders_colums, how='all')
        df_relief['date'] = last_sunday_str
    
        df_relief.replace([np.nan, 'nan', 'Nan', 'NaN', 'NAN','NaT', pd.NaT], np.nan)
        df_relief.dropna(how='all')
        
        df_relief['SourceFile']=file
        df_relief['unique_key']=unique_key
        df_relief['status']='relief rider'
        df_relief.replace('', pd.NA, inplace=True) 
        df_relief['other_sam'] = df_relief['other_sam'].astype(str).str.replace(" ", "").str.lower()
        df_relief['other_res'] = df_relief['other_res'].astype(str).str.replace(" ", "").str.lower()

        #sort the Reg Number
        df_relief['Bike_Registration_Number'] = df_relief['Bike_Registration_Number'].astype(str).str.replace(' ', '')
        
        df_relief['Province_'] = df_relief['Province_'].apply(sanitize_province)
        # Remove spaces only before and after words for 'Province_' and 'District_' columns
        # df_relief['Province_'] = df_relief['Province_'].str.strip()
        # df_relief['District_'] = df_relief['District_'].str.strip()
        
        mask_relief = df_relief['Province_'].notna()
        df_relief.loc[mask_relief, 'Province_'] = df_relief.loc[mask_relief, 'Province_'].astype(str).str.strip()

        mask1_relief = df_relief['District_'].notna()
        df_relief.loc[mask1_relief, 'District_'] = df_relief.loc[mask1_relief, 'District_'].astype(str).str.strip()

        relief_new_columns['SourceFile'] = 'SourceFile'
        relief_new_columns['unique_key'] = 'unique_key'
        relief_new_columns['status'] = 'status' 
        df_relief = df_relief[list(relief_new_columns.values())]
        df_relief=df_relief.replace([np.nan, 'nan', 'Nan', 'NaN', 'NAN','NaT', pd.NaT], np.nan)
        df_relief=df_relief.dropna(how='all')

        #-------------------------Lets start the drivers tab----------------------------------------


        #df_drivers['Unnamed: 0'].fillna(method='ffill', inplace=True)
        df_drivers['Unnamed: 0'].ffill(inplace=True)


        dirvers_new_columns={

                    'Unnamed: 0':'Driver_Sample_Status',
                    'Name of Driver ':'Name_of_Rider_',
                    'Vehicle Registration Number':'Bike_Registration_Number',
                    'Province ':'Province_',
                    'District ':'District_',
                    'V L Blood/Plasma ':'vl_plasma_sam',
                    'VL DBS ':'vl_dbs_sam',
                    'EID Blood':'eid_sam',
                    'EID DBS':'eid_dbs',
                    'Sputum':'sputum_sam',
                    'Sputum Culture DR (NTBRL)':'Sputum_Culture_DR_NTBRL',
                    'HPV':'HPV',
                    'Other [Specify e.g: 3 CD4, 17 COVID-19, 10 FBC, 15 Measles, 20 Cholera, 30 Stool]':'other_sam',
                    'V L Blood/Plasma .1':'vl_plasma_res', 
                    'VL DBS .1':'vl_dbs_res',
                    'EID Blood.1':'eid_res', 
                    'EID DBS.1':'eid_dbs_res',
                    'Sputum.1':'sputum_res',
                    'Sputum Culture DR (NTBRL).1':'Sputum_Culture_DR_NTBRL_res', 
                    'HPV.1':'HPV_res',
                    'Other [Specify e.g: 3 CD4, 17 COVID-19,10 FBC, 15 Measles, 20 Cholera, 30 Stool]':'other_res',
                    'Fuel allocated to driver this  week':'Fuel_allocated_to_rider_per_week', 
                    'Fuel used by driver this  week':'Fuel_used_by_rider_per_week',
                    'Distance travelled by driver this  week':'Distance_travelled_by_rider_per_week',
                    'Number of days  vehicle was functional':'Number_of_days_bike_was_functional',
                    'Number of scheduled visits':'Number_of__Scheduled_Visits_to_Clinic_per__Week',
                    'Number of actual visits':'Number_of_Visits_to_Clinic_per_week',
                    'Vehicle breakdown':'Bike_breakdown_',
                    'Vehicle on routine service/maintenance':'Bike_on_routine_service_and_mainte0nce', 
                    'Vehicle had no fuel':'Bike_had_no_fuel',
                    'Driver on sick leave':'Rider_on_Sick_Leave',
                    'Driver on annual leave':'Rider_on_Leave', 
                    'Inclement weather':'Inclement_weather',
                    'Vehicle  accident damaged':'Accident_damaged_bike_vehicle',
                    'Clinical IPs related issues':'Clinical_IPs_related_issues',
                    'Other Reasons [Specify e.g: 1 Vacant, 15 Rider reassigned, 20 Suspension]':'Other_Reasons__Specify_',
                    'Mitigation Measures':'Mitigation_measures_', 
                    'Comments':'Comments', 
                    'date':'date',
                    'Week ':'Week'

                     }




        df_drivers.rename(columns=dirvers_new_columns, inplace=True)
     

        drivers_colums=['Driver_Sample_Status', 'Name_of_Rider_', 
               'Province_', 'District_', 'vl_plasma_sam', 'vl_dbs_sam', 'eid_sam',
               'eid_dbs', 'sputum_sam', 'Sputum_Culture_DR_NTBRL', 'HPV', 'other_sam',
               'vl_plasma_res', 'vl_dbs_res', 'eid_res', 'eid_dbs_res', 'sputum_res',
               'Sputum_Culture_DR_NTBRL_res', 'HPV_res', 'other_res',
               'Fuel_allocated_to_rider_per_week', 'Fuel_used_by_rider_per_week',
               'Distance_travelled_by_rider_per_week',
               'Number_of_days_bike_was_functional',
               'Number_of__Scheduled_Visits_to_Clinic_per__Week',
               'Number_of_Visits_to_Clinic_per_week', 'Bike_breakdown_',
               'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
               'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
               'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues',
               'Other_Reasons__Specify_', 'Mitigation_measures_']

        df_drivers = df_drivers.dropna(subset=drivers_colums, how='all')




        drivers_colums_2nd_LAYER=['vl_plasma_sam', 'vl_dbs_sam', 'eid_sam',
               'eid_dbs', 'sputum_sam', 'Sputum_Culture_DR_NTBRL', 'HPV', 'other_sam',
               'vl_plasma_res', 'vl_dbs_res', 'eid_res', 'eid_dbs_res', 'sputum_res',
               'Sputum_Culture_DR_NTBRL_res', 'HPV_res', 'other_res',
               'Fuel_allocated_to_rider_per_week', 'Fuel_used_by_rider_per_week',
               'Distance_travelled_by_rider_per_week',
               'Number_of_days_bike_was_functional',
               'Number_of__Scheduled_Visits_to_Clinic_per__Week',
               'Number_of_Visits_to_Clinic_per_week', 'Bike_breakdown_',
               'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
               'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
               'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues',
               'Other_Reasons__Specify_', 'Mitigation_measures_']

        df_drivers = df_drivers.dropna(subset=drivers_colums_2nd_LAYER, how='all')



        df_drivers['date'] = last_sunday_str
        
        
        df_drivers['SourceFile']=file
        df_drivers['unique_key']=unique_key
        df_drivers['status']='Driver'
        df_drivers.replace('', pd.NA, inplace=True) 
        df_drivers['other_sam'] = df_drivers['other_sam'].astype(str).str.replace(" ", "").str.lower()
        df_drivers['other_res'] = df_drivers['other_res'].astype(str).str.replace(" ", "").str.lower()


        dirvers_new_columns['SourceFile'] = 'SourceFile'
        dirvers_new_columns['unique_key'] = 'unique_key'
        dirvers_new_columns['status'] = 'status' 
        df_drivers = df_drivers[list(dirvers_new_columns.values())]
        df_drivers=df_drivers.replace([np.nan, 'nan', 'Nan', 'NaN', 'NAN','NaT', pd.NaT], np.nan)
        df_drivers=df_drivers.dropna(how='all')

        #sort the Reg Number
        df_drivers['Bike_Registration_Number'] = df_drivers['Bike_Registration_Number'].astype(str).str.replace(' ', '')
        df_drivers['Province_'] = df_drivers['Province_'].apply(sanitize_province)
        # Remove spaces only before and after words for 'Province_' and 'District_' columns
        # df_drivers['Province_'] = df_drivers['Province_'].str.strip()
        # df_drivers['District_'] = df_drivers['District_'].str.strip()

        mask_driver = df_drivers['Province_'].notna()
        df_drivers.loc[mask_driver, 'Province_'] = df_drivers.loc[mask_driver, 'Province_'].astype(str).str.strip()

        mask1_driver = df_drivers['District_'].notna()
        df_drivers.loc[mask1_driver, 'District_'] = df_drivers.loc[mask1_driver, 'District_'].astype(str).str.strip()
        #------------------------------------------------------------------------------------
        #A_Bike_short = int(len(df) < 285)


        #--------------------End of the Driver tab--------------------------------------------

        # Define a variable to store the status
        has_blank = False
        columns_to_check=['Name_of_Rider_', 'Bike_Registration_Number', 'Province_', 'District_',
                            'Type_of_PEPFAR_Support', 'vl_plasma_sam', 'vl_dbs_sam', 'eid_sam',
                            'eid_dbs', 'sputum_sam', 'Sputum_Culture_DR_NTBRL', 'HPV', 'other_sam',
                            'vl_plasma_res', 'vl_dbs_res', 'eid_res', 'eid_dbs_res', 'sputum_res',
                            'Sputum_Culture_DR_NTBRL_res', 'HPV_res', 'other_res',
                            'Fuel_allocated_to_rider_per_week', 'Fuel_used_by_rider_per_week',
                            'Distance_travelled_by_rider_per_week',
                            'Number_of_days_bike_was_functional',
                            'Number_of__Scheduled_Visits_to_Clinic_per__Week',
                            'Number_of_Visits_to_Clinic_per_week', 'Bike_breakdown_',
                            'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
                            'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
                            'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues',

                            ]
        blank_col=[]

                # lets take valid rows
        df_copy= df[
            (df['Name_of_Rider_'].notna()) & 
            (df['Bike_Registration_Number'].notna())
            ] 
        # Loop through the columns and check for null values
        for col in columns_to_check:
            if df_copy[col].isnull().any():
                has_blank = True
                blank_col.append(col)    
        has_blank=int(has_blank)
        Error_blank_col=len(blank_col) #ERROR 1

        #---------------------------------RELIEF BLANKS---------------------------->
        has_blank_relief = False
        relief_columns_to_check=['Name_of_Rider_', 'Bike_Registration_Number', 'Province_', 'District_',
                            'Type_of_PEPFAR_Support', 'vl_plasma_sam', 'vl_dbs_sam', 'eid_sam',
                            'eid_dbs', 'sputum_sam', 'Sputum_Culture_DR_NTBRL', 'HPV', 'other_sam',
                            'vl_plasma_res', 'vl_dbs_res', 'eid_res', 'eid_dbs_res', 'sputum_res',
                            'Sputum_Culture_DR_NTBRL_res', 'HPV_res', 'other_res',
                            'Fuel_allocated_to_rider_per_week', 'Fuel_used_by_rider_per_week',
                            'Distance_travelled_by_rider_per_week',
                            'Number_of_days_bike_was_functional',
                            'Number_of__Scheduled_Visits_to_Clinic_per__Week',
                            'Number_of_Visits_to_Clinic_per_week', 'Bike_breakdown_',
                            'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
                            'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
                            'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues',

                            ]
        relief_blank_col=[]

        # lets take valid rows
        df_relief_copy= df_relief[
            (df_relief['Name_of_Rider_'].notna()) & 
            (df_relief['Bike_Registration_Number'].notna())
            ] 

        # Loop through the columns and check for null values
        for col in relief_columns_to_check:
            if df_relief_copy[col].isnull().any():
                has_blank_relief = True  #<------------to use this 
                relief_blank_col.append(col) 
                
        has_blank_relief=int(has_blank_relief)
        Error_relief_blank_col=len(relief_blank_col) #ERROR 9

        #---------------CHECK BIKE FUNCTIONALY VS REASONS-----------------------
        sum_series_1=df[['Number_of_days_bike_was_functional', 'Bike_breakdown_',
        'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
        'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
        'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues']].astype(float).sum(axis=1)
        
        
        mask_series = df['Other_Reasons__Specify_'].notna()             
        sum_series_2= df.loc[mask_series, 'Other_Reasons__Specify_'].astype(str).str.extract('(\d+)').fillna(0).astype(int)
        total_sum_series = sum_series_1 + sum_series_2[0]



        Error_BikeFunctional_NoRexn=int(any(total_sum_series!=5)) #ERROR 2
        masking = total_sum_series != 5
        notEqualto5 = df[masking][['Name_of_Rider_', 'Bike_Registration_Number']]

    





     

        

        
       


        df_check=df.copy() #we dont want to mess with our df so we create a copy df
        df_check['Error_BikeFunctional_NoRexn'] = (total_sum_series != 5).astype(int)
        # Finally, let's filter the dataframe to get the desired columns
        List_of_bike_not_Fxnal_NO_REXN = df_check[df_check['Error_BikeFunctional_NoRexn'] == 1][['Name_of_Rider_', 'Bike_Registration_Number','Number_of_days_bike_was_functional']]
        List_of_bike_not_Fxnal_NO_REXN  # LIST OF THOSE BIKES

        # #Error on Relief riders not indicated on mitigation measures vs relief rider tab
        # relief_riders_list=df_relief.query("`status`=='relief rider'")[['Name_of_Rider_', 'Bike_Registration_Number']]
        # # Using the query method with the correct syntax for string containment
        # result_corrected_query = df[df['Mitigation_measures_'].str.contains('relief', case=False, na=False)][['Name_of_Rider_', 'Bike_Registration_Number','Mitigation_measures_']]
        # Error_Relief_riders_not_Match=int(len(relief_riders_list)!=len(result_corrected_query) )  #ERROR 3

        # Filter for relief riders
        relief_riders_list = df_relief.query("`status`=='relief rider'")[['Name_of_Rider_', 'Bike_Registration_Number']]
        # Remove rows where either 'Name_of_Rider_' or 'Bike_Registration_Number' is null
        relief_riders_list = relief_riders_list[
            (relief_riders_list['Name_of_Rider_'].notna()) & 
            (relief_riders_list['Bike_Registration_Number'].notna())
        ]
        # Using the query method with the correct syntax for string containment
        #result_corrected_query = df[df['Mitigation_measures_'].str.contains('relief', case=False, na=False)][['Name_of_Rider_', 'Bike_Registration_Number','Mitigation_measures_']]
        result_corrected_query = df[df['Mitigation_measures_'].astype(str).str.contains('relief', case=False, na=False)][['Name_of_Rider_', 'Bike_Registration_Number','Mitigation_measures_']]

        # Check if lengths are not equal and assign to Error_Relief_riders_not_Match as int (0 or 1)
        Error_Relief_riders_not_Match = int(len(relief_riders_list) != len(result_corrected_query))


        #Checking the Bike reg Number if its correct---------------------------------------------

        # Regular expression pattern to match the desired formats
        pattern = r'^(?:[A-Za-z]{3}[0-9]{4}|[A-Za-z]{4}[0-9]{4}|[A-Za-z]{3}[0-9]{5})$'

        df_not_null=df[df['Bike_Registration_Number'].notna()]
        # Check if each registration number matches the format
        matches_format = df_not_null['Bike_Registration_Number'].str.match(pattern)
        # Store the result in a new variable to check if there's any anomaly
        anomaly_detected = not matches_format.all()
        # Use the boolean series to filter out the rows with anomalies
        anomalous_df = df_not_null[~matches_format][['Name_of_Rider_', 'Bike_Registration_Number']] 
        Error_Reg_Number_Anomaly_detected=int(anomaly_detected)  #ERROR 4


        #----------------relief rider bike--reg-------------------
        # # Check if each registration number matches the format
        # relief_matches_format = df_relief['Bike_Registration_Number'].str.match(pattern)
        # relief_matches_format = relief_matches_format.fillna(False)
        # # Store the result in a new variable to check if there's any anomaly
        # anomaly_detected_relief = not relief_matches_format.all()
        # # Use the boolean series to filter out the rows with anomalies
        # anomalous_df_relief = df_relief[~relief_matches_format][['Name_of_Rider_', 'Bike_Registration_Number']] 
        # Error_Reg_Number_Anomaly_detected_relief=int(anomaly_detected_relief)  #ERROR 5


        if len(df_relief)>0:

            # Only consider rows where 'Bike_Registration_Number' is not NaN
            df_relief_not_null = df_relief[df_relief['Bike_Registration_Number'].notna()]
            # Check if each registration number matches the format
            relief_matches_format = df_relief_not_null['Bike_Registration_Number'].str.match(pattern)
            #relief_matches_format = relief_matches_format.fillna(False)  # This may not be necessary now
            # Store the result in a new variable to check if there's any anomaly
            anomaly_detected_relief = not relief_matches_format.all()
            # Use the boolean series to filter out the rows with anomalies
            anomalous_df_relief = df_relief_not_null[~relief_matches_format][['Name_of_Rider_', 'Bike_Registration_Number']] 
            Error_Reg_Number_Anomaly_detected_relief = int(anomaly_detected_relief)
        else:
            Error_Reg_Number_Anomaly_detected_relief=0
            anomalous_df_relief=None



        #--------------------------------
        series_1=df[[ 'Bike_breakdown_',
        'Bike_on_routine_service_and_mainte0nce', 'Bike_had_no_fuel',
        'Rider_on_Sick_Leave', 'Rider_on_Leave', 'Inclement_weather',
        'Accident_damaged_bike_vehicle', 'Clinical_IPs_related_issues']].astype(float).sum(axis=1)
       
        #df['Other_Reasons__Specify_'].str.extract('(\d+)').fillna(0).astype(int)
        
        series_2=df.loc[mask_series, 'Other_Reasons__Specify_'].astype(str).str.extract('(\d+)').fillna(0).astype(int)

        total_sum_series = series_1 + series_2[0]
        maski = (total_sum_series != 0) & (df['Mitigation_measures_'].isnull())
        no_comments_df = df[maski][['Name_of_Rider_', 'Bike_Registration_Number']]
        ERROR_NO_Mitigation_Measures =len(no_comments_df)  #ERROR No comment on mitigation measure  ERROR 6




        #-----------------------------------------------------------------------------------------

        # Refined regular expression pattern to detect missing commas
        pattern_other = r'(?!(.*cd4.*|.*covid19.*|.*covid-19.*|.*hba1c.*))[a-zA-Z]+[\d\s.]+[a-zA-Z]+'


        



        # Create masks for 'other_sam' and 'other_res'
        mask_sam = df['other_sam'].str.contains(pattern_other)
        mask_res = df['other_res'].str.contains(pattern_other)

        # Fill NaN values in the masks with False
        mask_sam = mask_sam.fillna(False)
        mask_res = mask_res.fillna(False)

        # Filter rows that match the criteria
        filtered_df = df[mask_sam ][['Name_of_Rider_', 'Bike_Registration_Number', 'other_sam']]
        filtered_d = df[mask_res][['Name_of_Rider_', 'Bike_Registration_Number', 'other_res']]
        isit=len(filtered_df)>0
        isit_res=len(filtered_d)>0
        Error_Other_Samples_not_formated_well=int(isit)         # ERROR 7
        Error_Other_Results_not_formated_well=int(isit_res)     # ERROR 8

        #---------------------------relief----------------------------------------------------------------
        
        pattern_other_relief = r'(?!(.*cd4.*|.*covid19.*|.*covid-19.*|.*hba1c.*))[a-zA-Z]+[\d\s.]+[a-zA-Z]+'




        if len(df_relief)>0:            

            # Create masks for 'other_sam' and 'other_res'
            mask_sam_relief = df_relief['other_sam'].str.contains(pattern_other_relief)
            mask_res_relief = df_relief['other_res'].str.contains(pattern_other_relief)
            
          
            # Fill NaN values in the masks with False
            mask_sam_relief = mask_sam_relief.fillna(False)
            mask_res_relief = mask_res_relief.fillna(False)

            # Filter rows that match the criteria
            filtered_df_relief = df_relief[mask_sam_relief ][['Name_of_Rider_', 'Bike_Registration_Number', 'other_sam']]
            filtered_d_relief = df_relief[mask_res_relief][['Name_of_Rider_', 'Bike_Registration_Number', 'other_res']]
            isit_relief=len(filtered_df_relief)>0
            isit_res_relief=len(filtered_d_relief)>0
            Error_Other_Samples_not_formated_well_relief=int(isit_relief)         
            Error_Other_Results_not_formated_well_relief=int(isit_res_relief)     # ERROR 

        else:
            Error_Other_Samples_not_formated_well_relief=0
            Error_Other_Results_not_formated_well_relief=0

        #----------------------------------------------------------------------------------------
        
        #---------------------------Driver----------------------------------------------------------------
        
        pattern_other_driver = r'(?!(.*cd4.*|.*covid19.*|.*covid-19.*|.*hba1c.*))[a-zA-Z]+[\d\s.]+[a-zA-Z]+'





        # Create masks for 'other_sam' and 'other_res'
        mask_sam_driver = df_drivers['other_sam'].str.contains(pattern_other_driver)
        mask_res_driver = df_drivers['other_res'].str.contains(pattern_other_driver)

        # Fill NaN values in the masks with False
        mask_sam_driver = mask_sam_driver.fillna(False)
        mask_res_driver = mask_res_driver.fillna(False)

        # Filter rows that match the criteria
        filtered_df_driver = df_drivers[mask_sam_driver ][['Name_of_Rider_', 'Bike_Registration_Number', 'other_sam']]
        filtered_d_driver = df_drivers[mask_res_driver][['Name_of_Rider_', 'Bike_Registration_Number', 'other_res']]
        isit_driver=len(filtered_df_driver)>0
        isit_res_driver=len(filtered_d_driver)>0
        Error_Other_Samples_not_formated_well_driver=int(isit_driver)         
        Error_Other_Results_not_formated_well_driver=int(isit_res_driver)     # ERROR 
        #----------------------------------------------------------------------------------------
        
        #---------------------CHECKING THE DUPS------------------------------------------------      

        # Define your database connection parameters
        params = urllib.parse.quote_plus('DRIVER=' + driver + ';SERVER=' + server +
                                            ';DATABASE=' + database + ';UID=' + username +
                                            ';PWD=' + password)

        # Create the database engine
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)

        # Compute the date range in Python -= make it 11
        #end_date = datetime.now() - timedelta(days=4)
        end_date = datetime.now()
        start_date = datetime.now() - timedelta(days=11)

        # Convert both to 'YYYY-MM-DD' string format
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')

        # Define your SQL query
        sql = f'''

        SELECT  Name_of_Rider_  ,Bike_Registration_Number
            ,Province_ ,District_  ,Type_of_PEPFAR_Support  
            ,vl_plasma_sam ,vl_dbs_sam ,eid_sam ,sputum_sam
            ,other_sam  ,vl_plasma_res
            ,vl_dbs_res ,eid_res ,sputum_res ,other_res
            ,Number_of__Scheduled_Visits_to_Clinic_per__Week
            ,Number_of_Visits_to_Clinic_per_week ,Fuel_used_by_rider_per_week
            ,Fuel_allocated_to_rider_per_week ,Distance_travelled_by_rider_per_week
            ,Bike_breakdown_      ,Bike_on_routine_service_and_mainte0nce ,Bike_had_no_fuel
            ,Rider_on_Sick_Leave ,Rider_on_Leave
            ,Inclement_weather ,Accident_damaged_bike_vehicle
            ,Clinical_IPs_related_issues ,Other_Reasons__Specify_ ,Mitigation_measures_
            ,Comments ,eid_dbs,Sputum_Culture_DR_NTBRL ,HPV
            ,eid_dbs_res ,Sputum_Culture_DR_NTBRL_res ,HPV_res,Number_of_days_bike_was_functional
            ,status
        FROM [LSS].[dbo].[IST_National]
        WHERE date IS NOT NULL AND date BETWEEN :start_date AND :end_date

        '''

        # Execute the query, passing in the date range as parameters
        df_last_week = pd.read_sql(
            text(sql), 
            engine, 
            params={'start_date': start_date_str, 'end_date': end_date_str}
        )
        # df_last_week = convert_columns_to_int(df_last_week)
        # df = convert_columns_to_int(df)
        

        # CHECK IF THE RECORD EXIST IN DATABASE
        df_tuples =set(df[[ 'Name_of_Rider_'  ,'Bike_Registration_Number'
            ,'Province_' ,'District_'  ,'Type_of_PEPFAR_Support'  
            ,'vl_plasma_sam' ,'vl_dbs_sam' ,'eid_sam' ,'sputum_sam'
            ,'other_sam'  ,'vl_plasma_res'
            ,'vl_dbs_res' ,'eid_res' ,'sputum_res' ,'other_res'
            ,'Number_of__Scheduled_Visits_to_Clinic_per__Week'
            ,'Number_of_Visits_to_Clinic_per_week' ,'Fuel_used_by_rider_per_week'
            ,'Fuel_allocated_to_rider_per_week' ,'Distance_travelled_by_rider_per_week'
            ,'Bike_breakdown_'      ,'Bike_on_routine_service_and_mainte0nce' ,'Bike_had_no_fuel'
            ,'Rider_on_Sick_Leave' ,'Rider_on_Leave'
            ,'Inclement_weather' ,'Accident_damaged_bike_vehicle'
            ,'Clinical_IPs_related_issues' ,'Other_Reasons__Specify_' ,'Mitigation_measures_'
            ,'Comments' ,'eid_dbs','Sputum_Culture_DR_NTBRL' ,'HPV'
            ,'eid_dbs_res' ,'Sputum_Culture_DR_NTBRL_res' ,'HPV_res','Number_of_days_bike_was_functional'
            ,'status' ]].apply(tuple, axis=1))
        
        df_last_week_tuples = set(df_last_week[[ 'Name_of_Rider_'  ,'Bike_Registration_Number'
            ,'Province_' ,'District_'  ,'Type_of_PEPFAR_Support'  
            ,'vl_plasma_sam' ,'vl_dbs_sam' ,'eid_sam' ,'sputum_sam'
            ,'other_sam'  ,'vl_plasma_res'
            ,'vl_dbs_res' ,'eid_res' ,'sputum_res' ,'other_res'
            ,'Number_of__Scheduled_Visits_to_Clinic_per__Week'
            ,'Number_of_Visits_to_Clinic_per_week' ,'Fuel_used_by_rider_per_week'
            ,'Fuel_allocated_to_rider_per_week' ,'Distance_travelled_by_rider_per_week'
            ,'Bike_breakdown_'      ,'Bike_on_routine_service_and_mainte0nce' ,'Bike_had_no_fuel'
            ,'Rider_on_Sick_Leave' ,'Rider_on_Leave'
            ,'Inclement_weather' ,'Accident_damaged_bike_vehicle'
            ,'Clinical_IPs_related_issues' ,'Other_Reasons__Specify_' ,'Mitigation_measures_'
            ,'Comments' ,'eid_dbs','Sputum_Culture_DR_NTBRL' ,'HPV'
            ,'eid_dbs_res' ,'Sputum_Culture_DR_NTBRL_res' ,'HPV_res','Number_of_days_bike_was_functional'
            ,'status' ]].apply(tuple, axis=1))




        # CHECKING DUPLICATES
        common_records=df_tuples.intersection(df_last_week_tuples)


        if not df_tuples.intersection(df_last_week_tuples): #no pure dups
            error_dups=0
        else:
            error_dups=1
            
            
          

  

        # now = datetime.now()
        # # Calculate the number of days to subtract to get the previous Sunday
        # days_to_subtract = (now.weekday() + 1) % 7
        # # Subtract the necessary number of days to find last Sunday
        # last_sunday = now - timedelta(days=days_to_subtract)
        # # Format the date to standard 'YYYY-MM-DD' format
        # last_sunday_str = last_sunday.strftime('%Y-%m-%d')
        # #last_sunday_str='2023-10-08'
        
        # Convert 'date' column in the DataFrame to the same 'YYYY-MM-DD' format for accurate comparison
        df['date'] = pd.to_datetime(df['date']).dt.date
        df_relief['date'] = pd.to_datetime(df_relief['date']).dt.strftime('%Y-%m-%d')
        df_drivers['date'] = pd.to_datetime(df_drivers['date']).dt.strftime('%Y-%m-%d')

        # Check if there's any record in the DataFrame where the date is not equal to the last Sunday
        any_record_not_last_sunday =  int(any(df['date'].astype(str) != last_sunday_str))

        if len(df_relief)>0:
            relief_any_record_not_last_sunday =  int(any(df_relief['date'].astype(str) != last_sunday_str))
        else:
            relief_any_record_not_last_sunday=0

        driver_any_record_not_last_sunday =  int(any(df_drivers['date'].astype(str) != last_sunday_str))
        
        
        #Create a condition for filtering
        condition = df['date'] != last_sunday_str
        condition_relief = df_relief['date'] != last_sunday_str
        condition_driver = df_drivers['date'] != last_sunday_str
        
        df_nosunday = df[condition][['Name_of_Rider_', 'Bike_Registration_Number', 'date']]
        relief_nosunday = df_relief[condition_relief][['Name_of_Rider_', 'Bike_Registration_Number', 'date']]
        driver_nosunday = df_drivers[condition_driver][['Name_of_Rider_', 'Bike_Registration_Number', 'date']]
        
        # Check if there are any rows where 'Number_of__Scheduled_Visits_to_Clinic_per__Week' is 0 or NaN
        any_zero_or_nan = df['Number_of__Scheduled_Visits_to_Clinic_per__Week'].isna() | (df['Number_of__Scheduled_Visits_to_Clinic_per__Week'] == 0)

        # Check if there's any record meeting the condition
        Sheduled_Visits_has_any_zero_or_nan_records = any(any_zero_or_nan)
        
        
        

        raised_errors= []
        list_of_Errors=[has_blank,
                        has_blank_relief,
                        Error_BikeFunctional_NoRexn,
                        Error_Relief_riders_not_Match,
                        Error_Reg_Number_Anomaly_detected,
                       # Error_Reg_Number_Anomaly_detected_relief,
                        ERROR_NO_Mitigation_Measures,
                        Error_Other_Samples_not_formated_well,
                        Error_Other_Results_not_formated_well,
                       Error_Other_Samples_not_formated_well_relief,
                       Error_Other_Results_not_formated_well_relief,
                       Error_Other_Samples_not_formated_well_driver,
                       Error_Other_Results_not_formated_well_driver,
                       error_dups
                       #,A_Bike_short
                      # ,any_record_not_last_sunday,relief_any_record_not_last_sunday,driver_any_record_not_last_sunday
                       ,Sheduled_Visits_has_any_zero_or_nan_records
                       ,Bike_Shortage,duplicate_entries
                       ]

        variable_names=['has_blank',
                        'has_blank_relief',
                        'Error_BikeFunctional_NoRexn',
                        'Error_Relief_riders_not_Match',
                        'Error_Reg_Number_Anomaly_detected',
                        #'Error_Reg_Number_Anomaly_detected_relief',
                        'ERROR_NO_Mitigation_Measures',
                        'Error_Other_Samples_not_formated_well',
                        'Error_Other_Results_not_formated_well',
                        'Error_Other_Samples_not_formated_well_relief',
                        'Error_Other_Results_not_formated_well_relief',
                        'Error_Other_Samples_not_formated_well_driver',
                        'Error_Other_Results_not_formated_well_driver',
                        'error_dups'
                        #,'A_Bike_short'
                        #,'any_record_not_last_sunday','relief_any_record_not_last_sunday','driver_any_record_not_last_sunday'
                        ,'Sheduled_Visits_has_any_zero_or_nan_records'
                        ,'Bike_Shortage','duplicate_entries'
                        ]

        # Check if all variables are equal to 0
        if all(variable == 0 for variable in list_of_Errors):   

            #Load Data for dashboard in database
            #engine_insert.execute("ROLLBACK")
            df.to_sql('IST_National', con=engine_insert, if_exists='append', index=False, chunksize=10000)  # Adjust chunksize as needed
            df_relief.to_sql('IST_National', con=engine_insert, if_exists='append', index=False, chunksize=10000)  # Adjust chunksize as needed
            df_drivers.to_sql('IST_National', con=engine_insert, if_exists='append', index=False, chunksize=10000)  # Adjust chunksize as needed
            
            # Execute the stored procedure
            with engine.connect() as connection:
                connection.execute("EXEC [dbo].[Delete_IST_duplicates]")

            error_messages=':Report:  Successfully Uploaded'
             # Example: Return the processed data
            return error_messages
           
        else:
            
            for i in range(len(list_of_Errors)):
                
                if list_of_Errors[i]!=0:
                    raised_errors.append(variable_names[i])
                    
                Meanings_of_Errors = {
                                        'has_blank': 'In the Riders tab: There are some columns which have blanks: replace with 0 if there is nothing:\n <br> {}'.format(blank_col),
                                        'error_dups': 'Duplicate detected: File already uploaded',
                                        'Sheduled_Visits_has_any_zero_or_nan_records':'In the Riders Tab: On Scheduled Visit column , there is a record where the scheduled visit is zero , we aggreed that this columnn must have standard / constant scheduled visits regardless of the rider being relieved or not , on the relief rider we only need the Actual days he worked , the relief rider doesnt necessarily has scheduled visit , leave it with 0 ',
                                        'has_blank_relief': 'In Relief Riders Tab: There are some columns which have blanks : \n  <br>{} '.format(relief_blank_col),

                                        'Error_BikeFunctional_NoRexn': 'In the Riders tab:. The Reasons - Number_of_days_bike_was_functional + Bike_breakdown +  Bike_on_routine_service_and_mainte0nce+ Bike_had_no_fuel Rider_on_Sick_Leave + Rider_on_Leave + Inclement_weather +  Accident_damaged_bike_vehicle + Clinical_IPs_related_issues and Other - All these must equal to 5 working days: check these:\n  <br>{} '.format(notEqualto5.to_html()),

                                        'Error_Relief_riders_not_Match': 'The Relief Riders indicated in the Riders tab (mitigation measures): ({} Relief riders) and total number of relief riders in relief riders tab: ({} Relief riders) does not match'.format(len(result_corrected_query),len(relief_riders_list)),
                                        'Error_Reg_Number_Anomaly_detected': 'The Riders Registration Number  entered is not correct :\n <br> {} '.format(anomalous_df.to_html()),
                                        'ERROR_NO_Mitigation_Measures':'You didnt put the Mitigation measures where its crystal clear that there is need for them on these entries:\n <br> {}'.format(no_comments_df.to_html()),
                                        'Error_Other_Samples_not_formated_well':'Riders Tab: In the Other SAMPLES columns, we must adhere to the syntax, samples must be seperated by a comma eg 3malaria, 6fbc:  Correct these: \n <br> {}'.format(filtered_df.to_html()),
                                        'Error_Other_Results_not_formated_well':'Riders Tab: In the Other RESULT columns, we must adhere to the syntax, results must be seperated by a comma eg 3malaria, 6fbc:  Correct these: \n <br> {}'.format(filtered_d.to_html()),
                                        'Error_Other_Samples_not_formated_well_relief':'Relief Riders Tab: In the Other SAMPLES columns, we must adhere to the syntax, samples must be seperated by a comma eg 3malaria, 6fbc:  Correct these:\n <br>',
                                        'Error_Other_Results_not_formated_well_relief':'Relief Riders Tab: In the Other RESULT columns, we must adhere to the syntax, results must be seperated by a comma eg 3malaria, 6fbc:  Correct these:  \n <br>',
                                        'Error_Other_Samples_not_formated_well_driver':'Drivers Tab: In the Other SAMPLES columns, we must adhere to the syntax, samples must be seperated by a comma eg 3malaria, 6fbc:  Correct these: \n <br> {}'.format(filtered_df_driver.to_html()),
                                        'Error_Other_Results_not_formated_well_driver':'Drivers Tab: In the Other RESULT columns, we must adhere to the syntax, results must be seperated by a comma eg 3malaria, 6fbc:  Correct these:\n <br>{}'.format(filtered_d_driver.to_html()),
                                        'Bike_Shortage':'In the Rider Tab: There is/ are some bikes missing  may you check the number of bikes and or check the spellings of your province:',
                                        'duplicate_entries':'In riders Tab: You have duplicate entries in these riders \n <br> {}'.format(df_duplicates.to_html()),
                                    
                                    }

                    
            #Sent Errors to emails
            # Create a log file with error details
            #print(associated_emails,lab)
        
                #Lets send emails

            
          

          

            error_messages = f''' Make the Corrections in the IST Excel !<br>
                            '''

            for error in raised_errors:
                if error in Meanings_of_Errors:
                    error_message = Meanings_of_Errors[error]
                    error_messages += f'<li><strong>{error} :</strong> {error_message}</li><br>'

            error_messages += '</ol>'

       # Example: Return the processed data
        return error_messages
    

  
    #except Exception as e:
#         # Handle any exceptions that may occur during data processing
#         error_messages="An error identified in the file:" + str(e)
#         return error_messages
    
        
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()

        error_messages = f"An error identified in the file: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
        return error_messages
            
