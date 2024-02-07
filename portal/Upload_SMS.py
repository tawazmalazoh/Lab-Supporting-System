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


from datetime import timedelta
from datetime import datetime, timedelta

import shutil
import traceback

import matplotlib.patches as mpatches
import datetime
import datetime as dt

import warnings

with warnings.catch_warnings(record=True):
    warnings.simplefilter('ignore', UserWarning)


server = 'mssql-133539-0.cloudclusters.net,17983'
database = 'LSS'
username = 'admin'
password = 'Adm!n123'
driver = 'ODBC Driver 17 for SQL Server'


# server = '.\SQLXPRESS'
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

def replace_characters(df):
    new_columns = [re.sub("[<>'=.:]", "_", col) for col in df.columns]
    df.columns = new_columns
    return df

def deduplicate_table(table_name, unique_columns, engine):
    with engine.connect() as conn:
        deduplication_query = f"""
            WITH Duplicates AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {', '.join(unique_columns)} 
                    ORDER BY (unique_key) 
                ) AS rn
                FROM {table_name}
            )
            DELETE FROM Duplicates 
            WHERE rn > 1
            AND  CAST([update_date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
            AND CAST([update_date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
        """
        conn.execute(deduplication_query)


def generate_unique_key(lab_name,date):
    # Generate a random number between 1000 and 9999
    random_number = random.randint(1000, 9999)
    formated_time=datetime.datetime.now().strftime("%H:%M:%S")
    # Get the current date
    current_date = date

    # Combine the random number, lab name, and date
    unique_key = f"{random_number}_{lab_name}_{current_date}_{formated_time}"

    return unique_key


def convert_excel_date(df, date_column):
    def convert_to_date(x):
        if pd.isnull(x) or x == 'NaT':
            return x  # Return the original value if it's null or 'NaT'
        elif isinstance(x, datetime):
            return datetime.strftime(x, '%Y-%m-%d %H:%M:%S.0000000')
        elif isinstance(x, str):
            return x  # Return the original string value
        else:
            try:
                date = pd.to_datetime(x, format='%m/%d/%Y')
                return datetime.strftime(date, '%Y-%m-%d %H:%M:%S.0000000')
            except ValueError:
                try:
                    days = float(x)
                    return datetime.strftime(datetime(1899, 12, 30) + timedelta(days=days), '%Y-%m-%d %H:%M:%S.0000000')
                except ValueError:
                    return x
    
    df[date_column] = df[date_column].apply(convert_to_date)
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


# x = '2023-05-28 00:00:00'
# # Example usage:
# input_date = x  # Replace this with the date you want to check
# result = is_last_sunday(input_date)





#file='C:/Users/T Dadirai/Desktop/LATEST REPORT/Weekly Dashboard Collection Tool V18.xlsx'

def readingSMS_data(df,file,user): 

    try:        
        new_columns = df.columns.str.replace('[\s()]+', '_', regex=True)
        df.columns = new_columns
        now = datetime.datetime.now()
        offset = (now.weekday() - 6) % 7
        last_sunday = now - datetime.timedelta(days=offset)
        df['last_Sunday_date'] = last_sunday.strftime('%Y-%m-%d')            
        #df['SourceFile'] = os.path.basename(file)
        df['SourceFile'] = os.path.basename(file.name)
        df['user']=user 
        unique_key = generate_unique_key(os.path.basename(file.name),last_sunday)
        df['unique_key']=unique_key
        
        
        df.to_sql('SMS_Data_DLR_Customer', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
            #Lets run the deduplication if there is any in the table 
        deduplicate_table('SMS_Data_DLR_Customer', ['Job_#' ,'SMSID','Sender_ID' ,'Phone_Number' ,'Country','Operator' ,'SMS_Source','Encoding','Length' ,'Parts' ,'Submit_Date_UTC_' ,'Sent_Date_UTC_'  ,'SourceFile' ,'Job_#_1' ,'Sender_ID_1' ,'last_Sunday_date' ], engine_insert)
        
        error_messages='successfully uploaded'
        return error_messages
    

    
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()

        error_messages = f"An error while uploading SMS data:\n\nType: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
        #print(error_messages)
        return error_messages  




