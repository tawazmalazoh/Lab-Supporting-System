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
from sqlalchemy.pool import QueuePool 
import traceback



    # Load Data for dashboard in database

server = '#'
database = '#'
username = '#'
password = '#'
driver = 'ODBC Driver 17 for SQL Server'


# server = '.\SQLXPRESS'
# database = '#'
# username = '#'
# password = '#'
# driver = settings.DATABASES['default']['OPTIONS']['driver']

engine= create_engine(
    f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&timeout=5380',
    echo=False,
    poolclass=QueuePool,
    pool_size=10,           # Number of connections to keep open in the pool. Adjust as needed.
    max_overflow=20,        # Number of connections to create if pool is exhausted. Adjust as needed.
    pool_timeout=30         # Time to wait before getting a connection from the pool. Adjust as needed.
                      )




# Function to limit the text length to 253 characters
def limit_text_length(text):
    if isinstance(text, str) and len(text) > 253:
        return text[:253]
    return text


        
def reduce_column_length(df):
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].astype(str).str[:254]
    return df


def reading_vls(file,user):
    df = None

    try:
        if 'tat' in  file.name.lower():
            df = pd.read_excel(file, sheet_name=0,skiprows=1, engine='openpyxl')
            columns_to_keep = ['Date Captured', 'Province', 'District', 'Site/Facility ',
                                'Testing Lab', 'Transmittal Serial Number', 'Number of Results',
                                'Date Specimen Collected', 'Time Specimen Collected', 'Specimen Type',
                                'Date of Pick-up', 'Time of Pick-up', 'Date Specimen Received at Hub',
                                'Time Specimen Received at Hub',
                                'Date Specimen Received at Conventional Testing Lab',
                                'Time Specimen Received at Conventional Testing Lab',
                                'Date Results Dispatched from Conventional Testing Lab',
                                'Date Results Received/ Available at Hub',
                                'Date Results Received at Clinic', 'Comment']
            actual_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
            df = df.loc[:, actual_columns_to_keep]
            table_name='VL_TAT'
            
        elif 'vl published' in file.name.lower():
            df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
            columns_to_keep = ['Sample ID', 'Client Sample ID', 'Testing Lab', 'Referring Lab',
                                'Province', 'District', 'Facility', 'Client Contact',
                                'Client Patient ID', 'Patient Name', 'Sex', 'DOB', 'Age',
                                'Breast Feeding', 'Pregnant', 'Reason', 'Regimen', 'HIV Status',
                                'Reason For TB Exam', 'Requested By', 'Date Sampled', 'Date Registered',
                                'Date Received', 'Date Received at Hub', 'Date Rejected', 'Creator',
                                'Date of Testing', 'Date of Dispatch', 'Date Printed', 'Print Location',
                                'Registration Location', 'Analysis Profile', 'Category', 'Method',
                                'Test', 'Result', 'Comment', 'Analyst', 'Verifier', 'Review State',
                                'Retest', 'Rejection Reasons']
            actual_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
            df = df.loc[:, actual_columns_to_keep]
 
            table_name='VL_Published'
            
        elif ('received fortnight' in file.name.lower()) or  ('fortnight received' in file.name.lower()) or ('vl received fortnight' in file.name.lower()) or ('vl fortnight received' in file.name.lower()):
            df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
            columns_to_keep =       ['Sample ID' ,'Testing Lab' ,'Referring Lab','Province','District' ,'Facility'
                                      ,'Client Contact' ,'Client Patient ID' ,'Patient Name','Sex','DOB','Age'
                                      ,'Breast Feeding'   ,'Pregnant','Reason','Regimen' ,'HIV Status' ,'Reason For TB Exam'
                                      ,'Requested By' ,'Date Sampled' ,'Date Registered' ,'Date Received'  ,'Date Received at Hub'
                                      ,'Date Rejected' ,'Creator'  ,'Date of Testing','Date of Dispatch','Date Printed'
                                      ,'Print Location'  ,'Registration Location'  ,'Analysis Profile' ,'Category'
                                      ,'Method'  ,'Test' ,'Result' ,'Comment' ,'Analyst' ,'Verifier'  ,'Review State'
                                      ,'Rejection Reasons' ,'SourceFile'  ,'Client Sample ID' ,'Retest' ,'Review State.1' ,'PRE TAT'
                                      ,'YearMonth']
            actual_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
            df = df.loc[:, actual_columns_to_keep]

 
            table_name='fortnight_received'
            
        elif 'vl received' in file.name.lower():
            df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
            columns_to_keep = ['Sample ID', 'Client Sample ID', 'Testing Lab', 'Referring Lab',
                                'Province', 'District', 'Facility', 'Client Contact',
                                'Client Patient ID', 'Patient Name', 'Sex', 'DOB', 'Age',
                                'Breast Feeding', 'Pregnant', 'Reason', 'Regimen', 'HIV Status',
                                'Reason For TB Exam', 'Requested By', 'Date Sampled', 'Date Registered',
                                'Date Received', 'Date Received at Hub', 'Date Rejected', 'Creator',
                                'Date of Testing', 'Date of Dispatch', 'Date Printed', 'Print Location',
                                'Registration Location', 'Analysis Profile', 'Category', 'Method',
                                'Test', 'Result', 'Comment', 'Analyst', 'Verifier', 'Review State',
                                'Retest', 'Rejection Reasons']
            actual_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
            df = df.loc[:, actual_columns_to_keep]

            table_name='VL_Received'
        
        elif 'reject' in file.name.lower():
            df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
            columns_to_keep = ['Sample ID', 'Province', 'District', 'Facility', 'Date Sampled',
                            'Date Registered', 'Date Received', 'Sample Type', 'Reasons']
            actual_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
            df = df.loc[:, actual_columns_to_keep]
            table_name='VL_Rejected'
            
 
        df.columns = df.columns.str.replace('<', '_').str.replace('=', '_').str.replace('/', '_').str.replace('(', '_').str.replace(')', '_').str.replace('-', '_')
        df['SourceFile'] = file.name
        df['user']=user 
        df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

        df = df.applymap(limit_text_length)
        df = reduce_column_length(df)

                
        # Truncate all string columns to 254 characters
        for col in df.columns:
            if df[col].dtype == 'object' and all(isinstance(val, str) for val in df[col]):
                df[col] = df[col].str.slice(0, 254)

        # Replace empty values with NULL
        df.replace('', pd.NA, inplace=True)
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
        
        read_response="Files uploaded successfully!!!!!! "
        return read_response
    
    # except Exception as e:
    #     read_response="An error occurred during reading the file:" + str(e)
    #     return read_response


    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()

        read_response = f"An error occurred during reading_dash:\n\nType: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
        return read_response
         
    read_response='Thanx you'
    return read_response
    
    
    
def reading_Creation_reports(df,user): 
  
    try:    
        columns_to_keep=['Sample ID', 'Client Sample ID', 'Testing Lab', 'Referring Lab',
                            'Province', 'District', 'Facility', 'Client Contact',
                            'Client Patient ID', 'Patient Name', 'Sex', 'DOB', 'Age',
                            'Breast Feeding', 'Pregnant', 'Reason', 'Regimen', 'HIV Status',
                            'Reason For TB Exam', 'Requested By', 'Date Sampled', 'Date Registered',
                            'Date Received', 'Date Received at Hub', 'Date Rejected', 'Creator',
                            'Date of Testing', 'Date of Dispatch', 'Date Printed', 'Print Location',
                            'Registration Location', 'Analysis Profile', 'Category', 'Method',
                            'Test', 'Result', 'Comment', 'Analyst', 'Verifier', 'Review State',
                            'Retest', 'Rejection Reasons','SourceFile','Status']
        # Get the intersection of columns_to_keep and the actual columns in the dataframe
        actual_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
        # Subset the dataframe
        df = df.loc[:, actual_columns_to_keep]
        #df = df.loc[:, columns_to_keep]        

        df.columns = df.columns.str.replace('<', '_').str.replace('=', '_').str.replace('/', '_').str.replace('(', '_').str.replace(')', '_').str.replace('-', '_')
        df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
        # Replace empty values with NULL
        df.replace('', pd.NA, inplace=True)
        df = df.applymap(limit_text_length)
        df = reduce_column_length(df)
        # Iterate over the columns of the DataFrame
        for col in df.columns:
            # Check if the column's data type is 'object' and contains string values
            if df[col].dtype == 'object' and df[col].apply(lambda x: isinstance(x, str)).all():
                df[col] = df[col].str.slice(0, 254)
                
        df['user']=user 
        df['update_date']=datetime.now()

        df.to_sql('Creation_Reports_Hub_Lab', con=engine, if_exists='append', index=False, chunksize=10000)
        read_response = f"Uploaded {len(df)} records successfully."
        return read_response

    except Exception as e:
        read_response = "An error occurred during reading the file: " + str(e)
        # Consider logging the error for debugging purposes
        # logger.error(read_response)
        return read_response       
        
     
     
         
    read_response='Thanx you'
    return read_response
    
