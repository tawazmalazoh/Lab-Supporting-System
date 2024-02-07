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





    # Load Data for dashboard in database

server = 'mssql-133539-0.cloudclusters.net,17983'
database = 'LSS'
username = 'admin'
password = 'Adm!n123'
driver = 'ODBC Driver 17 for SQL Server'

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

def replace_characters(df):
    new_columns = [re.sub("[<>'=.:]", "_", col) for col in df.columns]
    df.columns = new_columns
    return df




def generate_unique_key(lab_name,date):
    # Generate a random number between 1000 and 9999
    random_number = random.randint(1000, 9999)
    formated_time=datetime.now().strftime("%H:%M:%S")
    # Get the current date
    current_date = date

    # Combine the random number, lab name, and date
    unique_key = f"{random_number}_{lab_name}_{current_date}_{formated_time}"

    return unique_key

# Successful upload directory
#backup_path ='C:/Users/Server/OneDrive - Biomedical Research and Training Institute/M and E/CDC BRTI Weekly Reports/Dashboard Submissions/COP22'
backup_path='G:/My Drive/1_VL -EID-TAT-COVID'
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


         

def reading_dash(file):
    try:
        
                # Handle InMemoryUploadedFile
        # if isinstance(file, InMemoryUploadedFile):
        #     file = io.BytesIO(file.read())
            
        #Lets read data in different sheets
        df = pd.read_excel(file, sheet_name=1, engine='openpyxl')

        df_week_samp_received = pd.read_excel(file, sheet_name=3, engine='openpyxl')
        df_Age_Sex_Disagg = pd.read_excel(file, sheet_name=4,skiprows=1, engine='openpyxl')              
        df_Reffered_Samples = pd.read_excel(file, sheet_name=5, engine='openpyxl')                
        df_Sample_Run =pd.read_excel(file, sheet_name=6,skiprows=1, engine='openpyxl')  

        df_Testing_Capacity =pd.read_excel(file, sheet_name=7, engine='openpyxl')
        df_Lab_Metrics_Waste_Mgt =pd.read_excel(file, sheet_name=8, engine='openpyxl')
        df_LIMS_Functionality =pd.read_excel(file, sheet_name=9, engine='openpyxl')

        df_CLI =pd.read_excel(file, sheet_name=12,skiprows=1, engine='openpyxl')
        df_QMS =pd.read_excel(file, sheet_name=13, engine='openpyxl')
        df_Power_Outage =pd.read_excel(file, sheet_name=14, engine='openpyxl')
        
        
    
        #Extract the File Name 
        #file_name = os.path.basename(file)
        file_name = file.name if isinstance(file, InMemoryUploadedFile) else os.path.basename(file)
        # print(f'this is the file name:{file_name}')
        # print(f'this is the df: {df.head()}')
        
        
        #lets now begin the cleaning up of the data 
                       
        #1: FORMATING CARRYOVER
        df.columns.values[0] = 'Date'  
        df.columns.values[1] = 'Name_of_Lab'
        df.columns.values[2] = 'Sample_Type'
        df.columns.values[3] = 'Test_Type'
        df.columns.values[4] = 'NEVERTESTED_Samples_in_Lab'
        df.columns.values[5] = 'FAILED_Samples_in_Lab'
        df.columns.values[6] = 'BACKLOG_Samples_intraTAT_7mo'
        df.columns.values[7] = 'CARRYOVER_Samples_urgent'
        df.columns.values[8] = 'CARRYOVER_Samples_rebleeds'
        df.columns.values[9] = 'CARRYOVER_Samples_rejected'
        df.columns.values[10] = 'REJECTED_Quality_issue'
        df.columns.values[11] = 'REJECTED_Quantity_insuff'
        df.columns.values[12] = 'REJECTED_Patient_SampleINFO'
        df.columns.values[13] = 'REJECTED_Missing_requestForm'
        df.columns.values[14] = 'REJECTED_Sample_Missing'
        df.columns.values[15] = 'Days_for_OLDEST_CarryoverSample'
        df.columns.values[16] = 'Days_for_YOUNGEST_CarryoverSample'
        df.columns.values[17] = 'NUMBER_carryover_sample_TOO_OLD_test'
        df.columns.values[18] = 'NUMBER_carryover_samples_in_LIMS'
        df.columns.values[19] = 'Carry_Over_Sample_LIMS_Backlog_tobelogged'
        df.columns.values[20] = 'comment'
        #df.columns.values[21] = 'Data_Quality_Checks'

        new_names = ['Date', 'Name_of_Lab', 'Sample_Type','Test_Type','NEVERTESTED_Samples_in_Lab','FAILED_Samples_in_Lab',
                        'BACKLOG_Samples_intraTAT_7mo','CARRYOVER_Samples_urgent','CARRYOVER_Samples_rebleeds','CARRYOVER_Samples_rejected'
                    ,'REJECTED_Quality_issue','REJECTED_Quantity_insuff','REJECTED_Patient_SampleINFO','REJECTED_Missing_requestForm'
                    ,'REJECTED_Sample_Missing','Days_for_OLDEST_CarryoverSample','Days_for_YOUNGEST_CarryoverSample','NUMBER_carryover_sample_TOO_OLD_test'
                    ,'NUMBER_carryover_samples_in_LIMS','Carry_Over_Sample_LIMS_Backlog_tobelogged','comment']
        df.columns = new_names
        df = convert_excel_date(df, 'Date')
        
        df['SourceFile']=file_name
        
        lab=df['Name_of_Lab'][0]
        #Assigning Unique Key
        unique_key = generate_unique_key(df['Name_of_Lab'][0], df['Date'][0])
        df['unique_key']=unique_key
        df.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset = df.iloc[:, :4]
        df = df.dropna(subset=subset.columns, how='all')
        
        let=df.iloc[:, 3:-1]
        df = df.dropna(subset=let.columns, how='all')
        # Append the additional columns to the list
        new_names.extend(['SourceFile', 'unique_key'])
        # Keep only the columns in new_names and droping anything
        df = df.loc[:, new_names]
        #df.drop('Data_Quality_Checks',axis=1, inplace=True) #dropping the added columns
        
        #print( df['Date'][0])
        
        #2: FORMATING THIS WEEKS SAMPLE RECEIVED
        week_new_columns = {
                    'Date\n(Month/Day/Year)': 'Date',
                    'Lab\n(Name of Lab)': 'Name_of_Lab',
                    'Sample type\n(DBS/Plasma)': 'Sample_Type',
                    'Test type\n(VL/EID)': 'Test_Type',
                    'Total number of samples received ': 'Total_samples_received',
                    'Number of urgent samples received': 'Urgent_Samples_received',
                    'Number of samples that are rebleeds ': 'Num_ReBleed_Samples',
                    'Weekly sample receipt target': 'Weekly_sample_receipt_target',
                    'Number of samples rejected ': 'Num_Rejected_Samples',
                    'Weekly rejection rate maximum threshold': 'weekly_max_threshhold',
                    'Number of samples rejected: sample quality compromised': 'REJECTED_Quality_issue',
                    'Number of samples rejected: sample quantity insufficient': 'REJECTED_Quantity_insuff',
                    'Number of samples rejected: patient and sample information inconsistent': 'REJECTED_Patient_SampleINFO',
                    'Number of samples rejected: request form missing': 'REJECTED_Missing_requestForm',
                    'Number of samples rejected: sample missing': 'REJECTED_Sample_Missing',
                    'Number of samples  entered into LIMS on day of arrival': 'Num_Samples_entered_LIMSonArrival',
                    'LIMS Backlog (number of samples yet to be entered)': 'LIMs_Backlog_yetTObeEntered',
                    'Comments: [Please input any comments regarding  samples received and  samples rejections] ': 'comments'
                }

        df_week_samp_received.rename(columns=week_new_columns, inplace=True) 
        
        df_week_samp_received = convert_excel_date(df_week_samp_received, 'Date')
        df_week_samp_received['SourceFile']=file_name
        df_week_samp_received['unique_key']=unique_key
        df_week_samp_received.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_wk = df_week_samp_received.iloc[:, :4]
        df_week_samp_received = df_week_samp_received.dropna(subset=subset_wk.columns, how='all')
        
        lets=df_week_samp_received.iloc[:, 3:-1]
        df_week_samp_received = df_week_samp_received.dropna(subset=lets.columns, how='all')
        #df_week_samp_received.drop('Data Quality Checks',axis=1, inplace=True)

        week_new_columns['SourceFile'] = 'SourceFile'
        week_new_columns['unique_key'] = 'unique_key'       
        df_week_samp_received = df_week_samp_received[list(week_new_columns.values())]
        
        
        
        
        #3: CLEANING AGE_SEX_AGGREGATION TAB
        Age_Newcolumns = {
            'Date\n(Month/Day/Year)': 'Date',
            'Lab\n(Name of Lab)': 'Name_of_Lab',
            '<15': 'Males_<15',
            '15-19': 'Males_15_19',
            '20-24': 'Males_20_24',
            '25-49': 'Males_25_49',
            '50+': 'Males_>50',
            'Unknown ': 'Males_Unknown',
            
            '<15.1': 'Fem_Non_PBFW_<15',
            '15-19.1': 'Fem_Non_PBFW_15_19',
            '20-24.1': 'Fem_Non_PBFW_20_24',
            '25-49.1': 'Fem_Non_PBFW_25_49',
            '50+.1':'Fem_Non_PBFW_>50',                    
            'Unknown .1': 'Fem_Non_PBFW_Unknown',
            
            '<15.2': 'Female_PBFW_<15' ,
            '15-19.2':'Female_PBFW_15_19' ,
            '20-24.2':'Female_PBFW_20_24',
            '25-49.2':'Female_PBFW_25_49' ,
                '50+.2':'Female_PBFW_>50',                    
            'Unknown .2':'Female_PBFW_Unknown' ,
            
            '<15.3': 'Unknown_Sex_<15',
            '15-19.3': 'Unknown_Sex_15_19',
            '20-24.3':'Unknown_Sex_20_24' ,
            '25-49.3':'Unknown_Sex_25_49' ,
            '50+.3':'Unknown_Sex_>50',
            'Unknown .3': 'Unknown_Sex_Unknown',
            'Unnamed: 26': 'Comments'
        }

        df_Age_Sex_Disagg.rename(columns=Age_Newcolumns, inplace=True)      
        df_Age_Sex_Disagg = convert_excel_date(df_Age_Sex_Disagg, 'Date')
        df_Age_Sex_Disagg['SourceFile']=file_name
        df_Age_Sex_Disagg['unique_key']=unique_key
        df_Age_Sex_Disagg.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_Age = df_Age_Sex_Disagg.iloc[:, :4]
        df_Age_Sex_Disagg = df_Age_Sex_Disagg.dropna(subset=subset_Age.columns, how='all')
        
        letsd=df_Age_Sex_Disagg.iloc[:, 3:-1]
        df_Age_Sex_Disagg = df_Age_Sex_Disagg.dropna(subset=letsd.columns, how='all')
        
        Age_Newcolumns['SourceFile'] = 'SourceFile'
        Age_Newcolumns['unique_key'] = 'unique_key'
        df_Age_Sex_Disagg = df_Age_Sex_Disagg[list(Age_Newcolumns.values())]
        #df_Age_Sex_Disagg.drop('Unnamed: 27',axis=1, inplace=True)
        
        
        #4: CLEANING REFFERED 
        Reffered_Newcolumns={'Date\n(Month/Day/Year)':'Date', 
                                'Lab\n(Name of Lab)':'Name_of_Lab',
                            'Sample type\n(DBS/Plasma)':'Sample_Type',
                                'Test type\n(VL/EID)':'Test_Type',
                                'Total number of samples referred out':'Samples_reffered_Out',
                                'Lab Samples Referred to':'Lab_Samples_referred_to',
                                'Total number of referred samples received':'Referred_Sample_Received',
                                'Number of referred samples received that were carry-overs':'CARRYOVER_Referred_Sample_Received',
                                'Number of referred samples received that were not carry-overs (collected / sent for testing this week)':'NOT_CARRYOVER_Referred_Sample_Received',
                                'Referred from':'Referred_From',
                                'Number of referred samples marked as urgent':'URGENT_Referred_Samples',
                                'Total number of referred samples that are rebleeds':'REBLEED_Referred_Samples',
                                'Total number of referred ultimately rejected':'REJECTED_Referred_Samples',
                                'Number of samples referred due to reagent/kit stockout ':'REFERRED_Reagent_Stockout',
                                'Number of samples referred due to instrument failure':'REFERRED_Instrument_Failure',
                                'Number of samples referred due to insufficient HR capacity':'REFERRED_Insuff_HR_Capacity',
                                'Number of samples referred due to insufficient instrument capacity ':'REFERRED_Insuff_Instrument_Capacity',
                                'Number of Samples captured through LIMS referral module':'Samples_Captured_thru_LIMS', 'Comments':'Comments'}

        df_Reffered_Samples.rename(columns=Reffered_Newcolumns, inplace=True)
        #df_Reffered_Samples = convert_excel_date(df_Reffered_Samples, 'Date')
        df_Reffered_Samples['SourceFile']=file_name
        df_Reffered_Samples['unique_key']=unique_key
        df_Reffered_Samples.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_Reff = df_Reffered_Samples.iloc[:, :4]
        df_Reffered_Samples = df_Reffered_Samples.dropna(subset=subset_Reff.columns, how='all')
        
        letsdr=df_Reffered_Samples.iloc[:, 3:-1]
        df_Reffered_Samples = df_Reffered_Samples.dropna(subset=letsdr.columns, how='all')
        
        Reffered_Newcolumns['SourceFile'] = 'SourceFile'
        Reffered_Newcolumns['unique_key'] = 'unique_key'
        df_Reffered_Samples = df_Reffered_Samples[list(Reffered_Newcolumns.values())]
        

        #df_Reffered_Samples.drop('Data Quality Checks',axis=1, inplace=True)
        
        
        
        #5 TESTING CAPACITY
        Capacity_Newcolumns={'Date\n(Month/Day/Year)':'Date',
                            'Lab\n(Name of Lab)':'Name_of_Lab', 
                            'Test type\n(VL/EID)':'Test_Type',
                            'Platform (Roche, Abbott, Hologic, BMX)':'Platform_Roche_Abbott_Hologic_BMX',
                        'Weekly program assigned target ':'Target_Weekly_assigned',
                        'Weekly tests done (Valid Results, do not include failed tests)':'VALID_Weekly_Tests_done' ,
                        'Tests needed to hit weekly target':'TEST_to_HIT_Weekly_Targets',
                        'Reagent kits received from NatPharm (in the reporting week)':'NatPharm_Kits_Received_inThisWK',
                        'Date Received at Lab':'Date_Received_at_Lab',
                        'Reagent tests kits loaned out to other labs':'Reagent_kits_to_OTHER_Labs',
                        'Loaned to (Lab Name)':'Lab_Name_Loaned_to',
                        'Swift Consignment Number (Reagents Transported)':'Reagents_Transported_ConsgnNum',
                        'Reagent tests kits received on loan from other labs':'Reagent_kits_RECEIVED_from_OTHER_Labs',
                        'Received from (Lab Name)':'Lab_Name_Received_from',  
                        'Reagent tests kits Stock on hand':'Reagent_tests_kits_Stock_on_hand', 
                        'Reagent tests kits  available':'Reagent_tests_kits_available', 
                        'Reagent tests kits  available Batch/lot Number':'Reagent_tests_kits_available_Batch_lot_Number',
                        'Reagent tests kits  available Expiry Date':'Reagent_tests_kits_available_Expiry_Date',
                        'Reagent stockout (days)':'Reagent_stockout_days',
                        'Tests expired this month before use':'Tests_expired_this_month_before_use',
                        'Cost of transportation (swift or BRTI)':'Cost_of_transportation',
                        'Comments Reagent Stock Status':'Comments_Reagent_Stock_Status'}

        df_Testing_Capacity.rename(columns=Capacity_Newcolumns, inplace=True)
        
        df_Testing_Capacity = convert_excel_date(df_Testing_Capacity, 'Date')
        df_Testing_Capacity['SourceFile']=file_name
        df_Testing_Capacity['unique_key']=unique_key
        df_Testing_Capacity.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        # subset_Capacity = df_Testing_Capacity.iloc[:, :4]
        # df_Testing_Capacity = df_Testing_Capacity.dropna(subset=subset_Capacity.columns, how='all')
        
        # letsdrr=df_Testing_Capacity.iloc[:, 3:-1]
        # df_Testing_Capacity = df_Testing_Capacity.dropna(subset=letsdrr.columns, how='all')
        
        Capacity_Newcolumns['SourceFile'] = 'SourceFile'
        Capacity_Newcolumns['unique_key'] = 'unique_key'
        df_Testing_Capacity = df_Testing_Capacity[list(Capacity_Newcolumns.values())]
        #print(df_Testing_Capacity.columns)

 
        
        

        
        #6: LAB METRICS AND WASTE MGT
        Metrics_NewColumns={'Date\n(Month/Day/Year)':'Date',
                            'Lab\n(Name of Lab)':'Name_of_Lab', 
                            'Test type\n(VL/EID)':'Test_Type',
                        'Platform (Roche, Abbott, Hologic, BMX)':'Platform_Roche_Abbott_Hologic_BMX' ,
                        'Weekly 8-hour test capacity (0% downtime)':'Weekly_8hour_test_capacity',
                        'Planned number of days platform used':'Planned_number_ofdays_platform_used',
                        'Actual number of days platform used':'Actual_number_of_days_platform_used', 
                        'Weekly planned number of tests':'Weekly_planned_number_of_tests',
                        'Total number of machine breakdowns ':'Num_Machine_Breakdowns',
                        'Number of hours in a shift (e.g 8hr, 12hr, 16hr or 24 hr shift)': 'Hrs_in_Shift',
                        'Downtime (hours lost) due to power outage':'Downtime_Power_Outage',
                        'Downtime (hours lost) due to Technical/Mechanical Failure':'Downtime_Mechanical_Failure',
                        'Downtime (hours lost) due to reagent stock out / expiry ':'Downtime_Reagent_Stockout_Expiry',
                        'Downtime (hours lost) due to staff unavailability':'Downtime_Staff_Unavailability',
                        'Total operational hours ':'Total_Operational_Hrs',
                        'Tests needed to be done (samples received + carry over)':'Tests_tobe_Done_Received&Carryover',
                        'Tests completed (total number of successfully tested samples)':'Tests_Successfuly_completed',
                        'Amount of Liquid Waste (Litres)':'Amt_of_Liquid_Waste_ltrs',
                        'Comments (include specific error codes for mechanical failure)':'Comments_on_ErrorCodes_for_Mach_failure'}

        df_Lab_Metrics_Waste_Mgt.rename(columns=Metrics_NewColumns, inplace=True)

        df_Lab_Metrics_Waste_Mgt = convert_excel_date(df_Lab_Metrics_Waste_Mgt, 'Date')
        df_Lab_Metrics_Waste_Mgt['SourceFile']=file_name
        df_Lab_Metrics_Waste_Mgt['unique_key']=unique_key
        df_Lab_Metrics_Waste_Mgt.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_Metrics = df_Lab_Metrics_Waste_Mgt.iloc[:, :4]
        df_Lab_Metrics_Waste_Mgt = df_Lab_Metrics_Waste_Mgt.dropna(subset=subset_Metrics.columns, how='all')
        
        letsdrroo=df_Lab_Metrics_Waste_Mgt.iloc[:, 3:-1]
        df_Lab_Metrics_Waste_Mgt = df_Lab_Metrics_Waste_Mgt.dropna(subset=letsdrroo.columns, how='all')
        
        Metrics_NewColumns['SourceFile'] = 'SourceFile'
        Metrics_NewColumns['unique_key'] = 'unique_key'
        df_Lab_Metrics_Waste_Mgt = df_Lab_Metrics_Waste_Mgt[list(Metrics_NewColumns.values())]
        

        #df_Lab_Metrics_Waste_Mgt.drop('Data Quality Checks',axis=1, inplace=True)
        
        
        # LIMS FUNCTIONALITY
        LIMS_FuncNewColumns={ 'Date\n(Month/Day/Year)':'Date', 'Lab\n(Name of Lab)':'Name_of_Lab',
                        'Hours of Functionality ':'Hours_of_Functionality', 
                            'Time the downtime occurred e.g. 10:15am ':'Time_Of_Downtime',
                        'Hours of Downtime': 'Hours_of_Downtime', 
                            'Number of Downtime Instants':'Number_of_Downtime_Instants',
                        'Reason for downtime':'Reason_for_downtime',
                        'Time between LIMS going down and initiation of downtime procedures (hours)':'TimeHrs_btwn_LIMSDOWNTIME_INITIATIONofDowntime',
                        'Time between LIMS going down and LIMS technician responding to issue (hours)':'TimeHrs_btwn_LIMSDOWNTIME_LIMSTECHNICIAN_responding',
                        'Time between LIMS going down and resolution of issue (hours)':'TimeHrs_btwn_LIMSDOWNTIME_RESOLUTION',
                        'Comments ':'Comments'}
        df_LIMS_Functionality.rename(columns=LIMS_FuncNewColumns, inplace=True)
        #df_LIMS_Functionality = convert_excel_date(df_LIMS_Functionality, 'Date')
        df_LIMS_Functionality['SourceFile']=file_name
        df_LIMS_Functionality['unique_key']=unique_key
        df_LIMS_Functionality.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_Fxns = df_LIMS_Functionality.iloc[:, :4]
        df_LIMS_Functionality = df_LIMS_Functionality.dropna(subset=subset_Fxns.columns, how='all')
        
        letsdrroop=df_LIMS_Functionality.iloc[:, 3:-1]
        df_LIMS_Functionality = df_LIMS_Functionality.dropna(subset=letsdrroop.columns, how='all')
        
        LIMS_FuncNewColumns['SourceFile'] = 'SourceFile'
        LIMS_FuncNewColumns['unique_key'] = 'unique_key'
        df_LIMS_Functionality = df_LIMS_Functionality[list(LIMS_FuncNewColumns.values())]
        

        


        
        #SAMPLE RUN
        Run_NewColumns={'Date\n(Month/Day/Year)':'Date',
                            'Lab\n(Name of Lab)':'Name_of_Lab',
                        'Sample Type\n(DBS/Plasma)': 'Sample_Type', 
                        'Test Type\n(VL/EID)':'Test_Type',
                        'Platform (Roche, Abbott, Hologic, BMX)':'Platform_Roche_Abbott_Hologic_BMX',
                        'Total number of carry-over samples run':'CARRYOVER_Samples_RUN',
                        'Number of urgent carry-over samples run':'CARRYOVER_URGENT_Samples_RUN',
                        'Number of carry-over rebleeds run':'CARRYOVER_reBleeds_RUN',
                        'Number of failed carry-over samples eligible for repeat':'CARRYOVER_FAILED_samples_elig_repeat',
                        'Number of failed carry-over samples not eligible for repeat':'CARRYOVER_FAILED_samples_Ntelig_repeat',
                        'Number of carry-over repeats run':'CARRYOVER_repeats_RUN',
                        'Total number of failed carry-over samples after final repeat testing':'failed_CARRYOVER_samplesafter_finalrepeat_testing',
                        'Number of failed tests: sample quality/quantity issues ':'CARRYOVER_FAILED_quality_quantity_issues',
                        '# of failed tests due to reagent quality issues ':'CARRYOVER_FAILED_reagent_quality_issues',
                        'Number of failed tests: QC failure ':'CARRYOVER_FAILED_QC_failure',
                        'Number of failed tests: power failure':'CARRYOVER_FAILED_power_failure',
                        'Number of failed tests: mechanical failure ':'CARRYOVER_FAILED_mechanical_failure',
                        'Number of failed carry over tests: sample processing error':'FAILED_CARRYOVER_sample_processing_error',

                        'Other, Specify':'CARRYOVER_OTHER', 

                        'Number of results printed from LIMS by VL Lab':'CARRYOVER_Results_printed_from_LIMS_by_VL_Lab',
                        'Number of results dispatched by lab':'CARRYOVER_Results_dispatched_by_lab', 


                        'Total number of samples run':'RECEIVED_TOTAL_Sample_RUN',
                        'Number of urgent samples run':'RECEIVED_URGENT_Sample_RUN',
                        'Number of rebleeds run':'RECEIVED_REBLEEDS_RUN',
                        'Total number of samples failed but eligible for repeat':'RECEIVED_FAILED_bt_Elig_REPEAT',
                        'Total number of samples failed not eligible for repeat':'RECEIVED_FAILED_bt_NOT_Elig_REPEAT',
                        'Total number of repeats run':'RECEIVED_REPEATS_RUN',
                        'Total number of samples failed after final repeat testing':'RECEIVED_FAILED_after_FINAL_repeat_testing',
                        'Number of failed tests: sample quality / quantity issues ':'RECEIVED_FAILED_quality_quantity_issues',
                        '# of failed tests due to reagent quality issues .1':'RECEIVED_FAILED_reagent_quality_issues',
                        'Number of failed tests: QC failure .1':'RECEIVED_FAILED_QC_failure',
                        'Number of failed tests: power failure.1':'RECEIVED_FAILED_power_failure',
                        'Number of failed tests: mechanical failure .1':'RECEIVED_FAILED_mechanical_failure',
                        'Number of failed tests: sample processing error':'FAILED_RECEIVED_sample_processing_error', 

                            'Other, Specify.1':'RECEIVED_OTHER',

                        'Number of results entered into LIMS':'RECEIVED_Entered_In_LIMS',
                        'Number of results printed from LIMS  by VL lab':'RECEIVED_Results_printed_from_LIMS_by_VL_Lab',
                        'Number of results  Dispatched / Accessed Using Online Link ':'RECEIVED_Dispatched_Accessed_by_Online_Link',
                        'Number of results dispatched by lab.1':'RECEIVED_Results_dispatched_by_lab',


                        'Total number of samples run.1':'REFERRED_Samples_RUN',
                        'Number of urgent samples run.1':'REFERRED_Urgent_Samples_RUN',
                        'Number of rebleeds run.1':'REFERRED_ReBLEEDs_RUN',
                        'Total number of samples failed but eligible for repeat.1':'REFERRED_FAILED_bt_Elig_REPEAT',
                        'Total number of samples failed not eligible for repeat.1':'REFERRED_FAILED_bt_NOT_Elig_REPEAT',
                        'Total number of repeats run.1':'REFERRED_REPEATS_RUN',
                        'Total number of samples failed after final repeat testing.1':'REFERRED_FAILED_after_FINAL_repeat_testing',
                        'Number of failed tests: sample quality / quantity issues .1':'REFERRED_FAILED_quality_quantity_issues',
                        '# of failed tests due to reagent quality issues .2':'REFERRED_FAILED_reagent_quality_issues',
                        'Number of failed tests: power failure.2':'REFERRED_FAILED_power_failure',
                        'Number of failed tests: mechanical failure .2':'REFERRED_FAILED_mechanical_failure',
                        'Number of failed tests: sample processing error.1':'FAILED_REFERRED_sample_processing_error',

                        'Other, Specify.2':'REFERRED_OTHER',
                        'Comments':'Comments'
                        }


        df_Sample_Run.rename(columns=Run_NewColumns, inplace=True)
        df_Sample_Run = convert_excel_date(df_Sample_Run, 'Date')
        df_Sample_Run['SourceFile']=file_name
        df_Sample_Run['unique_key']=unique_key
        df_Sample_Run.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_run = df_Sample_Run.iloc[:, :4]
        df_Sample_Run = df_Sample_Run.dropna(subset=subset_run.columns, how='all')
        
        lets_drop_other=df_Sample_Run.iloc[:, 3:-1]
        df_Sample_Run = df_Sample_Run.dropna(subset=lets_drop_other.columns, how='all')
        
        Run_NewColumns['SourceFile'] = 'SourceFile'
        Run_NewColumns['unique_key'] = 'unique_key'
        df_Sample_Run = df_Sample_Run[list(Run_NewColumns.values())]


        series1=df_Sample_Run.loc[:,'REFERRED_OTHER']
        series2=df_Sample_Run.loc[:,'RECEIVED_OTHER']
        series3=df_Sample_Run.loc[:,'CARRYOVER_OTHER']

        Commemnts_not_in_ProperSection=int(any([(series1.dtype != 'int64'), (series2.dtype != 'int64'), (series3.dtype != 'int64')]))

        
        
        
        #CLI SHEET
        CLI_NewColumns={'Date\n(Month/Day/Year)':'Date', 
                        'Lab\n(Name of Lab)':'Name_of_Lab',
                        'Number of rebleed requests sent':'ReBLEEDs_requests_sent',
                        'Number of incidents requiring communication between lab and clinics ':'Incidents#_nid_Comms_btwn_LAB_CLINICS',
                        'Number of correspondences ':'Number_of_correspondences',
                        'Number of action items requiring follow-up post-correspondence':'Post_Corresp#_nid_Followups',
                        'Number of action items performed ':'Action_items_performed',
                        'Unresolved missing results from last week':'LAST_WEEK_Unresolved_missing_results',
                        'Number of missing results requested by facilities this week':'THIS_WEEK_Missing_results_req_by_Facilities',
                        
                        'Missing results outcomes:\nResults found and shared with facilities':'RESOLVED_Missing_results_outcomes_found_Shared_with_facilities',
                        'LIMS Interface Failed':'RESOLVED_LIMS_Interface_Failed',
                        'Results not documented at facility':'RESOLVED_Results_not_documented_at_facility',
                        'Results sent to wrong facility':'RESOLVED_Results_sent_to_wrong_facility',
                        'Results not yet dispatched/printed':'RESOLVED_Results_not_yet_dispatched_printed',

                        'Specimen not received: rebleed sent':'RESOLVED_Specimens_not_received_rebleed_sent',
                        'Specimen rejected':'RESOLVED_Specimens_rejected',
                        #'Specimens rejected':'RESOLVED_Specimens_rejected',
                        'Result pending publishing':'RESOLVED_Result_pending_publishing', 
                        
                            'Pending testing':'UNRESOLVED_Pending_testing',
                            'Referred - awaiting results':'UNRESOLVED_Referred_awaiting_results', 
                            'Investigation in progress':'UNRESOLVED_Investigation_in_progress'}

        df_CLI.rename(columns=CLI_NewColumns, inplace=True)
        #df_CLI = convert_excel_date(df_CLI, 'Date')
        df_CLI['SourceFile']=file_name
        df_CLI['unique_key']=unique_key
        df_CLI.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_cli = df_CLI.iloc[:, :4]
        df_CLI = df_CLI.dropna(subset=subset_cli.columns, how='all')
        
        letsdrroopu=df_CLI.iloc[:, 3:-1]
        df_CLI = df_CLI.dropna(subset=letsdrroopu.columns, how='all')
        
        CLI_NewColumns['SourceFile'] = 'SourceFile'
        CLI_NewColumns['unique_key'] = 'unique_key'
        df_CLI = df_CLI[list(CLI_NewColumns.values())]
        

        #df_CLI.drop('Data Quality Checks',axis=1, inplace=True)
        
        
        #QMS SHEET
        QMS_NewColumns={'Date\n(Month/Day/Year) ':'Date', 
                    'Lab Name ':'Lab_name',
                    'Date of Recent  Audit ':'Date_of_Recent_Audit',
                    'Audit Type (SADCAS)':'Audit_Type_SADCAS',
                    'Total NCs from recent Audit ':'Total_NCs_from_recent_Audit', 
                    'NCs closed this week ':'NCs_closed_this_week',
                    'NCs not yet closed ':'NCs_not_yet_closed', 
                    'Number of NCs Submitted to SADCAS':'NCs_Submitted_to_SADCAS',
                    'SADCAS Feedback Received (Date)':'SADCAS_Feedback_Received_Date', 
                        'Number of NCs Cleared by SADCAS':'NCs_Cleared_by_SADCAS',
                    'Number of NCs awaiting feedback from SADCAS':'NCs_awaiting_feedback_from_SADCAS',
                    'Number of NCs Requiring Resubmission':'NCs_Requiring_Resubmission',
                        'Follow-Up Resubmission (Date)':'Follow_Up_Resubmission_Date',
                    'Cleared by SADCAS(Yes/No)':'Cleared_by_SADCAS_Yes_No'}

        df_QMS.rename(columns=QMS_NewColumns, inplace=True)
        df_QMS = convert_excel_date(df_QMS, 'Date')
        df_QMS['SourceFile']=file_name
        df_QMS['unique_key']=unique_key
        df_QMS.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_qms = df_QMS.iloc[:, :4]
        df_QMS = df_QMS.dropna(subset=subset_qms.columns, how='all')
        
        letsdrroopuQ=df_QMS.iloc[:, 3:-1]
        df_QMS = df_QMS.dropna(subset=letsdrroopuQ.columns, how='all')
        
        QMS_NewColumns['SourceFile'] = 'SourceFile'
        QMS_NewColumns['unique_key'] = 'unique_key'
        df_QMS = df_QMS[list(QMS_NewColumns.values())]
        

        
        #POWER OUTAGE
        Power_Newcolumns={'Date\n(Month/Day/Year)':'Date', 
                            'Number of hours with no electricity ':'Hours_with_no_electricity',
                            'Number of hours generator was on ':'Hours_generator_was_on',
                            'Litres of fuel added to generator ':'Fuel_ltrs_added_to_generator',
                            'Number of hours machine/s was not being used due to power cut ':'Hrs_Machine_idle_coz_PowerCuts',
                            'Total  Tests done using generator':'Total_Tests_done_using_generator'}

        df_Power_Outage.rename(columns=Power_Newcolumns, inplace=True)
        df_Power_Outage = convert_excel_date(df_Power_Outage, 'Date')
        df_Power_Outage['SourceFile']=file_name
        df_Power_Outage['unique_key']=unique_key
        df_Power_Outage.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_power = df_Power_Outage.iloc[:, :4]
        df_Power_Outage = df_Power_Outage.dropna(subset=subset_power.columns, how='all')
        
        letsdrroopuQP=df_Power_Outage.iloc[:, 3:-1]
        df_Power_Outage = df_Power_Outage.dropna(subset=letsdrroopuQP.columns, how='all')
        
        Power_Newcolumns['SourceFile'] = 'SourceFile'
        Power_Newcolumns['unique_key'] = 'unique_key'
        df_Power_Outage = df_Power_Outage[list(Power_Newcolumns.values())]
        
 
        
        
        
        #LETS IMPLEMENT THE CHECKS AT THIS LEVEL

        
        #1: CLI CHECK Internal-RESOLVED (K-Q = J)-------------------------------
        xx=df_CLI[['RESOLVED_Missing_results_outcomes_found_Shared_with_facilities']].values
        zzz=df_CLI[['RESOLVED_LIMS_Interface_Failed','RESOLVED_Results_not_documented_at_facility','RESOLVED_Results_sent_to_wrong_facility','RESOLVED_Results_not_yet_dispatched_printed','RESOLVED_Specimens_not_received_rebleed_sent','RESOLVED_Specimens_rejected','RESOLVED_Result_pending_publishing']].astype(float).sum(axis=1).values
        
        if np.any( xx[~np.isnan(xx)] == zzz[~np.isnan(zzz)] ):
            cli_internal_error = 0
        else:
            # Handle the case when the arrays are not equal
            cli_internal_error = 1
            
            #-----------------CLI- ---------------------   
        JRST=df_CLI[['RESOLVED_Missing_results_outcomes_found_Shared_with_facilities','UNRESOLVED_Pending_testing','UNRESOLVED_Referred_awaiting_results','UNRESOLVED_Investigation_in_progress']].astype(float).sum(axis=1).values 
        HI=df_CLI[['LAST_WEEK_Unresolved_missing_results','THIS_WEEK_Missing_results_req_by_Facilities']].astype(float).sum(axis=1).values
        if np.any( JRST== HI ):

            cli_internal_error_2=0

        else:
            cli_internal_error_2=1

        #===Column P  must not be empty
        age_of_oldestsample_empty=any(df['Days_for_OLDEST_CarryoverSample'].isnull())   
            
        
            
        #2: CARRYOVER CHECKS-----------------------------------
        carry=df.query("Sample_Type == 'Plasma' and Test_Type == 'VL'")[['NEVERTESTED_Samples_in_Lab', 'FAILED_Samples_in_Lab']].astype(float).sum(axis=1).sum()
        run=df_Sample_Run.query("Sample_Type == 'Plasma' and Test_Type == 'VL'")[['CARRYOVER_Samples_RUN','RECEIVED_TOTAL_Sample_RUN','REFERRED_Samples_RUN']].astype(float).sum(axis=1).sum()
        ref_out=df_Reffered_Samples.query("Sample_Type == 'Plasma' and Test_Type == 'VL'")['Samples_reffered_Out'].sum()
        ref_rec=df_Reffered_Samples.query("Sample_Type == 'Plasma' and Test_Type == 'VL'")['Referred_Sample_Received'].sum()
        this_I=df_week_samp_received.query("Sample_Type == 'Plasma' and Test_Type == 'VL'")['Num_Rejected_Samples'].sum()
        this_E=df_week_samp_received.query("Sample_Type == 'Plasma' and Test_Type == 'VL'")['Total_samples_received'].sum()
        CARRYOVER_WEEK_START_Plasma_VL=carry+run+ref_out-ref_rec+this_I-this_E
        
        dbscarry=df.query("Sample_Type == 'DBS' and Test_Type == 'VL'")[['NEVERTESTED_Samples_in_Lab', 'FAILED_Samples_in_Lab']].astype(float).sum(axis=1).sum()
        DBSrun=df_Sample_Run.query("Sample_Type == 'DBS' and Test_Type == 'VL'")[['CARRYOVER_Samples_RUN','RECEIVED_TOTAL_Sample_RUN','REFERRED_Samples_RUN']].astype(float).sum(axis=1).sum()
        DBSref_out=df_Reffered_Samples.query("Sample_Type == 'DBS' and Test_Type == 'VL'")['Samples_reffered_Out'].sum()
        DBSref_rec=df_Reffered_Samples.query("Sample_Type == 'DBS' and Test_Type == 'VL'")['Referred_Sample_Received'].sum()
        DBSthis_I=df_week_samp_received.query("Sample_Type == 'DBS' and Test_Type == 'VL'")['Num_Rejected_Samples'].sum()
        DBSthis_E=df_week_samp_received.query("Sample_Type == 'DBS' and Test_Type == 'VL'")['Total_samples_received'].sum()
        CARRYOVER_WEEK_START_DBS_VL=dbscarry+ DBSrun+ DBSref_out- DBSref_rec+ DBSthis_I- DBSthis_E
        
        EIDcarry = df.query("Sample_Type == 'DBS' and Test_Type == 'EID'")[['NEVERTESTED_Samples_in_Lab', 'FAILED_Samples_in_Lab']].astype(float).sum(axis=1).sum()
        EIDrun= df_Sample_Run.query("Sample_Type == 'DBS' and Test_Type == 'EID'")[['CARRYOVER_Samples_RUN','RECEIVED_TOTAL_Sample_RUN','REFERRED_Samples_RUN']].astype(float).sum(axis=1).sum()
        EIDref_out = df_Reffered_Samples.query("Sample_Type == 'DBS' and Test_Type == 'EID'")['Samples_reffered_Out'].sum()
        EIDref_rec = df_Reffered_Samples.query("Sample_Type == 'DBS' and Test_Type == 'EID'")['Referred_Sample_Received'].sum()
        EIDthis_I = df_week_samp_received.query("Sample_Type == 'DBS' and Test_Type == 'EID'")['Num_Rejected_Samples'].sum()
        EIDthis_E = df_week_samp_received.query("Sample_Type == 'DBS' and Test_Type == 'EID'")['Total_samples_received'].sum()
        CARRYOVER_WEEK_START_EID = EIDcarry + EIDrun + EIDref_out - EIDref_rec + EIDthis_I - EIDthis_E

        
        #Calculating the number of carryover samples
        Number_of_carryover_samples=df[['NEVERTESTED_Samples_in_Lab', 'FAILED_Samples_in_Lab']].astype(float).sum(axis=1).sum()
        
        
        #Backlog
        Backlog=df['BACKLOG_Samples_intraTAT_7mo'].sum()
        
        #Age of Oldest Sample
        Age_of_oldest_Plasma_sample=df.query("Sample_Type == 'Plasma'")['Days_for_OLDEST_CarryoverSample'].astype(float).max()
        Age_of_oldest_DBS_sample=df.query("Sample_Type == 'DBS' and Test_Type == 'VL'")['Days_for_OLDEST_CarryoverSample'].astype(float).max()
        Age_of_oldest_EID_sample=df.query("Sample_Type == 'DBS' and Test_Type == 'EID'")['Days_for_OLDEST_CarryoverSample'].astype(float).max()
        
        #Number of samples received this week
        Number_of_samples_received_this_week=df_week_samp_received.query("Test_Type == 'VL'")['Total_samples_received'].astype(float).sum()
        
        #Sex aggregated
        #Total_received_samples_with_Age_Sex_Disaggregation=df_Age_Sex_Disagg.loc[:, 'Males_<15':'Unknown_Sex_Unknown'].astype(float).sum(axis=1)
        Total_received_samples_with_Age_Sex_Disaggregation = df_Age_Sex_Disagg.loc[:, 'Males_<15':'Unknown_Sex_Unknown'].astype(float).sum().sum()

        
        Number_of_samples_that_are_rejected= df_week_samp_received['Num_Rejected_Samples'].astype(float).sum()
        
        #Number of rejected with reasons
        rej=df_week_samp_received[['REJECTED_Quality_issue', 'REJECTED_Quantity_insuff', 'REJECTED_Patient_SampleINFO', 'REJECTED_Missing_requestForm', 'REJECTED_Sample_Missing']].astype(float).sum(axis=1)
        Total_with_reasons_for_rejection=rej.sum()         
        
        #samples refered to lims               
        Number_of_samples_entered_into_LIMS_on_day_of_arrival=df_week_samp_received['Num_Samples_entered_LIMSonArrival'].astype(float).sum()
        
        #samples received
        Total_number_of_referred_samples_received= df_Reffered_Samples['Referred_Sample_Received'].astype(float).sum()
        
        #refered due to reasons
        ref=df_Reffered_Samples[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity', 'REFERRED_Insuff_Instrument_Capacity']].astype(float).sum(axis=1)
        Total_referred_with_reason_for_referral=ref.sum()
        
        #total samples run
        tot_run=df_Sample_Run[['CARRYOVER_Samples_RUN','RECEIVED_TOTAL_Sample_RUN','REFERRED_Samples_RUN']].astype(float).sum(axis=1)
        Total_number_of_samples_run=tot_run.sum()
        
        #failed and elig for repeats
        felig=df_Sample_Run[['CARRYOVER_FAILED_samples_elig_repeat','RECEIVED_FAILED_bt_Elig_REPEAT','REFERRED_FAILED_bt_Elig_REPEAT']].astype(float).sum(axis=1)
        Total_number_failed_eligible_for_repeat=felig.sum()  
        
        #failed adn not elig for repeats
        fNTelig=df_Sample_Run[['CARRYOVER_FAILED_samples_Ntelig_repeat','RECEIVED_FAILED_bt_NOT_Elig_REPEAT','REFERRED_FAILED_bt_NOT_Elig_REPEAT']].astype(float).sum(axis=1)
        Total_number_failed_not_eligible_for_repeat=fNTelig.sum()
        
        #Number with reasons for failure
        a=df_Sample_Run[['RECEIVED_FAILED_quality_quantity_issues',
                    'RECEIVED_FAILED_reagent_quality_issues', 'RECEIVED_FAILED_QC_failure',
                    'RECEIVED_FAILED_power_failure', 'RECEIVED_FAILED_mechanical_failure',
                    'FAILED_RECEIVED_sample_processing_error' ,'RECEIVED_OTHER' ]].astype(float).sum(axis=1) 



        b=df_Sample_Run[['CARRYOVER_FAILED_quality_quantity_issues',
                'CARRYOVER_FAILED_reagent_quality_issues',
                'CARRYOVER_FAILED_QC_failure', 'CARRYOVER_FAILED_power_failure',
                'CARRYOVER_FAILED_mechanical_failure',
                'FAILED_CARRYOVER_sample_processing_error', 'CARRYOVER_OTHER']].astype(float).sum(axis=1)

        c=df_Sample_Run[['REFERRED_FAILED_quality_quantity_issues',
                'REFERRED_FAILED_reagent_quality_issues',
                'REFERRED_FAILED_power_failure', 'REFERRED_FAILED_mechanical_failure',
                'FAILED_REFERRED_sample_processing_error', 'REFERRED_OTHER']].astype(float).sum(axis=1)
        Number_with_reason_for_failure=a.sum()+b.sum()+c.sum()
        
        #printed from lims by lab
        Number_of_results_printed_from_LIMS_by_VL_Lab=df_Sample_Run['CARRYOVER_Results_printed_from_LIMS_by_VL_Lab'].astype(float).sum()+df_Sample_Run['RECEIVED_Results_printed_from_LIMS_by_VL_Lab'].astype(float).sum()
        
                        
        #Dispatched by lab
        Number_of_results_dispatched_by_lab=df_Sample_Run['CARRYOVER_Results_dispatched_by_lab'].astype(float).sum()+df_Sample_Run['RECEIVED_Results_dispatched_by_lab'].astype(float).sum()
        
        #reagents kits available
        df_nona = df_Testing_Capacity.dropna(subset=['Reagent_tests_kits_available'])
        Reagent_test_kits_available=df_nona['Reagent_tests_kits_available'].astype('int64').sum()   
        
        
        Total_machine_downtime_hours= df_Lab_Metrics_Waste_Mgt[['Downtime_Power_Outage',
                                                                'Downtime_Mechanical_Failure', 'Downtime_Reagent_Stockout_Expiry',
                                                                'Downtime_Staff_Unavailability','Total_Operational_Hrs']].astype(float).sum(axis=1).sum()
        
        if (df_Lab_Metrics_Waste_Mgt['Hrs_in_Shift'].astype(float).sum()*7 !=Total_machine_downtime_hours):
            Error_issue_with_MachineDowntime=1
        else:
            Error_issue_with_MachineDowntime=0

        Lab_Metricts_Machine_Breakdown_NO_Comment=np.any( (df_Lab_Metrics_Waste_Mgt['Num_Machine_Breakdowns']>0 ) & (df_Lab_Metrics_Waste_Mgt['Comments_on_ErrorCodes_for_Mach_failure'].isnull()) )
            
            
        #lIMS Functionality    
        LIMS_Hours_of_functionality=df_LIMS_Functionality['Hours_of_Functionality'].astype(float).sum()
        
        #hrs with no electricity
        Number_of_hours_with_no_electricity=df_Power_Outage['Hours_with_no_electricity'].values[0]
        #generator
        Number_of_hours_generator_was_on=df_Power_Outage['Hours_generator_was_on'].values[0]
        
        # Create a dictionary for Dashboard indicators
        indicators = [
            {
                'Number_of_carryover_samples': Number_of_carryover_samples,
                'Backlog': Backlog,
                'Age_of_oldest_Plasma_sample': Age_of_oldest_Plasma_sample,
                'Age_of_oldest_DBS_sample': Age_of_oldest_DBS_sample,
                'Age_of_oldest_EID_sample': Age_of_oldest_EID_sample,
                'Number_of_samples_received_this_week': Number_of_samples_received_this_week,
                'Total_received_samples_with_Age_Sex_Disaggregation': Total_received_samples_with_Age_Sex_Disaggregation,
                'Number_of_samples_that_are_rejected': Number_of_samples_that_are_rejected,
                'Total_with_reasons_for_rejection': Total_with_reasons_for_rejection,
                'Number_of_samples_entered_into_LIMS_on_day_of_arrival': Number_of_samples_entered_into_LIMS_on_day_of_arrival,
                'Total_number_of_referred_samples_received': Total_number_of_referred_samples_received,
                'Total_referred_with_reason_for_referral': Total_referred_with_reason_for_referral,
                'Total_number_of_samples_run': Total_number_of_samples_run,
                'Calculated_carryover_at_start_of_week_Plasma_VL': CARRYOVER_WEEK_START_Plasma_VL,
                'Calculated_carryover_at_start_of_week_DBS_VL': CARRYOVER_WEEK_START_DBS_VL,
                'Calculated_carryover_at_start_of_week_DBS_EID': CARRYOVER_WEEK_START_EID,
                'Total_number_failed_eligible_for_repeat': Total_number_failed_eligible_for_repeat,
                'Total_number_failed_not_eligible_for_repeat': Total_number_failed_not_eligible_for_repeat,
                'Number_with_reason_for_failure': Number_with_reason_for_failure,
                'Number_of_results_printed_from_LIMS_by_VL_Lab': Number_of_results_printed_from_LIMS_by_VL_Lab,
                'Number_of_results_dispatched_by_lab': Number_of_results_dispatched_by_lab,
                'Reagent_test_kits_available': Reagent_test_kits_available,
                'Total_machine_downtime_hours': Total_machine_downtime_hours,
                'LIMS_Hours_of_functionality': LIMS_Hours_of_functionality,
                'Number_of_hours_with_no_electricity': Number_of_hours_with_no_electricity,
                'Number_of_hours_generator_was_on': Number_of_hours_generator_was_on
            }
        ]

        # Create a DataFrame from the list of dictionaries
        df_indicators = pd.DataFrame(indicators)
        df_indicators['Date']=df['Date'][0]
        df_indicators['SourceFile']=file_name
        df_indicators['LAB']=lab
        df_indicators['unique_key']=unique_key
        
        if (lab=='Masvingo' and df['Name_of_Lab'][0]=="Masvingo "):
            df['Name_of_Lab']='Masvingo'


        Home_reagent_test_kit =int(any(df_indicators['Reagent_test_kits_available'].values>1000000))
        print(df_indicators['Reagent_test_kits_available'].values)
        
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

        # Define your SQL query
        sql = f'''
        SELECT Date, Name_of_Lab, Sample_Type, Test_Type
        FROM Dash_Carryover_Sample_inventory
        WHERE Date BETWEEN :start_date AND :end_date
        '''
        
        # Execute the query, passing in the date range as parameters
        df_last_week = pd.read_sql(
            text(sql), 
            engine, 
            params={'start_date': start_date, 'end_date': end_date}
        )
                        
        # CHECK IF THE RECORD EXIST IN DATABASE
        df_tuples = set(df[['Date', 'Name_of_Lab', 'Sample_Type', 'Test_Type']].apply(tuple, axis=1))
        df_last_week_tuples = set(df_last_week.query("Name_of_Lab == @lab")[['Date', 'Name_of_Lab', 'Sample_Type', 'Test_Type']].apply(tuple, axis=1))
        


    
        # CHECKING DUPLICATES
        common_records=df_tuples.intersection(df_last_week_tuples)


        if not df_tuples.intersection(df_last_week_tuples): #no pure dups
            error_dups=0
        else:
            error_dups=1
            
        #---------------------------stockout---------------------------
        # Error_Stockout_Days_not_Indicated_well= int(np.any( 
        #                                                 ( df_Testing_Capacity['Reagent_stockout_days'] > 0 ) & 
        #                                                 (df_Lab_Metrics_Waste_Mgt['Downtime_Reagent_Stockout_Expiry'] <= 0) &
        #                                                ( df_Testing_Capacity['Platform_Roche_Abbott_Hologic_BMX'].equals(df_Lab_Metrics_Waste_Mgt['Platform_Roche_Abbott_Hologic_BMX'])) &
        #                                                 (df_Testing_Capacity['Test_Type']==df_Lab_Metrics_Waste_Mgt['Test_Type']) 
        #                                                  )
        #                                             )


        #----------------------------COMPARE FIGURES WITH LAST WEEK---------------------------------------
        
        
            # Define your SQL query
        sql_Dash = f'''
        SELECT *
        FROM Dashboard_Indicators
        WHERE Update_date BETWEEN :start_date AND :end_date
        '''
        
        # Execute the query, passing in the date range as parameters
        df_Dash_lastWeek = pd.read_sql(
            text(sql_Dash), 
            engine, 
            params={'start_date': start_date, 'end_date': end_date}
        )
        
        
        lab_lastweekDash = df_Dash_lastWeek.query("LAB == @lab")[['LAB','Calculated_carryover_at_start_of_week_Plasma_VL', 'Calculated_carryover_at_start_of_week_DBS_VL', 'Calculated_carryover_at_start_of_week_DBS_EID']].copy()

        if len(lab_lastweekDash) >= 1:
            
            lab_lastweekDash['Plasma_Difference'] = df_indicators['Calculated_carryover_at_start_of_week_Plasma_VL'].astype(float).values - lab_lastweekDash['Calculated_carryover_at_start_of_week_Plasma_VL'].astype(float).values
            lab_lastweekDash['Plasma_Percentage_Difference'] = np.divide(lab_lastweekDash['Plasma_Difference'].values, lab_lastweekDash['Calculated_carryover_at_start_of_week_Plasma_VL'].values, out=np.zeros_like(lab_lastweekDash['Plasma_Difference'].values), where=lab_lastweekDash['Calculated_carryover_at_start_of_week_Plasma_VL'].values != 0)

            
            
            lab_lastweekDash['DBSVL_Difference'] = df_indicators['Calculated_carryover_at_start_of_week_DBS_VL'].values - lab_lastweekDash['Calculated_carryover_at_start_of_week_DBS_VL'].values
            lab_lastweekDash['DBSVL_Percentage_Difference'] = np.divide(lab_lastweekDash['DBSVL_Difference'].values, lab_lastweekDash['Calculated_carryover_at_start_of_week_DBS_VL'].values, out=np.zeros_like(lab_lastweekDash['DBSVL_Difference'].values), where=lab_lastweekDash['Calculated_carryover_at_start_of_week_DBS_VL'].values != 0)

            lab_lastweekDash['EID_Difference'] = df_indicators['Calculated_carryover_at_start_of_week_DBS_EID'].values - lab_lastweekDash['Calculated_carryover_at_start_of_week_DBS_EID'].values
            lab_lastweekDash['EID_Percentage_Difference'] = np.divide(lab_lastweekDash['EID_Difference'].values, lab_lastweekDash['Calculated_carryover_at_start_of_week_DBS_EID'].values, out=np.zeros_like(lab_lastweekDash['EID_Difference'].values), where=lab_lastweekDash['Calculated_carryover_at_start_of_week_DBS_EID'].values != 0)
            
            p_diff_perci=lab_lastweekDash.query("LAB == @lab")['Plasma_Percentage_Difference'].values
            plasma_lasti=lab_lastweekDash.query("LAB == @lab")[['Calculated_carryover_at_start_of_week_Plasma_VL']].values
            p_diff=lab_lastweekDash.query("LAB == @lab")['Plasma_Difference']
            #Calculate the difference
            # Check if the absolute value of percentage differences exceeds 30% - 0 for those less than 30 and 1 for greater

        
        else:
            lab_lastweekDash['Plasma_Difference'] = 0
            lab_lastweekDash['Plasma_Percentage_Difference'] = 0
            lab_lastweekDash['DBSVL_Difference'] = 0
            lab_lastweekDash['DBSVL_Percentage_Difference'] = 0
            lab_lastweekDash['EID_Difference'] = 0
            lab_lastweekDash['EID_Percentage_Difference'] = 0
            plasma_lasti=''
            p_diff_perci=''
            
        
        error_Plasma_diff = int(lab_lastweekDash['Plasma_Percentage_Difference'].abs().gt(0.3).any())
        error_DBSVL_diff = int( lab_lastweekDash['DBSVL_Percentage_Difference'].abs().gt(0.3).any())
        error_EID_diff = int( lab_lastweekDash['EID_Percentage_Difference'].abs().gt(0.3).any())
        




            
        #----------------------------LIMS FUNCTIONALITY-------------------------------------------------------
        #sum must be 24hrs
        if (df_LIMS_Functionality[['Hours_of_Functionality','Hours_of_Downtime']].astype(float).sum(axis=1).sum()==168):
            Error_LIMS_Downtime=0
        else:
            Error_LIMS_Downtime=1
            
        
        if np.any((df_LIMS_Functionality['Hours_of_Downtime'] > 0) & df_LIMS_Functionality['Comments'].isnull()):
            Error_LIMS_Fxn_NO_COMMENTS=1
        else:
            Error_LIMS_Fxn_NO_COMMENTS=0
        
        #-----------------------LAB PLATFORM---------------------------------------------------------
#                 numerator = df_Lab_Metrics_Waste_Mgt['Num_Machine_Breakdowns'].values
#                 denominator = df_Lab_Metrics_Waste_Mgt[[ 'Downtime_Power_Outage', 'Downtime_Mechanical_Failure', 'Downtime_Reagent_Stockout_Expiry', 'Downtime_Staff_Unavailability']].sum(axis=1).values
#                 # Check for NaN or null values in both arrays
#                 invalid_values = np.isnan(numerator) | pd.isnull(denominator)
#                 # Compare the arrays while ignoring invalid values
#                 if np.any(numerator[~invalid_values] != denominator[~invalid_values]):
#                     Error_LabPlatform_I_KN = 1
#                 else:
#                     Error_LabPlatform_I_KN = 0
        
        #-------------------------LAB CORROSPONDING WITH FOLDER ERROR--------------------------------------------
    

            
        if (df['Name_of_Lab'][0]!=lab):
            Error_Wrong_NAME_OF_LAB=1
        else:
            Error_Wrong_NAME_OF_LAB=0
            
        #----------------------------If we have any in Backlog and oldest <7days----------------------------------
        if np.any((df['BACKLOG_Samples_intraTAT_7mo'].values > 0) & (df['Days_for_OLDEST_CarryoverSample'].values < 7)):
            Error_Carryover_BACKLOG_Less7dys=1
        else:
            Error_Carryover_BACKLOG_Less7dys=0
        
        #------------CARRYOVER----------------if P(oldest sample)>14 but NO COMMENT----------------------
        
        if np.any((df['Days_for_OLDEST_CarryoverSample'] > 14) & df['comment'].isnull()):
            Error_Oldest_CarryOverSample_No_COMMENTS=1
        else:
            Error_Oldest_CarryOverSample_No_COMMENTS=0
            


        # --------------------P>60 plasma & P>90 DBS (VL/EID)
        if np.any((df.query("Sample_Type=='Plasma'")['Days_for_OLDEST_CarryoverSample'].values > 60) 
                    & (df.query("Sample_Type=='Plasma'")['NUMBER_carryover_sample_TOO_OLD_test'].values < 1)):
            Error_PLASMA_tooOLDSamples_No_AMT_to_Quantify = 1
        else:
            Error_PLASMA_tooOLDSamples_No_AMT_to_Quantify = 0

        # DBS too old samples - no quantity to mention how many--------------------    
        if np.any((df.query("Sample_Type=='DBS'")['Days_for_OLDEST_CarryoverSample'].values > 90) 
                    & (df.query("Sample_Type=='DBS'")['NUMBER_carryover_sample_TOO_OLD_test'].values < 1)):
            Error_DBS_tooOLDSamples_No_AMT_to_Quantify = 1
        else:
            Error_DBS_tooOLDSamples_No_AMT_to_Quantify = 0

        
        #------------THIS WEEEK TAB CHECKS------------------
        # Rejected Samples disaggregated on the reasons 

            
        totalrejected = df_week_samp_received[['Num_Rejected_Samples']].values
        breakeddwn = df_week_samp_received[['REJECTED_Quality_issue', 'REJECTED_Quantity_insuff', 'REJECTED_Patient_SampleINFO', 'REJECTED_Missing_requestForm', 'REJECTED_Sample_Missing']].astype(float).sum(axis=1).values
        #thenulls_values = np.isnan(totalrejected) | pd.isnull(breakeddwn)

        if np.any(totalrejected[~np.isnan(totalrejected)] != breakeddwn[~pd.isnull(breakeddwn)]):
            Error_Rejected_Samples_Not_Disaggregated = 1
        else:
            Error_Rejected_Samples_Not_Disaggregated = 0


        
        
        #------- Entered in LIMS must be less than Total Received Samples--
        mumarr=pd.isnull(df_week_samp_received['Num_Samples_entered_LIMSonArrival'].values)
        recnulls_values = np.isnan(df_week_samp_received['Total_samples_received'].values) 
        E=df_week_samp_received['Total_samples_received'].values[~recnulls_values]
        P=df_week_samp_received['Num_Samples_entered_LIMSonArrival'].values[~mumarr]
        Error_Carryover_EnteredLIMS_mothan_Received=int(np.any(P>E))
            
            
        #--------Disaggregated well by Age----------------------------------
        if (Number_of_samples_received_this_week !=Total_received_samples_with_Age_Sex_Disaggregation):
            Error_NOT_DISAGGREGAED_WELL_by_AGE=1
        else:
            Error_NOT_DISAGGREGAED_WELL_by_AGE=0
            
        
    
            #-------REFFERRED SHEET--------------------------------------------------
        #referred sample received apportioned-----G=H+I

#                 HI=df_Reffered_Samples[[ 'CARRYOVER_Referred_Sample_Received', 'NOT_CARRYOVER_Referred_Sample_Received']].astype(float).sum(axis=1).values
        
#                                 # Replace NaN values with 0 in both arrays

#                 HI[np.isnan(HI)] = 0
        
#                 if np.any(G!=HI):
#                     Error_Reffered_SamplesReceived_not_Seperated=1
#                 else:
#                     Error_Reffered_SamplesReceived_not_Seperated=0
        
        
        #-- Reasons for Referred Samples-- E/G=NOPQ--------------------------
        G_plasma=G = df_Reffered_Samples.query("Sample_Type=='Plasma'")['Referred_Sample_Received'].values
        E_plasma = df_Reffered_Samples.query("Sample_Type=='Plasma'")['Samples_reffered_Out'].values
        N_Q_plasma = df_Reffered_Samples.query("Sample_Type=='Plasma'")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity']].astype(float).sum(axis=1).values

        G_DBS_VL=G = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='VL' ")['Referred_Sample_Received'].values
        E_DBS_VL = df_Reffered_Samples.query(" Sample_Type=='DBS' and  Test_Type=='VL' ")['Samples_reffered_Out'].values
        N_Q_DBS_VL = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='VL' ")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity']].astype(float).sum(axis=1).values


        G_DBS_EID=G = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='EID' ")['Referred_Sample_Received'].values
        E_DBS_EID = df_Reffered_Samples.query(" Sample_Type=='DBS' and  Test_Type=='EID' ")['Samples_reffered_Out'].values
        N_Q_DBS_EID = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='EID' ")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity']].astype(float).sum(axis=1).values

        G_plasma = np.nan_to_num(G_plasma, nan=0)
        E_plasma = np.nan_to_num(E_plasma, nan=0)
        N_Q_plasma = np.nan_to_num(N_Q_plasma, nan=0)

        G_DBS_VL = np.nan_to_num(G_DBS_VL, nan=0)
        E_DBS_VL = np.nan_to_num(E_DBS_VL, nan=0)
        N_Q_DBS_VL = np.nan_to_num(N_Q_DBS_VL, nan=0)

        G_DBS_EID = np.nan_to_num(G_DBS_EID, nan=0)
        E_DBS_EID = np.nan_to_num(E_DBS_EID, nan=0)
        N_Q_DBS_EID = np.nan_to_num(N_Q_DBS_EID, nan=0)

        plsama_vl_ref = int(np.any(G_plasma.size > 0 and E_plasma.size > 0 and G_plasma != N_Q_plasma) and 
                            np.any(E_plasma.size > 0 and N_Q_plasma.size > 0 and E_plasma != N_Q_plasma))

        dbs_vl_ref = int(np.any(G_DBS_VL.size > 0 and E_DBS_VL.size > 0 and G_DBS_VL != N_Q_DBS_VL) and 
                            np.any(E_DBS_VL.size > 0 and N_Q_DBS_VL.size > 0 and E_DBS_VL != N_Q_DBS_VL))

        dbs_eid_ref = int(np.any(G_DBS_EID.size > 0 and E_DBS_EID.size > 0 and G_DBS_EID != N_Q_DBS_EID) and 
                            np.any(E_DBS_EID.size > 0 and N_Q_DBS_EID.size > 0 and E_DBS_EID != N_Q_DBS_EID))

        
        # if H>0 then J must have lab from----------------------------------------
        
        numc=df_Reffered_Samples['CARRYOVER_Referred_Sample_Received']
        criteria= (numc>0) & df_Reffered_Samples['Referred_From'].isna()
        Error_Referred_FROM_Blank=int(np.any(criteria))
        
        
        
        #-----SAMPLE RUN --SHEET------------------------------------------------------
        #--Samples Failed but NO comments -------------------


        carr=df_Sample_Run[['CARRYOVER_FAILED_samples_elig_repeat', 'CARRYOVER_FAILED_samples_Ntelig_repeat']].astype(float).sum(axis=1)
        carr_condition=(carr>0) & df_Sample_Run['Comments'].isna()
        Error_Carryover_Repeats_NO_COMMENT=int(np.any(carr_condition))


        temp_sum = df_Sample_Run[['RECEIVED_FAILED_bt_Elig_REPEAT', 'RECEIVED_FAILED_bt_NOT_Elig_REPEAT']].astype(float).sum(axis=1)
        condition = (temp_sum > 0) & df_Sample_Run['Comments'].isna()
        Error_Received_Repeats_NO_COMMENT=int(np.any(condition))

        reff=df_Sample_Run[['REFERRED_FAILED_bt_Elig_REPEAT', 'REFERRED_FAILED_bt_NOT_Elig_REPEAT']].astype(float).sum(axis=1)
        reff_condition=(reff>0) &  df_Sample_Run['Comments'].isna()
        Error_Referred_Repeats_NO_COMMENT=int(np.any(reff_condition))

            
        
                #--------------Failed Elig & Not Elig (I+J)= M to S-------------------------
        C_failed=df_Sample_Run[['CARRYOVER_FAILED_samples_elig_repeat', 'CARRYOVER_FAILED_samples_Ntelig_repeat']].astype(float).sum(axis=1).values
        Rec_failed=df_Sample_Run [['RECEIVED_FAILED_bt_Elig_REPEAT', 'RECEIVED_FAILED_bt_NOT_Elig_REPEAT']].astype(float).sum(axis=1).values
        Ref_failed=df_Sample_Run[['REFERRED_FAILED_bt_Elig_REPEAT', 'REFERRED_FAILED_bt_NOT_Elig_REPEAT']].astype(float).sum(axis=1).values


            
        ref=df_Sample_Run[['REFERRED_FAILED_quality_quantity_issues',
                            'REFERRED_FAILED_reagent_quality_issues',
                            'REFERRED_FAILED_power_failure', 'REFERRED_FAILED_mechanical_failure',
                            'FAILED_REFERRED_sample_processing_error', 'REFERRED_OTHER']].astype(float).sum(axis=1).values


        rec=df_Sample_Run[['RECEIVED_FAILED_quality_quantity_issues',
                        'RECEIVED_FAILED_reagent_quality_issues', 'RECEIVED_FAILED_QC_failure',
                        'RECEIVED_FAILED_power_failure', 'RECEIVED_FAILED_mechanical_failure',
                        'FAILED_RECEIVED_sample_processing_error', 'RECEIVED_OTHER']].astype(float).sum(axis=1).values

        carrx=df_Sample_Run[['CARRYOVER_FAILED_quality_quantity_issues',
                        'CARRYOVER_FAILED_reagent_quality_issues',
                        'CARRYOVER_FAILED_QC_failure', 'CARRYOVER_FAILED_power_failure',
                        'CARRYOVER_FAILED_mechanical_failure',
                        'FAILED_CARRYOVER_sample_processing_error', 'CARRYOVER_OTHER']].astype(float).sum(axis=1).values
            #carryover
        Error_CARRYOVER_Sample_Failed_no_reason=int (np.any( carrx!=C_failed) )

            
            #received
        Error_RECEIVED_Sample_Failed_no_reason=int( np.any( rec!=Rec_failed) )


            #referred
        Error_REFERRED_Sample_Failed_no_reason= int( np.any( ref!=Ref_failed) )

        
        #--------------------------------------------------------------------------------------------

        Error_Testing_Cap_Received_No_DATE= any((df_Testing_Capacity['NatPharm_Kits_Received_inThisWK']>0)  & (df_Testing_Capacity['Date_Received_at_Lab'].isnull())  )

        df_filtered = df_Testing_Capacity.dropna(subset=['Reagent_stockout_days'])

        Error_LIMS_Fxnality_Kits_NoEXP_or_Batchno=int(np.any( (df_Testing_Capacity['Reagent_tests_kits_Stock_on_hand'] > 0) & 
                                                                (df_Testing_Capacity['Reagent_tests_kits_available_Expiry_Date'].isnull() )
                                                            )
                                                    )
        Error_LIMS_Fxnality_Kits_no_Batchno=int(np.any( (df_Testing_Capacity['Reagent_tests_kits_Stock_on_hand'] > 0) & 
                                                ( df_Testing_Capacity['Reagent_tests_kits_available_Batch_lot_Number'].isnull())
                                            )
                                    )


        Error_Testing_Capacity_NO_Kits_NO_DAYS= int(np.any((df_filtered['Reagent_tests_kits_Stock_on_hand'] == 0) & (df_filtered['Reagent_stockout_days'] ==0)) )

       
        #-----------------------------------NEW ERRORS --------------------------------------------------------------
        plasma_carry_diff = (df.query("Sample_Type=='Plasma' and Test_Type=='VL'")[['NEVERTESTED_Samples_in_Lab','BACKLOG_Samples_intraTAT_7mo']].astype(float).iloc[:, 0] - df.query("Sample_Type=='Plasma' and Test_Type=='VL'")[['NEVERTESTED_Samples_in_Lab','BACKLOG_Samples_intraTAT_7mo']].astype(float).iloc[:, 1]).values
        dbsvl_carry_diff = (df.query("Sample_Type=='DBS' and Test_Type=='VL'")[['NEVERTESTED_Samples_in_Lab','BACKLOG_Samples_intraTAT_7mo']].astype(float).iloc[:, 0] - df.query("Sample_Type=='DBS' and Test_Type=='VL'")[['NEVERTESTED_Samples_in_Lab','BACKLOG_Samples_intraTAT_7mo']].astype(float).iloc[:, 1]).values
        dbseid_carry_diff = (df.query("Sample_Type=='DBS' and Test_Type=='EID'")[['NEVERTESTED_Samples_in_Lab','BACKLOG_Samples_intraTAT_7mo']].astype(float).iloc[:, 0] - df.query("Sample_Type=='DBS' and Test_Type=='EID'")[['NEVERTESTED_Samples_in_Lab','BACKLOG_Samples_intraTAT_7mo']].astype(float).iloc[:, 1]).values

        plasmarec = df_week_samp_received.query("Sample_Type=='Plasma' and Test_Type=='VL'")['Total_samples_received'].values
        dbsvlrec = df_week_samp_received.query("Sample_Type=='DBS' and Test_Type=='VL'")['Total_samples_received'].values
        dbseidrec = df_week_samp_received.query("Sample_Type=='DBS' and Test_Type=='EID'")['Total_samples_received'].values

        #errors
        error_Plasma_Carryover_greater_that_Received = int(any(plasma_carry_diff > plasmarec))
        error_DBSVL_Carryover_greater_that_Received = int(any(dbsvl_carry_diff > dbsvlrec))
        error_DBSEID_Carryover_greater_that_Received = int(any(dbseid_carry_diff > dbseidrec))

        

        
       

        # Extract 'Date' columns with non-null values from each DataFrame
        dates_df_indicators  = pd.to_datetime(df_indicators[df_indicators['Date'].notna()]['Date'])
        dates_df_week_samp_received = pd.to_datetime(df_week_samp_received[df_week_samp_received['Date'].notna()]['Date'])
        dates_df_Age_Sex_Disagg = pd.to_datetime(df_Age_Sex_Disagg[df_Age_Sex_Disagg['Date'].notna()]['Date'])
        dates_df_Reffered_Samples = pd.to_datetime(df_Reffered_Samples[df_Reffered_Samples['Date'].notna()]['Date'])
        dates_df_Sample_Run = pd.to_datetime(df_Sample_Run[df_Sample_Run['Date'].notna()]['Date'])
        dates_df_Testing_Capacity = pd.to_datetime(df_Testing_Capacity[df_Testing_Capacity['Date'].notna()]['Date'])
        dates_df_Lab_Metrics_Waste_Mgt = pd.to_datetime(df_Lab_Metrics_Waste_Mgt[df_Lab_Metrics_Waste_Mgt['Date'].notna()]['Date'])
        dates_df_LIMS_Functionality = pd.to_datetime(df_LIMS_Functionality[df_LIMS_Functionality['Date'].notna()]['Date'])
        dates_df_CLI = pd.to_datetime(df_CLI[df_CLI['Date'].notna()]['Date'])
        dates_df_Power_Outage = pd.to_datetime(df_Power_Outage[df_Power_Outage['Date'].notna()]['Date'])
        
        
        # Extract unique dates from each DataFrame's 'Date' column
        unique_dates_df_indicators = set(dates_df_indicators.unique())


        # Check if all sets of unique dates are the same
        all_dates_are_the_same = all([
            unique_dates_df_indicators == set( dates_df_week_samp_received.unique()),
            unique_dates_df_indicators == set(dates_df_Age_Sex_Disagg.unique()),
            unique_dates_df_indicators == set(dates_df_Reffered_Samples.unique()),
            unique_dates_df_indicators == set(dates_df_Sample_Run.unique()),
            unique_dates_df_indicators == set(dates_df_Testing_Capacity.unique()),
            unique_dates_df_indicators == set(dates_df_Lab_Metrics_Waste_Mgt.unique()),
            unique_dates_df_indicators == set(dates_df_LIMS_Functionality.unique()),
            unique_dates_df_indicators == set(dates_df_CLI.unique()),
            unique_dates_df_indicators == set(dates_df_Power_Outage.unique())
        ])

        #error
        Error_Dates_differ=int(not all_dates_are_the_same)
      







        raised_errors= []
        list_of_Errors=[cli_internal_error,cli_internal_error_2,Error_issue_with_MachineDowntime,error_dups,Error_LIMS_Fxn_NO_COMMENTS,Error_LIMS_Downtime,Error_Wrong_NAME_OF_LAB,Error_Carryover_BACKLOG_Less7dys,
                        Error_Oldest_CarryOverSample_No_COMMENTS,Error_PLASMA_tooOLDSamples_No_AMT_to_Quantify,Error_DBS_tooOLDSamples_No_AMT_to_Quantify,
                        Error_Rejected_Samples_Not_Disaggregated,Error_Carryover_EnteredLIMS_mothan_Received,Error_NOT_DISAGGREGAED_WELL_by_AGE,
                        Error_Referred_FROM_Blank, plsama_vl_ref, dbs_vl_ref, dbs_eid_ref ,Error_LIMS_Fxnality_Kits_NoEXP_or_Batchno,Error_LIMS_Fxnality_Kits_no_Batchno,
                        Error_Carryover_Repeats_NO_COMMENT, Error_Received_Repeats_NO_COMMENT, Error_Referred_Repeats_NO_COMMENT, Lab_Metricts_Machine_Breakdown_NO_Comment,
                        Error_CARRYOVER_Sample_Failed_no_reason, Error_RECEIVED_Sample_Failed_no_reason, Error_REFERRED_Sample_Failed_no_reason,age_of_oldestsample_empty  ,Error_Testing_Capacity_NO_Kits_NO_DAYS ,
                        Error_Testing_Cap_Received_No_DATE ,   Home_reagent_test_kit #,Commemnts_not_in_ProperSection 
                        ,error_Plasma_Carryover_greater_that_Received ,error_DBSVL_Carryover_greater_that_Received, error_DBSEID_Carryover_greater_that_Received ,Error_Dates_differ         
                        ]
        
        variable_names=['cli_internal_error','cli_internal_error_2','Error_issue_with_MachineDowntime','error_dups','Error_LIMS_Fxn_NO_COMMENTS','Error_LIMS_Downtime','Error_Wrong_NAME_OF_LAB','Error_Carryover_BACKLOG_Less7dys',
                        'Error_Oldest_CarryOverSample_No_COMMENTS','Error_PLASMA_tooOLDSamples_No_AMT_to_Quantify','Error_DBS_tooOLDSamples_No_AMT_to_Quantify',
                        'Error_Rejected_Samples_Not_Disaggregated','Error_Carryover_EnteredLIMS_mothan_Received','Error_NOT_DISAGGREGAED_WELL_by_AGE',
                        'Error_Referred_FROM_Blank', 'plsama_vl_ref','dbs_vl_ref','dbs_eid_ref','Error_LIMS_Fxnality_Kits_NoEXP_or_Batchno','Error_LIMS_Fxnality_Kits_no_Batchno',
                        'Error_Carryover_Repeats_NO_COMMENT','Error_Received_Repeats_NO_COMMENT','Error_Referred_Repeats_NO_COMMENT', 'Lab_Metricts_Machine_Breakdown_NO_Comment',
                        'Error_CARRYOVER_Sample_Failed_no_reason','Error_RECEIVED_Sample_Failed_no_reason','Error_REFERRED_Sample_Failed_no_reason','age_of_oldestsample_empty','Error_Testing_Capacity_NO_Kits_NO_DAYS',
                        'Error_Testing_Cap_Received_No_DATE','Home_reagent_test_kit' #,'Commemnts_not_in_ProperSection'
                        ,'error_Plasma_Carryover_greater_that_Received','error_DBSVL_Carryover_greater_that_Received','error_DBSEID_Carryover_greater_that_Received','Error_Dates_differ'
                        ]

        # Check if all variables are equal to 0
        if all(variable == 0 for variable in list_of_Errors):
                                
            
            
            #Load Data for dashboard in database
            # engine_insert = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}', echo=False)
            
            df_indicators.to_sql('Dashboard_Indicators', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
            
            df.to_sql('Dash_Carryover_Sample_inventory', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
                                
            df_week_samp_received.to_sql('Dash_This_week_Rec_Samples', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_Age_Sex_Disagg.to_sql('Dash_Age_Sex_Disaggregation', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
            
            
            df_Reffered_Samples.to_sql('Dash_Referred_Samples', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
            
                
            df_Sample_Run.to_sql('Dash_Sample_Run', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
            
            #print(df_Testing_Capacity.head())

            df_Testing_Capacity.to_sql('Dash_Testing_Capacity', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed           

            
                                            
            df_Lab_Metrics_Waste_Mgt.to_sql('Dash_Lab_Metrics_Waste_Mgt', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
            
                                            
            df_LIMS_Functionality.to_sql('Dash_LIMS_Functionality', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

                                                                
            df_CLI.to_sql('Dash_CLI', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

                                                                
            df_QMS.to_sql('Dash_QMS', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
                
                                                                                
            df_Power_Outage.to_sql('Dash_Power_Outage', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
            
            
            

            # Create directory and backup

            error_messages=lab +'    :Dashboard:  Successfully Uploaded'
             # Example: Return the processed data
            return error_messages
           
            
            
        else: 
            
            
            for i in range(len(list_of_Errors)):
                
                if list_of_Errors[i]==1:
                    raised_errors.append(variable_names[i])
                    
                Meanings_of_Errors = {
                                        'cli_internal_error': 'In CLI Tab: Column J = Sum of columns (K) to (Q), this criteria is not met.\n'
                                                                'i.e RESOLVED Missing results outcomes found and Shared with_facilities must be disaggregated in columns (K-Q).',

                                        'cli_internal_error_2': 'In CLI Tab: Column (H and I)- Unresolved missing results from last week and Number of missing results requested by facilities this week MUST BE EQUAL TO (column JRST) pending testing,Referred waiting results, investigation in progress  and Results found and shared with facilities ',

                                        'Error_issue_with_MachineDowntime': 'In Lab Metrics & Waste Mgt Tab: The Total Machine Downtime (hrs) Sum of (Downtime coz Power Outage, Downtime coz of Mechanical Failure, Downtime coz of Reagent Stockout Expiry, Downtime coz of Staff Unavailability, Total_Operational_Hrs) MUST BE EQUAL TO  Hrs in working Shift (column J * 7).',

                                        'error_dups': 'The Dashboard  was Uploaded already or You failed to change the dates to this Week Dashboard-, correct that. <br> it reads :{}'.format(df['Date'][0]),

                                        'error_Plasma_diff': 'The Difference of This Week (Calculated carryover at start of week - Plasma VL) and Last week is 30% more, there must be something wrong.<br>  For this Week Plasma :{} - Last week:{} --> % Difference :{}% <br>  '.format(CARRYOVER_WEEK_START_Plasma_VL, plasma_lasti, p_diff_perci*100),

                                        'error_DBSVL_diff': 'The Difference of This Week (Calculated carryover at start of week - DBS VL) and Last week is 30% more, there must be something wrong.',

                                        'error_EID_diff':'The Difference of This Week (Calculated carryover at start of week - DBS EID) and Last week is 30% more, there must be something wrong.',

                                            'Error_LIMS_Fxn_NO_COMMENTS':'In LIMS Functionality Tab: You Indicate that there are Hours in which LIMS was down (F) but You dont give a comment as to what happened' ,

                                        'Error_LIMS_Downtime': 'In LIMS Functionality Tab: Hours_of_Functionality and Hours_of_Downtime are not matching.',
                    
                                        'plsama_vl_ref':'In Referred Samples tab:  Plasma VL, Column  (samples referred out) or  (referred samples received) is not tallying with reasons  ',
                                        'dbs_vl_ref':'In Referred Samples tab:  DBS VL, Column  (samples referred out) or  (referred samples received) is not tallying with reasons ',
                                        'dbs_eid_ref':'In Referred Samples tab:  DBS EID, Column (samples referred out) or  (referred samples received)is not tallying with reasons',
                                        
                                        'Error_Wrong_NAME_OF_LAB': 'You failed to change the Name of the LAB to suit the Lab you are working on.',

                                        'Error_Carryover_BACKLOG_Less7dys': 'In Carryover Sample Inventory Tab: If you have Backlog Samples with Intra TAT greater than 7 days (column G), then the (column R) Days for Oldest samples can\'t be less than 7 days.',

                                        'Error_Oldest_CarryOverSample_No_COMMENTS': 'In Carryover Sample Inventory Tab: If you have Oldest Carryover Samples (column P>14 days), we expect a comment for justification.',

                                        'Error_PLASMA_tooOLDSamples_No_AMT_to_Quantify': 'In Carryover Sample Inventory Tab: For Plasma Samples, if Days_for_OLDEST_CarryoverSample (P) > 60, NUMBER of the carryover sample TOO OLD test(R) must not be 0.',

                                        'Error_DBS_tooOLDSamples_No_AMT_to_Quantify': 'In Carryover Sample Inventory Tab: For DBS Samples, if Days_for_OLDEST_CarryoverSample (P) > 90 days, NUMBER of the carryover sample TOO OLD test(R) must not be 0.',

                                        'Error_Rejected_Samples_Not_Disaggregated': 'In This week Sample Received Tab: Number of Rejected Samples (I) are not disaggregated by reason why they are rejected (K to O).',

                                        'Error_Carryover_EnteredLIMS_mothan_Received': 'This week Sample Received Tab: You entered Samples in LIMS (P) more than what you received (E).',

                                        'Error_NOT_DISAGGREGAED_WELL_by_AGE': 'In Age Sex Disaggregation Tab: You didn\'t disaggregate well with Age. The disaggregation must be equal to Number_of_samples_received_this_week',

                                        'Error_Referred_FROM_Blank': 'In Referred Samples Tab: You forgot to put Referred to facility.',

                                        'Error_Carryover_Repeats_NO_COMMENT': 'In Samples Run Tab: We expect a comment for (CARRYOVER FAILURE -I & J) Actual Reasons for assay failure.',

                                        'Error_Received_Repeats_NO_COMMENT': 'In Samples Run Tab: We expect a comment for (RECEIVED FAILURE -Y & Z) Actual Reasons for assay failure.',
                                        'Commemnts_not_in_ProperSection':'In sample Run Tab: You put the comments in Carry Over OTHER ,Reffered  OTHER or Received Other INSTEAD  you must comment on the COMMENT Column! OR you leave it blank if there is nothing there must all be ZEROs',
                                        'Error_Referred_Repeats_NO_COMMENT': 'In Samples Run Tab: We expect a comment for (REFERRED FAILURE -AQ & AR )Actual Reasons for assay failure.',
                                        'age_of_oldestsample_empty':'In Carryover Sample Inventory: Age of Oldest Sample(column P) MUST NOT BE EMPTY.',
                                        'Error_CARRYOVER_Sample_Failed_no_reason': 'Sample Run Tab: Number of Failed Carryover for repeats and Not for repeats must have a reason why they failed (I+J) = (M to S).',
                                        'Lab_Metricts_Machine_Breakdown_NO_Comment':'In Lab Matrics and Waste Mst tab: If you indicate the Machine breakdown, we expect you to write a comment to include specific error code for machine failure',
                                        'Error_RECEIVED_Sample_Failed_no_reason': 'Sample Run Tab: Samples Failed - (Y+Z) must have a reason (AC to AI).',
                                        'Error_REFERRED_Sample_Failed_no_reason': 'Sample Run Tab: Samples Failed - (AQ+AR) must have a reason (AU to AZ).',
                                        'Error_LIMS_Fxnality_Kits_NoEXP_or_Batchno':'In Testing Capacity Tab: If we have Reagents test kits stock in hand (O) then we need to know Expiary date ',
                                        'Error_LIMS_Fxnality_Kits_no_Batchno':'In Testing Capacity Tab: If we have Reagents test kits stock in hand (O) then we need to know Batch Number ',
                                        'Error_Testing_Capacity_NO_Kits_NO_DAYS':'In Testing Capacity Tab: If we DONT have Kits in stock (O) we expect to see the number of days Reagents stockout',
                                        'Error_Testing_Cap_Received_No_DATE': 'In Testing Capacity Tab: You indicate that you received Kits , and fail to put the DATE on when you received them',
                                        'Error_Stockout_Days_not_Indicated_well': 'In Testing Capacity Tab: You indicate that you have stockout but in LAB Matrics Tab - you fail to indicate Downtime due to Stockout',
                                        'Home_reagent_test_kit':'At HOME tab : Check the Reagent_test_kits_available the figure is above 1 million, to  trouble shoot check df_Testing_Capacity[Reagent_tests_kits_available]-column P',
                                        'error_Plasma_Carryover_greater_that_Received':'In Carryover Sample Inventory & This Week Received: Plasma Samples younger than 7 days(samples_in_lab - samples_in_backlog)<= Samples_this_Week : The diff can not be greater that samples received during the week- Justify why these are backlogs  ',
                                        'error_DBSVL_Carryover_greater_that_Received':'In Carryover Sample Inventory & This Week Received: DBS VL Samples younger than 7 days(samples_in_lab - samples_in_backlog)<= Samples_this_Week : The diff can not be greater that samples received during the week- Justify why these are backlogs  ',
                                        'error_DBSEID_Carryover_greater_that_Received':'In Carryover Sample Inventory& This Week Received: DBS EID Samples younger than 7 days(samples_in_lab - samples_in_backlog)<= Samples_this_Week : The diff can not be greater that samples received during the week- Justify why these are backlogs  ',
                                        'Error_Dates_differ': 'Your Dates in tabs are not the same may you recheck the Date column in all tabs'
                                    
                                    }

                    
            #Sent Errors to emails
            # Create a log file with error details
            #print(associated_emails,lab)
        
                #Lets send emails

            
          

          

            error_messages = f''' Make the Corrections in the Dashboard Excel !<br>
                            '''

            for error in raised_errors:
                if error in Meanings_of_Errors:
                    error_message = Meanings_of_Errors[error]
                    error_messages += f'<li><strong>{error} :</strong> {error_message}</li><br>'

            error_messages += '</ol>'

       # Example: Return the processed data
        return error_messages
    
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()

        error_messages = f"An error occurred during reading_dash:\n\nType: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
        return error_messages
  
 #    except Exception as e:
 #        # Handle any exceptions that may occur during data processing
 #        error_messages="An error occurred during reading_dash:" + str(e)
 #        return error_messages
 # 