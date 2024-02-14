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

import warnings

with warnings.catch_warnings(record=True):
    warnings.simplefilter('ignore', UserWarning)



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






def readingHUBdashboard(file):
    
    try:    

                    # Handle InMemoryUploadedFile
            # if isinstance(file, InMemoryUploadedFile):
            #     file = io.BytesIO(file.read())

            #Lets read data in different sheets
        df = pd.read_excel(file, sheet_name='Home', engine='openpyxl')  

        df_week_samp_received = pd.read_excel(file, sheet_name='Sample_Inventory_Received', engine='openpyxl') #sample Inventory received                 
        df_Reffered_Samples = pd.read_excel(file, sheet_name='Referred_Samples', engine='openpyxl')                
        df_Sample_Run =pd.read_excel(file, sheet_name='Sample_Testing', engine='openpyxl')  #Sample Testing  

        df_Testing_Capacity =pd.read_excel(file, sheet_name='Stock_Inventory', engine='openpyxl') #Stock_Inventory

        df_Lab_Metrics_Waste_Mgt =pd.read_excel(file, sheet_name='Operational_Metrics', engine='openpyxl') #Operational Matrics
        df_LIMS_Functionality =pd.read_excel(file, sheet_name='LIMS Functionality', engine='openpyxl')
        df_CLI =pd.read_excel(file, sheet_name='CLI',skiprows=1, engine='openpyxl')

        file_name = file.name if isinstance(file, InMemoryUploadedFile) else os.path.basename(file)

        #2: FORMATING THIS WEEKS SAMPLE RECEIVED
        week_new_columns = {
            'Date':'Date', 
            'Lab':'Name_of_Lab', 
            'Sample type':'Sample_Type',
            'Test type': 'Test_Type',
            'Samples in the lab (Carryover)':'Carryover_Samples_in_the_lab',
           'Total number of samples received ':'Total_samples_received',
           'Number of urgent samples received':'Urgent_Samples_received',
           'Number of samples that are rebleeds ':'Num_ReBleed_Samples',
           'Number of samples entered into LIMS':'Num_Samples_entered_LIMSonArrival',
            'LIMS Backlog (number of samples to be logged in)':'LIMs_Backlog_yetTObeEntered',
            'Number of samples rejected ': 'Num_Rejected_Samples',
            'Number of samples rejected: too old to test (60 days for plasma; 90 days DBS)':'REJECTED_too_old_to_test',        
           'Number of samples rejected: sample quality compromised':'REJECTED_Quality_issue',
           'Number of samples rejected: sample quantity insufficient':'REJECTED_Quantity_insuff',
           'Number of samples rejected: sample quality/quantity compromised in transit ':'REJECTED_Quanti_Quali_intransit_compromised',
           'Number of samples rejected: patient and sample information inconsistent':'REJECTED_Patient_SampleINFO',
           'Number of samples rejected: request form missing':'REJECTED_Missing_requestForm',
           'Number of samples rejected: sample missing': 'REJECTED_Sample_Missing',
           'Number of samples rejected: other reasons':'REJECTED_other_reasons',
            'Comments': 'comments',
           'Data Quality Checks':'Data_Quality_Checks' 


                }

        df_week_samp_received.rename(columns=week_new_columns, inplace=True) 

        df_week_samp_received = convert_excel_date(df_week_samp_received, 'Date')
        unique_key = generate_unique_key(df_week_samp_received['Name_of_Lab'][0], df_week_samp_received['Date'][0])
        df_week_samp_received['unique_key']=unique_key
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
        df_week_samp_received = df_week_samp_received.drop('Data_Quality_Checks', axis=1)



        #Deleting an entry where there is nothing
        columns_to_check = [
            'Total_samples_received', 'Urgent_Samples_received',
            'Num_ReBleed_Samples', 'Carryover_Samples_in_the_lab',
             'Num_Rejected_Samples',
            'REJECTED_too_old_to_test', 'REJECTED_Quality_issue',
            'REJECTED_Quantity_insuff',
            'REJECTED_Quanti_Quali_intransit_compromised',
            'REJECTED_Patient_SampleINFO', 'REJECTED_Missing_requestForm',
            'REJECTED_Sample_Missing', 'REJECTED_other_reasons',
              'LIMs_Backlog_yetTObeEntered',

        ]

        # Drop rows where all values in the specified columns are NaN or NaT
        df_week_samp_received = df_week_samp_received.dropna(subset=columns_to_check, how='all')
        df_week_samp_received['Date'] = pd.to_datetime(df_week_samp_received['Date']).dt.date
        df_week_samp_received['Status']='Hub'
        lab=df_week_samp_received['Name_of_Lab'][0]





        #------------THIS WEEEK TAB CHECKS------------------
        # Rejected Samples disaggregated on the reasons 


        totalrejected = df_week_samp_received[['Num_Rejected_Samples']].values
        breakeddwn = df_week_samp_received[['REJECTED_too_old_to_test', 'REJECTED_Quality_issue',
           'REJECTED_Quantity_insuff',
           'REJECTED_Quanti_Quali_intransit_compromised',
           'REJECTED_Patient_SampleINFO', 'REJECTED_Missing_requestForm',
           'REJECTED_Sample_Missing', 'REJECTED_other_reasons']].astype(float).sum(axis=1).values
        #thenulls_values = np.isnan(totalrejected) | pd.isnull(breakeddwn)

        if np.any(totalrejected[~np.isnan(totalrejected)] != breakeddwn[~pd.isnull(breakeddwn)]):
            Error_Rejected_Samples_Not_Disaggregated = 1  #**
        else:
            Error_Rejected_Samples_Not_Disaggregated = 0





        #--------------------------------------------------------------------------------------------->
        Reffered_Newcolumns={
             'Date':'Date',
            'Lab':'Name_of_Lab', 
             'Sample type':'Sample_Type', 
             'Test type':'Test_Type',
           'Total number of samples referred out':'Samples_reffered_Out', 
            'Lab Samples Referred to':'Lab_Samples_referred_to',
            'Number of urgent samples referred':'URGENT_Referred_Samples',

           'Number of urgent samples referred due to cartridge stockout ':'REFERRED_Reagent_Stockout',        
           'Number of urgent samples referred due to instrument failure':'REFERRED_Instrument_Failure',        
           'Number of urgent samples referred due to insufficient HR capacity':'REFERRED_Insuff_HR_Capacity',
           'Number of urgent samples referred due to insufficient instrument capacity ':'REFERRED_Insuff_Instrument_Capacity',
           'Number of urgent samples referred due to other reasons':'REFERRED_Other_rexns',        
           'Number of samples referred before being captured in LIMS':'REFERRED_before_captured_LIMS',       

            'Comments':'Comments',
           'Data Quality Checks':'Data_Quality_Checks'


                                }

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
        df_Reffered_Samples = df_Reffered_Samples.drop('Data_Quality_Checks', axis=1)
        df_Reffered_Samples['Status']='Hub'


            #-- Reasons for Referred Samples-- E/G=NOPQ--------------------------

        E_plasma = df_Reffered_Samples.query("Sample_Type=='Plasma'")['URGENT_Referred_Samples'].values
        N_Q_plasma = df_Reffered_Samples.query("Sample_Type=='Plasma'")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity','REFERRED_Other_rexns']].astype(float).sum(axis=1).values


        E_DBS_VL = df_Reffered_Samples.query(" Sample_Type=='DBS' and  Test_Type=='VL' ")['URGENT_Referred_Samples'].values
        N_Q_DBS_VL = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='VL' ")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity','REFERRED_Other_rexns']].astype(float).sum(axis=1).values



        E_DBS_EID = df_Reffered_Samples.query(" Sample_Type=='DBS' and  Test_Type=='EID' ")['URGENT_Referred_Samples'].values
        N_Q_DBS_EID = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='EID' ")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity','REFERRED_Other_rexns']].astype(float).sum(axis=1).values


        E_plasma = np.nan_to_num(E_plasma, nan=0)
        N_Q_plasma = np.nan_to_num(N_Q_plasma, nan=0)


        E_DBS_VL = np.nan_to_num(E_DBS_VL, nan=0)
        N_Q_DBS_VL = np.nan_to_num(N_Q_DBS_VL, nan=0)


        E_DBS_EID = np.nan_to_num(E_DBS_EID, nan=0)
        N_Q_DBS_EID = np.nan_to_num(N_Q_DBS_EID, nan=0)

        plsama_vl_ref = int( np.any(E_plasma.size > 0 and N_Q_plasma.size > 0 and E_plasma != N_Q_plasma))

        dbs_vl_ref = int( np.any(E_DBS_VL.size > 0 and N_Q_DBS_VL.size > 0 and E_DBS_VL != N_Q_DBS_VL))

        dbs_eid_ref = int(np.any(E_DBS_EID.size > 0 and N_Q_DBS_EID.size > 0 and E_DBS_EID != N_Q_DBS_EID))


        # if H>0 then J must have lab from----------------------------------------

        numc=df_Reffered_Samples['Samples_reffered_Out']
        criteria= (numc>0) & df_Reffered_Samples['Lab_Samples_referred_to'].isna()
        Error_Referred_TO_Blank=int(np.any(criteria))



    #---------------------------------------------------------------------------------------------------------------


        #SAMPLE RUN
        Run_NewColumns={

            'Date\n(Month/Day/Year)':'Date',
            'Lab':'Name_of_Lab',
            'Sample Type':'Sample_Type',
            'Test Type':'Test_Type', 
            'Platform':'Platform_Roche_Abbott_Hologic_BMX',        
           'Total number of samples tested':'RECEIVED_TOTAL_Sample_RUN', 
            'Total number of failed tests eligible for retesting':'RECEIVED_FAILED_bt_Elig_REPEAT',
            'Total number of retests run':'RECEIVED_REPEATS_RUN',
            'Total number of failed tests after final retesting':'RECEIVED_FAILED_after_FINAL_repeat_testing',
            ' Number of failed tests: sample handling error at lab':'RECEIVED_FAILED_sample_handling_error_lab',
            'Number of failed tests: reagent quality issues ':'RECEIVED_FAILED_reagent_quality_issues',
            'Number of failed tests: QC failure':'RECEIVED_FAILED_QC_failure',
            'Number of failed tests: power failure':'RECEIVED_FAILED_power_failure',
            'Number of failed tests: mechanical failure ':'RECEIVED_FAILED_mechanical_failure',
            'Number failed tests: other reasons':'RECEIVED_OTHER',
              'Total number of samples with a valid test result':'Sample_with_Valid_test_result',
           'Total number of samples with a failed test result ':'Sample_with_FAILED_test_result',
           'Number of results dispatched by lab':'RECEIVED_Results_dispatched_by_lab', 
            'Comments':'Comments',
           'Data Quality Checks':'Data_Quality_Checks' 


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
        df_Sample_Run = df_Sample_Run.drop('Data_Quality_Checks', axis=1)
        df_Sample_Run['Status']='Hub'



        sampleRUN_to_check=['RECEIVED_TOTAL_Sample_RUN',      
           'RECEIVED_FAILED_bt_Elig_REPEAT',
           'RECEIVED_REPEATS_RUN', 'RECEIVED_FAILED_after_FINAL_repeat_testing',
           'RECEIVED_FAILED_sample_handling_error_lab',
           'RECEIVED_FAILED_reagent_quality_issues', 'RECEIVED_FAILED_QC_failure',
           'RECEIVED_FAILED_power_failure', 'RECEIVED_FAILED_mechanical_failure',
           'RECEIVED_OTHER', 'Sample_with_Valid_test_result',
           'Sample_with_FAILED_test_result', 'RECEIVED_Results_dispatched_by_lab']
        # Drop rows where all values in the specified columns are NaN or NaT
        df_Sample_Run = df_Sample_Run.dropna(subset=sampleRUN_to_check, how='all')

        temp_sum = df_Sample_Run[['RECEIVED_FAILED_bt_Elig_REPEAT']].astype(float).sum(axis=1)
        condition = (temp_sum > 0) & df_Sample_Run['Comments'].isna()
        Error_Repeats_but_NO_COMMENT=int(np.any(condition))



                #--------------Failed Elig & Not Elig (I+J)= M to R-------------------------

        Rec_failed=df_Sample_Run [['RECEIVED_FAILED_bt_Elig_REPEAT']].astype(float).sum(axis=1).values


        rec=df_Sample_Run[['RECEIVED_FAILED_reagent_quality_issues', 'RECEIVED_FAILED_QC_failure',
                        'RECEIVED_FAILED_power_failure', 'RECEIVED_FAILED_mechanical_failure',
                        'RECEIVED_FAILED_sample_handling_error_lab', 'RECEIVED_OTHER']].astype(float).sum(axis=1).values

            #received
        Error_Sample_Failed_no_reason=int( np.any( rec!=Rec_failed) )



    #------------------------------------------------------------------------------------------------------------------->
      #5 TESTING CAPACITY
        Capacity_Newcolumns={
            'Date':'Date',
            'Lab':'Name_of_Lab', 
            'Test type':'Test_Type',
            'Platform':'Platform_Roche_Abbott_Hologic_BMX',
           'Number of cartridges received from NATPHAM':'NatPharm_Kits_Received_inThisWK',
           'Cartridges loaned out to other labs':'Reagent_kits_to_OTHER_Labs', 
            'Loaned to':'Lab_Name_Loaned_to',        
           'Cartridges received on loan from other labs':'Reagent_kits_RECEIVED_from_OTHER_Labs', 
            'Loaned from':'Lab_Name_Received_from',
           'Cartridge stock on hand':'Reagent_tests_kits_Stock_on_hand', 
            'Expiry date of catridges':'Reagent_tests_kits_available_Expiry_Date',        
           'Cartridges expired before use':'Tests_expired_this_month_before_use',
           'Comments':'Comments_Reagent_Stock_Status'


            }

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
        df_Testing_Capacity['Status']='Hub'

        #print(df_Testing_Capacity.columns)

        Capacity_to_check=['NatPharm_Kits_Received_inThisWK', 'Reagent_kits_to_OTHER_Labs',
           'Lab_Name_Loaned_to', 'Reagent_kits_RECEIVED_from_OTHER_Labs',
           'Lab_Name_Received_from', 'Reagent_tests_kits_Stock_on_hand',
           'Reagent_tests_kits_available_Expiry_Date',
           'Tests_expired_this_month_before_use']
        # Drop rows where all values in the specified columns are NaN or NaT
        df_Testing_Capacity = df_Testing_Capacity.dropna(subset=Capacity_to_check, how='all')
        df_Testing_Capacity['Date'] = pd.to_datetime(df_Testing_Capacity['Date']).dt.date



        Error_Loaned_to_LAB_not_mentioned= any((df_Testing_Capacity['Reagent_kits_to_OTHER_Labs']>0)  & (df_Testing_Capacity['Lab_Name_Loaned_to'].isnull())  )

        Error_Loaned_FROM_LAB_not_mentioned= any((df_Testing_Capacity['Reagent_kits_RECEIVED_from_OTHER_Labs']>0)  & (df_Testing_Capacity['Lab_Name_Received_from'].isnull())  )



        Error_Reagents_kits_NoEXP=int(np.any( (df_Testing_Capacity['Reagent_tests_kits_Stock_on_hand'] > 0) & 
                                                                (df_Testing_Capacity['Reagent_tests_kits_available_Expiry_Date'].isnull() )
                                                            )
                                     )

     #------------------------------------------------------------------------------------------------------------------->


        #6: LAB METRICS AND WASTE MGT
        Metrics_NewColumns={
            'Date':'Date',
            'Lab':'Name_of_Lab',     
            'Platform ':'Platform_Roche_Abbott_Hologic_BMX',
           'Number of hours in a shift (e.g 8hr, 12hr, 16hr or 24 hr shift)': 'Hrs_in_Shift',
            'Machine Capacity (Modules)':'Machine_Capacity_Modules',
            'Number of functional modules':'Number_of_functional_modules' ,       
           'Total operational hours ':'Actual_number_of_days_platform_used',

           'Downtime (hours lost) due to power outage':'Downtime_Power_Outage',        
           'Downtime (hours lost) due to technical/mechanical failure':'Downtime_Mechanical_Failure',        
           'Downtime due to cartidge stock out/expiry ':'Downtime_Reagent_Stockout_Expiry',
           'Downtime due to HRH unavailability':'Downtime_Staff_Unavailability', 
           'Comments':'Comments_on_ErrorCodes_for_Mach_failure', 
               }

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

        df_Lab_Metrics_Waste_Mgt['Status']='Hub'

        Metrics_to_check=[ 'Hrs_in_Shift', 'Actual_number_of_days_platform_used',
           'Downtime_Power_Outage', 'Downtime_Mechanical_Failure',
           'Downtime_Reagent_Stockout_Expiry',  'Downtime_Staff_Unavailability',
         ]
        # Drop rows where all values in the specified columns are NaN or NaT
        df_Lab_Metrics_Waste_Mgt = df_Lab_Metrics_Waste_Mgt.dropna(subset=Metrics_to_check, how='all')
        df_Lab_Metrics_Waste_Mgt['Date'] = pd.to_datetime(df_Lab_Metrics_Waste_Mgt['Date']).dt.date



        Total_machine_downtime_hours= df_Lab_Metrics_Waste_Mgt[['Downtime_Power_Outage', 
                                                                'Downtime_Mechanical_Failure', 'Downtime_Reagent_Stockout_Expiry',
                                                                'Downtime_Staff_Unavailability','Actual_number_of_days_platform_used']].astype(float).sum(axis=1).sum()

        if (df_Lab_Metrics_Waste_Mgt['Hrs_in_Shift'].astype(float).sum()*7 !=Total_machine_downtime_hours):
            Error_issue_with_MachineDowntime=1
        else:
            Error_issue_with_MachineDowntime=0

        #Lab_Metricts_Machine_Breakdown_NO_Comment=np.any( (df_Lab_Metrics_Waste_Mgt['Actual_number_of_days_platform_used']< ) & (df_Lab_Metrics_Waste_Mgt['Comments_on_ErrorCodes_for_Mach_failure'].isnull()) )

    #----------------------------------------------------------------------------------------------------------------------------

        # LIMS FUNCTIONALITY
        LIMS_FuncNewColumns={

            'Day':'Day', 
            'Date\n(Month/Day/Year)':'Date',
            'Lab':'Name_of_Lab',
           'Total number of hours LIMS was operational':'Hours_of_Functionality',
           'Total number of hours LIMS was offline': 'Hours_of_Downtime',
           'Downtime due to hardware problem (hours)':'Downtime_due_to_hardware_problem',
           'Downtime due to internet connection interrupted (hours) ':'Downtime_due_to_internet_connection',
           'Downtime due to software problem (hours)':'Downtime_due_to_software_problem',

           'Time between LIMS going down and the incident being reported in help desk (hours)':'TimeLIMS_going_down_HELPDESK',
           'Time between the incident being reported in help desk, and technician responding to issue (hours)':'TimeHELPDESK_TechRESPONSE',
           'Time between technician responding to issue and resolution of issue (hours)':'TimeTechRESPONSE_Resolution',
           'Comments':'Comments'

                     }

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
        df_LIMS_Functionality = df_LIMS_Functionality.drop('Day', axis=1)
        df_LIMS_Functionality['Status']='Hub'



        Functionality_column_to_check=['Hours_of_Functionality',
           'Hours_of_Downtime', 'Downtime_due_to_hardware_problem',
           'Downtime_due_to_internet_connection',
           'Downtime_due_to_software_problem',
           'TimeLIMS_going_down_HELPDESK', 'TimeHELPDESK_TechRESPONSE',
           'TimeTechRESPONSE_Resolution']

        # Drop rows where all values in the specified columns are NaN or NaT
        df_LIMS_Functionality = df_LIMS_Functionality.dropna(subset=Functionality_column_to_check, how='all')
        df_LIMS_Functionality['Date'] = pd.to_datetime(df_LIMS_Functionality['Date']).dt.date



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

    #---------------------------------------------------------------------------------------------------------------->


        #CLI SHEET
        CLI_NewColumns={
            'Date':'Date', 
            'Lab':'Name_of_Lab',
           'Number of incidents requiring communication between lab and clinics ':'Incidents_nid_Comms_btwn_LAB_CLINICS' ,
           'Number of correspondences ':'Number_of_correspondences',
           'Number of correspondences for rebleed requests sent for rejected, failed or lost samples':'ReBLEEDs_requests_sent',

           'Number of correspondences for clarifications needed from clinic regarding test requests':'Corrsp_test_requests_clinic_clarificatons',
           'Number of correspondences for clinics to send samples to different laboratory':'Corrsp_clinic_send_spec_to_diff_lab',
            'Number of correspondences for clinics  to adjust testing algorithm due to laboratory stock outs or capacity':'Corrsp_clinic_adjust_algorith_stockouts',
           'Number of correspondences for clinics to expect delays in testing and result return due to laboratory stock outs or capacity':'Corrsp_clinic_expect_delay_due_lab_stockouts',
           'Number of correspondences for clinics to halt sample collection due to laboratory stock outs or capacity':'Corrsp_clinic_halt_spec_collection_due_lab_stockouts',

           'Unresolved missing results from last week':'LAST_WEEK_Unresolved_missing_results',          
           'Number of missing results requested by facilities this week':'THIS_WEEK_Missing_results_req_by_Facilities',   

           'Missing results outcomes:\nResults found and shared with facilities':'RESOLVED_Missing_results_outcomes_found_Shared_with_facilities',       
           'LIMS Interface Failed':'RESOLVED_LIMS_Interface_Failed', 
            'Results not documented at facility':'RESOLVED_Results_not_documented_at_facility',
           'Results sent to wrong facility':'RESOLVED_Results_sent_to_wrong_facility', 
            'Results not yet dispatched/printed':'RESOLVED_Results_not_yet_dispatched_printed',
           'Specimen not received: rebleed sent':'RESOLVED_Specimens_not_received_rebleed_sent',         
            'Specimen rejected':'RESOLVED_Specimens_rejected',        
           'Result transmission to hub failed':'Result_transmission_to_hub_failed',         
            'Result pending publishing':'RESOLVED_Result_pending_publishing',        
           'Pending testing':'UNRESOLVED_Pending_testing', 
            'Referred - awaiting results':'UNRESOLVED_Referred_awaiting_results',
           'Investigation in progress':'UNRESOLVED_Investigation_in_progress',
            'Comments':'Comments',
            'Data Quality Checks':'Data_Quality_Checks'   



                      }

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
        df_CLI = df_CLI.drop('Data_Quality_Checks', axis=1)
        df_CLI['Status']='Hub'


        CLI_to_check=['Incidents_nid_Comms_btwn_LAB_CLINICS',
           'Number_of_correspondences', 'ReBLEEDs_requests_sent',
           'Corrsp_test_requests_clinic_clarificatons',
           'Corrsp_clinic_send_spec_to_diff_lab',
           'Corrsp_clinic_expect_delay_due_lab_stockouts',
           'Corrsp_clinic_halt_spec_collection_due_lab_stockouts',
           'LAST_WEEK_Unresolved_missing_results',
           'THIS_WEEK_Missing_results_req_by_Facilities',
           'RESOLVED_Missing_results_outcomes_found_Shared_with_facilities',
           'RESOLVED_LIMS_Interface_Failed',
           'RESOLVED_Results_not_documented_at_facility',
           'RESOLVED_Results_sent_to_wrong_facility',
           'RESOLVED_Results_not_yet_dispatched_printed',
           'RESOLVED_Specimens_not_received_rebleed_sent',
           'RESOLVED_Specimens_rejected', 'Result_transmission_to_hub_failed',
           'RESOLVED_Result_pending_publishing', 
            'UNRESOLVED_Pending_testing',
           'UNRESOLVED_Referred_awaiting_results',
           'UNRESOLVED_Investigation_in_progress']
        # Drop rows where all values in the specified columns are NaN or NaT
        df_CLI = df_CLI.dropna(subset=CLI_to_check, how='all')
        df_CLI['Date'] = pd.to_datetime(df_CLI['Date']).dt.date


        #1: CLI CHECK Internal-RESOLVED (K-Q = J)-------------------------------
        xx=df_CLI[['RESOLVED_Missing_results_outcomes_found_Shared_with_facilities']].values
        zzz=df_CLI[['RESOLVED_LIMS_Interface_Failed','RESOLVED_Results_not_documented_at_facility','RESOLVED_Results_sent_to_wrong_facility','RESOLVED_Results_not_yet_dispatched_printed','RESOLVED_Specimens_not_received_rebleed_sent','RESOLVED_Specimens_rejected','Result_transmission_to_hub_failed','RESOLVED_Result_pending_publishing']].astype(float).sum(axis=1).values

        if np.any( xx[~np.isnan(xx)] == zzz[~np.isnan(zzz)] ):
            Error_cli_resolved_not_disaggregated = 0
        else:
            # Handle the case when the arrays are not equal
            Error_cli_resolved_not_disaggregated = 1

      #-----------------CLI- ---------------------   
        JRST=df_CLI[['RESOLVED_Missing_results_outcomes_found_Shared_with_facilities','UNRESOLVED_Pending_testing','UNRESOLVED_Referred_awaiting_results','UNRESOLVED_Investigation_in_progress']].astype(float).sum(axis=1).values 
        HI=df_CLI[['LAST_WEEK_Unresolved_missing_results','THIS_WEEK_Missing_results_req_by_Facilities']].astype(float).sum(axis=1).values
        if np.any( JRST== HI ):

            Error_resolved_unresolved_notMATCH=0

        else:
            Error_resolved_unresolved_notMATCH=1


        # CORRESP=df_CLI [['Number_of_correspondences']].astype(float).sum(axis=1).values  

        # DIS_CORRESP=df_CLI[['ReBLEEDs_requests_sent',
        #                        'Corrsp_test_requests_clinic_clarificatons',
        #                        'Corrsp_clinic_send_spec_to_diff_lab',
        #                        'Corrsp_clinic_expect_delay_due_lab_stockouts',
        #                        'Corrsp_clinic_halt_spec_collection_due_lab_stockouts']].astype(float).sum(axis=1).values

        #     #received
        # Error_Correspondances_notDisaggregated=int( np.any( DIS_CORRESP!=CORRESP) )


    #----------------------------------------------------------------------------------------------------------->



        #Number of samples received this week
        Number_of_samples_received_this_week=df_week_samp_received.query("Test_Type == 'VL'")['Total_samples_received'].astype(float).sum()

        Number_of_samples_that_are_rejected= df_week_samp_received['Num_Rejected_Samples'].astype(float).sum()

        #Number of rejected with reasons
        rej=df_week_samp_received[[  'REJECTED_too_old_to_test', 'REJECTED_Quality_issue',
                                'REJECTED_Quantity_insuff',
                                'REJECTED_Quanti_Quali_intransit_compromised',
                                'REJECTED_Patient_SampleINFO', 'REJECTED_Missing_requestForm',
                                'REJECTED_Sample_Missing', 'REJECTED_other_reasons']].astype(float).sum(axis=1)
        Total_with_reasons_for_rejection=rej.sum()     


        # Create a dictionary for Dashboard indicators
        indicators = [
            {           
                'Number_of_samples_received_this_week': Number_of_samples_received_this_week,             
                'Number_of_samples_that_are_rejected': Number_of_samples_that_are_rejected,
                'Total_with_reasons_for_rejection': Total_with_reasons_for_rejection
            }
        ]

        # Create a DataFrame from the list of dictionaries
        df_indicators = pd.DataFrame(indicators)
        df_indicators['Date']=df_week_samp_received['Date'][0]
        df_indicators['SourceFile']=file_name
        df_indicators['LAB']=lab
        df_indicators['unique_key']=unique_key


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
            SELECT DISTINCT
                a.[Date], a.[Name_of_Lab] 
                FROM   [LSS].[dbo].[Dash_This_week_Rec_Samples] a
            WHERE  CAST(a.[date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
              AND CAST(a.[date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
               and a.status='hub'
        '''

        # Execute the query, passing in the date range as parameters
        df_last_week = pd.read_sql(
            text(sql), 
            engine 
          
        )

        df_week_samp_received['Date'] = pd.to_datetime(df_week_samp_received['Date']).dt.strftime('%Y-%m-%d')
        df_last_week['Date'] = pd.to_datetime(df_last_week['Date']).dt.strftime('%Y-%m-%d')
        # CHECK IF THE RECORD EXIST IN DATABASE
        df_tuples = set(df_week_samp_received[['Date', 'Name_of_Lab']].apply(tuple, axis=1))
        df_last_week_tuples = set(df_last_week.query("Name_of_Lab == @lab")[['Date', 'Name_of_Lab']].apply(tuple, axis=1))




        # CHECKING DUPLICATES
        common_records=df_tuples.intersection(df_last_week_tuples)


        if not df_tuples.intersection(df_last_week_tuples): #no pure dups
            error_dups=0
        else:
            error_dups=1







        # Extract 'Date' columns with non-null values from each DataFrame

        dates_df_week_samp_received = pd.to_datetime(df_week_samp_received[df_week_samp_received['Date'].notna()]['Date'])  
        dates_df_Reffered_Samples = pd.to_datetime(df_Reffered_Samples[df_Reffered_Samples['Date'].notna()]['Date'])
        dates_df_Sample_Run = pd.to_datetime(df_Sample_Run[df_Sample_Run['Date'].notna()]['Date'])
        dates_df_Testing_Capacity = pd.to_datetime(df_Testing_Capacity[df_Testing_Capacity['Date'].notna()]['Date'])
        dates_df_Lab_Metrics_Waste_Mgt = pd.to_datetime(df_Lab_Metrics_Waste_Mgt[df_Lab_Metrics_Waste_Mgt['Date'].notna()]['Date'])
        dates_df_LIMS_Functionality = pd.to_datetime(df_LIMS_Functionality[df_LIMS_Functionality['Date'].notna()]['Date'])
        dates_df_CLI = pd.to_datetime(df_CLI[df_CLI['Date'].notna()]['Date'])



        # Extract unique dates from each DataFrame's 'Date' column
        unique_dates_df_indicators = set(dates_df_week_samp_received.unique())


        # Check if all sets of unique dates are the same
        all_dates_are_the_same = all([     
            unique_dates_df_indicators == set(dates_df_Sample_Run.unique()),
            unique_dates_df_indicators == set(dates_df_Testing_Capacity.unique()),
            unique_dates_df_indicators == set(dates_df_Lab_Metrics_Waste_Mgt.unique()),
            unique_dates_df_indicators == set(dates_df_LIMS_Functionality.unique()),
            unique_dates_df_indicators == set(dates_df_CLI.unique()),

        ])

        #error
        Error_Dates_differ=int(not all_dates_are_the_same)


        raised_errors= []
        list_of_Errors=[Error_Rejected_Samples_Not_Disaggregated,Error_Referred_TO_Blank,plsama_vl_ref,
                        dbs_vl_ref,dbs_eid_ref,Error_Repeats_but_NO_COMMENT,Error_Sample_Failed_no_reason,Error_Loaned_to_LAB_not_mentioned,
                        Error_Loaned_FROM_LAB_not_mentioned,Error_Reagents_kits_NoEXP
                        ,Error_issue_with_MachineDowntime,
                        Error_LIMS_Downtime,Error_LIMS_Fxn_NO_COMMENTS,Error_cli_resolved_not_disaggregated,Error_resolved_unresolved_notMATCH,
                                                      error_dups,Error_Dates_differ
                         ]

        variable_names=['Error_Rejected_Samples_Not_Disaggregated','Error_Referred_TO_Blank','plsama_vl_ref',
                        'dbs_vl_ref','dbs_eid_ref','Error_Repeats_but_NO_COMMENT','Error_Sample_Failed_no_reason','Error_Loaned_to_LAB_not_mentioned',
                        'Error_Loaned_FROM_LAB_not_mentioned','Error_Reagents_kits_NoEXP'
                         ,'Error_issue_with_MachineDowntime',
                        'Error_LIMS_Downtime','Error_LIMS_Fxn_NO_COMMENTS','Error_cli_resolved_not_disaggregated','Error_resolved_unresolved_notMATCH',
                                                         'error_dups','Error_Dates_differ'
                       ]

            # Check if all variables are equal to 0
        if all(variable == 0 for variable in list_of_Errors):

            df_week_samp_received.to_sql('Dash_This_week_Rec_Samples', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_Reffered_Samples.to_sql('Dash_Referred_Samples', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_Sample_Run.to_sql('Dash_Sample_Run', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_Testing_Capacity.to_sql('Dash_Testing_Capacity', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed           

            df_Lab_Metrics_Waste_Mgt.to_sql('Dash_Lab_Metrics_Waste_Mgt', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_LIMS_Functionality.to_sql('Dash_LIMS_Functionality', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_CLI.to_sql('Dash_CLI', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed





     #             df.to_sql('Dash_Carryover_Sample_inventory', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed





            error_messages=lab +'    :Dashboard:  Successfully Uploaded'
             # Example: Return the processed data
            return error_messages



        else: 



            for i in range(len(list_of_Errors)):

                if list_of_Errors[i]==1:
                    raised_errors.append(variable_names[i])

                Meanings_of_Errors = {

                'error_dups': 'Dashboard already uploaded or you didnt change the lab name',       
                'Error_Referred_TO_Blank': 'In Referred Samples Tab: You forgot to put Referred to facility.',

                'plsama_vl_ref':'In Referred Samples tab:  Plasma VL, Column  (samples referred out)  is not tallying with reasons (H-L)  ',
                'dbs_vl_ref':'In Referred Samples tab:  DBS VL, Column  (samples referred out)  is not tallying with reasons  (H-L)',
                'dbs_eid_ref':'In Referred Samples tab:  DBS EID, Column (samples referred out) is not tallying with reasons  (H-L)',
                'Error_Sample_Failed_no_reason': 'Sample Testing Tab: Samples Failed - (G) must have a reason (J to O).',
                'Error_Repeats_but_NO_COMMENT': 'In Samples Testing Tab: We expect a comment for (TESTS FAILURE -I & J) Actual Reasons for assay failure.',
                'Error_Loaned_to_LAB_not_mentioned':'In Stock Inventory Tab: You indicated that Reagent kits was loaned out but fail to indicate the Lab Receiving those kits' ,                         
                'Error_Loaned_FROM_LAB_not_mentioned':'In Stock Inventory Tab: You indicated that Reagent kits was loaned  but fail to indicate the Lab where those kits are coming from' , 
                'Error_Reagents_kits_NoEXP':'In Stock Inventory Tab: If we have Reagents test kits stock in hand (O) then we need to know Expiary date ',            
                'Error_Rejected_Samples_Not_Disaggregated': 'In  Sample Inventory Received Tab: Number of Rejected Samples (J) are not disaggregated by reason why they are rejected (K to R).',
                 'Error_issue_with_MachineDowntime': 'In Operational Matrics Tab: The Total Machine Downtime (hrs) Sum of (Downtime coz Power Outage, Downtime coz of Mechanical Failure, Downtime coz of Reagent Stockout Expiry, Downtime coz of Staff Unavailability, Total_Operational_Hrs)- [F to M] MUST BE EQUAL TO  Hrs in working Shift (column E * 7) -).',
                'Error_LIMS_Downtime': 'In LIMS Functionality Tab: Hours_of_Functionality and Hours_of_Downtime are not matching.',
                'Error_LIMS_Fxn_NO_COMMENTS':'In LIMS Functionality Tab: You Indicate that there are Hours in which LIMS was down (F) but You dont give a comment as to what happened' ,
                'Error_cli_resolved_not_disaggregated': 'In CLI Tab: Column L = Sum of columns (M) to (T), this criteria is not met.\n'
                                                                     'i.e RESOLVED Missing results outcomes found and Shared with_facilities must be disaggregated in columns (M-T).',

                'Error_resolved_unresolved_notMATCH': 'In CLI Tab: Column (J and K)- Unresolved missing results from last week and Number of missing results requested by facilities this week MUST BE EQUAL TO (column LUVW) pending testing,Referred waiting results, investigation in progress  and Results found and shared with facilities ',
                #'Error_Correspondances_notDisaggregated':'In CLI Tab: The number of correspondences (D) are not well disaggregated (E to I)',
               
                'Error_Dates_differ': 'Your Dates in tabs are not the same may you recheck the Date column in all tabs'


                                        }







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
