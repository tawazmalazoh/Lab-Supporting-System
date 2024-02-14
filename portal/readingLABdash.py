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
            AND  CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
            AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
        """
        conn.execute(deduplication_query)


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


# x = '2023-05-28 00:00:00'
# # Example usage:
# input_date = x  # Replace this with the date you want to check
# result = is_last_sunday(input_date)





#file='C:/Users/T Dadirai/Desktop/LATEST REPORT/Weekly Dashboard Collection Tool V18.xlsx'

def readingLabdashboard(file):
    

    try:    

        df = pd.read_excel(file, sheet_name='Home', engine='openpyxl') 
        df_week_samp_received = pd.read_excel(file, sheet_name='Sample_Inventory_Received', engine='openpyxl') #sample Inventory received                 
        df_Reffered_Samples = pd.read_excel(file, sheet_name='Referred_Samples', engine='openpyxl')                
        df_Sample_Run =pd.read_excel(file, sheet_name='Sample_Testing', engine='openpyxl')  #Sample Testing 
        df_Testing_Capacity =pd.read_excel(file, sheet_name='Stock_Inventory', engine='openpyxl') #Stock_Inventory        
        df_Lab_Metrics_Waste_Mgt =pd.read_excel(file, sheet_name='Operational_Metrics', engine='openpyxl') #Operational Matrics
        df_LIMS_Functionality =pd.read_excel(file, sheet_name='LIMS_Functionality', engine='openpyxl')
        df_Specimen_Transport =pd.read_excel(file, sheet_name='Specimen_Transport', engine='openpyxl')
        df_CLI =pd.read_excel(file, sheet_name='CLI',skiprows=1, engine='openpyxl')
        df_power_outages =pd.read_excel(file, sheet_name='Power Outages', engine='openpyxl')

        file_name = file.name if isinstance(file, InMemoryUploadedFile) else os.path.basename(file)

        #2: FORMATING THIS WEEKS SAMPLE RECEIVED
        week_new_columns = {
            'Date':'Date', 
            'Lab':'Name_of_Lab', 
            'Sample type':'Sample_Type',
            'Test type': 'Test_Type',
           'Total number of samples received ':'Total_samples_received',
           'Number of urgent samples received':'Urgent_Samples_received',
           'Number of samples that are rebleeds ':'Num_ReBleed_Samples',
           'Samples in the lab (Carryover)':'Carryover_Samples_in_the_lab',
           'Samples in backlog (Intra lab TAT >7 days)':'Samples_in_backlog_Intra_lab_TAT_7_days',
           'Number of samples rejected ': 'Num_Rejected_Samples',
           'Number of samples rejected: too old to test (60 days plasma & 90 days DBS)':'REJECTED_too_old_to_test',
           'Number of samples rejected: sample quality compromised':'REJECTED_Quality_issue',
           'Number of samples rejected: sample quantity insufficient':'REJECTED_Quantity_insuff',
           'Number of samples rejected: sample quality/quantity compromised in transit ':'REJECTED_Quanti_Quali_intransit_compromised',
           'Number of samples rejected: patient and sample information inconsistent':'REJECTED_Patient_SampleINFO',
           'Number of samples rejected: request form missing':'REJECTED_Missing_requestForm',
           'Number of samples rejected: sample missing': 'REJECTED_Sample_Missing',
           'Number of samples rejected: other reasons':'REJECTED_other_reasons',

           'Number of samples logged into LIMS at hub prior to arrival':'LIMS_hub_logged_prior_to_arrival',
           'Number of samples logged into LIMS during week of arrival':'LIMS_logged_during_week_of_arrival',

           'LIMS Backlog (number of samples to be logged in)':'LIMs_Backlog_yetTObeEntered',
           'LIMS Backlog (number of shipments to be received)':'LIMS_Backlog_shipments_to_be_received', 
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
            'Samples_in_backlog_Intra_lab_TAT_7_days', 'Num_Rejected_Samples',
            'REJECTED_too_old_to_test', 'REJECTED_Quality_issue',
            'REJECTED_Quantity_insuff',
            'REJECTED_Quanti_Quali_intransit_compromised',
            'REJECTED_Patient_SampleINFO', 'REJECTED_Missing_requestForm',
            'REJECTED_Sample_Missing', 'REJECTED_other_reasons',
            'LIMS_hub_logged_prior_to_arrival',
            'LIMS_logged_during_week_of_arrival', 'LIMs_Backlog_yetTObeEntered',
            'LIMS_Backlog_shipments_to_be_received'
        ]

        # Drop rows where all values in the specified columns are NaN or NaT
        df_week_samp_received = df_week_samp_received.dropna(subset=columns_to_check, how='all')
        df_week_samp_received['Date'] = pd.to_datetime(df_week_samp_received['Date']).dt.date
        df_week_samp_received['Status']='Lab'
        lab=df_week_samp_received['Name_of_Lab'][0]





        #------------THIS WEEEK TAB CHECKS------------------
        # Rejected Samples disaggregated on the reasons 


        totalrejected = df_week_samp_received[['Num_Rejected_Samples']].values
        breakeddwn = df_week_samp_received[['REJECTED_too_old_to_test', 'REJECTED_Quality_issue',
           'REJECTED_Quantity_insuff',
           'REJECTED_Quanti_Quali_intransit_compromised',
           'REJECTED_Patient_SampleINFO', 'REJECTED_Missing_requestForm',
           'REJECTED_Sample_Missing', 'REJECTED_other_reasons',]].astype(float).sum(axis=1).values
        #thenulls_values = np.isnan(totalrejected) | pd.isnull(breakeddwn)

        if np.any(totalrejected[~np.isnan(totalrejected)] != breakeddwn[~pd.isnull(breakeddwn)]):
            Error_Rejected_Samples_Not_Disaggregated = 1  #**
        else:
            Error_Rejected_Samples_Not_Disaggregated = 0


        #------- Entered in LIMS(prior+thisweek) must be less than Total Received Samples--
        mumarr=pd.isnull(df_week_samp_received['LIMS_logged_during_week_of_arrival'].values)  
        priormumarr=pd.isnull(df_week_samp_received['LIMS_hub_logged_prior_to_arrival'].values)  
        recnulls_values = np.isnan(df_week_samp_received['Total_samples_received'].values) 

        E=df_week_samp_received['Total_samples_received'].values[~recnulls_values]
        P=df_week_samp_received['LIMS_logged_during_week_of_arrival'].values[~mumarr]
        Z=df_week_samp_received['LIMS_hub_logged_prior_to_arrival'].values[~priormumarr]
        Error_Carryover_EnteredLIMS_mothan_Received=int(np.any(P+E>E))


        #--------------------------------------------------------------------------------------------->
        Reffered_Newcolumns={
             'Date':'Date',
            'Lab':'Name_of_Lab', 
             'Sample type':'Sample_Type', 
             'Test type':'Test_Type',
           'Total number of samples referred out':'Samples_reffered_Out', 
            'Lab Samples Referred to':'Lab_Samples_referred_to',
            'Swift Consignment \nNumber ':'Swift_Consignment_Number',
           'Number of samples referred through LIMS referral module':'Samples_Captured_thru_LIMS',

           'Number of samples referred due to reagent/kit stockout ':'REFERRED_Reagent_Stockout',
           'Number of samples referred due to instrument failure':'REFERRED_Instrument_Failure',
           'Number of samples referred due to insufficient HR capacity':'REFERRED_Insuff_HR_Capacity',
           'Number of samples referred due to insufficient instrument capacity ':'REFERRED_Insuff_Instrument_Capacity',
           'Number of samples referred due to other reasons':'REFERRED_Other_rexns',

           'Total number of referred samples received':'Referred_Sample_Received',
            'Referred from':'Referred_From',
           'Number of referred samples received that were carry-overs':'CARRYOVER_Referred_Sample_Received',         
           'Number of referred samples marked as urgent':'URGENT_Referred_Samples',
           'Total number of referred samples that are rebleeds':'REBLEED_Referred_Samples',
           'Total number of referred samples ultimately rejected':'REJECTED_Referred_Samples',
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
        df_Reffered_Samples['Status']='Lab'


            #-- Reasons for Referred Samples-- E/G=NOPQ--------------------------
        G_plasma=G = df_Reffered_Samples.query("Sample_Type=='Plasma'")['Referred_Sample_Received'].values
        E_plasma = df_Reffered_Samples.query("Sample_Type=='Plasma'")['Samples_reffered_Out'].values
        N_Q_plasma = df_Reffered_Samples.query("Sample_Type=='Plasma'")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity','REFERRED_Other_rexns']].astype(float).sum(axis=1).values

        G_DBS_VL=G = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='VL' ")['Referred_Sample_Received'].values
        E_DBS_VL = df_Reffered_Samples.query(" Sample_Type=='DBS' and  Test_Type=='VL' ")['Samples_reffered_Out'].values
        N_Q_DBS_VL = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='VL' ")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity','REFERRED_Other_rexns']].astype(float).sum(axis=1).values


        G_DBS_EID=G = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='EID' ")['Referred_Sample_Received'].values
        E_DBS_EID = df_Reffered_Samples.query(" Sample_Type=='DBS' and  Test_Type=='EID' ")['Samples_reffered_Out'].values
        N_Q_DBS_EID = df_Reffered_Samples.query(" Sample_Type=='DBS' and Test_Type=='EID' ")[['REFERRED_Reagent_Stockout', 'REFERRED_Instrument_Failure', 'REFERRED_Insuff_HR_Capacity',
                                    'REFERRED_Insuff_Instrument_Capacity','REFERRED_Other_rexns']].astype(float).sum(axis=1).values

        G_plasma = np.nan_to_num(G_plasma, nan=0)
        E_plasma = np.nan_to_num(E_plasma, nan=0)
        N_Q_plasma = np.nan_to_num(N_Q_plasma, nan=0)

        G_DBS_VL = np.nan_to_num(G_DBS_VL, nan=0)
        E_DBS_VL = np.nan_to_num(E_DBS_VL, nan=0)
        N_Q_DBS_VL = np.nan_to_num(N_Q_DBS_VL, nan=0)

        G_DBS_EID = np.nan_to_num(G_DBS_EID, nan=0)
        E_DBS_EID = np.nan_to_num(E_DBS_EID, nan=0)
        N_Q_DBS_EID = np.nan_to_num(N_Q_DBS_EID, nan=0)

        plsama_vl_ref = int( np.any(E_plasma.size > 0 and N_Q_plasma.size > 0 and E_plasma != N_Q_plasma))

        dbs_vl_ref = int( np.any(E_DBS_VL.size > 0 and N_Q_DBS_VL.size > 0 and E_DBS_VL != N_Q_DBS_VL))

        dbs_eid_ref = int(np.any(E_DBS_EID.size > 0 and N_Q_DBS_EID.size > 0 and E_DBS_EID != N_Q_DBS_EID))


        # if H>0 then J must have lab from----------------------------------------

        numc=df_Reffered_Samples['Samples_reffered_Out']
        criteria= (numc>0) & df_Reffered_Samples['Lab_Samples_referred_to'].isna()
        Error_Referred_TO_Blank=int(np.any(criteria))
        
   
        criteriaOUT= (numc>0) & df_Reffered_Samples['Swift_Consignment_Number'].isna()
        Error_Consignment_Blank=int(np.any(criteriaOUT))


        numcIN=df_Reffered_Samples['Referred_Sample_Received']
        criteriaIN= (numcIN>0) & df_Reffered_Samples['Referred_From'].isna()
        Error_Referred_FROM_Blank=int(np.any(criteriaIN))

    #---------------------------------------------------------------------------------------------------------------


        #SAMPLE RUN
        Run_NewColumns={

            'Date':'Date',
            'Lab':'Name_of_Lab',
            'Sample Type':'Sample_Type',
            'Test Type':'Test_Type', 
            'Platform':'Platform_Roche_Abbott_Hologic_BMX',
            'Target':'Target',
           'Total number of tests done':'RECEIVED_TOTAL_Sample_RUN', 
           'Total number of urgent samples tested':'RECEIVED_URGENT_Sample_RUN',
           'Number of controls that failed':'Controls_that_failed',

           'Total number of failed tests not eligible for retesting':'RECEIVED_FAILED_bt_NOT_Elig_REPEAT',
           'Total number of failed tests eligible for retesting':'RECEIVED_FAILED_bt_Elig_REPEAT',

           'Total number of retests run':'RECEIVED_REPEATS_RUN',
           'Total number of failed tests after final retesting':'RECEIVED_FAILED_after_FINAL_repeat_testing',

           ' Number of failed tests: sample handling error at lab':'RECEIVED_FAILED_sample_handling_error_lab',        
           'Number of failed tests: reagent quality issues ':'RECEIVED_FAILED_reagent_quality_issues',
           'Number of failed tests: QC failure':'RECEIVED_FAILED_QC_failure',
           'Number of failed tests: power failure':'RECEIVED_FAILED_power_failure',
           'Number of failed tests: mechanical failure ':'RECEIVED_FAILED_mechanical_failure',
           'Number of failed tests: processing error ':'FAILED_RECEIVED_sample_processing_error',
           'Number of failed tests: sample quality/quantity':'RECEIVED_FAILED_quality_quantity_issues',
           'Number failed tests: other reasons':'RECEIVED_OTHER',

           'Total number of samples with a valid test result':'Sample_with_Valid_test_result',
           'Total number of samples with a failed test result ':'Sample_with_FAILED_test_result',
           'Total number of test results dispatched by lab':'RECEIVED_Results_dispatched_by_lab', 
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
        df_Sample_Run['Status']='Lab'

        sampleRUN_to_check=['RECEIVED_TOTAL_Sample_RUN',
           'RECEIVED_URGENT_Sample_RUN', 'Controls_that_failed',
           'RECEIVED_FAILED_bt_NOT_Elig_REPEAT', 'RECEIVED_FAILED_bt_Elig_REPEAT',
           'RECEIVED_REPEATS_RUN', 'RECEIVED_FAILED_after_FINAL_repeat_testing',
           'FAILED_RECEIVED_sample_processing_error',
           'RECEIVED_FAILED_reagent_quality_issues', 'RECEIVED_FAILED_QC_failure',
           'RECEIVED_FAILED_power_failure', 'RECEIVED_FAILED_mechanical_failure',
           'RECEIVED_OTHER', 'Sample_with_Valid_test_result',
           'Sample_with_FAILED_test_result', 'RECEIVED_Results_dispatched_by_lab']
        # Drop rows where all values in the specified columns are NaN or NaT
        df_Sample_Run = df_Sample_Run.dropna(subset=sampleRUN_to_check, how='all')

        temp_sum = df_Sample_Run[['RECEIVED_FAILED_bt_Elig_REPEAT', 'RECEIVED_FAILED_bt_NOT_Elig_REPEAT']].astype(float).sum(axis=1)
        condition = (temp_sum > 0) & df_Sample_Run['Comments'].isna()
        Error_Repeats_but_NO_COMMENT=int(np.any(condition))



                #--------------Failed Elig & Not Elig (I+J)= M to R-------------------------

        Rec_failed=df_Sample_Run [['RECEIVED_FAILED_bt_Elig_REPEAT', 'RECEIVED_FAILED_bt_NOT_Elig_REPEAT']].astype(float).sum(axis=1).values


        rec=df_Sample_Run[[  'RECEIVED_FAILED_sample_handling_error_lab','RECEIVED_FAILED_reagent_quality_issues','RECEIVED_FAILED_QC_failure'
                          ,'RECEIVED_FAILED_power_failure','RECEIVED_FAILED_mechanical_failure','FAILED_RECEIVED_sample_processing_error',
                           'RECEIVED_FAILED_quality_quantity_issues','RECEIVED_OTHER']].astype(float).sum(axis=1).values

            #received
        Error_Sample_Failed_no_reason=int( np.any( rec!=Rec_failed) )



    #------------------------------------------------------------------------------------------------------------------->
      #5 TESTING CAPACITY
        Capacity_Newcolumns={
            'Date':'Date',
            'Lab':'Name_of_Lab', 
            'Test type':'Test_Type',
            'Platform':'Platform_Roche_Abbott_Hologic_BMX',
           'Number of reagents received from NATPHAM':'NatPharm_Kits_Received_inThisWK',
           'Date reagents received from NATPHAM':'Date_Received_at_Lab',
           'Reagent kits loaned out to other labs':'Reagent_kits_to_OTHER_Labs', 
            'Loaned to':'Lab_Name_Loaned_to',
           'Reagents kits received on loan from other labs':'Reagent_kits_RECEIVED_from_OTHER_Labs', 
            'Loaned from':'Lab_Name_Received_from',
           'Reagents kits on hand':'Reagent_tests_kits_Stock_on_hand', 
            'Expiry Date of Reagent':'Reagent_tests_kits_available_Expiry_Date',
           'Reagent kits expired before use':'Tests_expired_this_month_before_use',
            'Controls  loaned out to other labs':'Control_loaned_to_other_labs',
           'Loaned to.1':'Lab_name_receiving_controls', 
            'Controls received on loan from other labs':'Controls_received_from_other_labs',
           'Loaned from.1':'Lab_name_where_controls_from',
           'Stocks of Controls Available':'stock_of_control_available',
           'Expiry Date of Controls':'Expiry_Date_of_Controls',
           'Stocks of bottleneck consumable available (tests)':'Stocks_of_bottleneck_consumable_available',
            'Name of bottleneck consumable':'Name_of_bottleneck_consumable',
           'Months of stock of bottleneck consumable':'Months_of_stock_of_bottleneck_consumable', 
           'Comments ':'Comments_Reagent_Stock_Status'


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
        df_Testing_Capacity['Status']='Lab'

        #print(df_Testing_Capacity.columns)

        Capacity_to_check=['NatPharm_Kits_Received_inThisWK', 'Reagent_kits_to_OTHER_Labs',
           'Lab_Name_Loaned_to', 'Reagent_kits_RECEIVED_from_OTHER_Labs',
           'Lab_Name_Received_from', 'Reagent_tests_kits_Stock_on_hand',
           'Reagent_tests_kits_available_Expiry_Date',
           'Tests_expired_this_month_before_use', 'Control_loaned_to_other_labs',
           'Lab_name_receiving_controls', 'Controls_received_from_other_labs',
           'Lab_name_where_controls_from', 'stock_of_control_available',
           'Expiry_Date_of_Controls', 'Stocks_of_bottleneck_consumable_available',
           'Months_of_stock_of_bottleneck_consumable']
        # Drop rows where all values in the specified columns are NaN or NaT
        df_Testing_Capacity = df_Testing_Capacity.dropna(subset=Capacity_to_check, how='all')
        df_Testing_Capacity['Date'] = pd.to_datetime(df_Testing_Capacity['Date']).dt.date



        Error_Loaned_to_LAB_not_mentioned= any((df_Testing_Capacity['Reagent_kits_to_OTHER_Labs']>0)  & (df_Testing_Capacity['Lab_Name_Loaned_to'].isnull())  )

        Error_Loaned_FROM_LAB_not_mentioned= any((df_Testing_Capacity['Reagent_kits_RECEIVED_from_OTHER_Labs']>0)  & (df_Testing_Capacity['Lab_Name_Received_from'].isnull())  )
        Error_CONTROLS_Loaned_TO_LAB_not_mentioned= any((df_Testing_Capacity['Control_loaned_to_other_labs']>0)  & (df_Testing_Capacity['Lab_name_receiving_controls'].isnull())  )
        Error_CONTROLS_Received_FROM_LAB_not_mentioned= any((df_Testing_Capacity['Controls_received_from_other_labs']>0)  & (df_Testing_Capacity['Lab_name_where_controls_from'].isnull())  )

        #df_filtered = df_Testing_Capacity.dropna(subset=['Reagent_stockout_days'])

        Error_Reagents_kits_NoEXP=int(np.any( (df_Testing_Capacity['Reagent_tests_kits_Stock_on_hand'] > 0) & 
                                                                (df_Testing_Capacity['Reagent_tests_kits_available_Expiry_Date'].isnull() )
                                                            )
                                                    )


        Error_CONTROLS_NoEXP=int(np.any( (df_Testing_Capacity['stock_of_control_available'] > 0) & 
                                                                (df_Testing_Capacity['Expiry_Date_of_Controls'].isnull() )
                                                            )
                                                    )

        Error_No_Bottlenecks = int(
                                    np.any(
                                        (df_Testing_Capacity['Months_of_stock_of_bottleneck_consumable'] > 0) &
                                        (
                                            df_Testing_Capacity['Name_of_bottleneck_consumable'].isnull() |
                                            (df_Testing_Capacity['Name_of_bottleneck_consumable'] == 0) |
                                            (df_Testing_Capacity['Stocks_of_bottleneck_consumable_available'] == 0) |
                                            df_Testing_Capacity['Stocks_of_bottleneck_consumable_available'].isnull()
                                        )
                                    )
                                )


       # Check if stocks are available and if the months of stock is less than 1 or null in either DataFrame
        condition1 = df_Testing_Capacity['Stocks_of_bottleneck_consumable_available'] > 0
        condition2 = (df_Testing_Capacity['Months_of_stock_of_bottleneck_consumable'] < 0.0001) | df_Testing_Capacity['Months_of_stock_of_bottleneck_consumable'].isnull()

        # Combine conditions and check if any row satisfies the combined condition
        Error_Months_of_bottlenecks = int((condition1 & condition2).any())
     #------------------------------------------------------------------------------------------------------------------->

        # Filter the DataFrame to include only rows where stock_of_control_available is greater than 0
        df_filtered_TC = df_Testing_Capacity[df_Testing_Capacity['stock_of_control_available'] > 0]

        # Convert the 'Reagent_tests_kits_available_Expiry_Date' column to datetime format
        # only for the filtered DataFrame
        df_filtered_TC['Reagent_tests_kits_available_Expiry_Date'] = pd.to_datetime(
            df_filtered_TC['Reagent_tests_kits_available_Expiry_Date'], 
            errors='coerce', 
            format='%Y-%m-%d'
        )


 

        # Check for any values that couldn't be converted (excluding nulls)
        if df_filtered_TC['Reagent_tests_kits_available_Expiry_Date'].isna().any() and not df_filtered_TC['Reagent_tests_kits_available_Expiry_Date'].isna().all():
            Reagent_Expiary_date_wrong_format = 1
        else:
            Reagent_Expiary_date_wrong_format = 0


     #-------------------------------------------------------------------------------------------------------------------->


        #6: LAB METRICS AND WASTE MGT
        Metrics_NewColumns={
            'Date':'Date',
            'Lab':'Name_of_Lab', 
            'Test type':'Test_Type',
            'Platform':'Platform_Roche_Abbott_Hologic_BMX',
           'Number of hours in a shift (e.g 8hr, 12hr, 16hr or 24 hr shift)': 'Hrs_in_Shift',
           'Total actual operational hours ':'Actual_number_of_days_platform_used',
           'Downtime (hours lost) due to power outage':'Downtime_Power_Outage',
           'Downtime (hours lost) due to technical/mechanical failure':'Downtime_Mechanical_Failure',
           'Downtime (hours lost) due to reagent stock out / expiry ':'Downtime_Reagent_Stockout_Expiry',
           'Downtime (hours lost) due to controls stock out / expiry ':'Downtime_Controls_Stockout_Expiry',
           'Downtime (hours lost) due to controls failure':'Downtime_Controls_Failure',
           'Downtime (hours lost) due to HRH unavailability':'Downtime_Staff_Unavailability', 
            'Downtime (other)':'Downtime_coz_other_reasons',
           'Comments':'Comments_on_ErrorCodes_for_Mach_failure', 
            'Data Quality Checks':'Data_Quality_Checks'



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
        df_Lab_Metrics_Waste_Mgt = df_Lab_Metrics_Waste_Mgt.drop('Data_Quality_Checks', axis=1)
        df_Lab_Metrics_Waste_Mgt['Status']='Lab'

        Metrics_to_check=[ 'Hrs_in_Shift', 'Actual_number_of_days_platform_used',
           'Downtime_Power_Outage', 'Downtime_Mechanical_Failure',
           'Downtime_Reagent_Stockout_Expiry', 'Downtime_Controls_Stockout_Expiry',
           'Downtime_Controls_Failure', 'Downtime_Staff_Unavailability',
           'Downtime_coz_other_reasons']
        # Drop rows where all values in the specified columns are NaN or NaT
        df_Lab_Metrics_Waste_Mgt = df_Lab_Metrics_Waste_Mgt.dropna(subset=Metrics_to_check, how='all')
        df_Lab_Metrics_Waste_Mgt['Date'] = pd.to_datetime(df_Lab_Metrics_Waste_Mgt['Date']).dt.date



        Total_machine_downtime_hours= df_Lab_Metrics_Waste_Mgt[['Downtime_Power_Outage', 'Downtime_Controls_Failure','Downtime_coz_other_reasons',
                                                                'Downtime_Mechanical_Failure', 'Downtime_Reagent_Stockout_Expiry','Downtime_Controls_Stockout_Expiry',
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
           'Downtime (Other reasons)':'Downtime_Other_reasons',
           'Time between LIMS going down and the incident being reported in help desk (hours)':'TimeLIMS_going_down_HELPDESK',
           'Time between the incident being reported in help desk, and technician responding to issue (hours)':'TimeHELPDESK_TechRESPONSE',
           'Time between technician responding to issue and resolution of issue (hours)':'TimeTechRESPONSE_Resolution',
           'Comments':'Comments', 
            'Data Quality Checks':'Data_Quality_Checks'   




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
        df_LIMS_Functionality = df_LIMS_Functionality.drop(['Data_Quality_Checks'], axis=1)
        df_LIMS_Functionality['Status']='Lab'



        Functionality_column_to_check=['Hours_of_Functionality',
           'Hours_of_Downtime', 'Downtime_due_to_hardware_problem',
           'Downtime_due_to_internet_connection',
           'Downtime_due_to_software_problem', 'Downtime_Other_reasons',
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
           'Number of correspondences for clinics to send specimens to different laboratory':'Corrsp_clinic_send_spec_to_diff_lab',
           'Number of correspondences for clinics to expect delays in testing and result return due to laboratory stock outs or capacity':'Corrsp_clinic_expect_delay_due_lab_stockouts',
           'Number of correspondences for clinics to halt specimens collection due to laboratory stock outs or capacity':'Corrsp_clinic_halt_spec_collection_due_lab_stockouts',

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
        df_CLI['Status']='Lab'


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


    #-------------------------------------------------------------------------------------------------------------->
        #CLI SHEET
        SPecTransport_NewColumns={
            'Date':'Date',
            'District':'District', 
            'Number of rider accidents':'Number_of_rider_accidents',
           'Total number of incomplete bike transport trips ':'incomplete_bike_transport_trips',
           'Total number of specimens transported by non-IST methods':'specimens_transported_by_non_IST_methods',
           'Number of specimens transported by ambulance ':'specimens_transported_by_ambulance',
           'Number of specimens transported by alternative IP transport':'specimens_transported_by_alternative_IP_transport',
           'Number of specimens transported by MoHCC arranged transport':'specimens_transported_by_MoHCC_arranged_transport',
           'Number of specimens transported by courier':'specimens_transported_by_courier',
           'Number of specimens transported by other non-IST methods':'specimens_transported_by_other_non_IST_methods',
            'Comments':'Comments',
           'Data Quality Checks':'Data_Quality_Checks'     


                      }

        df_Specimen_Transport.rename(columns=SPecTransport_NewColumns, inplace=True)
        #df_CLI = convert_excel_date(df_CLI, 'Date')
        df_Specimen_Transport['SourceFile']=file_name
        df_Specimen_Transport['unique_key']=unique_key
        df_Specimen_Transport.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_cli_x = df_Specimen_Transport.iloc[:, :4]
        df_Specimen_Transport = df_Specimen_Transport.dropna(subset=subset_cli_x.columns, how='all')

        letsdrroopu_x=df_Specimen_Transport.iloc[:, 3:-1]
        df_Specimen_Transport = df_Specimen_Transport.dropna(subset=letsdrroopu_x.columns, how='all')

        SPecTransport_NewColumns['SourceFile'] = 'SourceFile'
        SPecTransport_NewColumns['unique_key'] = 'unique_key'
        df_Specimen_Transport = df_Specimen_Transport[list(SPecTransport_NewColumns.values())]
        df_Specimen_Transport = df_Specimen_Transport.drop('Data_Quality_Checks', axis=1)
        df_Specimen_Transport['Status']='Lab'

        trans_to_check=['Number_of_rider_accidents',
           'incomplete_bike_transport_trips',
           'specimens_transported_by_non_IST_methods',
           'specimens_transported_by_ambulance',
           'specimens_transported_by_alternative_IP_transport',
           'specimens_transported_by_MoHCC_arranged_transport',
           'specimens_transported_by_courier',
           'specimens_transported_by_other_non_IST_methods']
        df_Specimen_Transport = df_Specimen_Transport.dropna(subset=trans_to_check, how='all')
        df_Specimen_Transport['Date'] = pd.to_datetime(df_Specimen_Transport['Date']).dt.date


        totalTrans=df_Specimen_Transport [['specimens_transported_by_non_IST_methods']].astype(float).sum(axis=1).values  

        DIS_trans=df_Specimen_Transport[['specimens_transported_by_ambulance',
           'specimens_transported_by_alternative_IP_transport',
           'specimens_transported_by_MoHCC_arranged_transport',
           'specimens_transported_by_courier',
           'specimens_transported_by_other_non_IST_methods']].astype(float).sum(axis=1).values

            #received
        Error_Specimens_notDisaggregated=int( np.any( DIS_trans!=totalTrans) )

    #----------------------------------------------------------------------------------------------------------->
        

        # LIMS FUNCTIONALITY
        power={

           'Date\n(Month/Day/Year)':'Date',
            'Laboratory':'Laboratory',
               'Number of hours with no electricity ':'Hours_with_no_electricity',
               'Number of hours generator was on ':'Hours_generator_was_on',
               'Litres of fuel added to generator ':'Fuel_ltrs_added_to_generator',
               'Number of hours machine/s was not being used due to power cut ':'Hrs_Machine_idle_coz_PowerCuts',
               'Total  Tests done using generator':'Total_Tests_done_using_generator',
            'Comments':'Comments'  
                     }

        df_power_outages.rename(columns=power, inplace=True)
        #poweroutage = convert_excel_date(df_LIMS_Functionality, 'Date')
        df_power_outages['SourceFile']=file_name
        df_power_outages['unique_key']=unique_key
        df_power_outages.replace('', pd.NA, inplace=True)
        # Drop the rows where all values in the subset are NaN
        subset_Fxns = df_power_outages.iloc[:, :4]
        df_power_outages = df_power_outages.dropna(subset=subset_Fxns.columns, how='all')

        letsdrroop=df_power_outages.iloc[:, 3:-1]
        df_power_outages = df_power_outages.dropna(subset=letsdrroop.columns, how='all')

        power['SourceFile'] = 'SourceFile'
        power['unique_key'] = 'unique_key'
        df_power_outages = df_power_outages[list(power.values())]

        df_power_outages['Status']='Lab'
        power_column_to_check=['Hours_with_no_electricity',
               'Hours_generator_was_on', 'Fuel_ltrs_added_to_generator',
               'Hrs_Machine_idle_coz_PowerCuts', 'Total_Tests_done_using_generator']

        # # Drop rows where all values in the specified columns are NaN or NaT
        df_power_outages = df_power_outages.dropna(subset=power_column_to_check, how='all')
        df_power_outages['Date'] = pd.to_datetime(df_power_outages['Date']).dt.date






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
            WHERE  CAST(a.[date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
              AND CAST(a.[date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
               and a.status='Lab'

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
        dates_df_Specimen_Transport = pd.to_datetime(df_Specimen_Transport[df_Specimen_Transport['Date'].notna()]['Date'])


        # Extract unique dates from each DataFrame's 'Date' column
        unique_dates_df_indicators = set(dates_df_week_samp_received.unique())


        # Check if all sets of unique dates are the same
        all_dates_are_the_same = all([     
            unique_dates_df_indicators == set(dates_df_Sample_Run.unique()),
            unique_dates_df_indicators == set(dates_df_Testing_Capacity.unique()),
            unique_dates_df_indicators == set(dates_df_Lab_Metrics_Waste_Mgt.unique()),
            unique_dates_df_indicators == set(dates_df_LIMS_Functionality.unique()),
            unique_dates_df_indicators == set(dates_df_CLI.unique()),
            #unique_dates_df_indicators == set(dates_df_Specimen_Transport.unique())
        ])

        #error
        Error_Dates_differ=int(not all_dates_are_the_same)


        raised_errors= []
        list_of_Errors=[Error_Rejected_Samples_Not_Disaggregated,Error_Referred_TO_Blank,Error_Referred_FROM_Blank,plsama_vl_ref,
                        dbs_vl_ref,dbs_eid_ref,Error_Repeats_but_NO_COMMENT,Error_Sample_Failed_no_reason,Error_Loaned_to_LAB_not_mentioned,
                        Error_Loaned_FROM_LAB_not_mentioned,Error_Reagents_kits_NoEXP,Error_CONTROLS_Loaned_TO_LAB_not_mentioned,
                        Error_CONTROLS_Received_FROM_LAB_not_mentioned,Error_CONTROLS_NoEXP,Error_Months_of_bottlenecks,Error_issue_with_MachineDowntime,
                        Error_LIMS_Downtime,Error_LIMS_Fxn_NO_COMMENTS,Error_cli_resolved_not_disaggregated,Error_resolved_unresolved_notMATCH,
                                                             Error_Specimens_notDisaggregated,error_dups,Error_Dates_differ,Error_No_Bottlenecks,
                                                             Reagent_Expiary_date_wrong_format,Error_Consignment_Blank
                         ]

        variable_names=['Error_Rejected_Samples_Not_Disaggregated','Error_Referred_TO_Blank','Error_Referred_FROM_Blank','plsama_vl_ref',
                       'dbs_vl_ref','dbs_eid_ref','Error_Repeats_but_NO_COMMENT','Error_Sample_Failed_no_reason','Error_Loaned_to_LAB_not_mentioned',
                        'Error_Loaned_FROM_LAB_not_mentioned','Error_Reagents_kits_NoEXP','Error_CONTROLS_Loaned_TO_LAB_not_mentioned',
                        'Error_CONTROLS_Received_FROM_LAB_not_mentioned','Error_CONTROLS_NoEXP','Error_Months_of_bottlenecks','Error_issue_with_MachineDowntime',
                        'Error_LIMS_Downtime','Error_LIMS_Fxn_NO_COMMENTS','Error_cli_resolved_not_disaggregated','Error_resolved_unresolved_notMATCH',
                                                             'Error_Specimens_notDisaggregated','error_dups','Error_Dates_differ','Error_No_Bottlenecks',
                                                             'Reagent_Expiary_date_wrong_format','Error_Consignment_Blank'
                       ]

            # Check if all variables are equal to 0
        if all(variable == 0 for variable in list_of_Errors):

            df_week_samp_received.to_sql('Dash_This_week_Rec_Samples', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_Reffered_Samples.to_sql('Dash_Referred_Samples', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_Sample_Run.to_sql('Dash_Sample_Run', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_Testing_Capacity.to_sql('Dash_Testing_Capacity', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed           

            df_Lab_Metrics_Waste_Mgt.to_sql('Dash_Lab_Metrics_Waste_Mgt', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_LIMS_Functionality.to_sql('Dash_LIMS_Functionality', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed


            df_Specimen_Transport.to_sql('Dash_Specimen_Transport', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed


            df_CLI.to_sql('Dash_CLI', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed

            df_power_outages.to_sql('Dash_Power_Outage', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed



            #Lets run the deduplication if there is any in the table 
            deduplicate_table('Dash_This_week_Rec_Samples', ['Date', 'Name_of_Lab','Sample_Type','Test_Type' ], engine_insert)
            deduplicate_table('Dash_Referred_Samples', ['Date', 'Name_of_Lab','Sample_Type','Test_Type' ], engine_insert)
            deduplicate_table('Dash_Sample_Run', ['Date', 'Name_of_Lab','Sample_Type','Test_Type','Platform_Roche_Abbott_Hologic_BMX'], engine_insert)
            deduplicate_table('Dash_Testing_Capacity', ['Date', 'Name_of_Lab','Test_Type','Platform_Roche_Abbott_Hologic_BMX'], engine_insert)
            deduplicate_table('Dash_Lab_Metrics_Waste_Mgt', ['Date', 'Name_of_Lab','Test_Type','Platform_Roche_Abbott_Hologic_BMX'], engine_insert)
            deduplicate_table('Dash_LIMS_Functionality', ['Day','Date','Name_of_Lab'], engine_insert)
            deduplicate_table('Dash_Specimen_Transport', ['District' ,'Number_of_rider_accidents','Incomplete_bike_transport_trips','Specimens_transported_by_non_IST_methods','Specimens_transported_by_ambulance','Specimens_transported_by_alternative_IP_transport' ,'Specimens_transported_by_MoHCC_arranged_transport'  ,'Specimens_transported_by_courier','Specimens_transported_by_other_non_IST_methods' ,'Comments','Status'], engine_insert)
            deduplicate_table('Dash_CLI', ['Date', 'Name_of_Lab'], engine_insert)
            deduplicate_table('Dash_Power_Outage', ['Date', 'Laboratory'], engine_insert)
            
            error_messages=lab +'    :Dashboard:  Successfully Uploaded'
             # Example: Return the processed data
            return error_messages
            #print(error_messages)



        else: 



            for i in range(len(list_of_Errors)):

                if list_of_Errors[i]==1:
                    raised_errors.append(variable_names[i])

                Meanings_of_Errors = {

                'error_dups': 'Dashboard already uploaded or you didnt change the lab name',       
                'Error_Referred_TO_Blank': 'In Referred Samples Tab: You forgot to put Referred to facility.',
                'Error_Referred_FROM_Blank': 'In Referred Samples Tab: You forgot to put Referred FROM facility.',
                'plsama_vl_ref':'In Referred Samples tab:  Plasma VL, Column  (samples referred out)  is not tallying with reasons (H-L)  ',
                'dbs_vl_ref':'In Referred Samples tab:  DBS VL, Column  (samples referred out)  is not tallying with reasons  (H-L)',
                'dbs_eid_ref':'In Referred Samples tab:  DBS EID, Column (samples referred out) is not tallying with reasons  (H-L)',
                'Error_Sample_Failed_no_reason': 'Sample Testing Tab: Samples Failed - (J+K) must have a reason (N to U).',
                'Error_Repeats_but_NO_COMMENT': 'In Samples Testing Tab: We expect a comment for (TESTS FAILURE -) Actual Reasons for assay failure.',
                'Error_Loaned_to_LAB_not_mentioned':'In Stock Inventory Tab: You indicated that Reagent kits was loaned out but fail to indicate the Lab Receiving those kits' ,                         
                'Error_Loaned_FROM_LAB_not_mentioned':'In Stock Inventory Tab: You indicated that Reagent kits was loaned  but fail to indicate the Lab where those kits are coming from' , 
                'Error_Reagents_kits_NoEXP':'In Stock Inventory Tab: If we have Reagents test kits stock in hand (O) then we need to know Expiary date ',            
                'Error_Rejected_Samples_Not_Disaggregated': 'In  Sample Inventory Received Tab: Number of Rejected Samples (J) are not disaggregated by reason why they are rejected (K to R).',
                'Error_CONTROLS_Loaned_TO_LAB_not_mentioned':'In Stock Inventory Tab: You indicated you Loaned out the Controls but fail to indicated the Lab receiving the controls',
                'Error_CONTROLS_Received_FROM_LAB_not_mentioned':'In Stock Inventory Tab: You indicated you Received the Controls but fail to indicated the Lab giving you the controls',
                'Error_CONTROLS_NoEXP':'In Stock Inventory Tab: You have the controls but you fail to provide the Expiary date of those controls',
                'Error_Months_of_bottlenecks' :'In Stock Inventory Tab: The months of Stock of bottlenecks consumables not well calculated' ,     
                'Error_issue_with_MachineDowntime': 'In Operational Matrics Tab: The Total Machine Downtime (hrs) Sum of (Downtime coz Power Outage, Downtime coz of Mechanical Failure, Downtime coz of Reagent Stockout Expiry, Downtime coz of Staff Unavailability, Total_Operational_Hrs)- [F to M] MUST BE EQUAL TO  Hrs in working Shift (column E * 7) -).',
                'Error_LIMS_Downtime': 'In LIMS Functionality Tab: Hours_of_Functionality and Hours_of_Downtime are not matching.',
                'Error_LIMS_Fxn_NO_COMMENTS':'In LIMS Functionality Tab: You Indicate that there are Hours in which LIMS was down (F) but You dont give a comment as to what happened' ,
                'Error_cli_resolved_not_disaggregated': 'In CLI Tab: Column L = Sum of columns (M) to (T), this criteria is not met.\n'
                                                                     'i.e RESOLVED Missing results outcomes found and Shared with_facilities must be disaggregated in columns (M-T).',

                'Error_resolved_unresolved_notMATCH': 'In CLI Tab: Column (J and K)- Unresolved missing results from last week and Number of missing results requested by facilities this week MUST BE EQUAL TO (column LUVW) pending testing,Referred waiting results, investigation in progress  and Results found and shared with facilities ',
                #'Error_Correspondances_notDisaggregated':'In CLI Tab: The number of correspondences (D) are not well disaggregated (E to I)',
                'Error_Specimens_notDisaggregated':'In Specimen Transport Tab:  The samples transported by non IST  (E) are not well disaggregated (F to J)',
                'Error_Dates_differ': 'Your Dates in tabs are not the same may you recheck the Date column in all tabs',
                'Error_No_Bottlenecks':'You cant have the months of stocks of bottlenecks, when you didnt indicate the name of bottleneck or its quantity',
                'Reagent_Expiary_date_wrong_format':'In In Stock Inventory Tab: The Expiary date of reagents is in wrong format , may you correct that',
                'Error_Consignment_Blank':  ' In Referred Tab:  You forget to put the consignment number'


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
        #print(error_messages)






    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()

        error_messages = f"An error occurred during reading_dash:\n\nType: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
        #print(error_messages)
        return error_messages  
