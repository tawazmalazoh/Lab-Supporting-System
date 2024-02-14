import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import re
import matplotlib.pyplot as plt
from matplotlib import pyplot
import seaborn as sns
from scipy.stats import norm
from scipy.stats import ttest_ind
import statistics
from IPython.display import Image
import plotly.graph_objects as go
import json
from datetime import timedelta
from datetime import datetime, timedelta
import shutil

from sqlalchemy.pool import QueuePool
import pyodbc
import matplotlib.patches as mpatches
import datetime

from PIL import Image

import io
from io import StringIO

import os
from sqlalchemy import create_engine, text,inspect
from datetime import datetime
from datetime import timedelta
from datetime import datetime, timedelta
import collections
from openpyxl import load_workbook
import time
from pandas.io.excel._base import read_excel
import urllib.parse
import traceback
import tempfile
import warnings


from docx import Document
from docxtpl import DocxTemplate
import random
from docx2pdf import convert
from docxtpl import DocxTemplate, InlineImage

warnings.filterwarnings("ignore", category=UserWarning)



# Suppress the SettingWithCopyWarning
# warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)
# warnings.simplefilter(action='ignore')
pd.set_option('mode.chained_assignment', None)

# Utility function to get the last Sunday
def get_last_sunday(date):
    return date - datetime.timedelta(days=date.weekday() + 1)

# Utility function to create and copy the Excel template
def create_and_copy_Word_template(template_path, destination_folder_base):
    today = datetime.today()
    current_year = today.year
    current_month = today.strftime('%B')
    # Get the current date and time
    current_date = datetime.now()
    # Check if today is Sunday
    if current_date.weekday() == 6:  # In Python, Monday is 0, so Sunday is 6
        recent_sunday = current_date
    else:
        # Calculate the most recent Sunday
        recent_sunday = current_date - timedelta(days=current_date.weekday() + 1)
    # Format the date in a readable format (e.g., YYYY-MM-DD)
    last_sunday_date = recent_sunday.strftime("%Y-%m-%d")

    year_folder = os.path.join(destination_folder_base, str(current_year))
    month_folder = os.path.join(year_folder, current_month)
    os.makedirs(month_folder, exist_ok=True)

    new_file_name = f'BRTI November  VL-EID Report Narrative_{last_sunday_date}.docx'
    new_file_path = os.path.join(month_folder, new_file_name)

    shutil.copy2(template_path, new_file_path)

    return new_file_path




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

 # Define your database connection parameters
params = urllib.parse.quote_plus('DRIVER=' + driver + ';SERVER=' + server +
                                    ';DATABASE=' + database + ';UID=' + username +
                                    ';PWD=' + password)

# Create the database engine
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)




def write_word_document_narrative(template_path, destination_folder_base):
	try: 

		# Compute the date range in Python -= make it 11
		#end_date = datetime.now() - timedelta(days=4)
		end_date = datetime.now()
		start_date = datetime.now() - timedelta(days=6)
		end_date_str = end_date.strftime('%Y-%m-%d')
		start_date_str = start_date.strftime('%Y-%m-%d')

		# Define your SQL query
		sql = f''' EXEC  [dbo].[sp_vlreceived_model]  '''
		sql_referred = f''' EXEC  [dbo].[sp_Referral_model]  '''
		sql_rejections=f''' select Test_Type, sum(cast(Num_Rejected_Samples as int)) Num_Rejected_Samples 
		                    from Dash_This_week_Rec_Samples
		                        where  status='Lab'
		                        and   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
		                  AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
		                  and Test_Type in ('VL' , 'EID')
		                  group by Test_Type '''

		sql_eidReceived=f'''     EXEC	 [dbo].[sp_EIDreceived_model]    '''


		sql_lab_assayfailures=f''' 

		                    select d.lab,
		                      case
		                          WHEN Platform_Roche_Abbott_Hologic_BMX in ('Abbott','Abbott 1','Abbott 2') THEN 'Abbott'
		                          WHEN  Platform_Roche_Abbott_Hologic_BMX in ('Roche C5800','Roche C6800','Roche C8800') THEN 'Roche Cobas'
		                          ELSE Platform_Roche_Abbott_Hologic_BMX
		                          END +'_'+Sample_Type [Platform],sum(cast(RECEIVED_TOTAL_Sample_RUN as int)) Tests_done, sum(cast (RECEIVED_FAILED_bt_NOT_Elig_REPEAT as int)) +sum(cast(RECEIVED_FAILED_bt_Elig_REPEAT as int))+ sum(cast(RECEIVED_FAILED_after_FINAL_repeat_testing as int)) failures


		                    from DATIM_Facility_names d
		                    INNER JOIN     Dash_Sample_Run r ON r.name_of_lab=d.Facility
		                        and   r.status='Lab'
		                        and   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
		                          AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
		                          and Sample_Type!=''

		                          group by 
		                         case
		                          WHEN Platform_Roche_Abbott_Hologic_BMX in ('Abbott','Abbott 1','Abbott 2') THEN 'Abbott'
		                          WHEN  Platform_Roche_Abbott_Hologic_BMX in ('Roche C5800','Roche C6800','Roche C8800') THEN 'Roche Cobas'
		                          ELSE Platform_Roche_Abbott_Hologic_BMX
		                          END +'_'+Sample_Type,d.lab
		                          order by d.lab '''




		sql_carryover_backlog=f'''     EXEC	 [dbo].[backlog_carryover_model]    '''

		Sql_Failures_absolute_values=f'''
		                            SELECT
		                                lab,
		                                SUM(CASE WHEN Platform = 'Abbott' THEN failures ELSE 0 END) AS [Abbott],
		                                SUM(CASE WHEN Platform = 'Roche Cobas' THEN failures ELSE 0 END) AS [Roche Cobas],
		                                SUM(CASE WHEN Platform = 'Hologic Panther' THEN failures ELSE 0 END) AS [Hologic Panther],
		                                SUM(CASE WHEN Platform = 'GeneXpert' THEN failures ELSE 0 END) AS [GeneXpert],
		                                SUM(CASE WHEN Platform = 'Roche CAPCTM' THEN failures ELSE 0 END) AS [Roche CAPCTM]
		                            FROM (
		                                SELECT
		                                    d.lab,
		                                    CASE
		                                        WHEN Platform_Roche_Abbott_Hologic_BMX IN ('Abbott', 'Abbott 1', 'Abbott 2') THEN 'Abbott'
		                                        WHEN Platform_Roche_Abbott_Hologic_BMX IN ('Roche C5800', 'Roche C6800', 'Roche C8800') THEN 'Roche Cobas'
		                                        ELSE Platform_Roche_Abbott_Hologic_BMX
		                                    END AS Platform,
		                                    SUM(CAST(RECEIVED_TOTAL_Sample_RUN AS INT)) AS Tests_done,
		                                    SUM(CAST(RECEIVED_FAILED_bt_NOT_Elig_REPEAT AS INT)) +
		                                    SUM(CAST(RECEIVED_FAILED_bt_Elig_REPEAT AS INT)) +
		                                    SUM(CAST(RECEIVED_FAILED_after_FINAL_repeat_testing AS INT)) AS failures
		                                FROM DATIM_Facility_names d
		                                INNER JOIN Dash_Sample_Run r ON r.name_of_lab = d.Facility
		                                    AND r.status = 'Lab'
		                                    AND CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
		                                    AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
		                                    AND Sample_Type != ''
		                                GROUP BY
		                                    CASE
		                                        WHEN Platform_Roche_Abbott_Hologic_BMX IN ('Abbott', 'Abbott 1', 'Abbott 2') THEN 'Abbott'
		                                        WHEN Platform_Roche_Abbott_Hologic_BMX IN ('Roche C5800', 'Roche C6800', 'Roche C8800') THEN 'Roche Cobas'
		                                        ELSE Platform_Roche_Abbott_Hologic_BMX
		                                    END, d.lab
		                            ) AS BASE
		                            WHERE failures != 0
		                            GROUP BY lab;
		                            '''

		sql_patient_run=f'''
		          SELECT 
		           d.lab Lab
		              ,sum ( cast(RECEIVED_TOTAL_Sample_RUN as int)) Samples_run,
		              sum (cast(RECEIVED_FAILED_bt_Elig_REPEAT as int)) Fail_elig_repeat,
		              sum(cast(RECEIVED_FAILED_bt_NOT_Elig_REPEAT as int)) Fail_NOT_elig_repeat,
		              sum (cast (RECEIVED_REPEATS_RUN as int)) retest_run,
		              sum (cast(RECEIVED_FAILED_after_FINAL_repeat_testing as int))  Failed_tests_after_final  

		              ,sum ( cast(RECEIVED_TOTAL_Sample_RUN as int))-sum (cast(RECEIVED_FAILED_bt_Elig_REPEAT as int)) Patient_run

		          FROM DATIM_Facility_names d
		          Left join [LSS].[dbo].[Dash_Sample_Run] r on r.Name_of_Lab=d.facility
		            and    r.status='Lab'  
		          and   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
		           AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
		           and  [Test_Type]='VL'
		           Group by  d.lab
		        '''

		sql_eid_run_model=f''' 
		         SELECT 
		           d.lab Lab
		              ,sum ( cast(RECEIVED_TOTAL_Sample_RUN as int)) Samples_run,
		              sum (cast(RECEIVED_FAILED_bt_Elig_REPEAT as int)) Fail_elig_repeat,
		              sum(cast(RECEIVED_FAILED_bt_NOT_Elig_REPEAT as int)) Fail_NOT_elig_repeat,
		              sum (cast (RECEIVED_REPEATS_RUN as int)) retest_run,
		              sum (cast(RECEIVED_FAILED_after_FINAL_repeat_testing as int))  Failed_tests_after_final  

		              ,sum ( cast(RECEIVED_TOTAL_Sample_RUN as int))-sum (cast(RECEIVED_FAILED_bt_Elig_REPEAT as int)) Patient_run

		          FROM DATIM_Facility_names d
		          Left join [LSS].[dbo].[Dash_Sample_Run] r on r.Name_of_Lab=d.facility
		            and    r.status='Lab'  
		          and   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
		           AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
		           and  [Test_Type]='EID'
		       
		           Group by  d.lab
		                    '''
		sql_stock_status=f''' EXEC	 [dbo].[sp_STOCK_STATUS_TABLE_WEEKLY] '''

	

	

	

		sql_relief_riders_samples=f'''
		            SELECT
		                    CASE 
		                        WHEN Province_ IN ('Masonaland West', 'Mashonaland West', 'MASHWEST') THEN 'Mash West'
		                        WHEN Province_ = 'Matebeleland North' THEN 'Mat North'
		                        WHEN Province_ = 'Mashonaland Central' THEN 'Mash Central'
		                        WHEN Province_ = 'Matebeleland South' THEN 'Mat South'
		                        WHEN Province_ = 'Mashonaland East' THEN 'Mash East'
		                        ELSE Province_
		                    END as Province_,

		                    -- Summing up samples and results for various tests
		                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
		                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
		                    
		                    -- Sputum tests
		                    SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
		                    SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
		                    
		                    -- Individual sums for certain tests
		                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
		                    SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
		                    SUM(CAST([HPV] AS int)) AS HPV,
		                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
		                    SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
		                    SUM(CAST([HPV_res] AS int)) AS HPV_res,

		                    -- total all
		                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) +SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int))+  SUM(CAST([HPV] AS int)) AS total_sam,
		                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) + SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int))+SUM(CAST([HPV_res] AS int))  AS total_res
		                    

		                FROM 
		                    [LSS].[dbo].[IST_National]
		                WHERE 
		                    status = 'relief rider'
		                    AND  CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
		           AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
		                GROUP BY  
		                
		                    CASE 
		                        WHEN Province_ IN ('Masonaland West', 'Mashonaland West', 'MASHWEST') THEN 'Mash West'
		                        WHEN Province_ = 'Matebeleland North' THEN 'Mat North'
		                        WHEN Province_ = 'Mashonaland Central' THEN 'Mash Central'
		                        WHEN Province_ = 'Matebeleland South' THEN 'Mat South'
		                        WHEN Province_ = 'Mashonaland East' THEN 'Mash East'
		                        ELSE Province_
		                    END             '''

		sql_bikeFxn=sql_Dash = f'''
			EXEC [dbo].[GetBike_Functionality]
			  @StartDate ='{start_date_str}', @EndDate='{end_date_str}'
					'''
		sql_nonFxntyRxns=f'''
					EXEC	[dbo].[GetReasons_BikeNonFunctional]
				  @StartDate ='{start_date_str}', @EndDate='{end_date_str}'
				'''

		sql_nonDaysFxn=f'''
						EXEC	[dbo].[GetReasons_Summing]
                       @StartDate ='{start_date_str}', @EndDate='{end_date_str}'               
					
						'''

		sql_districts_ist=f'''
							EXEC	[dbo].[GetDistricts_with_reasons]
							  @StartDate ='{start_date_str}', @EndDate='{end_date_str}' 
                          '''

		# Execute the query, passing in the date range as parameters
		df_last_week = pd.read_sql( text(sql),  engine )
		df_referrals = pd.read_sql( text(sql_referred),  engine )
		df_rejections = pd.read_sql( text(sql_rejections),  engine )
		df_eidreceived = pd.read_sql( text(sql_eidReceived),  engine )
		df_backlog_carryover=pd.read_sql( text(sql_carryover_backlog), engine )

		df_LABassay_failure_rate = pd.read_sql( text(sql_lab_assayfailures),  engine )
		df_abso_failures = pd.read_sql( text(Sql_Failures_absolute_values),  engine )
		df_patient_run = pd.read_sql( text(sql_patient_run),  engine )
		df_EID_run_model = pd.read_sql( text(sql_eid_run_model),  engine )
		df_stock_status = pd.read_sql( text(sql_stock_status),  engine )		 
		df_relief_riders_samples = pd.read_sql( text(sql_relief_riders_samples),  engine )
  
		df_bikeFxnality = pd.read_sql(text(sql_bikeFxn), engine)
		df_nonFxntyRxns = pd.read_sql(text(sql_nonFxntyRxns), engine)
		df_nonDaysFxn = pd.read_sql(text(sql_nonDaysFxn), engine)
		df_districts_ist = pd.read_sql(text(sql_districts_ist), engine)



		
		new_document_file_path = create_and_copy_Word_template(template_path, destination_folder_base)
		doc = DocxTemplate(new_document_file_path)

		# placeholder = "UNIQUE_PLACEHOLDER_TEXT"
		# for paragraph in doc.paragraphs:
		#     if placeholder in paragraph.text:
		#         paragraph.text = paragraph.text.replace(placeholder, 'Your new text here.')

		#--------------------Dealing with dates------------------------------------------------------------------>
		now = datetime.now()
		offset = (now.weekday() - 6) % 7
		last_sunday = now - timedelta(days=offset)
		last_sunday = last_sunday.strftime('%Y-%m-%d')

		# Assuming last_sunday is a datetime.date object
		last_sunday = datetime.strptime(last_sunday, '%Y-%m-%d').date()
		# Get the first day of the month
		first_day_of_month = last_sunday.replace(day=1)
		# Calculate the difference in days
		day_difference = (last_sunday - first_day_of_month).days
		# Calculate the week of the month
		week_of_month = (day_difference // 7) + 1

		#------------------------------------------------------------------------------------------------------>
		backlog_carryoverTblRows=df_backlog_carryover.to_dict('records')




		targets_data = {'Lab': ['Beitbridge', 'Bindura', 'BRIDH', 'Chinhoyi', 'Gwanda', 'Gweru', 'Kadoma', 'Marondera', 'Masvingo', 'Mpilo', 'Mutare', 'NMRL', 'St Lukes', 'Vic Falls'],
		                'target': [270, 1224, 2217, 1542, 751, 1887, 1027, 1331, 2045, 2261, 2002, 1065, 822, 598]}
		targets_df = pd.DataFrame(targets_data)
		lab_df=df_last_week[['Lab','Total_samples_received_Plasma_VL','Total_samples_received_DBS_VL']]
		# Perform an inner join on the 'Lab' column
		result_df = pd.merge(lab_df, targets_df, on='Lab', how='inner')
		vlreceived=result_df['Total_samples_received_Plasma_VL'].sum()
		eidreceived=result_df['Total_samples_received_DBS_VL'].sum()
		target=result_df['target'].sum()
		#--------------------------------->
		totalEIDreceived_true=df_eidreceived['Total_samples_received'].sum()

		selected_columns = df_last_week[['Lab', 'Total_samples_received_Plasma_VL', 'Total_samples_received_DBS_VL']]
		selected_columns['Total_samples_received'] = selected_columns['Total_samples_received_Plasma_VL'] + selected_columns['Total_samples_received_DBS_VL']
		df_summ = selected_columns[['Lab', 'Total_samples_received']]
		summ_received=df_summ['Total_samples_received'].sum()
		percentage_achievement =round((summ_received/target)*100,2)


		received_target = pd.merge(df_summ, targets_df, on='Lab', how='inner')
		received_target
		mask = received_target['Total_samples_received'] > received_target['target']
		count_greater_than_target = mask.sum()

		samplesreceived_df = pd.merge(result_df, df_summ, on='Lab', how='inner')
		samplesreceivedTblRows=samplesreceived_df.to_dict('records')
		#--------------------------------------------------------------------------------->
		plasma_ref_out=df_referrals['Plasma_Samples_reffered_Out'].sum()
		dbs_ref_out=df_referrals['DBS_Samples_reffered_Out'].sum()
		total_referrals=plasma_ref_out+dbs_ref_out


		# Initialize an empty list to store the narrative statements
		narratives = []

		# Iterate through the DataFrame rows
		for index, row in df_referrals.iterrows():
		    lab_name = row['name_of_lab']
		    
		    # Check if Plasma samples were referred out
		    if row['Plasma_Samples_reffered_Out'] > 0:
		        narrative = f"from {lab_name} referring {row['Plasma_Samples_reffered_Out']} Plasma samples to {row['Plasma_refered_to_lab']},"
		        narratives.append(narrative)
		    
		    # Check if DBS samples were referred out
		    if row['DBS_Samples_reffered_Out'] > 0:
		        narrative = f"from {lab_name} referring {row['DBS_Samples_reffered_Out']} DBS samples to {row['DBS_refered_to_lab']},"
		        narratives.append(narrative)

		# Combine the narrative statements into one variable (separated by newline)
		narrative_text = '\n'.join(narratives)

		#---------------------------------------------------------------->
		eid_rejections = df_rejections[df_rejections['Test_Type'] == 'EID']['Num_Rejected_Samples'].sum()
		vl_rejections = df_rejections[df_rejections['Test_Type'] == 'VL']['Num_Rejected_Samples'].sum()
		# Round the elements in the arrays
		eid_rej_rate = round((eid_rejections / totalEIDreceived_true) * 100, 2)
		vl_rej_rate =  round((vl_rejections / summ_received) * 100, 2)
		#--------------------------------------------------------------------------->
		#Assay Failure Rate
		failed=df_LABassay_failure_rate['failures'].sum()
		testeddone=df_LABassay_failure_rate['Tests_done'].sum()
		Assay_failure_rate= round((failed/testeddone)*100,2)


		df_filtered_assay = df_LABassay_failure_rate[(df_LABassay_failure_rate['failures'] != 0) & (pd.notna(df_LABassay_failure_rate['failures']))].copy()
		df_filtered_assay['percfailed'] = round((df_filtered_assay['failures'] / df_filtered_assay['Tests_done']) * 100, 2)
		AssayFailureTbl=df_filtered_assay.to_dict('records')
		#--------------------------------------------------------------------------------->
		df_abso_failures = df_abso_failures.rename(columns=lambda x: x.replace(' ', '_'))
		failedsamplesTblRows=df_abso_failures.to_dict('records')

		#---------------------------------------------------------------------------------->
		df_patient_run_merged = pd.merge(df_patient_run, targets_df, on='Lab', how='inner')
		df_patient_run_merged['percAch']=round((df_patient_run_merged['Patient_run']/df_patient_run_merged['target']*100),2)
		df_patient_tmpl=df_patient_run_merged[['Lab','Patient_run', 'target','percAch']]
		total_patient_run=df_patient_run_merged['Patient_run'].sum()
		run_percentage_achievement=round((total_patient_run/target)*100,2)
		percAchievementTblRows=df_patient_tmpl.to_dict('records')


		mask_run = df_patient_run_merged['Patient_run'] > df_patient_run_merged['target']
		RUN_count_greater_than_target = mask_run.sum()
		#------------------------------------------------------------------------------->
		df_carryEID=df_eidreceived[['Lab', 'Carryover_Samples_in_the_lab','Total_samples_received']]
		df_edirun=df_EID_run_model[['Lab', 'Samples_run']]
		df_eid_merged=pd.merge(df_carryEID, df_edirun, on='Lab', how='right')
	
		
		df_eid_merged = df_eid_merged.loc[~(df_eid_merged['Carryover_Samples_in_the_lab'].isna() &
		                                    df_eid_merged['Total_samples_received'].isna() &
		                                    (df_eid_merged['Samples_run'] == 0))]
		df_eid_merged = df_eid_merged.fillna(0)
		columns_to_convert = ['Carryover_Samples_in_the_lab', 'Total_samples_received', 'Samples_run']  
		for column in columns_to_convert:
		    df_eid_merged[column] = df_eid_merged[column].astype(int)
		
		EidnottestedTbl=df_eid_merged.to_dict('records')
		#----------------------------------------------------------------------------->
		df_stock_status = df_stock_status.fillna(0)
		stockstatusTbl=df_stock_status.to_dict('records')
		#------------------------------------------------------------------------------>
		functional_bikes=df_bikeFxnality['functional_bikes'].sum()
		bike_functionality=df_bikeFxnality['bike_functionality'].sum()
		relief_riders=df_bikeFxnality['relief_riders'].sum()
		averted_missedpkups=df_bikeFxnality['relief_actual_vst'].sum()
		total_missed_pickups=df_bikeFxnality['total_missed_pickups'].sum()

		Bike_breakdown=df_nonFxntyRxns['Bike_breakdown'].sum()
		Maintanance_service=df_nonFxntyRxns['Maintanance_service'].sum()
		No_fuel=df_nonFxntyRxns['No_fuel'].sum()
		Sick_leave=df_nonFxntyRxns['Sick_leave'].sum()
		Annual_leave=df_nonFxntyRxns['Annual_leave'].sum()
		Inclement_weather=df_nonFxntyRxns['Inclement_weather'].sum()
		Accident_damage=df_nonFxntyRxns['Accident_damage'].sum()
		Clinical_issues=df_nonFxntyRxns['Clinical_issues'].sum()

		Days_Bike_breakdown=df_nonDaysFxn['Bike_breakdown'].sum()
		Days_Maintanance_service=df_nonDaysFxn['Maintanance_service'].sum()
		Days_No_fuel=df_nonDaysFxn['No_fuel'].sum()
		Days_Sick_leave=df_nonDaysFxn['Sick_leave'].sum()
		Days_Annual_leave=df_nonDaysFxn['Annual_leave'].sum()
		Days_Inclement_weather=df_nonDaysFxn['Inclement_weather'].sum()
		Days_Accident_damage=df_nonDaysFxn['Accident_damage'].sum()
		Days_Clinical_issues=df_nonDaysFxn['Clinical_issues'].sum()


		samples_transported_relief=df_relief_riders_samples['total_sam'].sum()
		results_transported_relief=df_relief_riders_samples['total_res'].sum()
		vleid_sam_transported_relief=df_relief_riders_samples['total_vl_eid_sam'].sum()
		vleid_res_transported_relief=df_relief_riders_samples['total_vl_eid_res'].sum()
		tb_sam_transported_relief=df_relief_riders_samples['Sputum_Culture_DR_NTBRL'].sum()
		tb_res_transported_relief=df_relief_riders_samples['Sputum_Culture_DR_NTBRL_res'].sum()
		hpv_sam_transported_relief=df_relief_riders_samples['HPV'].sum()
		hpv_res_transported_relief=df_relief_riders_samples['HPV_res'].sum()

		VL_Plasma_DBS_transported_relief=df_relief_riders_samples['VL_Plasma_DBS'].sum()
		resVL_Plasma_DBS_transported_relief=df_relief_riders_samples['VL_Plasma_DBS_res'].sum()

		EID_Plasma_DBS_transported_relief=df_relief_riders_samples['EID_Plasma_DBS'].sum()
		resEID_Plasma_DBS_transported_relief=df_relief_riders_samples['VL_Plasma_DBS_res'].sum()


		Bike_breakdown_districts=df_districts_ist['Bike_breakdown_districts']
		Bike_breakdown_districts=Bike_breakdown_districts.tolist()
		Maintanance_service_districts=df_districts_ist['Maintanance_service_districts']
		Maintanance_service_districts=Maintanance_service_districts.tolist()
		No_fuel_districts=df_districts_ist['No_fuel_districts']
		No_fuel_districts=No_fuel_districts.tolist()
		Sick_leave_districts=df_districts_ist['Sick_leave_districts']
		Sick_leave_districts=Sick_leave_districts.tolist()
		Annual_leave_districts=df_districts_ist['Annual_leave_districts']
		Annual_leave_districts=Annual_leave_districts.tolist()
		Inclement_weather_districts=df_districts_ist['Inclement_weather_districts']
		Inclement_weather_districts=Inclement_weather_districts.tolist()
		Accident_damage_districts=df_districts_ist['Accident_damage_districts']
		Accident_damage_districts=Accident_damage_districts.tolist()
		Clinical_issues_districts=df_districts_ist['Clinical_issues_districts']
		Clinical_issues_districts=Clinical_issues_districts.tolist()
		    
		info = {
		        "reportDtStr": last_sunday,
		        "vlreceived":vlreceived,
		        "eidreceived":eidreceived,
		        "summ_received":summ_received,
		        "count_greater_than_target":count_greater_than_target,
		        "percentage_achievement":percentage_achievement,
		        "samplesreceivedTblRows":samplesreceivedTblRows,    #dict
		        "plasma_ref_out":plasma_ref_out,
		        "dbs_ref_out":dbs_ref_out,
		        "total_referrals":total_referrals,
		        "narrative_text":narrative_text,
		        "totalEIDreceived_true":totalEIDreceived_true,
		        "eid_rej_rate":eid_rej_rate,
		        "vl_rej_rate":vl_rej_rate,
		        "percAchievementTblRows":percAchievementTblRows, #dict
		        "Assay_failure_rate":Assay_failure_rate,
		        "AssayFailureTbl":AssayFailureTbl,
		        "failedsamplesTblRows":failedsamplesTblRows,            
		        "run_percentage_achievement":run_percentage_achievement,
		        "total_patient_run":total_patient_run,
		        "RUN_count_greater_than_target":RUN_count_greater_than_target,
		        "EidnottestedTbl":EidnottestedTbl, #dict
		    "stockstatusTbl":stockstatusTbl ,  #dict
		    "functional_bikes":functional_bikes,
		    "bike_functionality":bike_functionality,
		    "relief_riders":relief_riders,
		    "averted_missedpkups":averted_missedpkups,
		    "total_missed_pickups":total_missed_pickups,
		    "Bike_breakdown":Bike_breakdown,
		    "Maintanance_service":Maintanance_service,
		    "No_fuel":No_fuel,
		    "Sick_leave":Sick_leave,
		    "Annual_leave":Annual_leave,
		    "Inclement_weather":Inclement_weather,
		    "Clinical_issues":Clinical_issues,
		    
		    "Days_Bike_breakdown":Days_Bike_breakdown,
		    "Days_Maintanance_service":Days_Maintanance_service,
		    "Days_No_fuel":Days_No_fuel,
		    "Days_Sick_leave":Days_Sick_leave,
		    "Days_Annual_leave":Days_Annual_leave,
		    "Days_Inclement_weather":Days_Inclement_weather,
		    "Days_Clinical_issues":Days_Clinical_issues,
		    "samples_transported_relief":samples_transported_relief,
		    "results_transported_relief":results_transported_relief,
		    "VL_Plasma_DBS_transported_relief":VL_Plasma_DBS_transported_relief,
		    "EID_Plasma_DBS_transported_relief": EID_Plasma_DBS_transported_relief,
		    "tb_sam_transported_relief": tb_sam_transported_relief,
		    "tb_res_transported_relief":tb_res_transported_relief,
		    "hpv_sam_transported_relief":hpv_sam_transported_relief,
		    
		    "Bike_breakdown_districts":Bike_breakdown_districts,
		    "Maintanance_service_districts":Maintanance_service_districts,
		    "No_fuel_districts":No_fuel_districts,
		    "Sick_leave_districts":Sick_leave_districts,
		    "Annual_leave_districts":Annual_leave_districts,
		    "Inclement_weather_districts":Inclement_weather_districts,
		    "Accident_damage_districts":Accident_damage_districts,
		    "Clinical_issues_districts":Clinical_issues_districts,
		    "backlog_carryoverTblRows":backlog_carryoverTblRows,
		   

		    


		    
		 }


		# # # inject image into the context
		# # fig, ax = plt.subplots()
		# # ax.bar([x["name"] for x in salesTblRows], [x["revenue"] for x in salesTblRows])
		# # fig.tight_layout()
		# # fig.savefig("images/trendImg.png")
		# # context['trendImg'] = InlineImage(doc, 'images/trendImg.png')



		doc.render(info)
		doc.save(new_document_file_path)
		error_messages='Successfully written'
		return  error_messages

	except Exception as e:

		error_type = type(e).__name__
		error_message = str(e)
		error_traceback = traceback.format_exc()

		error_messages = f"An error occurred on writting word:\n\nType: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
		return error_messages
