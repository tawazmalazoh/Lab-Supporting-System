from django.contrib import messages
from django.core.mail import message
from django.core.files.storage import FileSystemStorage
from django.shortcuts import render,redirect
from django.contrib.auth import authenticate,login,logout
from django.core.mail import send_mail
from django.http import HttpResponse
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
import plotly.graph_objects as go
# sns.set(color_codes=True)
# from pandasai import PandasAI
# from pandasai.llm.openai import OpenAI
# from pandasai.llm.open_assistant import OpenAssistant
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
from django.db import connections
from .forms import IstForm
from .forms import RtcqiForm
from .forms import WeeklyForm
from .customised import reading_dash
from .SqlQueries import allqueries
from .views import get_filtered_data
from .views import get_default_data



def rtcql_view(request):
    gsheet_id = "1Et0JDJhbYrynigViGSjrhhqOdShBWlNNQbTn4Sw4IPQ"
    export_url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv"
    df = pd.read_csv(export_url)
    df = df.rename(columns={"Timestamp":"Timestamp",
    "Date of Audit ":"Date_of_Audit",
    "Audit Start Time (hh:mm) :":"Audit_Start_Time_hh_mm",
    "Audit Round No":"Audit_Roun_No",
    "Testing Facility Name:":"Testing_Facility_Name",
    "Testing Facility ID (if applicable)":"Testing_Facility_ID",
    "Type of testing point (Circle One)    ":"Type_of_testing_point",
    "Level (Circle One and specify name)    ":"Level",
                            
    "Specify name of level":"Specify_name_of_level",
    "Affiliation":"Affiliation",
    "Number of Testers at Site:":"Number_of_Testers_at_Site",
    "Number of clients tested for HIV: Past Month:                                    ":"Number_of_clients_tested_for_HIV_Past_Month",
                            
    "Number of clients tested for HIV: Past Quarter  ":"Number_of_clients_tested_for_HIV_Past_Quarter",
                                         
    "Number of newly identified HIV positives in Past Month:                                     ":"Number_of_newly_identified_HIV_positives_in_Past_Month",

    "Number of newly identified HIV positives in   Past Quarter:":"Number_of_newly_identified_HIV_positives_in_Past_Quarter",
    "Number of HIV negatives in Past Month:                                     ":"Number_of_HIV_negatives_in_Past_Month",
    "Number of HIV negatives in  Past Quarter:":"Number_of_HIV_negatives_in_Past_Quarter",
    "Number of newly identified HIV positives tested by RTRI: Past Month":"Number_of_newly_identified_HIV_positives_tested_by_RTRI_Past_Month",
    "Number of newly identified HIV positives tested by RTRI: Past Quarter:":"Number_of_newly_identified_HIV_positives_tested_by_RTRI_Past_Quarter",
    "Number of Recent by RTRI or RITA: Past Month":"Number_of_Recent_by_RTRI_or_RITA_Past_Month",
    "Number of Recent by RTRI or RITA:   Past Quarter:":"Number_of_Recent_by_RTRI_or_RITA_Past_Quarter",
    "Name of the Auditor 1:":"Name_of_the_Auditor_1",
    "Name of the Auditor 2:":"Name_of_the_Auditor_2",
    "1.1 Have all testers received a comprehensive training on HIV rapid testing using the nationally approved curriculum?":"_1_1",
    "1.1 comment":"_1_1_comment",
                            
    "\n1.2 Are the testers trained on the use of standardized HIV testing registers/logbooks?":"_1_2",
    "1.2 comment":"_1_2_comment",

    "\n1.3 Are the testers trained on external quality assessment (EQA) or proficiency testing (PT) process?":"_1_3",
    "1.3 Comment":"_1_3_comment",
    "\n1.4 Are the testers trained on quality control (QC) process?":"_1_4",
    "1.4 Comment":"_1_4_comment",
    "\n1.5 Are the testers trained on safety and waste management procedures and practices?":"_1_5",
    "1.5 Comment":"_1_5_comment",
    "\n1.6 Have all testers received a refresher training within the last two years?":"_1_6",
    "1.6 Comment":"_1_6_comment",
    "\n1.7 Are there records indicating all testers have demonstrated competency in HIV rapid testing prior to client testing?":"_1_7",
    "1.7 Comment":"_1_7_comment",
    "\n1.8 Have all testers been certified through a national certification program?     ":"_1_8",
    "1.8 Comment":"_1_8_comment",
    "1.9 Are only certified testers performing HIV rapid testing at the site?":"_1_9",
    "1.9 Comment":"_1_9_comment",
    "\n1.10 Are all testers re-certified periodically (e.g., every two years)?":"_1_10",
    "1.10 Comment":"_1_10_comment",
    "2.1\tIs there a designated area for HIV testing?":"_2_1",
    "2.1 Comment":"_2_1_comment",
    "\n2.2 Is the testing area clean and organized for HIV rapid testing?":"_2_2",
    "2.2 Comment":"_2_2_comment",
    "\n2.3 Is sufficient lighting available in the designated testing area?":"_2_3",
    "2.3 Comment ":"_2_3_comment ",
    "\n2.4 Are the test kits stored according to manufacturers’ instructions?":"_2_4",
    "2.4 Comment ":"_2_4_comment ",
    "\n2.5 Is there sufficient storage space for test kits and other consumables?":"_2_5",
    "2.5 Comment":"_2_5_comment",
    "3.1 Are there SOPs and/or job aides in place to implement safety practices?":"_3_1",
    "3.1 Comment":"_3_1_comment",
    "\n3.2 Are there SOPs and/or job aides in place to address accidental exposure to potentially infectious body fluids through a needle stick injury, splash or other sharps injury?":"_3_2",
    "3.2 Comment":"_3_2_comment",
    "\n3.3 Are testers and those visiting the testing area following the safety practices outlined in the SOPs and/or job aides?":"_3_3",
    "3.3 Comment ":"_3_3_comment ",
    "\n3.4 Is personal protective equipment (PPE) always available to testers?":"_3_4",
    "3.4 Comment":"_3_4_comment",
    "\n3.5 Is PPE properly used by all testers consistently throughout the testing process?":"_3_5",
    "3.5 Comment":"_3_5_comment",
    "\n3.6 Is there clean water and soap available for hand washing and is it consistently used?":"_3_6",
    "3.6 Comment":"_3_6_comment",
    "\n3.7 Is there an appropriate disinfectant to clean the work area available?":"_3_7",
    "3.7 Comment":"_3_7_comment",
    "\n3.8 Is the disinfectant solution available properly labeled with content, date of preparation and date of expiration?":"_3_8",
    "3.8 Comment":"_3_8_comment",
    "\n3.9 Are sharps, infectious and non-infectious waste disposed of according to the segregation instructions?":"_3_9",
    "3.9 Comment":"_3_9_comment",
    "\n3.10 Are infectious and non-infectious waste containers emptied regularly per the SOP and/or job aides?":"_3_10",
    "3.10 Comment":"_3_10_comment",
    "\n4.1 Are there national HIV testing guidelines available at the testing point? ":"_4_1",
    "4.1 Comment":"_4_1_comment",
    "\n4.2 Is the national HIV testing algorithm(s) consistently being used at the testing site?":"_4_2",
    "4.2 Comment ":"_4_2_comment ",
    "4.3 Are SOPs and/or job aides on HIV rapid test procedures and the national HIV rapid test algorithm(s) available and easily accessible at the testing site?":"_4_3",
    "4.3 Comment ":"_4_3_comment ",
    "4.4 Are SOPs and/or job aides on HIV rapid test procedures and the national testing algorithm up-to-date and accurate?":"_4_4",
    "4.4 Comment":"_4_4_comment",
    "\n4.5 Are only nationally approved HIV rapid test kits available for use?":"_4_5",
    "4.5 Comment":"_4_5_comment",
    "\n4.6 Are all the test kits currently in use within the expiration date?":"_4_6",
    "4.6 Comment":"_4_6_comment",
    "\n4.7 Are all required test kit components (i.e. test device, buffer, sample collection device, etc.) and supplies available prior to testing?":"_4_7",
    "4.7 Comment ":"_4_7_comment ",
    "\n4.8 Is there a process in place for stock management?":"_4_8",
    "4.8 Comment":"_4_8_comment",
    "4.9 Is there a documented inventory system in place at the testing point for test kits received (i.e. who received them, date of receipt, etc.)?":"_4_9",
    "4.9 Comment":"_4_9_comment",
    "4.10 Are job aides on finger prick or venous blood collection available and posted at the testing point?":"_4_10",
    "4.10 Comment":"_4_10_comment",
    "4.11 Are there sufficient supplies available for finger prick or venous blood collection (i.e. lancet, gauze, alcohol swab, etc.)?":"_4_11",
    "4.11 Comment":"_4_11_comment",
    "4.12 Are there SOPs and/or job aides describing how client identification should be recorded in the HIV testing register?":"_4_12",
    "4.12 Comment":"_4_12_comment",
    "\n4.13 Are client identifiers recorded in the HIV testing register and on test devices per SOPs and/or job aide?":"_4_13",
    "4.13 Comment":"_4_13_comment",
    "\n5.1 Are SOPs and/or job aides on HIV testing procedures and the national testing algorithm being referred to and followed during testing?":"_5_1",
    "5.1 Comment":"_5_1_comment",
    "\n5.2 Are there timers available and used for HIV rapid testing?":"_5_2",
    "5.2 Comment":"_5_2_comment",
    "\n5.3 Are sample collection devices (e.g., capillary tube, loop, disposable pipettes, etc.) used accurately to perform the test?":"_5_3",
    "5.3 Comment ":"_5_3_comment ",
    "\n5.4 Are testing procedures adequately followed?":"_5_4",
    "5.4 Comment":"_5_4_comment",
    "\n5.5 Are external positive and negative quality control (QC) specimens routinely used (e.g., daily, weekly or monthly) according to SOPs or guidelines?":"_5_5",
    "5.5 Comment":"_5_5_comment",
    "\n5.6 Are QC results properly recorded?":"_5_6",
    "5.6 Comments ":"_5_6_comments ",
    "\n5.7 Are incorrect and/or invalid QC results properly recorded?":"_5_7",
    "5.7 Comment":"_5_7_comment",
    "\n5.8 Are appropriate steps taken and documented when QC results are incorrect and/or invalid?":"_5_8",
    "5.8 Comment":"_5_8_comment",
    "5.9 Are QC records reviewed by the person in charge routinely?":"_5_9",
    "5.9 Comment ":"_5_9_comment ",
    "6.1 Is there a national standardized HIV rapid testing register/logbook that includes all of the key quality elements available and in use?":"_6_1",
    "6.1 Comments ":"_6_1_comments ",
    "\n6.2 Are all the elements in the register/ logbook recorded/captured correctly?  (e.g., client demographics, kit names, lot numbers, expiration dates, tester name, individual and final HIV results, etc.)?":"_6_2",
    "6.2 Comment ":"_6_2_comment ",
    "\n6.3 Is the total summary at the end of each page of the register/logbooks complied accurately?    ":"_6_3",
    "6.3 Comment":"_6_3_comment",
    "\n6.4 Are invalid test results recorded properly in the register/logbook?":"_6_4",
    "6.4 Comment":"_6_4_comment",
    "6.5 Are appropriate steps taken and documented when a result is invalid?":"_6_5",
    "6.5 Comment":"_6_5_comment",
    "6.6 Are the register/logbook pages routinely reviewed for accuracy and completeness by the person in charge?":"_6_6",
    "6.6 Comment":"_6_6_comment",
    "6.7 Are all client documents and records securely kept throughout all phases of the testing process?":"_6_7",
    "6.7 Comment":"_6_7_comment",
    "6.8 Are all registers/logbooks and other documents kept in a secure location when not in use?":"_6_8",
    "6.8 Comment":"_6_8_comment",
    "\n6.9 Are registers/logbooks properly labeled and archived when full?":"_6_9",
    "6.9 Comment ":"_6_9_comment ",
    "\n7.1 Is the testing point enrolled in an EQA/PT program?":"_7_1",
    "7.1 Comment":"_7_1_comment",
    "\n7.2 Do all testers at the testing point test the EQA/PT samples?":"_7_2",
    "7.2 Comment":"_7_2_comment",
    "\n7.3 Does the person in charge at the testing point review the /PT results before submission to NRL or designee?":"_7_3",
    "7.3 Comment":"_7_3_comment",
    "\n7.4 Is an EQA/PT report received from NRL and reviewed by testers and/or the person in charge at the testing point?":"_7_4",
    "7.4 Comment":"_7_4_comment",
    "7.5 Does the testing point implement corrective action in case of unsatisfactory results?":"_7_5",
    "7.5 Comment":"_7_5_comment",
    "\n7.6 Does the testing point receive periodic supervisory visits?":"_7_6",
    "7.6 Comment":"_7_6_comment",
    "\n7.7 Is feedback provided during supervisory visit and documented?":"_7_7",
    "7.7 Comment":"_7_7_comment",
    "\n7.8 If testers need to be retrained, are they being retrained during the supervisory visit?":"_7_8",
    "7.8 Comment":"_7_8_comment",
    "Is the testing site providing the Rapid Test for Recent Infection (RTRI)":"_7_8_RTRI",
    "\n8.1 Have all testers received a comprehensive training on RTRI?":"_8_1",
    "8.1 Comment":"_8_1_comment",
    "\n8.2 Are there records indicating all testers have demonstrated competency in RTRI prior to testing?":"_8_2",
    "8.2 Comment":"_8_2_comment",
    "\n8.3 Are all current versions of recency/RTRI SOPs and/or job aids readily available at the site? ":"_8_3",
    "8.3 Comment":"_8_3_comment",
    "8.4 Is there a sufficient supply of RTRI tests available at the site? Please provide number of tests currently available…….":"_8_4",
    "8.4  Please provide number of tests currently available":"_8_4_Number",
    "8.4 comment":"_8_4_comment",
    "\n8.5 Are the test kits kept in a temperature-controlled environment based on the manufacturers’ instructions?":"_8_5",
    "8.5 Comment":"_8_5_comment",
    "8.6 Are RTRI testing procedures being followed (i.e. right volume of sample using correct sample application device, correct read time, correct result interpretation)?":"_8_6",
    "8.6 comment":"_8_6_comment",
    "8.7 Are the RTRI results documented in the data capture form or logbook correctly (e.g. client demographics, kit name, lot number, expiration dates, tester name, RTRI visual results and recency interpretation) and reviewed by the person in charge?":"_8_7",
    "8.7 Comment":"_8_7_Comment",
    "8.8 Are external quality control (QC) specimens (i.e. long-term (LT), recent and negative) routinely used (i.e. monthly) for RTRI?":"_8_8",
    "8.8 Comment":"_8_8_Comment",
    "8.9 Are QC results for RTRI properly recorded (e.g. kit name, lot number, expiration dates, tester name, RTRI visual results and recency interpretation for each level of QC) and reviewed by person in charge?":"_8_9",
    "8.9 Comment":"_8_9_Comment",
    "8.10\tAre appropriate steps taken and documented according to the SOP or guidelines when RTRI QC results are incorrect?":"_8_10",
    "8.10 Comment":"_8_10_Comment",
    "8.11 Are appropriate steps taken and documented according to the SOP or guidelines for invalid RTRI test results?\n":"_8_11",
    "If yes, how many in the last 3 months…………":"_8_11yes",
    "8.11 Comment":"_8_11_Comment",
    "Facility Name:":"Facility_Name",
    "Site Type:":"Site_Type",
    "Site code (if applicable):":"Site_code",
    "Staff Audited Name:":"Staff_Audited_Name",
    "No. of Tester(s):":"No_of_Testers",
    "Audit End Time (hh:mm):":"Audit_End_Time_hh_mm" })



    columns_to_replace = ['_1_1','_1_2', '_1_3', '_1_4', '_1_5', '_1_6', '_1_7', '_1_8', '_1_9', '_1_10', '_2_1', '_2_2', '_2_3',
                          '_2_4', '_2_5', '_3_1', '_3_2', '_3_3', '_3_4', '_3_5', '_3_6', '_3_7', '_3_8', '_3_9', '_3_10', '_4_1',
                          '_4_2', '_4_3', '_4_4', '_4_5', '_4_6', '_4_7', '_4_8', '_4_9', '_4_10', '_4_11', '_4_12', '_4_13', '_5_1',
                          '_5_2', '_5_3', '_5_4', '_5_5', '_5_6', '_5_7', '_5_8', '_5_9', '_6_1', '_6_2', '_6_3', '_6_4', '_6_5', '_6_6',
                          '_6_7', '_6_8', '_6_9', '_7_1', '_7_2', '_7_3', '_7_4', '_7_5', '_7_6', '_7_7', '_7_8', '_8_1',
                          '_8_2', '_8_3', '_8_4', '_8_5', '_8_6', '_8_7', '_8_8', '_8_9', '_8_10', '_8_11']

    values_to_replace = {'Yes': 1, 'Partial': 0.5, 'No': 0}

    for column in columns_to_replace:
        df[column] = df[column].replace(values_to_replace)

    yesno={'Yes': 1,  'No': 0}
    df['_7_8_RTRI']=df['_7_8_RTRI'].replace(yesno)

    df['Date_of_Audit'] = df['Date_of_Audit'].replace('09/03/0023', '09/03/2023')
    df['Date_of_Audit'] = df['Date_of_Audit'].replace('10/03/0023', '10/03/2023')
    df['Date_of_Audit'] = df['Date_of_Audit'].replace('27/02/0023', '27/02/2023')
    df['Date_of_Audit'] = pd.to_datetime(df['Date_of_Audit'], format='%d/%m/%Y', dayfirst=True)



    # Create a new column 'Expected_Score' with initial values as null and populate the Expected scores
    df['Expected_Score'] = np.nan

    df.loc[(df['_8_1'].isnull() | (df['_8_1'] == 0)) & 
           (df['_8_2'].isnull() | (df['_8_2'] == 0)) & 
           (df['_8_3'].isnull() | (df['_8_3'] == 0)), 'Expected_Score'] = 64 


    df.loc[ df['_8_1'].notnull()  & 
            df['_8_2'].notnull() &
            df['_8_3'].notnull() , 'Expected_Score'] = 75 

    #Creating another column for total_Score
    df['Total_Score'] = df[columns_to_replace].sum(axis=1)
    df['_Score']= (df['Total_Score']/ df['Expected_Score'])*100

    df['Level_Score'] = np.nan
    df.loc[ df['_Score']<40, 'Level_Score'] =0
    df.loc[df['_Score'].between(40, 59.9), 'Level_Score'] = 1
    df.loc[df['_Score'].between(60, 79.9), 'Level_Score'] = 2
    df.loc[df['_Score'].between(80, 89.9), 'Level_Score'] = 3
    df.loc[df['_Score']>=90, 'Level_Score'] = 4






    Section1=['_1_1','_1_2', '_1_3', '_1_4', '_1_5', '_1_6', '_1_7', '_1_8', '_1_9', '_1_10']
    Section2=['_2_1', '_2_2', '_2_3','_2_4', '_2_5']
    Section3=['_3_1', '_3_2', '_3_3', '_3_4', '_3_5', '_3_6', '_3_7', '_3_8', '_3_9', '_3_10']
    Section4=[ '_4_1','_4_2', '_4_3', '_4_4', '_4_5', '_4_6', '_4_7', '_4_8', '_4_9', '_4_10', '_4_11', '_4_12', '_4_13']
    Section5=['_5_1','_5_2', '_5_3', '_5_4', '_5_5', '_5_6', '_5_7', '_5_8', '_5_9']
    Section6=['_6_1', '_6_2', '_6_3', '_6_4', '_6_5', '_6_6', '_6_7', '_6_8', '_6_9']
    Section7=['_7_1', '_7_2', '_7_3', '_7_4', '_7_5', '_7_6', '_7_7', '_7_8']
    Section8=['_8_1', '_8_2', '_8_3', '_8_4', '_8_5', '_8_6', '_8_7', '_8_8', '_8_9', '_8_10', '_8_11']

    sections = [Section1, Section2, Section3, Section4, Section5, Section6, Section7, Section8]
    section_names = ['Section1', 'Section2', 'Section3', 'Section4', 'Section5', 'Section6', 'Section7', 'Section8']

    for section, section_name in zip(sections, section_names):
        df[section_name + '_total'] = pd.concat([df[col] for col in section], axis=1).sum(axis=1)
        
        



    #------------- creating columns with %scores per section------------
    secxion = ['_Section1_Score', '_Section2_Score', '_Section3_Score', '_Section4_Score', '_Section5_Score', '_Section6_Score', '_Section7_Score', '_Section8_Score']
    expt_score = [10, 5, 10, 13, 9, 9, 8, 11]
    secxion_total = ['Section1_total', 'Section2_total', 'Section3_total', 'Section4_total', 'Section5_total', 'Section6_total', 'Section7_total', 'Section8_total']
    for i in range(len(secxion)):
        section_name = secxion[i]
        df[section_name] =  ( df[secxion_total[i]] / expt_score[i])*100
        

    #_------- creating the Non Compliance columns score per section---------------
    df['NC_Section1'] = df[Section1].eq(0).sum(axis=1) 
    df['NC_Section2'] = df[Section2].eq(0).sum(axis=1)   
    df['NC_Section3'] = df[Section3].eq(0).sum(axis=1)
    df['NC_Section4'] = df[Section4].eq(0).sum(axis=1) 
    df['NC_Section5'] = df[Section5].eq(0).sum(axis=1) 
    df['NC_Section6'] = df[Section6].eq(0).sum(axis=1) 
    df['NC_Section7'] = df[Section7].eq(0).sum(axis=1) 
    df['NC_Section8'] = df[Section8].eq(0).sum(axis=1) 
     
        
     #Create a  column for High Impact  
    High_Impact_Facilities = [
    "Filabusi ", "Filabusi District Hospital ",
    "Masvingo Provincial Hospital FCH ",
    "Bindura provincial Hospital", "Bindura Provincial Hospital",
    "Makumbe District Hospital","Masvingo Provincial Hospital ", "Masvingo Provincial Hospital OIC ",
    "Pelandaba Clinic ","Pelandaba Clinic","Kadoma Hospital OPD",
    "Kadoma Hospital Matenity ","Kadoma Hospital Maternity ","St Joseph's Mission Hospital",
    "MASVINGO PROVINCIAL HOSPITAL","St Joseph's Mission Hospital",
    "Chinhoyi Provincial Hospital ","Kwekwe Hospital One Shop Center ",
    "Mabvuku Polyclinic ANC", "Mabvuku poly clinic ANC"
    ]
    # Create the 'High_Impact' column
    df['High_Impact'] = df['Testing_Facility_Name'].apply(lambda x: 1 if x in High_Impact_Facilities else 0)
        

    # Extracting the first two words from Testing_Facility_Name
    df['First_Two_Words'] = df['Testing_Facility_Name'].str.split().str[:2].str.join(' ')
    # Combining First_Two_Words and Type_of_testing_point
    df['Testing_Facility'] = df['First_Two_Words'] + '_' + df['Type_of_testing_point']
    
        # Load Data for dashboard in database
    server = settings.DATABASES['default']['HOST']
    database = settings.DATABASES['default']['NAME']
    username = settings.DATABASES['default']['USER']
    password = settings.DATABASES['default']['PASSWORD']
    driver = settings.DATABASES['default']['OPTIONS']['driver']
    engine_insert = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}', echo=False)

    # Check if 'rtcqm.csv' file exists in the folder
    if not os.path.exists('rtcqm.csv'):
        df.to_csv('rtcqm.csv', index=False)  # Save DataFrame to CSV

        df.dropna(how='all')
        df.to_sql('RTCQM_Data', con=engine_insert, if_exists='append', index=False, chunksize=1000)  # Upload the whole DataFrame to the table


    else:
        # Read the existing CSV and get the last row
        existing_df = pd.read_csv('rtcqm.csv')
        last_row = existing_df.tail(1)

        # Identify new rows not present in the existing CSV
        new_rows = df[~df.isin(existing_df)].dropna(how='all')

        if not new_rows.empty:
            # Append new rows to the existing DataFrame
            updated_df = pd.concat([existing_df, new_rows], ignore_index=True)
            
            # Save the updated DataFrame as the CSV file
            updated_df.to_csv('rtcqm.csv', index=False)
            
            # Upload only the new rows to the 'RTCQM_Data' table
            new_rows.to_sql('RTCQM_Data', con=engine_insert, if_exists='append', index=False, chunksize=1000)


            data = get_filtered_data(start_date, end_date)

            return data
    else:
        form = RtcqiForm()

    # Get default data
    default_data = get_default_data()

    return default_data
    