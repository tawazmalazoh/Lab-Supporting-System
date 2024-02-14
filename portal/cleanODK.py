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
import datetime as dt
import pyodbc
import urllib
import smtplib
from email.mime.text import MIMEText
import warnings
import urllib3
import datetime as dt
import pysurveycto
import requests
import traceback
from sqlalchemy.pool import QueuePool 



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





def loading_ODK_to_Dbase(dataframe):
    try:
        dataframe = dataframe.astype(str) 
        # Truncate all string columns to 254 characters
        for col in dataframe.columns:
            if dataframe[col].dtype == 'object' and all(isinstance(val, str) for val in dataframe[col]):
                dataframe[col] = dataframe[col].str.slice(0, 254)
        dataframe.to_sql('ODK_Specimen_and_Results', con=engine, if_exists='append', index=False, chunksize=1000)  # Adjust chunksize as needed
        
        return 'Data sucessfully loaded'
        
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()

        error_messages = f"An error occurred during reading_dash:\n\nType: {error_type}\nMessage: {error_message}\n\nTraceback:\n{error_traceback}"
        return error_messages
    

def pull_data(url, username, password, keyfile=None):

    # This function will extract records or media files from the SurveyCTO Rest API depending on the url provided
    # Include the keyfile parameter if your form or media file needs to be decrypted

    try:

        if keyfile == None:
            response = requests.get(
                url, auth=requests.auth.HTTPBasicAuth(username, password))
        else:
            files = {'private_key': open(keyfile, 'rb')}
            response = requests.post(
                url, files=files, auth=requests.auth.HTTPBasicAuth(username, password))

    except Exception as e:

        response = False
        print(e)

    return response


def save_media_file(file_bytes, file_name):

    f = open(file_name, 'wb')
    f.write(file_bytes)
    f.close()


def construct_url(form_id, servername,initial_date):

    url = f'https://{servername}.surveycto.com/api/v2/forms/data/wide/json/{form_id}?date={initial_date}'

    return url





def sanitize_data(df):
    df.loc[df['prov'] == 'buBulawayoProvince', 'prov'] = 1
    df.loc[df['prov'] == 'maManicalandProvince', 'prov'] = 3
    df.loc[df['prov'] == 'mcMashonalandCentralProvince', 'prov'] = 4
    df.loc[df['prov'] == 'meMashonalandEastProvince', 'prov'] = 5
    df.loc[df['prov'] == 'miMidlandsProvince', 'prov'] = 10
    df.loc[df['prov'] == 'mnMatabelelandNorthProvince', 'prov'] = 8
    df.loc[df['prov'] == 'msMatabelelandSouthProvince', 'prov'] = 9
    df.loc[df['prov'] == 'mvMasvingoProvince', 'prov'] = 7
    df.loc[df['prov'] == 'mwMashonalandWestProvince', 'prov'] = 6

    df.loc[df['dis'] == 'buBulawayoDistrict', 'dis'] = 2013
    df.loc[df['dis'] == 'maBuheraDistrict', 'dis'] = 2015
    df.loc[df['dis'] == 'maChimanimaniDistrict', 'dis'] = 2016
    df.loc[df['dis'] == 'maChipingeDistrict', 'dis'] = 2017
    df.loc[df['dis'] == 'maMakoniDistrict', 'dis'] = 2018
    df.loc[df['dis'] == 'maMutareCity', 'dis'] = 2019
    df.loc[df['dis'] == 'maMutareDistrict', 'dis'] = 2020
    df.loc[df['dis'] == 'maMutasaDistrict', 'dis'] = 2021
    df.loc[df['dis'] == 'mcCentenaryDistrict', 'dis'] = 2024
    df.loc[df['dis'] == 'mcGuruveDistrict', 'dis'] = 2025
    df.loc[df['dis'] == 'mcMazoweDistrict', 'dis'] = 2026

    df.loc[df['dis'] == 'meChikombaDistrict', 'dis'] = 2031
    df.loc[df['dis'] == 'meMaronderaDistrict', 'dis'] = 2034
    df.loc[df['dis'] == 'meMudziDistrict', 'dis'] = 2035
    df.loc[df['dis'] == 'meMurewaDistrict', 'dis'] = 2036
    df.loc[df['dis'] == 'meSekeDistrict', 'dis'] = 2038
    df.loc[df['dis'] == 'miChirumhanzuDistrict', 'dis'] = 2040
    df.loc[df['dis'] == 'miGokweNorthDistrict', 'dis'] = 2041
    df.loc[df['dis'] == 'miGweruDistrict', 'dis'] = 2043
    df.loc[df['dis'] == 'miMberengwaDistrict', 'dis'] = 2045
    df.loc[df['dis'] == 'miShurugwiDistrict', 'dis'] = 2046
    df.loc[df['dis'] == 'mnHwangeDistrict', 'dis'] = 2050
    df.loc[df['dis'] == 'mnUmguzaDistrict', 'dis'] = 2054
    df.loc[df['dis'] == 'msBulilimaDistrict', 'dis'] = 2056
    df.loc[df['dis'] == 'msGwandaDistrict', 'dis'] = 2057
    df.loc[df['dis'] == 'msInsizaDistrict', 'dis'] = 2058
    df.loc[df['dis'] == 'msMangweDistrict', 'dis'] = 2059
    df.loc[df['dis'] == 'msMatoboDistrict', 'dis'] = 2060
    df.loc[df['dis'] == 'mvBikitaDistrict', 'dis'] = 2062
    df.loc[df['dis'] == 'mvChiredziDistrict', 'dis'] = 2063
    df.loc[df['dis'] == 'mvChiviDistrict', 'dis'] = 2064
    df.loc[df['dis'] == 'mvGutuDistrict', 'dis'] = 2065
    df.loc[df['dis'] == 'mvMasvingoDistrict', 'dis'] = 2066
    df.loc[df['dis'] == 'mvMweneziDistrict', 'dis'] = 2067
    df.loc[df['dis'] == 'mvZakaDistrict', 'dis'] = 2068
    df.loc[df['dis'] == 'mwChegutuDistrict', 'dis'] = 2069
    df.loc[df['dis'] == 'mwKaribaDistrict', 'dis'] = 2071
    df.loc[df['dis'] == 'mwMhondoroDistrict', 'dis'] = 2073
    df.loc[df['dis'] == 'mwSanyatiDistrict', 'dis'] = 2074
    df.loc[df['dis'] == 'mwZvimbaDistrict', 'dis'] = 2075
    df.loc[df['dis'] == 'Bindura', 'dis'] =2023
    df.loc[df['dis'] == 'Bulawayo', 'dis'] =2013 
    df.loc[df['dis'] == 'Chegutu', 'dis'] = 2069
    df.loc[df['dis'] == 'Chikomba', 'dis'] = 2031
    df.loc[df['dis'] == 'Harare', 'dis'] = 2014



    df.loc[df['prov'] ==1 , 'prov'] = 'Bulawayo Province'
    df.loc[df['prov'] ==2 , 'prov'] = 'Harare Province'
    df.loc[df['prov'] ==3 , 'prov'] = 'Manicaland Province'
    df.loc[df['prov'] ==4 , 'prov'] = 'Mashonaland Central Province'
    df.loc[df['prov'] ==5 , 'prov'] = 'Mashonaland East Province'
    df.loc[df['prov'] ==6 , 'prov'] = 'Mashonaland West Province'
    df.loc[df['prov'] ==7 , 'prov'] = 'Masvingo Province'
    df.loc[df['prov'] ==8 , 'prov'] = 'Matebeleland North Province'
    df.loc[df['prov'] ==9 , 'prov'] = 'Matebeleland South Province'
    df.loc[df['prov'] ==10 , 'prov'] ='Midlands Province'

    df.loc[df['prov'] =='1' , 'prov'] = 'Bulawayo Province'
    df.loc[df['prov'] =='2' , 'prov'] = 'Harare Province'
    df.loc[df['prov'] =='3' , 'prov'] = 'Manicaland Province'
    df.loc[df['prov'] =='4' , 'prov'] = 'Mashonaland Central Province'
    df.loc[df['prov'] =='5' , 'prov'] = 'Mashonaland East Province'
    df.loc[df['prov'] =='6' , 'prov'] = 'Mashonaland West Province'
    df.loc[df['prov'] =='7' , 'prov'] = 'Masvingo Province'
    df.loc[df['prov'] =='8' , 'prov'] = 'Matebeleland North Province'
    df.loc[df['prov'] =='9' , 'prov'] = 'Matebeleland South Province'
    df.loc[df['prov'] =='10' , 'prov'] ='Midlands Province'

    df.loc[df['dis'] =='2013','dis'] ='Bulawayo District'
    df.loc[df['dis'] =='2014','dis'] ='Harare District'
    df.loc[df['dis'] =='2015','dis'] ='Buhera District'
    df.loc[df['dis'] =='2016','dis'] ='Chimanimani District'
    df.loc[df['dis'] =='2017','dis'] ='Chipinge District'
    df.loc[df['dis'] =='2018','dis'] ='Makoni District'
    df.loc[df['dis'] =='2019','dis'] ='Mutare City'
    df.loc[df['dis'] =='2020','dis'] ='Mutare District'
    df.loc[df['dis'] =='2021','dis'] ='Mutasa District'
    df.loc[df['dis'] =='2022','dis'] ='Nyanga District'
    df.loc[df['dis'] =='2023','dis'] ='Bindura District'
    df.loc[df['dis'] =='2024','dis'] ='Centenary District'
    df.loc[df['dis'] =='2025','dis'] ='Guruve District'
    df.loc[df['dis'] =='2026','dis'] ='Mazowe District'
    df.loc[df['dis'] =='2027','dis'] ='Mbire District'
    df.loc[df['dis'] =='2028','dis'] ='Mount Darwin District'
    df.loc[df['dis'] =='2029','dis'] ='Rushinga District'
    df.loc[df['dis'] =='2030','dis'] ='Shamva District'
    df.loc[df['dis'] =='2031','dis'] ='Chikomba District'
    df.loc[df['dis'] =='2032','dis'] ='Goromonzi District'
    df.loc[df['dis'] =='2033','dis'] ='Hwedza District'
    df.loc[df['dis'] =='2034','dis'] ='Marondera District'
    df.loc[df['dis'] =='2035','dis'] ='Mudzi District'
    df.loc[df['dis'] =='2036','dis'] ='Murewa District'
    df.loc[df['dis'] =='2037','dis'] ='Mutoko District'
    df.loc[df['dis'] =='2038','dis'] ='Seke District'
    df.loc[df['dis'] =='2039','dis'] ='Uzumba Maramba Pfungwe District'
    df.loc[df['dis'] =='2040','dis'] ='Chirumhanzu District'
    df.loc[df['dis'] =='2041','dis'] ='Gokwe North District'
    df.loc[df['dis'] =='2042','dis'] ='Gokwe South District'
    df.loc[df['dis'] =='2043','dis'] ='Gweru District'
    df.loc[df['dis'] =='2044','dis'] ='Kwekwe District'
    df.loc[df['dis'] =='2045','dis'] ='Mberengwa District'
    df.loc[df['dis'] =='2046','dis'] ='Shurugwi District'
    df.loc[df['dis'] =='2047','dis'] ='Zvishavane District'
    df.loc[df['dis'] =='2048','dis'] ='Binga District'
    df.loc[df['dis'] =='2049','dis'] ='Bubi District'
    df.loc[df['dis'] =='2050','dis'] ='Hwange District'
    df.loc[df['dis'] =='2051','dis'] ='Lupane District'
    df.loc[df['dis'] =='2052','dis'] ='Nkayi District'
    df.loc[df['dis'] =='2053','dis'] ='Tsholotsho District'
    df.loc[df['dis'] =='2054','dis'] ='Umguza District'
    df.loc[df['dis'] =='2055','dis'] ='Beitbridge District'
    df.loc[df['dis'] =='2056','dis'] ='Bulilima District'
    df.loc[df['dis'] =='2057','dis'] ='Gwanda District'
    df.loc[df['dis'] =='2058','dis'] ='Insiza District'
    df.loc[df['dis'] =='2059','dis'] ='Mangwe District'
    df.loc[df['dis'] =='2060','dis'] ='Matobo District'
    df.loc[df['dis'] =='2061','dis'] ='Umzingwane District'
    df.loc[df['dis'] =='2062','dis'] ='Bikita District'
    df.loc[df['dis'] =='2063','dis'] ='Chiredzi District'
    df.loc[df['dis'] =='2064','dis'] ='Chivi District'
    df.loc[df['dis'] =='2065','dis'] ='Gutu District'
    df.loc[df['dis'] =='2066','dis'] ='Masvingo District'
    df.loc[df['dis'] =='2067','dis'] ='Mwenezi District'
    df.loc[df['dis'] =='2068','dis'] ='Zaka District'
    df.loc[df['dis'] =='2069','dis'] ='Chegutu District'
    df.loc[df['dis'] =='2070','dis'] ='Hurungwe District'
    df.loc[df['dis'] =='2071','dis'] ='Kariba District'
    df.loc[df['dis'] =='2072','dis'] ='Makonde District'
    df.loc[df['dis'] =='2073','dis'] ='Mhondoro District'
    df.loc[df['dis'] =='2074','dis'] ='Sanyati District'
    df.loc[df['dis'] =='2075','dis'] ='Zvimba District'




    df.loc[df['dis'] ==2013,'dis'] ='Bulawayo District'
    df.loc[df['dis'] ==2014,'dis'] ='Harare District'
    df.loc[df['dis'] ==2015,'dis'] ='Buhera District'
    df.loc[df['dis'] ==2016,'dis'] ='Chimanimani District'
    df.loc[df['dis'] ==2017,'dis'] ='Chipinge District'
    df.loc[df['dis'] ==2018,'dis'] ='Makoni District'
    df.loc[df['dis'] ==2019,'dis'] ='Mutare City'
    df.loc[df['dis'] ==2020,'dis'] ='Mutare District'
    df.loc[df['dis'] ==2021,'dis'] ='Mutasa District'
    df.loc[df['dis'] ==2022,'dis'] ='Nyanga District'
    df.loc[df['dis'] ==2023,'dis'] ='Bindura District'
    df.loc[df['dis'] ==2024,'dis'] ='Centenary District'
    df.loc[df['dis'] ==2025,'dis'] ='Guruve District'
    df.loc[df['dis'] ==2026,'dis'] ='Mazowe District'
    df.loc[df['dis'] ==2027,'dis'] ='Mbire District'
    df.loc[df['dis'] ==2028,'dis'] ='Mount Darwin District'
    df.loc[df['dis'] ==2029,'dis'] ='Rushinga District'
    df.loc[df['dis'] ==2030,'dis'] ='Shamva District'
    df.loc[df['dis'] ==2031,'dis'] ='Chikomba District'
    df.loc[df['dis'] ==2032,'dis'] ='Goromonzi District'
    df.loc[df['dis'] ==2033,'dis'] ='Hwedza District'
    df.loc[df['dis'] ==2034,'dis'] ='Marondera District'
    df.loc[df['dis'] ==2035,'dis'] ='Mudzi District'
    df.loc[df['dis'] ==2036,'dis'] ='Murewa District'
    df.loc[df['dis'] ==2037,'dis'] ='Mutoko District'
    df.loc[df['dis'] ==2038,'dis'] ='Seke District'
    df.loc[df['dis'] ==2039,'dis'] ='Uzumba Maramba Pfungwe District'
    df.loc[df['dis'] ==2040,'dis'] ='Chirumhanzu District'
    df.loc[df['dis'] ==2041,'dis'] ='Gokwe North District'
    df.loc[df['dis'] ==2042,'dis'] ='Gokwe South District'
    df.loc[df['dis'] ==2043,'dis'] ='Gweru District'
    df.loc[df['dis'] ==2044,'dis'] ='Kwekwe District'
    df.loc[df['dis'] ==2045,'dis'] ='Mberengwa District'
    df.loc[df['dis'] ==2046,'dis'] ='Shurugwi District'
    df.loc[df['dis'] ==2047,'dis'] ='Zvishavane District'
    df.loc[df['dis'] ==2048,'dis'] ='Binga District'
    df.loc[df['dis'] ==2049,'dis'] ='Bubi District'
    df.loc[df['dis'] ==2050,'dis'] ='Hwange District'
    df.loc[df['dis'] ==2051,'dis'] ='Lupane District'
    df.loc[df['dis'] ==2052,'dis'] ='Nkayi District'
    df.loc[df['dis'] ==2053,'dis'] ='Tsholotsho District'
    df.loc[df['dis'] ==2054,'dis'] ='Umguza District'
    df.loc[df['dis'] ==2055,'dis'] ='Beitbridge District'
    df.loc[df['dis'] ==2056,'dis'] ='Bulilima District'
    df.loc[df['dis'] ==2057,'dis'] ='Gwanda District'
    df.loc[df['dis'] ==2058,'dis'] ='Insiza District'
    df.loc[df['dis'] ==2059,'dis'] ='Mangwe District'
    df.loc[df['dis'] ==2060,'dis'] ='Matobo District'
    df.loc[df['dis'] ==2061,'dis'] ='Umzingwane District'
    df.loc[df['dis'] ==2062,'dis'] ='Bikita District'
    df.loc[df['dis'] ==2063,'dis'] ='Chiredzi District'
    df.loc[df['dis'] ==2064,'dis'] ='Chivi District'
    df.loc[df['dis'] ==2065,'dis'] ='Gutu District'
    df.loc[df['dis'] ==2066,'dis'] ='Masvingo District'
    df.loc[df['dis'] ==2067,'dis'] ='Mwenezi District'
    df.loc[df['dis'] ==2068,'dis'] ='Zaka District'
    df.loc[df['dis'] ==2069,'dis'] ='Chegutu District'
    df.loc[df['dis'] ==2070,'dis'] ='Hurungwe District'
    df.loc[df['dis'] ==2071,'dis'] ='Kariba District'
    df.loc[df['dis'] ==2072,'dis'] ='Makonde District'
    df.loc[df['dis'] ==2073,'dis'] ='Mhondoro District'
    df.loc[df['dis'] ==2074,'dis'] ='Sanyati District'
    df.loc[df['dis'] ==2075,'dis'] ='Zvimba District'
    
    return df
