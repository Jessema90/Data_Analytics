# Data Connection, Read and Clean
# MSc Jesse Mauricio Beltran Soto

#1)  Install the influx-db libraries and streamlit libraries


#!pip install influxdb-client
#!pip install streamlit

#2)  Import libraries to use

import influxdb_client, os, time
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import ssl
import urllib3
from datetime import datetime
from influxdb_client import InfluxDBClient
import time


#3) Connection to the influxdb database

# In the next line of code, we import the data from influxdb, 
# taking into account that it is necesary to have a token and a
# bucket that is managed through the influxdb cloud platform
http = urllib3.PoolManager(cert_reqs=ssl.CERT_REQUIRED)
resp = http.request('GET', 'https://eu-central-1-1.aws.cloud2.influxdata.com/ping')

start_time = time.time()
config = {
    #'start': datetime.date(datetime(2022, 7, 5)),
    #'stop': datetime.date(datetime(2021, 7, 10)),
    'bucket': "NODE_RED_DATA",
    'org': "bsjessem@unal.edu.co",
    'url': "https://eu-central-1-1.aws.cloud2.influxdata.com",
    'token': "KQr0l07umK1Z-ur3z7UjADORj6qCEZbZ6TISpjV56XFXn3AuYquuyM3tyOTXuc5qnT7_LuPq4z1OS36m9tCErA==",
    'measurement-name': "PM02",
    # PM02 is the name of the second group of solar inverters
    'measurement-name_2': "IRRADIANCE"
}
client = InfluxDBClient(
    url="https://eu-central-1-1.aws.cloud2.influxdata.com",
    org=config['org'],
    bucket=config['bucket'],
    token=config['token'],
    # verify_ssl=False
)

query_api = client.query_api()
PM02_DF_V1 = query_api.query_data_frame(
    f'from(bucket:"{config["bucket"]}")'
        f'|> range(start: 2022-07-24T00:00:00Z, stop: 2022-12-31T00:00:00Z) '
        '|> filter(fn: (r) =>'
            f' r._measurement == "{config["measurement-name"]}"'
        ') '
)
IRRADIANCE_DF_V1=query_api.query_data_frame(
    f'from(bucket:"{config["bucket"]}")'
        f'|> range(start: 2022-07-24T00:00:00Z, stop: 2022-12-31T00:00:00Z) '
        '|> filter(fn: (r) =>'
            f' r._measurement == "{config["measurement-name_2"]}"'
        ') '
)
print("Query time %s seconds ---" % (time.time() - start_time))
print(PM02_DF_V1)
print(IRRADIANCE_DF_V1)


# 4)  Backup of data

PM02_DF=PM02_DF_V1.copy(deep=True)
IRRADIANCE_DF=IRRADIANCE_DF_V1.copy(deep=True)

#PM01_DF.head()

# 5) Delete columns
# The "_result", "table", "_start" and "_stop" columns will be removed, 
# leaving only the "_time", "value" , "_field" and "measurement" fields, 
# using the following lines:
PM02_DF=PM02_DF_V1.drop(['result','table','_start','_stop'], axis=1)
IRRADIANCE_DF=IRRADIANCE_DF_V1.drop(['result','table','_start','_stop'], axis=1)


# 6) Display with 3 decimal places
pd.options.display.float_format='{:,.3f}'.format


# 7) Pivot tables
PM02_DF=pd.pivot_table(PM02_DF,index='_time',columns='_field',values=['_value'])
IRRADIANCE_DF=pd.pivot_table(IRRADIANCE_DF,index='_time',columns='_field',values=['_value'])

#PM02_DF.dtypes

# 8) Dataframe order after pivot
PM02_DF.columns = PM02_DF.columns.droplevel(0) 
PM02_DF.columns.name = None               
PM02_DF= PM02_DF.reset_index()    
IRRADIANCE_DF.columns = IRRADIANCE_DF.columns.droplevel(0) 
IRRADIANCE_DF.columns.name = None               
IRRADIANCE_DF= IRRADIANCE_DF.reset_index()  


# 9) Modifications in the time structure and creation of a new time column.

PM02_DF['new_time']=pd.to_datetime(PM02_DF['_time'],unit='ms', utc=True).dt.tz_convert('America/Bogota')
IRRADIANCE_DF['new_time']=pd.to_datetime(IRRADIANCE_DF['_time'],unit='ms', utc=True).dt.tz_convert('America/Bogota')
PM02_DF['Year']=PM02_DF['new_time'].dt.year 
PM02_DF['Month']=PM02_DF['new_time'].dt.month
PM02_DF['Hour']=PM02_DF['new_time'].dt.hour
PM02_DF['Min']=PM02_DF['new_time'].dt.minute


# 10) Modifications format to a "new_time" column
PM02_DF['new_time']=pd.to_datetime(PM02_DF["new_time"].dt.strftime("%Y-%m-%d %H:%M"))
IRRADIANCE_DF['new_time']=pd.to_datetime(IRRADIANCE_DF["new_time"].dt.strftime("%Y-%m-%d %H:%M"))


# 11) PM02_DF and IRRADIANCE_DF dataframes union
df=PM02_DF.merge(IRRADIANCE_DF[['IRRADIANCE_PLC1(W/m^2)','IRRADIANCE_PLC2(W/m^2)','new_time']], on='new_time')

#12) Count null values (Rows)
df.isnull().sum()


# 13) Delete null values
df = df.dropna() # Eliminamos los valores nulos
df = df.reset_index(drop=True) # Reseteamos el index del dataframe


# 15) Save the dataframe in .csv file
df.to_csv(r'E:\JESSE\PLATZI\PYTHON_MV1\src\df_solar_01.csv', index=False, header=True)



