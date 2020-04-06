#!/home/ervin/anaconda3/bin/python3
import boto3
import datetime
import pandas as pd
import numpy as np
from io import StringIO
import requests 
import camelot
import re
import PyPDF2
from pdfminer.pdfparser import PDFParser
import urllib.request
import io

today = datetime.datetime.today().strftime('%Y-%m-%d')

#################################################################################### OLD MATERIAL ################################################################
# yesterday = (datetime.datetime.today() - datetime.timedelta(days = 1)).strftime('%Y-%m-%d')

# corona =pd.read_csv('/home/ervin/python_experiments/corona_project/corona_data_updates.csv')
# corona['date'] = yesterday


# client = boto3.client(
#     's3',
#     aws_access_key_id='#######################',
#     aws_secret_access_key='#############################',
    
# )

# client.delete_bucket(Bucket = 'corona-experiments')

# #create a bucket
# client.create_bucket(ACL = 'private',Bucket = 'corona-experiments')

# folder_names = ['nys', 'world']

# for i in folder_names:
#   client.put_object(Bucket = 'corona-experiments', Key = (f"data/{i}" + "/") )

# bucket = 'corona-experiments'
# csv_buffer = StringIO()
# corona.to_csv(csv_buffer)
# s3_resource = boto3.resource('s3',
#   aws_access_key_id='AKIAICWXJP6SFJX6HPAQ',
#     aws_secret_access_key='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'
#     )

# s3_resource.Object(bucket, f'data/nys/corona_nys_{yesterday}.csv').put(Body=csv_buffer.getvalue())

######################################################################### nys data ########################################################################################
# the commented out code below was used to get nys data from what was the only source avaliable to me from March 22 to April 2
#url = 'https://covid19tracker.health.ny.gov/views/NYS-COVID19-Tracker/NYSDOHCOVID-19Tracker-TableView?%3Aembed=yes&%3Atoolbar=no'
#html = requests.get(url).content
#df_list = pd.read_html(html)
#corona = df_list[-1]
#corona['date'] = today

#the new source of nys corona infection data
# this includes historical information as well
corona_nys  = pd.read_csv('https://health.data.ny.gov/resource/xdss-u53e.csv')
corona_nys['test_date'] = pd.to_datetime(corona_nys['test_date'])


# upload a file to a bucket
s3_resource = boto3.resource('s3',
    aws_access_key_id='AKIAICWXJP6SFJX6HPAQ',
    aws_secret_access_key='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'
    )
bucket = 'corona-experiments'
csv_buffer = StringIO()
corona_nys.to_csv(csv_buffer)
s3_resource.Object(bucket, f'data/nys/corona_nys_{today}_v2.csv').put(Body=csv_buffer.getvalue())

####################################################################### world data #########################################################################################
year_todate = []
date = datetime.datetime(2019,12,31)
while date < datetime.datetime.today():
    date += datetime.timedelta(days = 1)
    year_todate.append(date.strftime('%m-%d-%Y'))
                       
year_todate

for i in year_todate:
    try:
        url = f'https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_daily_reports/{i}.csv'
        html = requests.get(url).content
        df_list = pd.read_html(html)
        df = df_list[-1]
        bucket = 'corona-experiments'
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource.Object(bucket, f'data/world/corona_world_{i}.csv').put(Body=csv_buffer.getvalue()) # I got the credentials for this resource above
    except:
        continue
################################################################################################ nyc ########################################################################

#datasource
# url = 'https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary.pdf'

# #this gets only the table/s from the PDF
# tables = camelot.read_pdf(url, pages = "1-end")

# borough_list = ['Bronx', 'Brooklyn', 'Queens', 'Manhattan', 'Staten Island']
# covid_boro_infections = pd.DataFrame(tables[0].df)
# covid_boro_infections.columns = ['boro','stats']
# covid_boro_infections_2 = covid_boro_infections[covid_boro_infections['boro'].str.contains('|'.join(borough_list))]

# covid_boro_infections_2['boro'] = covid_boro_infections_2['boro'].str.extract(r'([A-Za-z]+)')
# covid_boro_infections_2['stats'] = covid_boro_infections_2['stats'].str.extract(r'(\d+)')


# #get the pdf file
# open = urllib.request.urlopen(url).read()
# #this is done because the data is in bytes format
# memoryFile = io.BytesIO(open)

# #parser = PDFParser(memoryFile)

# # creating a pdf reader object 
# pdfReader = PyPDF2.PdfFileReader(memoryFile) 
  
# # printing number of pages in pdf file 
# print(pdfReader.numPages) 


# # creating a page object 
# pageObj = pdfReader.getPage(0) 
  
# # extracting text from page 
# #print(pageObj.extractText())

# # this string contains all the text from the first page
# my_string = pageObj.extractText() 

# #this regular expression extracts the date from the above string

# date_string = re.search(r'[A-Za-z]+(\s)?(\d){1,2}(\s)?(,)(\s)+?(\d){4}', my_string).group(0)

# # in my initial sgtring, the date had a \n in it
# if "\n" in date_string:
#     report_date = "".join(date_string.split('\n'))
# else:
#     report_date = date_string

# covid_boro_infections_2['date'] = pd.to_datetime(report_date).date()

# covid_boro_infections_2

# bucket = 'corona-experiments'
# csv_buffer = StringIO()
# covid_boro_infections_2.to_csv(csv_buffer)
# s3_resource.Object(bucket, f'data/nys/corona_nyc_{report_date}.csv').put(Body=csv_buffer.getvalue())

 ############# doh data

cases_nyc_zip = pd.read_csv('https://raw.githubusercontent.com/nychealth/coronavirus-data/master/tests-by-zcta.csv')
cases_nyc_zip = cases_nyc_zip[~cases_nyc_zip['MODZCTA'].isnull()]
cases_nyc_zip['date'] = pd.to_datetime('today').date()

bucket = 'corona-experiments'
csv_buffer = StringIO()
cases_nyc_zip.to_csv(csv_buffer)
s3_resource.Object(bucket, f'data/nys/corona_nyc_zip_{today}.csv').put(Body=csv_buffer.getvalue())


cases_nyc_boro = pd.read_csv('https://raw.githubusercontent.com/nychealth/coronavirus-data/master/boro.csv')
cases_nyc_boro['date'] = pd.to_datetime('today').date()

bucket = 'corona-experiments'
csv_buffer = StringIO()
cases_nyc_boro.to_csv(csv_buffer)
s3_resource.Object(bucket, f'data/nys/corona_nyc_boro_{today}.csv').put(Body=csv_buffer.getvalue())


cases_nyc_date = pd.read_csv('https://raw.githubusercontent.com/nychealth/coronavirus-data/master/case-hosp-death.csv')

bucket = 'corona-experiments'
csv_buffer = StringIO()
cases_nyc_date.to_csv(csv_buffer)
s3_resource.Object(bucket, f'data/nys/corona_nyc_data_{today}.csv').put(Body=csv_buffer.getvalue())

################################################################################################ execution log ##############################################################

datet = str(datetime.datetime.today())
tekst = b'the file was last executed on %b' %(str.encode(datet))

# I got the credentials for this resource above
object = s3_resource.Object(bucket, 'logs/logs.txt')
object.put(Body = tekst)
