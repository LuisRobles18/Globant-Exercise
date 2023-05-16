import pandas as pd
import os
import numpy as np
import requests
import time
import pickle
import base64
from datetime import datetime
#Filtering warnings (if any)
import warnings
warnings.filterwarnings("ignore")

#Location of the CSV files
departments_csv = 'data/incoming/departments.csv'
jobs_csv = 'data/incoming/jobs.csv'
hired_employees_csv = 'data/incoming/hired_employees.csv'

#Current datetime
cur_datetime = str(datetime.now().strftime("%Y-%m-%d %H_%M_%S.%f"))

#Function to make an API call
#First parameter is the API endpoint
#Second parameter is the number of batches per transaction
#Third parameter represents the maximum number of attemps for a successful request
def post_data(df, table_name, endpoint, no_batches, no_tries):

    #This line will create the batches (according to the number of batches)
    lst_df = [df.iloc[i:i+no_batches] for i in range(0,len(df),no_batches)]
    #We iterate for each batch
    for batch in lst_df:
        no_tries = 1
        #We'll make three attemps to the API server
        while no_tries <= 3:
            #With pickle we convert the dataframe into a byte-like string (base64 encode)
            batch_pickled = pickle.dumps(batch)
            batch_pickled_b64 = base64.b64encode(batch_pickled)
            data = {
                'records': batch_pickled_b64
            }
            try:
                response = requests.post(url = endpoint, data = data)
                #Breaking the while loop once the call is successful
                if response.status_code == 200:
                    no_tries = 3
                    break
                else:
                    no_tries += 1
                    if no_tries < 3:
                        #If API service is not responding, we wait 1 second before performing the next attemp
                        time.sleep(1)
                    #Once we reach the maximum number of tries, we will log these records
                    #And they'll be added as well inside the "not_added" folder in a CSV file
                    else:
                        batch.to_csv("data/not_added/"+table_name+"/"+table_name+"("+cur_datetime+").csv", encoding="utf-8", mode='a', index=False, header=False)
                        json_response = response.json()
                        log_not_added_records(batch, table_name, "API service error, status code: "+str(response.status_code)+". Message from server: "+str(json_response['message']))
            except Exception as e:
                no_tries += 1
                if no_tries < 3:
                    #If there is an error connecting to the server, we wait 1 second before performing the next attemp
                    time.sleep(1)
                #Once we reach the maximum number of tries, we will log these records
                #And they'll be added as well inside the "not_added" folder in a CSV file
                else:
                    batch.to_csv("data/not_added/"+table_name+"/"+table_name+"("+cur_datetime+").csv", encoding="utf-8", mode='a', index=False, header=False)
                    log_not_added_records(batch, table_name, str(e))

#This function will log all the rows with invalid or missing cells
def log_not_added_records(df, table_name, reason):
    #Appending log file
    log_file = open('log.txt', 'a')
    for row in df.iterrows():
        log_file.write('* ('+cur_datetime+') The record with ID '+str(row[0])+' from the '+table_name+' table was not added. Reason: '+reason+'\r\n')
    log_file.close()

#========================= Algorithm to move data from the departments table ===========================

#Check if file exists in folder
if os.path.isfile(departments_csv):
    #If exists, we convert it to a pandas dataframe
    df_departments = pd.read_csv(departments_csv, encoding='utf8', index_col=False, names=['id', 'department'])
    #Replacing all empty values with NaN
    df_departments = df_departments.replace(' ', np.nan)
    #All the rows with missing values will be moved to another dataframe
    df_not_added_1 = df_departments[df_departments.isna().any(axis=1)] 
    #The rows with non-empty values will remain in the same dataframe
    df_departments = df_departments[~df_departments.isna().any(axis=1)] 

    #First we validate that the first column is numeric
    df_not_added_2 = df_departments[df_departments.id.apply(lambda x: not str(x).isnumeric())]
    #The rows with numeric values in the first column (id) will remain
    df_departments = df_departments[df_departments.id.apply(lambda x: str(x).isnumeric())]
    #There is no need to validate the second column as it is not empty and is not requred to be a number

    #All the missing and not valid rows will be added to a CSV file inside the "not_added" folder
    df_not_added = pd.concat([df_not_added_1, df_not_added_2], ignore_index=True, sort=False)

    if len(df_not_added) > 0:
        df_not_added.to_csv("data/not_added/departments/departments("+cur_datetime+").csv", encoding="utf-8", mode='a', index=False, header=False)
        #Moreover, it will be logged in a txt file
        log_not_added_records(df_not_added,"departments", "empty or invalid cells")

    #Insert the records successfully validated
    post_data(df_departments, "departments", "http://localhost:5000/departments/insert", 1000, 3)
    #Deleting the original file from the bucket(folder)
    os.remove(departments_csv)

#========================= Algorithm to move data from the jobs table ===========================

#Check if file exists in folder
if os.path.isfile(jobs_csv):
    #If exists, we convert it to a pandas dataframe
    df_jobs = pd.read_csv(jobs_csv, encoding='utf8', index_col=False, names=['id', 'job'])
    #Replacing all empty values with NaN
    df_jobs = df_jobs.replace(' ', np.nan)
    #All the rows with missing values will be moved to another dataframe
    df_not_added_1 = df_jobs[df_jobs.isna().any(axis=1)] 
    #The rows with non-empty values will remain in the same dataframe
    df_jobs = df_jobs[~df_jobs.isna().any(axis=1)] 

    #First we validate that the first column is numeric
    df_not_added_2 = df_jobs[df_jobs.id.apply(lambda x: not str(x).isnumeric())]
    #The rows with numeric values in the first column (id) will remain
    df_jobs = df_jobs[df_jobs.id.apply(lambda x: str(x).isnumeric())]
    #There is no need to validate the second column as it is not empty and is not requred to be a number

    #All the missing and not valid rows will be added to a CSV file inside the "not_added" folder
    df_not_added = pd.concat([df_not_added_1, df_not_added_2], ignore_index=True, sort=False)

    if len(df_not_added) > 0:
        df_not_added.to_csv("data/not_added/jobs/jobs("+cur_datetime+").csv", encoding="utf-8", mode='a', index=False, header=False)
        #Moreover, it will be logged in a txt file
        log_not_added_records(df_not_added,"jobs", "empty or invalid cells")

    #Insert the records successfully validated
    post_data(df_jobs, "jobs", "http://localhost:5000/jobs/insert", 1000, 3)
    #Deleting the original file from the bucket(folder)
    os.remove(jobs_csv)


#========================= Algorithm to move data from the hired_employees table ===========================
#Check if file exists in folder
if os.path.isfile(hired_employees_csv):
    #If exists, we convert it to a pandas dataframe
    df_hired_employees = pd.read_csv(hired_employees_csv, encoding='utf8', index_col=False, names=['id', 'name', 'datetime', 'department_id', 'job_id'])
    #Replacing all empty values with NaN
    df_hired_employees = df_hired_employees.replace(' ', np.nan)
    #All the rows with missing values will be moved to another dataframe
    df_not_added_1 = df_hired_employees[df_hired_employees.isna().any(axis=1)] 
    #The rows with non-empty values will remain in the same dataframe
    df_hired_employees = df_hired_employees[~df_hired_employees.isna().any(axis=1)] 

    #First we validate that the first column is numeric
    df_not_added_2 = df_hired_employees[df_hired_employees.id.apply(lambda x: not str(x).isnumeric())]
    #The rows with numeric values in the first column (id) will remain
    df_hired_employees = df_hired_employees[df_hired_employees.id.apply(lambda x: str(x).isnumeric())]
    
    #Validating the datetime (third column), as it must be in ISO format
    regex_datetime_iso = b'\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z)'
    df_not_added_3 = df_hired_employees[df_hired_employees.datetime.str.contains(regex_datetime_iso , regex= True, na=False)]
    #Rows with valid datetime in ISO format will remain
    df_hired_employees = df_hired_employees[~df_hired_employees.datetime.str.contains(regex_datetime_iso , regex= True, na=False)]
    
    #Validating the department_id column is numeric
    df_not_added_4 = df_hired_employees[df_hired_employees.department_id.apply(lambda x: not str(x).isnumeric())]
    #The rows with numeric values in the department_id column will remain
    df_hired_employees = df_hired_employees[df_hired_employees.department_id.apply(lambda x: str(x).isnumeric())]

    #Validating the job_id column is numeric
    df_not_added_5 = df_hired_employees[df_hired_employees.job_id.apply(lambda x: not str(x).isnumeric())]
    #The rows with numeric values in the job_id column will remain
    df_hired_employees = df_hired_employees[df_hired_employees.job_id.apply(lambda x: str(x).isnumeric())]

    #All the missing and not valid rows will be added to a CSV file inside the "not_added" folder
    df_not_added = pd.concat([df_not_added_1, df_not_added_2, df_not_added_3, df_not_added_4, df_not_added_5], ignore_index=True, sort=False)

    if len(df_not_added) > 0:
        df_not_added.to_csv("data/not_added/hired_employees/hired_employees("+cur_datetime+").csv", encoding="utf-8", mode='a', index=False, header=False)
        #Moreover, it will be logged in a txt file
        log_not_added_records(df_not_added,"hired_employees", "empty or invalid cells")
    
    #Insert the records successfully validated
    post_data(df_hired_employees, "hired_employees", "http://localhost:5000/hired_employees/insert", 1000, 3)
    #Deleting the original file from the bucket(folder)
    os.remove(hired_employees_csv)