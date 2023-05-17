import csv
from collections import namedtuple
from fastavro import parse_schema, writer
import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')
import time


def create_backup():
    cur_datetime = str(datetime.now().strftime("%Y-%m-%d %H_%M_%S.%f"))

    try:
        #MySQL connection
        conn = mysql.connector.connect(host='luisrobles.me',
                                            database='luisrobl_globant',
                                            user='luisrobl_globant',
                                            password='e42w0}d1t(T.')
        if conn.is_connected():
            #We store each table on separated CSV files for each table
            sql_query_departments = pd.read_sql_query('''SELECT * FROM departments''',conn)
            sql_query_departments.to_csv('backups/departments('+cur_datetime+').csv', encoding='utf-8', index=False)

            sql_query_jobs = pd.read_sql_query('''SELECT * FROM jobs''',conn)
            sql_query_jobs.to_csv('backups/jobs('+cur_datetime+').csv', encoding='utf-8', index=False)

            sql_query_hired_employees = pd.read_sql_query('''SELECT * FROM hired_employees''',conn)
            sql_query_hired_employees.to_csv('backups/hired_employees('+cur_datetime+').csv', encoding='utf-8', index=False)

    except Error as e:
        print("Error while connecting to the database", e)
    finally:
        if conn.is_connected():
            conn.close()



    #=============Creating the AVRO file for the hired_employees table================
    schema = {
            "namespace": "hired_employees.avro",
            "type": "record",
            "name": "hired_employees",
            "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "name", "type": "string"},
                        {"name": "datetime", "type": "string"},
                        {"name": "department_id", "type": "int"},
                        {"name": "job_id", "type": "int"}
                        ]
                    }
    reader = pd.read_csv('backups/hired_employees('+cur_datetime+').csv',
                            dtype = {'id': int, 'name': str, 'datetime': str, 'department_id': int, 'job_id':int})
    lst = reader.to_dict('records')

    with open("backups/hired_employees("+cur_datetime+").avro", "wb") as fp:
        writer(fp, schema, lst)

    #Deleting the CSV file
    os.remove('backups/hired_employees('+cur_datetime+').csv')

    #=============Creating the AVRO file for the jobs table================
    schema = {
            "namespace": "jobs.avro",
            "type": "record",
            "name": "jobs",
            "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "job", "type": "string"}
                        ]
                    }
    reader = pd.read_csv('backups/jobs('+cur_datetime+').csv',
                            dtype = {'id': int, 'job': str})
    lst = reader.to_dict('records')

    with open("backups/jobs("+cur_datetime+").avro", "wb") as fp:
        writer(fp, schema, lst)

    #Deleting the CSV file
    os.remove('backups/jobs('+cur_datetime+').csv')


    #=============Creating the AVRO file for the departments table================
    schema = {
            "namespace": "departments.avro",
            "type": "record",
            "name": "departments",
            "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "department", "type": "string"}
                        ]
                    }
    reader = pd.read_csv('backups/departments('+cur_datetime+').csv',
                            dtype = {'id': int, 'job': str})
    lst = reader.to_dict('records')

    with open("backups/departments("+cur_datetime+").avro", "wb") as fp:
        writer(fp, schema, lst)

    #Deleting the CSV file
    os.remove('backups/departments('+cur_datetime+').csv')


while True:
    #The backup function will be called every day
    create_backup()
    time.sleep(86400)