from fastavro import reader
import csv
import sys
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import os
import pandas as pd

#Reading filename from the argument

cur_datetime = str(datetime.now().strftime("%Y-%m-%d %H_%M_%S.%f"))

filename = sys.argv[1]
head = True
temp_filename = "temp("+cur_datetime+").csv"
f_csv = open(temp_filename, "w+", newline='')
f = csv.writer(f_csv)
with open(filename, 'rb') as fo:
    avro_reader = reader(fo)
    for emp in avro_reader:
        #Writing the header first
        if head == True:
            header = emp.keys()
            f.writerow(header)
            head = False
        #Writing the values for each row
        f.writerow(emp.values())

fo.close()
f_csv.close()
#Reading CSV in memory using pandas
df = pd.read_csv(temp_filename, delimiter=",")
#Removing temporary CSV file once it is loaded
os.remove(temp_filename)

try:
    #MySQL connection
    conn = mysql.connector.connect(host='luisrobles.me',
                                        database='luisrobl_globant',
                                        user='luisrobl_globant',
                                        password='e42w0}d1t(T.')
    if conn.is_connected():
        cur = conn.cursor()
        #Disabling foreign key checks temporarily
        cur.execute("SET FOREIGN_KEY_CHECKS=0")

        #We will execute the SQL query to a specific table depending on the filename
        if filename.startswith("backups/departments"):
            #First we truncate the table
            cur.execute("TRUNCATE TABLE departments")

            #Then we insert the data from the backup
            for index,row in df.iterrows():
                sql = "INSERT INTO departments(id,department) VALUES(%s, %s)"
                data = (row[0],row[1])
                cur.execute(sql, data)
        
        if filename.startswith("backups/hired_employees"):
            #First we truncate the table
            cur.execute("TRUNCATE TABLE hired_employees")
            #Then we insert the data from the backup
            for index,row in df.iterrows():
                sql = "INSERT INTO hired_employees(id,name,datetime,department_id,job_id) VALUES(%s, %s, %s, %s, %s)"
                data = (row[0],row[1],row[2],row[3],row[4])
                cur.execute(sql, data)
        
        if filename.startswith("backups/jobs"):
            #First we truncate the table
            cur.execute("TRUNCATE TABLE jobs")
            #Then we insert the data from the backup
            for index,row in df.iterrows():
                sql = "INSERT INTO jobs(id,job) VALUES(%s, %s)"
                data = (row[0],row[1])
                cur.execute(sql, data)
        
        #Re-enabling foreign key checks
        cur.execute("SET FOREIGN_KEY_CHECKS=1") 
    conn.commit()

except Error as e:
    print("Error while connecting to the database", e)
finally:
    if conn.is_connected():
        conn.close()
