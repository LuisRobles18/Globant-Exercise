import pymysql
from app import app
from db_config import mysql
from flask import jsonify
from flask import flash, request
import pickle
import base64

#Inserting records into the departments table
@app.route('/departments/insert', methods=['POST'])
def add_department():
    try:
        #Reading the dataframe in base64 format
        departments_pickle_b64 = request.form['records']
        departments_df = pickle.loads(base64.b64decode(departments_pickle_b64.encode()))

        #MySQL connection
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        #Inserting each record into the MySQL database
        for index,row in departments_df.iterrows():
            sql = "INSERT INTO departments(id,department) VALUES(%s, %s)"
            data = (row[0],row[1])
            cur.execute(sql, data)
        
        #We commit until all records are inserted successfully
        conn.commit()

        message = {
            'status': 200,
            'message': 'The records were inserted successfully'
        }
        resp = jsonify(message)
        resp.status_code = 200

        #Closing DB connection
        cur.close()
        conn.close()

        return resp
    
    except Exception as e: #(If there is an error, it will be returned in a JSON format)
        message = {
        'status': 500,
        'message': 'Error: '+str(e)
        }
        resp = jsonify(message)
        resp.status_code = 500
        return resp

#Inserting records into the jobs table
@app.route('/jobs/insert', methods=['POST'])
def add_job():
    try:
        #Reading the dataframe in base64 format
        jobs_pickle_b64 = request.form['records']
        #Converting back to dataframe
        jobs_df = pickle.loads(base64.b64decode(jobs_pickle_b64.encode()))

        #MySQL connection
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        #Inserting each record into the MySQL database
        for index,row in jobs_df.iterrows():
            sql = "INSERT INTO jobs(id,job) VALUES(%s, %s)"
            data = (row[0],row[1])
            cur.execute(sql, data)
        
        #We commit until all records are inserted successfully
        conn.commit()

        message = {
            'status': 200,
            'message': 'The records were inserted successfully'
        }
        resp = jsonify(message)
        resp.status_code = 200

        #Closing DB connection
        cur.close()
        conn.close()

        return resp
    
    except Exception as e: #(If there is an error, it will be returned in a JSON format)
        message = {
        'status': 500,
        'message': 'Error: '+str(e)
        }
        resp = jsonify(message)
        resp.status_code = 500
        return resp
    


#Inserting records into the hired_employees table
@app.route('/hired_employees/insert', methods=['POST'])
def add_hired_employee():
    try:
        #Reading the dataframe in base64 format
        hired_employees_pickle_b64 = request.form['records']
        #Converting back to dataframe
        hired_employees_df = pickle.loads(base64.b64decode(hired_employees_pickle_b64.encode()))

        #MySQL connection
        conn = mysql.connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        #Inserting each record into the MySQL database
        for index,row in hired_employees_df.iterrows():
            sql = "INSERT INTO hired_employees(id,name,datetime,department_id,job_id) VALUES(%s,%s,%s,%s,%s)"
            data = (row[0],row[1],row[2],row[3],row[4])
            cur.execute(sql, data)
        
        #We commit until all records are inserted successfully
        conn.commit()

        message = {
            'status': 200,
            'message': 'The records were inserted successfully'
        }
        resp = jsonify(message)
        resp.status_code = 200

        #Closing DB connection
        cur.close()
        conn.close()

        return resp
    
    except Exception as e: #(If there is an error, it will be returned in a JSON format)
        message = {
        'status': 500,
        'message': 'Error: '+str(e)
        }
        resp = jsonify(message)
        resp.status_code = 500
        return resp

#If the resource is not found
@app.errorhandler(404)
def not_found(error = None):
    message = {
        'status': 404,
        'message': 'Not found: '+request.url
    }

    resp = jsonify(message)
    resp.status_code = 404

    return resp

if __name__ == "__main__":
    app.run()
