# 
# Author: Priyank Arora
# UTA ID: 1001XXXX
# Assignment 2 
# 
#

import os, urllib, json, ibm_db, datetime, syslog
from flask import Flask,redirect,render_template,request

app = Flask(__name__)
''' Getting the GLOBAL Variable for credentionals and related informations.'''

if 'VCAP_SERVICES' in os.environ:
    ''' We are reading credentials of dashDB For Transactions first instance attached to flask app'''
    db2info = json.loads(os.environ['VCAP_SERVICES'])['dashDB For Transactions'][0]
    db2cred = db2info["credentials"]
    ''' We are reading credentials of cloud-object-storage first instance attached to flask app'''
else:
    raise ValueError('Cloud Services not added/found in environment')


# handle database request and query city information
def db_operation(sql):
    db2conn = ibm_db.connect("DATABASE="+db2cred['db']+";HOSTNAME="+db2cred['hostname']+";PORT="+str(db2cred['port'])+";UID="+db2cred['username']+";PWD="+db2cred['password']+";","","")
    if db2conn:
        stmt = ibm_db.prepare(db2conn,sql)
        ibm_db.execute(stmt)
        rows=[]
        # fetch the result
        header = ibm_db.fetch_assoc(stmt)
        result = ibm_db.fetch_assoc(stmt)
        while result != False:
            rows.append(result.copy())
            result = ibm_db.fetch_assoc(stmt)
        # close database connection
        ibm_db.close(db2conn)
    return header, rows

# main page to dump some environment information
@app.route('/')
def index():
   return render_template('index.html')

@app.route('/query1', methods=['GET'])
def query1():
    sql  = "select * from QUAKE where MAG > 5.0"
    header, result = db_operation(sql)
    sql  = "select count(*) as total from (select * from QUAKE where MAG > 5.0)"
    result2, _ = db_operation(sql)
    return render_template('result.html', count = result2, results=result, headers=header)

@app.route('/query2', methods=['GET'])
def query2():
    start = request.args.get("starting")
    end = request.args.get("ending")
    frmRange = request.args.get("range1")
    toRange = request.args.get("range2")
    sql = "select * from quake where ((to_date(substring(TIME,1,10),'YYYY-MM-DD') between %s and %s) AND (MAG between %s and %s))"%('\''+start+'\'', '\''+end+'\'',frmRange,toRange)
    header, result = db_operation(sql)
    return render_template('result2.html', headers = header, results = result)


@app.route('/query3', methods=['GET'])
def query3():
    km = request.args.get("km")
    location = request.args.get("location")
    combined_result = []
    header = [] 
    result = []
    if int(km) <10:
        sql = "select * from quake where ((place LIKE %s) AND (substring(place,1,1) between '0' and %s) AND (substring(place,2,2) = 'km'))"%('\'%'+location+'%\'', km)
        header, result = db_operation(sql)
        combined_result = result
    elif int(km) >= 10 and int(km) <= 99:
        sql = "select * from quake where ((place LIKE %s) AND (substring(place,1,2) between '10' and %s) AND (substring(place,3,2) = 'km'))"%('\'%'+location+'%\'', '\''+km+'\'')
        header, result = db_operation(sql)
        combined_result = result
        sql = "select * from quake where ((place LIKE %s) AND (substring(place,1,1) between '0' and %s) AND (substring(place,2,2) = 'km'))"%('\'%'+location+'%\'', km)
        header, result = db_operation(sql)
        combined_result.append(result)
    elif int(km) >=100 and int(km) <= 999:
        sql = "select * from quake where ((place LIKE %s) AND (substring(place,1,3) between '100' and %s) AND (substring(place,4,2) = 'km'))"%('\'%'+location+'%\'', km)
        header, result = db_operation(sql)
        combined_result = result
        sql = "select * from quake where ((place LIKE %s) AND (substring(place,1,2) between '10' and %s) AND (substring(place,3,2) = 'km'))"%('\'%'+location+'%\'', '\''+km+'\'')
        header, result = db_operation(sql)
        combined_result.append(result)
        sql = "select * from quake where ((place LIKE %s) AND (substring(place,1,1) between '0' and %s) AND (substring(place,2,2) = 'km'))"%('\'%'+location+'%\'', km)
        header, result = db_operation(sql)
        combined_result.append(result)

    return render_template('result2.html', headers = header, results = combined_result)

@app.route('/query4', methods=['GET'])
def query4():
    longitude = request.args.get("longitude")
    latitude = request.args.get("latitude")
    range1 = request.args.get("range")
    sql = "select * from quake where (latitude between %s and %s) and (longitude between %s and %s)"%(float(latitude) - float(range1), float(latitude) + float(range1), float(longitude) - float(range1), float(longitude) + float(range1))
    header, result = db_operation(sql)
    return render_template('result2.html', headers = header, results = result)

@app.route('/query5', methods=['GET'])
def query5():
    sql = "select SUM(case when substr(TIME,12,12)  >= '20:00:00.000' or substr(TIME,12,12) <= '08:00:00.000' then 1 else 0 end) as NightQuakes, SUM(case when substr(TIME,12,12)  >= '08:00:00.000' and substr(TIME,12,12) <= '20:00:00.000' then 1 else 0 end) as DayQuakes from (select * from QUAKE where MAG > 4.0)"
    header, result = db_operation(sql)
    return render_template('result3.html', headers = header, results = result)

port = os.getenv('PORT', '5000')
if __name__ == "__main__":
	app.run(host='0.0.0.0', port=int(port))
