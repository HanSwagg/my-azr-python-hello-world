# !flask/bin/python
import json
from collections import OrderedDict
import base64
import  time
from threading import Thread
import datetime
import sqlite3 as sqlz
import configparser
import os
from flask import Flask, jsonify, abort, request, make_response, url_for, render_template, g
#from flask_httpauth import HTTPBasicAuth

app = Flask(__name__, static_url_path="")
#auth = HTTPBasicAuth()

# Load the configuration file
configParser = configparser.RawConfigParser()

configFile = r'mkitdbtransfer.ini'

script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in

configFilepath = os.path.join(script_dir, configFile)

configParser.read(configFilepath)

dbpath = os.path.join(script_dir, configParser.get('DATABASE', 'path')).replace("\\","/")
dbtype = configParser.get('DATABASE', 'type')
jsonpath =  os.path.join(script_dir, configParser.get('JSONDATA', 'path')).replace("\\","/")
backuppath =  os.path.join(script_dir, configParser.get('JSONDATA', 'archivedir')).replace("\\","/")
apphost = configParser.get('SERVER', 'host')
portnumber = int(configParser.get('SERVER', 'port'))



##############################################################################
# Please Updade the version and revision number before commit in the SVN repo
# ############################################################################
print("Mkit DB Server - Version 1.10 - Revision 9202")

#@auth.get_password
def get_password(username):
    if username == 'mkituser':
        return 'pythonx'
    return None


#@auth.error_handler
def unauthorized():
    return make_response(jsonify({'error': 'Unauthorized access'}), 403)
    # return 403 instead of 401 to prevent browsers from displaying the default auth dialog


#@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad request'}), 400)


#@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/')
def hello_world():
  return  'dbpath: '+ dbpath + ' <br/>DB TYPE: ' +dbtype +'<br/>jSON PATH: '+ jsonpath +'<br/>BACKUP PATH: '+ backuppath +'<br/>HOST: '+ apphost

@app.route('/api/insert', methods=['POST'])
# @auth.login_required
def InsertData():

    content = request.get_json(silent=True)

    strfile = os.path.join(jsonpath,datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S") )+".json"

    #with open(strfile, 'w') as outfile:
    #    json.dump(content, outfile, sort_keys=False, indent=4, separators=(',', ': ') )

    mthread = Sendjsontodb_thread(content)
    mthread.start()
    mthread.join()
    print("packet received")
    return jsonify({'task': 'done'}), 201


@app.route('/mkitdb/api/sendtodb', methods=['POST'])
# @auth.login_required
def Sendjsontodb_thread(content):
    row = content[0]
    index = 0
    mythread = None
    for key, value in row.items():
        TableNameKey = base64.b64decode(key).decode('utf-8')     # == "MkitUser";
        TableName = base64.b64decode(value).decode('utf-8')   # == "phalconx";
    valueslist = []
    content.pop(0)
    for row in content:
        valueslist.extend([list(row.values())])
        # Create two threads as follows
    try:
        mythread =Thread( target=insert_readings, args=(TableName, row, valueslist))
    except:
            print("Error: unable to start thread")
            return None
    return mythread



##############################Thread process######################
# Define a function for the thread
def insert_readings(tablename, header, datalist):
        # create a data structure
        # Create table

        #columns = ' DECIMAL, '.join(values.keys())+' DECIMAL'
        #'({0})'.format(w)
        #for w in List

        DATABASE = dbpath
        with sqlz.connect(DATABASE) as cnx:
            cur = cnx.cursor()

            columns = ' DECIMAL, '.join('[{0}]'.format(w) for w in header.keys()) + ' DECIMAL'
            sqlqry = "Create TABLE if not exists {}({})".format(tablename, columns)

            sqlqry = sqlqry.replace('[Timestamp] DECIMAL','[Timestamp] DATETIME' )
            cur.execute(sqlqry)
            cnx.commit()

            columns = ', '.join('[{0}]'.format(w) for w in header.keys())
            placeholders = ', '.join('?' * len(header))
            sql = '''INSERT INTO '''+tablename+'''({}) VALUES ({})'''.format(columns, placeholders)
            toto = header.values()
            cur.executemany(sql, datalist)
            cnx.commit()
            print("packet inserted into DB")
        cnx.close()
        #os.rename(jsonstrfile, os.path.join(backuppath, filename))  # //Move the processed file to the archive directory


if __name__ == '__main__':
    app.run()
