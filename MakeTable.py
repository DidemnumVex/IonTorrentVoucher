# -*- coding: utf-8 -*-
"""
Created on Thu Mar 01 12:11:18 2018
MakeSQLTable.py
Make SQL table from excel file or csv file.
top row=column names
second row =data types
third row: end = data

@author: Tracy
"""


import pandas as pd
import re
import mysql.connector
import sqlalchemy
import numpy as np
#from MIRA_templateFilewriter import mysqlconnect#, writefile #don't need writefile, copied from other program
import mysql
#import plotly.plotly as py
#import plotly.graph_objs as go
#from plotly import __version__
#from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
#print __version__ 
#from Bio import Entrez
import time
from datetime import datetime, date, time
#import geocoder
#import LatLon
#import pyproj
import argparse
from mysql.connector import errorcode


def mysqlconnect(connectionpath): #uses the information in the connection file to open mysql connection
    connect = open(connectionpath, 'r')
#    print cnx.read()
    cnx = connect.read()
    cnx.strip('\n')
    return cnx

def mkschema(tblname,schemadic,pklst):
    
#    try:
    tblschm = ""
    for k in schemadic:
        print schemadic[k][0]
        if k == 'AUTOINCREMENT':
            tblschm = tblschm + "`id` MEDIUMINT NOT NULL AUTO_INCREMENT, "
        elif k in pklst:
            tblschm = tblschm + " `" + str(k) +"` " + schemadic[k][0] + " NOT NULL, "
        else:
            tblschm = tblschm + " `" + str(k) +"` " + schemadic[k][0] + ", "
            
    pk = ""
    for k in np.arange(len(pklst)):
        if k == len(pklst) -1 and k != 'AUTOINCREMENT':
#            print k
            pk = pk + "PRIMARY KEY (`" + schemadic[k][0] +"`) "
        else:
#            print k
            pk = pk + "PRIMARY KEY (`" + schemadic[k][0] +"`), "
    print pk        
    Table = ("CREATE TABLE IF NOT EXISTS `" + tblname + "` ("
         " " + str(tblschm) + " " + str(pk) +
                                                        ")")
    print Table
#    except:
#            print "Sorry"
    
    return Table

def WriteToInvertDB(name,tbl, seqdf,WD,User,Pass, IP, DB):#writes sequence records to InvertDB
   
    cnx = mysql.connector.connect(user=User, password= Pass, host= IP, database=DB)
    engine = sqlalchemy.create_engine('mysql://' + User + ':' + Pass+ '@' + IP +'/'+ DB)
    cursor = cnx.cursor()
    if WD == False:
        try:
            print("Creating table "+ name)
            cursor.execute(tbl)
        except:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print 'Table Exists'
    else:        
        try:
            cursor.execute(tbl)
            seqdf.to_sql(name,engine ,if_exists='append', index=False)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                seqdf.to_sql(name,engine ,if_exists='append', index=False)#, dtype= {'Plate': 'varchar(50)', 'Well': 'varchar(50)', 'VialID': 'varchar(50)', 'SeqID' : 'varchar(50)', 'Sequence': 'longtext' })
                print "Appending"
            else:
                print(err.msg)
        else:
            print("OK")
#    except:
#        print "sorry, nonetype"
    engine.dispose()
    cursor.close()
    cnx.close()
    cnx.disconnect()


def main():
#
    connect = mysqlconnect('C:\Users\User\scripts\login.txt')
    exec(connect) #executes declaration of variable using string from previous step
    cnx = mysql.connector.connect(user=connect['User'], password= connect['Pass'], host= connect['IP'], database=connect['DB'])
    engine = sqlalchemy.create_engine('mysql://' + connect['User'] + ':' + connect['Pass']+ '@' + connect['IP'] +'/'+ connect['DB'])#, pool_size=cores, max_overflow=10)
    
    
    
    
      
    
    parser = argparse.ArgumentParser(prog='MakeSQLTable', \
                                     usage='Make new Table in MySQL', description=
              'Make new Table in MySQL with or without data', \
              conflict_handler='error', add_help=True)
    parser.add_argument('--TableName', type = str, nargs = '+', \
                        help = 'Desired Table Name. Use "temp_" for temporary table')
    parser.add_argument('--FileIn', type = str, help = 'Path AND name of file with extension')
    parser.add_argument('--Type', type=str,default='csv', help = 'type "c" or "csv" for csv, "x" or "excel" or "xls" for excel')
    parser.add_argument('--InsertData', type = bool,default = True, help = 'If False, makes ' \
                        ' schema only and will not insert data. Default True')
    parser.add_argument('--PrimaryKey', type = str, nargs ='+', default = 'id', \
                        help = 'which column to use as primary key. default is AUTOINCREMENT or "id"')
    args = parser.parse_args()
    
    name = args.TableName
    
    #what about putting "PK_" in one of the column names??
    if 'id' in args.PrimaryKey:
       schema =  schema.update({'AUTOINCREMENT':'int'})
    
    if args.Type in ['x','X','excel','xls','xlsx']:
        Data = pd.read_excel(args.FileIn)
    else:
        Data = pd.read_csv(args.FileIn)
    
    for c in Data.columns:
        q = re.sub(r'\W+','_',c)
        r = re.sub(r'_(?=_)','',q)
        if r:
    #        print q.group(0)
            Data = Data.rename(columns={c:r})
            
    
    schema = Data[:1].to_dict(orient='list')
    
    tbl = mkschema(args.TableName, schema, args.PrimaryKey)
    
    WriteToInvertDB(args.TableName,tbl, Data[1:],args.InsertData,User,Pass, IP, DB)


if __name__ == "__main__":
    main()    
