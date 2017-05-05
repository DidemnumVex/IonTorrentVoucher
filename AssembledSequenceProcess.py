#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 09:32:34 2017
This script is intended to import the fasta files into a SQL database for BLAST
4/9/17--wrote all fasta sequences to SQL that were extracted from MIRA.
but still need to run the bash .sh scripts to pull the remaining.  

##THIS IS VERY SLOW. SHOULD INCORPORATE Multiprocessing##
Not so slow for one sequencing run.  Very slow for 6 runs at once.
@author: tcampbell
"""


import os
from Bio.SeqIO.FastaIO import SimpleFastaParser
import pandas as pd
import re
import mysql.connector
import sqlalchemy
import numpy as np
from MIRA_templateFilewriter import mysqlconnect
import mysql
import argparse



def filefinder(directory, assemdir, listname): #finds assembly files
    os.system("'ls' "+ directory + assemdir + " > " + directory + listname)

def readfiles(fastqfilelist):#reads assembly files
    with open(fastqfilelist, 'r') as tsvin:
        df = pd.read_csv(fastqfilelist, delimiter = '\n', header = None, names=["SubDir"])
        return df

def getplatewell(assemlisdf):#gets plate and well info from dir
    assemlisdf["Plate"] = np.nan
    assemlisdf["Well"] = np.nan
    ID = re.search(r'^([A-Za-z0-9\.\@\-\_]*)\_([0-9]{2}[A-H])\_assembly$',\
                           str(assemlisdf['SubDir']))
    if type(ID) != type(re.search('xxx', 'ppp')):#false doesn't work here. NoneType.
        assemlisdf["Plate"], assemlisdf["Well"] = ID.group(1), ID.group(2)
    return assemlisdf
    
def getvialID(infdf,cnx):
    infdf.to_sql('temppdDF', cnx, if_exists='replace')
    qry = ('Select temppdDF.Plate, temppdDF.Well, tblExtractions.VialID \
           from tblExtractions join \
           temppdDF on temppdDF.Plate = tblExtractions.ExtractionPlate \
           and temppdDF.Well = tblExtractions.ExtractionWell')
    vialids = pd.read_sql(qry, con=cnx)
    return vialids

def missingvials(df1,dfmiss,datadir, cnx):
    df1.to_sql('temppdDF',cnx, if_exists='replace')
    dfmiss.to_sql('temppdDF2',cnx,if_exists='replace')
    qry = ('select temppdDF.Plate,temppdDF.Well from temppdDF \
           left join temppdDF2 on temppdDF.Plate = temppdDF2.Plate and \
           temppdDF.Well = temppdDF2.Well where \
           temppdDF2.Plate is null and \
           temppdDF2.Well is null')
    missing = pd.read_sql(qry, con=cnx)
    missing.to_csv(datadir + 'missingvials.csv')
    return missing

def importfasta(assemdir, idsdf,cnx):#imports fasta files as list
    seqlist = []
    cols=['Plate','Well','VialID','SeqID','Sequence']
    seqdf = pd.DataFrame([[0,0,0,0,0],], columns = cols ) 
    j=0
    with open(assemdir + 'fastalist.csv','wb') as file:
        for i in idsdf.index:#Can also use this code to export list of fasta files
            subid = str(idsdf['Plate'][i]) + '_' + str(idsdf['Well'][i]) 
            subdirad = subid + '_assembly/' + subid + "_d_results/"
            fileid = subid + '_out.unpadded.fasta'
                if os.path.isfile(assemdir + subdirad + fileid):
                file.write(assemdir + subdirad + fileid + '\n')
                with open(assemdir + subdirad + fileid) as in_handle:
                    for title, seq in SimpleFastaParser(in_handle):
                        temp = pd.DataFrame([[idsdf['Plate'][i],idsdf['Well'][i], idsdf['VialID'][i], title, seq], ], columns = cols)
                        seqdf = seqdf.append(temp, ignore_index=True)
                    j+=1
    file.close                
    return seqdf
    
def WriteToInvertDB(seqdf,cnx):#writes sequence records to InvertDB
    sub = seqdf.drop_duplicates(subset= 'SeqID')
    sub.to_sql('VoucherSequences',cnx,if_exists='append',index=False, index_label='SeqID')#, dtype= {'Plate': 'varchar(50)', 'Well': 'varchar(50)', 'VialID': 'varchar(50)', 'SeqID' : 'varchar(50)', 'Sequence': 'longtext' })



def main():
#============PARSE ARGS================================================
    parser = argparse.ArgumentParser(prog='IonZipSplitv3', usage='Insert Assembled Contigs into MySQL database', description=
             'Insert Assembled Sequences into DB', conflict_handler='error', add_help=True)
    parser.add_argument('--SequenceFiles', type = str, nargs = '+', help = 'File names of Sequencing Runs to assemble')
    parser.add_argument('--Out', type = str, help = 'Output Directory, e.g. if /home/user/Documents/out/ then out/')
    parser.add_argument('--DataDir', type=str, help = 'The directory where your data are found. e.g. /home/user/Documents/')
    parser.add_argument('--AssemblyDir', type = str, help = 'Name of the subdirectory with assembly folders')
    args = parser.parse_args()


    #====================SQL Connection Open====================
    connect = mysqlconnect('/home/tcampbell/scripts/Testdict.txt') #gets the connection info from config file
    exec(connect) #executes declaration of variable using string from previous step
    cnx = mysql.connector.connect(user=connect['User'], password= connect['Pass'], host= connect['IP'], database=connect['DB'])
    engine = sqlalchemy.create_engine('mysql://' + connect['User'] + ':' + connect['Pass']+ '@' + connect['IP'] +'/'+ connect['DB']) # is used?
    cursor = cnx.cursor()
    #======================Code in here.====================
    listname = "tempAssemblyDirList"
    filefinder(args.DataDir,args.AssemblyDir,listname)
   
   
    assemlisdf = readfiles(args.DataDir + listname)#read list of directories with assemblies
    infdf = assemlisdf.apply(getplatewell, axis=1)#get plate well
    vialids = getvialID(infdf, engine)#dataframe of vials
    missing = missingvials(infdf, vialids,args.DataDir, engine)
    
    seqdf = importfasta(args.DataDir + args.AssemblyDir, vialids,engine)
    WriteToInvertDB(seqdf,engine)#writes to mysql
    
    #===================Ready to close cnx.=================
    cursor.close()
    cnx.close()
    return None
    
    
if __name__ == '__main__':
    main()
