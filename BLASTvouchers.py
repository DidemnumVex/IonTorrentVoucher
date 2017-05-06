#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  9 23:13:07 2017
BLASTvouchers.py
Will BLAST contigs made by MIRA against most recent Invertebrate db.

Modification: 11Apr17, now requires csv file list of fasta files to BLAST (because BLAST mod requires fasta input)

@author: tcampbell
"""

import os
import signal
import pandas as pd
import re
import mysql.connector
import sqlalchemy
import numpy as np
from MIRA_templateFilewriter import mysqlconnect
from IonZipSplitv3 import chunkdf #need to make sure this gets updated with new versions.

import mysql
import multiprocessing
from Bio.Blast import NCBIXML
import argparse

def BLASTvouchers(df, directory, assemdir, outdir):#this doesn't seem to be working with the Bio version of BLAST. should try with os.system BLAST cmd. 
    x = 0
    x+=1
    outfile = re.search(r'.*\/([A-Za-z0-9]*\_[0-9]{2}[A-H])_d_results/.*', str(df['FileName']))
    if os.path.exists(directory + outdir) == False:
        os.system('mkdir ' + directory + outdir )
    outf = directory + outdir + str(outfile.group(1)) + '.xml'
    print outf
    os.system('/home/tcampbell/BLASTdb/ncbi-blast-2.6.0+/bin/blastn -query ' + str(df['FileName']) +  ' -db /home/tcampbell/BLASTdb/MLML_Coarb_nosemi_10Apr17.fa -evalue  0.01 -outfmt  5 -out ' + outf)
Return None
    
    

def applyblast(df):
    processid = os.getpid()
    print processid
    df[0].apply(BLASTvouchers, axis=1, args=(df[1], df[2],df[3] ))
    os.kill(processid, signal.SIGTERM)
    return None

def parsxml(filein):#Want to write own BLAST xml parser. this is ridiculously slow.
    result_handle = open(filein)
    blast_records = NCBIXML.parse(result_handle)
    assembly = 0
    for blast_record in blast_records:
        count = 0
        assembly += 1
        print "Assembly Sequence:", assembly
        for alignment in blast_record.alignments[:3]:
            for hsp in alignment.hsps:
                print('****Alignment****')
                print('sequence:', alignment.title)
                print('e value:', hsp.expect)
                print('length:', alignment.length)
                print('score:', hsp.score)
                print('alignment length:', hsp.align_length)
                print('identity:',hsp.identities)
                pctID = float(hsp.identities) / float(hsp.align_length) * 100
                print 'Percent ID:', pctID
                print('Query Start:', hsp.query_start)
                count +=1
                print "BLAST alignment Num", count
    return blast_records            



def main():
    #===============PARSE ARGS===========================
    parser = argparse.ArgumentParser(prog='IonZipSplitv3', usage='Insert Assembled Contigs into MySQL database', description=
             'Insert Assembled Sequences into DB', conflict_handler='error', add_help=True)
    parser.add_argument('--SequenceFiles', type = str, nargs = '+', help = 'File names of Sequencing Runs to assemble')
    parser.add_argument('--Out', type = str, help = 'Output Directory, e.g. if /home/user/Documents/out/ then out/')
    parser.add_argument('--DataDir', type=str, help = 'The directory where your data are found. e.g. /home/user/Documents/')
    parser.add_argument('--AssemblyDir', type = str, help = 'Name of the subdirectory with assembly folders')
    parser.add_argument('--Cores', type = int, default = 56, help = 'Number of processing cores to use. Default is 56.')
    args = parser.parse_args()
   
    
#===============SQL Connect====================================================  
    connect = mysqlconnect('/home/tcampbell/scripts/Testdict.txt') #gets the connection info from config file
    exec(connect) #executes declaration of variable using string from previous step
    cnx = mysql.connector.connect(user=connect['User'], password= connect['Pass'], host= connect['IP'], database=connect['DB'])
    engine = sqlalchemy.create_engine('mysql://' + connect['User'] + ':' + connect['Pass']+ '@' + connect['IP'] +'/'+ connect['DB']) # is used?
   
    cursor = cnx.cursor()
#======================Begin Main===================================== 
    fastas = pd.read_csv(args.DataDir + args.AssemblyDir + 'fastalist.csv', header = None)#Fastalist is made in AssembledSequenceProcess.py
    fastas.columns = ['FileName']
    chuckfas, Cores = chunkdf(fastas, args.Cores)
    d = [args.DataDir, args.AssemblyDir, args.Out]
        #comment for debug 5/4/17
    chuckfas = [i + d for i in chuckfas]
    pool = multiprocessing.Pool(processes=Cores)
    
    re = pool.map(applyblast, chuckfas)
#    re = pool.apply(assemble,args = (chunkeddf), axis=1)
    pool.close()   
    
    

####END Main Guts######    
    cursor.close()
    cnx.close()
    return  None
    
    
if __name__ == '__main__':
    main()
