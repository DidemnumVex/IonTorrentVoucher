# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 14:07:00 2017

IonZipSplit.py

Will sort, rename, unzip, and split fastq files for downstream processing
V2 multiprocessing functional, leaves child processes open
V2_2 Now closes child processes without crashing
Not finished, need to add sys.args and if exists to not overwrite files
Dependencies: MIRA_templateFilewriter and MySQL connection text file
@author: tcampbell
"""

import os
import signal
import pandas as pd
import re
import mysql.connector
import sqlalchemy
from MIRA_templateFilewriter import mysqlconnect, writefile
import mysql
import multiprocessing
import argparse




def systemsplit(df, directory):
    if os.path.exists(directory + str(df['IonLibraryName']) + '/' + str(df['ExtractionPlate']) ) == False:
#    print 'Library', df['IonLibraryName'], 'plate', df['ExtractionPlate']+'.fastq'
        os.system('convert_fastaqual_fastq.py -c fastq_to_fastaqual -f '\
                  + directory  + str(df['IonLibraryName']) + '/'+ str(df['ExtractionPlate']) + '.fastq'\
                  + ' -o ' + directory  + str(df['IonLibraryName']) + '/' + str(df['ExtractionPlate']))


    
def get_wells(directory, out, cnx):#writes wellids to csv because it's faster than sql query when re-running
#    directory = common[0]
    wellfile = directory  + "WellIDS.csv"
    if os.path.isfile(wellfile):
        wellfile = wellfile
    else:
        query = "select * from tblListWells"
        df = pd.read_sql(query, con=cnx)
        df = df[1 : ]
        df.to_csv(directory  + "WellIDs.csv")
    return None

    
 
def split_libs(df,directory, out, func):###This was performed with map.  Input a df fed in as an iterable list. Worked great
    
    LG = "740"
    LH = "350"
    
    geller = ["GellerWellBarcodeMap.txt", 8, LG]
    heller = ["HellerWellBarcodeMap.txt", 10, LH]
    
    Maps = [geller, heller]
    wells = pd.read_csv(directory  + 'WellIDs.csv')
#    print wells
#    Maps = [geller]
    
    for f in Maps:
        fsub = re.match(r'([GH]eller)',str(f[0]))
        barlen = str(f[1])
#        print barlen, type(barlen)
        bcname = str(fsub.group(1))
#        print fsub.group(1)#added out
        platedir = directory  + str(df['IonLibraryName']) + "/" + str(df['ExtractionPlate'])\
        + "/" + str(df['ExtractionPlate'])
        fnadir = platedir + "_split" + bcname + "/"
        if func == 'split':
            x ='split_libraries.py -m ' + directory  + f[0] + \
            " -f " + platedir + '.fna -q ' + platedir + ".qual -l 75 -b " + barlen + " -o " + platedir + "_split" + bcname + " -w 50 --record_qual_scores -M 7 -s 19 --disable_primers"  
            os.system(x)
        elif func == 'fnamer' and os.path.isfile(fnadir + '*.fastq') == False:
            os.system("convert_fastaqual_fastq.py -f " + fnadir + "seqs.fna -q " + fnadir + "seqs_filtered.qual -o " + fnadir + " -m")
        elif func == 'mira':
            for w in wells['Well']:
                fastqfile = str(df['IonLibraryName']) + "/" + str(df['ExtractionPlate']) + "/" + str(df['ExtractionPlate']) + "_split" + bcname + "/seqs_" + str(w) + ".fastq"
                if os.path.isfile(fastqfile) == False:#writefile is the mira templatefile writer
                    writefile(fastqfile , directory,  str(df['IonLibraryName']), str(df['ExtractionPlate']), str(w), bcname)
#            print "/home/tcampbell/ENVTC/MIRA/mira_4.0.2_linux-gnu_x86_64_static/bin/mira" + \
            
        elif func == 'concat':
            if bcname == "Heller" and str(df['IonLibraryName']) == "CalNISii_iCOI_Library_Q":#should add heller column or input
                for w in wells['Well']:
                    fqfil = fnadir + "seqs_" + str(w)
                    os.system("cat " + fqfil + '.FW.fastq ' + fqfil + '.RV.fastq > ' + fqfil + '.fastq')
        else:
            pass

            
def assemble(df, directory, out):#might need global dir here because of num inputs allowed. also check filewriter inputs
#    global directory
    bcs = ["Geller", "Heller"]
    wells =pd.read_csv(directory  + "WellIDs.csv")
    dirs2mir = []
    lpop = range(len(df))
#    for i in lpop:
    for bcname in bcs:
        for w in wells['Well']:
#            temp = []
            rop = str(df['IonLibraryName']) + '/' + str(df['ExtractionPlate']) + '/' + str(df['ExtractionPlate']) + "_split" + str(bcname) + "/seqs_" + str(w) + ".fastq" 
            if os.path.isfile(directory + out + rop):
                os.chdir(directory + out + 'MIRA_assemblies')
#                os.system('pwd')
                os.system("/home/tcampbell/ENVTC/MIRA/mira_4.0.2_linux-gnu_x86_64_static/bin/mira " + \
                directory + out + str(df['IonLibraryName']) +'/'+ str(df['ExtractionPlate']) + '/'+ str(df['ExtractionPlate']) + '_'+ str(w) + '_' + bcname + '_miratemp_3.txt')
    print 'NEXT'        
    return None            

 

def applied(df):#args are "split" or "fnamer"#Need to either give this arguments or split it in two, use with map?
#    arg = "split_libs"    
#    if arg == "split_libs":
#    processid = os.getpid()#comment for now? 5/2/17
#    print processid
    if df[4] == 'convert':
        df[0].apply(systemsplit, args=(df[1],), axis = 1)
    else:
        df[0].apply(split_libs, args=(df[1],df[2], df[4],), axis = 1)  #works, but one thread at a time, could make the function an input
    


def appliedAssemble(chunkeddf):#change input to list? chunkeddf is an iteration
    processid = os.getpid()
    print processid
    chunkeddf[0].apply(assemble, axis=1, args = (chunkeddf[1], chunkeddf[2], ))#can I pass the PID here to child and not kill if PID == parent PID?
    if chunkeddf[3] != processid:
        os.kill(processid, signal.SIGTERM)


def chunkdf(df, Cores):#chunks the dataframe into smaller pieces. Should change process no 56 into an argument --did, cores
#    PID = os.getpid()
#    print 'df =', len(df)
####Begin comment troublshooting
    if len(df) <= Cores:
        Cores = len(df)
    else:
        Cores = Cores
    dvdf = len(df)//Cores
    moddf = len(df)% Cores
    if moddf > 0:
        LDF = []
        for i in range(0,(moddf*(dvdf+1)),dvdf+1):
            LDF.append([df[i:i+dvdf+1]])
        for i in range((moddf*(dvdf+1))+1,len(df), dvdf):
            LDF.append([df[i:i+dvdf]])
    elif moddf <= 0:
        LDF = []
        for i in range(0,len(df), dvdf):#can't step by zero? len = 32
            LDF.append([df[i:i+dvdf]])    
    return LDF, Cores


class SequenceMatrix:#Wrote as class because I thought about sending  more instaces to be manipulated in program.  Instead I pass a block of data through.
    def __init__(self,df):
        self.df = df
        self.uniqfiles = df.drop_duplicates(subset= 'FileName') #should I do this or mask?
#        self.dir = directory
        
#    def listplates(self, df, plate):
#        df.loc[df['IonPlateName'] == plate]
    def splitnrename(self, directory): #We already did this for the OLD DATA!!! Need to incorporate check of completion
        p=self.uniqfiles.index
#        self.dir = directory
        print p
        for items in p:
            sub = directory + str(self.uniqfiles['IonLibraryName'][items])#want this as subdir like others
            if os.path.exists(sub):
                print sub + 'already exists'
            else:
#                os.system("mkdir " + sub)#Works, but directories are already made
                filefolder = re.search(r'^([A-Za-z0-9\.\@\-\_]*)(\.zip)$', str(self.uniqfiles['FileName'][items]))
                print "Dir", filefolder.group(1), "Extension", filefolder.group(2)
                os.system("unzip " + directory +  str(self.uniqfiles['FileName'][items]) + " -d " + directory  + " && mv " + directory  + str(filefolder.group(1)) + " " + directory  + str(self.uniqfiles['IonLibraryName'][items]))
#        print "unzip " + directory + str(self.uniqfiles['FileName'][items]) + " -d " + directory + " && mv " + directory + str(filefolder.group(1)) + " " + directory + str(self.uniqfiles['IonLibraryName'][items])
#        os.system(")
            
    def BC2plate(self, directory, out, cnx):
        df5 = self.df
        pds = df5['IonPlateName'].str.extract(r'^([A-Za-z]*\_*[0-9]{2}[0-9]*)\_[COI]*[28S]*[CO3]*\_*[BC]*', expand =True) #removes the CalNISxx or JTMD_xx from CalNISxx_locus_BC
#        print "pds[0]",pds[0]
        df5.loc[:,'ExtractionPlate'] = pds[0]
#        print "df5", df5
        self.nonans = df5['ExtractionPlate'].fillna(df5['IonPlateName'], axis=0, inplace=True) #Returns a series
#       
        for item in df5.index: #should add line about checking for df5['ExtractionPlate'][item].fastq
            if os.path.isfile(directory + df5['IonLibraryName'][item] + '/' + df5['ExtractionPlate'][item] + '.fastq') == False:
                os.system('mv '  + directory + str(df5['IonLibraryName'][item]) + "/*" + str(df5['PlateFWDBarcode'][item]) + '*.fastq ' + directory + str(df5['IonLibraryName'][item]) + '/' + str(df5['ExtractionPlate'][item]) + '.fastq')        
        os.system("'ls' " + directory + "*/IonXpress_*.fastq -l >> " + directory + out + "renameplates.log")   # appends all the plates that weren't renamed properly, probably not in query   
        return self.nonans         

########THIS FUNCTION SHOULD BE ITS OWN  PROGRAM. Pull all this garbage out and stick it into main().         
    def qiime_split_libraries(self, directory, out, Cores):#See notebook. Must split files then, then recombine, split files by sample ID, concatenate
        #This should really be its own program.  Much of the stuff up until now is unneccessary if the splitting has been done.
        self.uniqplate = self.df.drop_duplicates(subset= 'ExtractionPlate') #Returns unique items
        self.smallplate = self.uniqplate[100: ]#subset for testing code
#        
        
#       
        q, Cores = chunkdf(self.uniqplate, Cores) #this chunks the data. Might pull this out of this function and put into main()
#        
        pool = multiprocessing.Pool(processes=Cores)

###########This stuff is all to write MIRA template FILES######################
        args = [q,"print"]
#        global results
#        results = []
        PID = os.getpid()
        d = [directory, out, PID]
        qi = [i + d + ['convert'] for i in q] #For systemsplit.  Goes SOOOOO FAST compared to single thread!!!
        re = pool.map(applied, qi)
        qi = [i + d + ['split'] for i in q]
        re = pool.map(applied, qi)   #this worked beautifully, print argument used to test
        qi = [i +d + ['fnamer'] for i in q]
        re = pool.map(applied,qi)
       
        #comment for debug
        qi = [i + d + ['concat'] for i in q]
        re = pool.map(applied, qi)
        qi = [i + d + ['mira'] for i in q]
        re = pool.map(applied, qi)
#        results.append(re)
        pool.close()#Is it closing them before they finish?
        return q, self.uniqplate




def Assemblehandler(chunkeddf, Cores): #not totally necessary as own function, but written already
    pool = multiprocessing.Pool(processes=Cores)
    re = pool.map(appliedAssemble, chunkeddf) #does this pass a parent PID? Can I pass a tuple with PID?
    pool.close()
    return None        
        


def rennamezips(filename, directory, out, cnx):#renames the fastq zip files to have plate info
    query3 = ("select tblPGMRuns.FileName, tblPGMPlates.IonLibraryName, tblPGMPlates.PlateFWDBarcode, tblPGMPlates.IonPlateName from tblPGMRuns \
join tblPGMPlates on tblPGMRuns.IonLibraryName = tblPGMPlates.IonLibraryName where tblPGMRuns.FileName like '" + str(filename[0])+"'")    

    df3 = pd.read_sql(query3, con=cnx)#Uses SQLalchemy for connection, other engine deprecated
    df3.to_csv(directory + out +'LibraryLog.csv')#add analysis name to output. put in output dir

    S = SequenceMatrix(df3)
#====================SHOULD BE A SEPARATE PROGRAM?==========================================================
    S.splitnrename(directory) #This actually splits the fasta files. Not necessary for old data. incorporate check like if exists..
#==============================================================================

    R = S.BC2plate(directory, out, cnx)
    return df3, S#, R
    
 
def main():
   
    Database = 'InvertData_Convert-26Jun15'
    parser = argparse.ArgumentParser(prog='IonZipSplitv2_2', usage='Split, demultiplex, and assemble Ion Torrent Files', description=
             'Query Data for Assembly', conflict_handler='error', add_help=True)

    parser.add_argument('--SequenceFiles', type = str, nargs = '+', help = 'File names of Sequencing Runs to assemble')
    parser.add_argument('--Out', type = str, help = 'Output Directory, e.g. if /home/user/Documents/out/ then out/')
    parser.add_argument('--DataDir', type=str, help = 'The directory where your data are found. e.g. /home/user/Documents/')
    parser.add_argument('--Cores', type = int, default = 56, help = 'Number of processing cores to use. Default is 56.')
    args = parser.parse_args()
    
    connect = mysqlconnect('/home/tcampbell/scripts/Testdict.txt') #gets the connection info from config file
    

    exec(connect) #executes declaration of variable using string from previous step
    cnx = mysql.connector.connect(user=connect['User'], password= connect['Pass'], host= connect['IP'], database=connect['DB'])
    engine = sqlalchemy.create_engine('mysql://' + connect['User'] + ':' + connect['Pass']+ '@' + connect['IP'] +'/'+ connect['DB'])#I don't like this.
    cursor = cnx.cursor()
      
    directory = args.DataDir #"/raid1/IonTorrentRuns/CalNISTorrentData/CalNISCOI_TorrentData/"
#    listname = "ziplist.txt" #add line for if exists...or overwrite?
#    common = {'directory': args.DataDir, 'out':args.Out, 'cnx': cnx,'engine': engine, 'cores': args.Cores}
    well = get_wells(directory, args.Out, cnx)
    if os.path.exists(directory + args.Out) == False:
        os.system('mkdir ' + directory +args.Out )

    df3, S = rennamezips( args.SequenceFiles, args.DataDir, args.Out, engine)#temp comment 5/2/17
    Q, X =S.qiime_split_libraries(directory,args.Out, args.Cores) #temp stop 4/5/17
    PID = os.getpid()
    d = [args.DataDir, args.Out, PID]
    Qi = [i + d for i in Q]
    V = Assemblehandler(Qi, args.Cores)#changed from chunkeddf (Q) to unique dataframe (X) #comment debug 5/2/17

    cursor.close()
    cnx.close()

    
    
    return None
    
if __name__ == "__main__":
    main()    
