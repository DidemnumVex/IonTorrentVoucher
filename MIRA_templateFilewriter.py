# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 17:09:40 2017

Make config templates for MIRA assemblies
for entire plate and well
This is slowly becoming library of repeatedly used functions
@author: tcampbell
"""


import os
import sys
import re
from FindFastaFiles import findmorphs as dblook


path = '/home/tcampbell/ENVTC/'

sys.path.append(path)

def writefile(fastqfile, directory, library, plate, well, bcname):
#    global plate
    cname = directory +  library + '/' + plate + '/'
#    coname = directory + out + library + '/' + plate + '/'
    templatefile = cname  +  plate +'_'+ well + "_" +  bcname + '_miratemp_3.txt'
    if os.path.exists(cname) == True and os.path.isfile(templatefile) == False:#make something that warns that file exists, ask to overwrite
#        print cname +  plate +'_'+ well + '_'+ bcname + '_miratemp.txt'
        with open(templatefile, 'w') as export:
#        num =range(len(files))
#        export.write('[')
#        for no in num:
                record = 'project = ' + plate + '_' + well+' \n\
job = genome, denovo, accurate \n\
\n\
readgroup = '+ plate + '_' + well + ' \n\
data='+ directory + fastqfile + ' \n\
technology = iontor \n\
\n\
parameters = COMMON_SETTINGS -GENERAL:number_of_threads = 1 \ \n\
                             -CO:mr=yes \ \n\
                             -ASSEMBLY:num_of_passes=3:spoiler_detection=on:use_emergency_search_stop=off\ \n\
                             -NW:check_duplicate_readnames=no \ \n\
             IONTOR_SETTINGS -ASSEMBLY:minimum_read_length=50 \ \n\
                             -ALIGN:min_relative_score=60 \ \n\
                             -CO:mgqrt=35 \ '
                export.write(str(record))
#            if len(files)-no >= 2:
#                export.write(',')
#        export.write('\n]')    
                export.close  
#         -DIR:tmp_redirected_to=/home/tcampbell/ENVTC/MIRA/temp \ \n\ #removed from common settings
    else:
        print "Mira Template Filepath Out " + cname + " does not exist."
        
def fastqfilefinder(directory):
#    subprocess.check_call(["ls"])
#    subprocess.check_output("exit 1", shell=True)
    os.system("'ls' "+ directory + " > " + directory + "fastqfilelist.txt")
    
    
def readfiles(fastqfilelist):
    with open(fastqfilelist, 'rb') as tsvin:
        tsvin = tsvin.csvread(tsvin, delimiter='\t')
#   
        for row in tsvin:
            well= str(row)
            plate = test
            bcname = 'Geller'
            if re.search(r'seqs_[0-9]{2}[A-H].fastq', well):
                p= re.search(r'seqs_([0-9]{2}[A-H]).fastq', well)                
                writefile(p.group(0), directory, plate, p.group(1), bcname)
                print "Match"
            else:
                print "NO MATCH"

def mysqlconnect(connectionpath): #uses the information in the connection file to open mysql connection
    connect = open(connectionpath, 'r')
#    print cnx.read()
    cnx = connect.read()
    cnx.strip('\n')
    return cnx



def main():
    
    ###=============THIS WORKS FOR READING FASTQ FILES TO MAKE MIRA TEMPLATE================################   
    
    directory = "/path/to/fastq/files/"
    fastqfilefinder(directory) 
    readfiles(directory + "fastqfilelist.txt") 
    ###=============THIS WORKS FOR READING FASTQ FILES TO MAKE MIRA TEMPLATE (END)================################  

    connect = mysqlconnect('/path/to/connectionfile.tsv')

    cnx =  connect #makes variable declaration a string
    exec(cnx) #executes declaration of variable using string from previous step
    cnx = mysql.connector.connect(user=connect['User'], password= connect['Pass'], host= connect['IP'], database=connect['DB'])

    cursor = cnx.cursor()

    

    cursor.close()
    cnx.close()
    
    
if __name__ == "__main__":
    morphiles = main()    
    

