# -*- coding: utf-8 -*-
"""
Created on Sun Mar 12 21:48:40 2017

@author: wilso
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Feb 19 12:27:23 2017

@author: wilso
"""
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 18 16:15:48 2017

@author: wilso
"""
#  IMPORT REQUIRED LIBRARIES
from scipy.stats import t as t
import datetime as dt
import math as mt
from mpi4py import MPI
import os
import cProfile
import pstats
import sys


Input=dt.timedelta(0)
Output=dt.timedelta(0)
Proc=dt.timedelta(0)
Starttime=dt.datetime.now()

pr=cProfile.Profile()
pr.enable()

# DEFINE REQUIRED FUNCTIONS
def date_parse(string):
    return dt.datetime.strptime(string,'%Y%m%d:%H:%M:%S.%f')



#INITIALIZE VARIABLES

n=0
logsum=0
logsq_sum=0
logcb_sum=0
logquad_sum=0

#INITIALIZE MPI
nprocs =(MPI.COMM_WORLD.Get_size())
myid=MPI.Comm.Get_rank(MPI.COMM_WORLD)
fh=MPI.File.Open(MPI.COMM_WORLD, sys.argv[1], MPI.MODE_RDONLY)
size=MPI.File.Get_size(fh)

#DETERMINE HOW TO SPLIT FILE OVER PROCESSES
segsize = int(size/nprocs)
seg_start = segsize*myid
buf=bytearray(100)

#           COMPUTING PROCESS BEGINNINGS AND ENDINGS

if myid==nprocs-1:
    seg_end=size
else:
    seg_end=seg_start+segsize-1;
    
if myid !=0:
    fh.Seek(seg_start)
    fh.Read(buf)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
    offset=len(str(rows[0]))
    seg_start+=offset+1
    


if myid !=(nprocs-1):    
    fh.Seek(seg_end)
    fh.Read(buf)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
    offset=len(str(rows[0]))
    seg_end+=offset
split=dt.datetime.now()
print myid, 'To split file into segments', split-Starttime
#BREAK EACH SEGMENT INTO BLOCKS WHICH EACH PROCESS WILL RUN THROUGH, BY A FIXED BLOCK SIZE

blocksize = 10000000  #DETERMINE BLOCK SIZE


block_starts=[seg_start]  #FIRST BLOCK BEGINS WHERE THE SEGMENT BEGINS

block_start=seg_start+blocksize  #SECOND BLOCK BEGINS ROUGHLY HERE
while block_start<seg_end:       #LOOP TO DETERMINE EACH BLOCKSTART IN THE SEGMENT, AND APPEND TO LIST
        buf=bytearray(100)
        fh.Seek(block_start)
        fh.Read(buf)
        rows=buf.decode('utf-8')
        rows=rows.split('\n')
        offset=len(str(rows[0]))+1
        adjblock_start=block_start+offset
        block_starts.append(adjblock_start)    #APPEND ADJUSTED BLOCK START TO LIST OF BLOCKSTARTS
        block_start+=blocksize
block_starts.append(seg_end)    
block=dt.datetime.now()
print myid, 'To split segment into blocks', block-split, 'Total number of blocks', len(block_starts)-1
Proc+=(block-split)

#READ IN EACH BLOCK

i=0

for block in block_starts [0:-1]:
    blocktime=dt.datetime.now()
    logret_list=[]                              #LOG RETURN LIST INITIALIZES AS EMPTY AT EACH BLOCK
    buffersize=int(block_starts[(i+1)]-block-1)
    buf=bytearray(buffersize)   
    i=i+1
    fh.Open
    fh.Seek(int(block))
    fh.Read(buf)
    fh.Close
    read=dt.datetime.now()
    print myid, 'To read block',i,read-blocktime
    Input+=(read-blocktime)
    buf=buf.decode('utf-8')
    buf=buf.split('\n')          
    buf=sorted(buf)                     #SORT LOG RETURNS, BY DATE
 
    prdate=dt.datetime.now()           #FIRST RETURN IN LIST IS ALWAYS NULL, BUT NEED DATETIME VALUE FOR COMPARISON
    prprice=0.0                        #CREATE A FLOAT VARIABLE TO REPRESENT PRIOR PRICE
    for row in buf:      
        cols=row.split(',')
        cols[0]=date_parse(cols[0])
        cols[1]=float(cols[1])        
        timediff=1000000*(cols[0]-prdate).total_seconds()   #COMPUTE THE TIME DIFFERENCE IN SORTED TIME SERIES
        try:                                                #TRY ALLOWS IT TO FILTER OUT DUPLICATES
            logret=mt.log10(cols[1]/prprice)/timediff       #COMPUTES LOG RETURN AND ADJUST FOR TIME
        except:
            prdate=cols[0]                                  #CURRENT DATETIME BECOMES PREVIOUS FOR NEXT ITERATION
            prprice=cols[1]                                 #CURRENT PRICE BECOMES PREVIOUS FOR NEXT ITERATION
            continue                                   
        logret_list.append(logret) 
        prdate=cols[0]
        prprice=cols[1]
    logret_list=logret_list[1:]                             #REMOVE FIRST ENTRY FROM LIST OF RETURNS
    log_sq=[x**2 for x in logret_list]                      #CREATE LIST OF LOG RETURNS TO VARYING EXPONENTS FOR FUTUER CALC
    log_cb=[x**3 for x in logret_list]
    log_quad=[x**4 for x in logret_list]
    n+=len(logret_list)                                     #COMPUTE SUM OF NECCESARY COMPUTATION
    logsum+=sum(logret_list)    
    logsq_sum+=sum(log_sq)
    logcb_sum+=sum(log_cb)
    logquad_sum+=sum(log_quad)
    endblock=dt.datetime.now()
    print myid, 'To process block',endblock-read
    Proc+=(endblock-read)
 
    
           

cumstat=[]                                                  #THESE VARIABLES ACCUMULATED THE SUM ACROSS EACH PROCESSES BLOCKS
stat=(n,logsum,logsq_sum,logcb_sum,logquad_sum)    
for x in stat:
    stat=MPI.COMM_WORLD.reduce(x,MPI.SUM,root=0)   
    cumstat.append(stat)                                    #ACCUMULATE AND REDUCE SUMS INTO SINGLE LIST ON PROCESS 0
pass_stat=dt.datetime.now()
print myid, 'To pass statistics', pass_stat-endblock
if myid==0:

    n = cumstat[0]
    logsums=cumstat[1]
    logavg=logsums/n
    logsq_sums=cumstat[2]
    logcb_sums=cumstat[3]
    logquad_sums=cumstat[4]                     #PROCESS 0 COMPUTES TOTAL SUM ACROSS ALL PROCESSES AND BLOCKS
    
    secondmoment=logsq_sums-2*logavg*logsums+n*logavg**2     #COMPUTE SPECIFIC MOMENTS 
    thirdmoment=logcb_sums-3*logavg*logsq_sums+3*logavg**2*logsums-logavg**3*n
    fourthmoment=logquad_sums-4*logavg*logcb_sums+6*logavg**2*logsq_sums-4*logavg**3*logsums+n*logavg**4
    
    
    skew= n*(n-1)**.5/(n-2) * thirdmoment/(secondmoment**1.5)         #CALCULATE SKEW
    kurt=n*fourthmoment*(n+1)*(n-1)/((secondmoment**2)*(n-2)*(n-3))   #CALULATE KURTOSIS
        
        
    SES=(6*n*(n-1)/((n-2)*(n+1)*(n+3)))**.5                    #CALULATE STANDARD ERROR OF SKEW AND KURTOSIS
    SEK=2*SES*((n**2-1)/((n-3)*(n-5)))**.5
    
        
    skew_t = abs(skew/SES)                                    #COMPUTE T-STATS
    kurt_t = abs((kurt-3)/SEK)
        
                                                            #CONDUCT T-TEST
    skew_p=1-t.cdf(skew_t,n-1)
    kurt_p=1-t.cdf(kurt_t,n-1)
    out=dt.datetime.now()
    Proc+=(out-endblock)    
    if skew_p <= .05 or kurt_p <= .05:
            print 'The data set does not come from a normal distribution. Its skewness is %.2f and its kurtosis is %.2f' %(skew,kurt)
            print 'The average log return is', logavg
            print 'There were %s observations' %(n)
            print 'The standard deviation is', secondmoment/(n-1)
    else:
            print 'The data may come from a normal distribution. Its skewness is %.2f and its kurtosis is %.2f' %(skew,kurt)
            print 'The average log return is', logavg
            print 'There were %s observations' %(n)
            print 'The standard deviation is', secondmoment/(n-1)
    
    end=dt.datetime.now()
    print myid, 'To compute final statistics', end-pass_stat, 'Total time',end-Starttime 
print myid, 'Total input time', Input
print myid, 'Total output time', Output
print myid,'Total process time',Proc
if myid==0:
    pr.disable()
    pr.dump_stats('profile')
    ps=pstats.Stats('profile',stream=sys.stdout).sort_stats('cumulative')
    ps.print_stats()
    MPI.Finalize()
