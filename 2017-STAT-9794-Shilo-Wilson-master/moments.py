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

from scipy import stats
import scipy
from scipy.stats import t as t
import datetime as dt
import math as mt
from mpi4py import MPI
import os
import cProfile
import pstats
import sys

#pr=cProfile.Profile()
#pr.enable()

def date_parse(string):
    return dt.datetime.strptime(string,'%Y%m%d:%H:%M:%S.%f')



f=open('C:/Users/wilso/Programming_analytics/logret.txt','w')
f.close
s=open('C:/Users/wilso/Programming_analytics/stat.txt','w')
s.close
z=open('C:/Users/wilso/Programming_analytics/finalstat.txt','w')
z.close
n=0
logsum=0
logsq_sum=0
logcb_sum=0
logquad_sum=0
nprocs =(MPI.COMM_WORLD.Get_size())
myid=MPI.Comm.Get_rank(MPI.COMM_WORLD)
fh=MPI.File.Open(MPI.COMM_WORLD, 'C:/Users/wilso/Programming_analytics/signal.txt', MPI.MODE_RDONLY)
size=MPI.File.Get_size(fh)

segsize = int(size/nprocs)
seg_start = segsize*myid
buf=bytearray(100)

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



blocksize = 10000000


block_starts=[seg_start]

block_start=seg_start+blocksize
while block_start<seg_end:
        buf=bytearray(100)
        fh.Seek(block_start)
        fh.Read(buf)
        rows=buf.decode('utf-8')
        rows=rows.split('\n')
        offset=len(str(rows[0]))+1
        adjblock_start=block_start+offset
        block_starts.append(adjblock_start)
        block_start+=blocksize
block_starts.append(seg_end)    





i=0

for block in block_starts [0:5]:
    
    logret_list=[]
    buffersize=int(block_starts[(i+1)]-block-1)
    buf=bytearray(buffersize)   
    i=i+1
    fh.Open
    fh.Seek(int(block))
    fh.Read(buf)
    fh.Close
    
    buf=buf.decode('utf-8')
    buf=buf.split('\n')    
      
    
   
    buf=sorted(buf)
 

    prdate=dt.datetime.now()
    prprice=0.0    
    for row in buf:      
        cols=row.split(',')
        cols[0]=date_parse(cols[0])
        cols[1]=float(cols[1])        
        timediff=1000000*(cols[0]-prdate).total_seconds()
        try:
            logret=mt.log10(cols[1]/prprice)/timediff    
        except:
            prdate=cols[0]
            prprice=cols[1]
            continue
        logret_list.append(logret) 
        prdate=cols[0]
        prprice=cols[1]
    logret_list=logret_list[1:]
    log_sq=[x**2 for x in logret_list]
    log_cb=[x**3 for x in logret_list]
    log_quad=[x**4 for x in logret_list]
    n+=len(logret_list)
    logsum+=sum(logret_list)    
    logsq_sum+=sum(log_sq)
    logcb_sum+=sum(log_cb)
    logquad_sum+=sum(log_quad)
    print(myid, 'reading',i, flush=True)
  
 
    
           

cumstat=[]   
stat=(n,logsum,logsq_sum,logcb_sum,logquad_sum)    
for x in stat:
    stat=MPI.COMM_WORLD.reduce(x,MPI.SUM,root=0)   
    cumstat.append(stat)


if myid==0:
    n = cumstat[0]
    logsums=cumstat[1]
    logavg=logsums/n
    logsq_sums=cumstat[2]
    logcb_sums=cumstat[3]
    logquad_sums=cumstat[4]
    
    secondmoment=logsq_sums-2*logavg*logsums+n*logavg**2
    thirdmoment=logcb_sums-3*logavg*logsq_sums+3*logavg**2*logsums-logavg**3*n
    fourthmoment=logquad_sums-4*logavg*logcb_sums+6*logavg**2*logsq_sums-4*logavg**3*logsums+n*logavg**4
    
    print('third moment',thirdmoment)
    print('fourth moment',fourthmoment)
    print('second moment,', secondmoment)
    print(n, logavg)
    skew= n*(n-1)**.5/(n-2) * thirdmoment/(secondmoment**1.5)
    kurt=n*fourthmoment*(n+1)*(n-1)/((secondmoment**2)*(n-2)*(n-3))
        
        
    SES=(6*n*(n-1)/((n-2)*(n+1)*(n+3)))**.5
    SEK=2*SES*((n**2-1)/((n-3)*(n-5)))**.5
    
        
    skew_t = abs(skew/SES)
    kurt_t = abs((kurt-3)/SEK)
        
        
    skew_p=1-t.cdf(skew_t,n-1)
    kurt_p=1-t.cdf(kurt_t,n-1)
        
    if skew_p <= .05 or kurt_p <= .05:
            print('The data set does not come from a normal distribution. Its skewness is %.2f and its kurtosis is %.2f' %(skew,kurt))
MPI.Finalize()   
"""
pr.disable()
pr.dump_stats('profile')
ps=pstats.Stats('profile',stream=sys.stdout).sort_stats('cumulative')
ps.print_stats()"""