# -*- coding: utf-8 -*-
"""
Created on Sun Mar  5 10:31:05 2017

@author: wilso
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 15:57:31 2017

@author: wilso
"""

from mpi4py import MPI
import os
import re
import cProfile
import pstats
import sys
import datetime as dt


pr=cProfile.Profile()
pr.enable()
Input=dt.timedelta(0)
Output=dt.timedelta(0)
Proc=dt.timedelta(0)
Starttime=dt.datetime.now()
#OPEN REQUIRED FILES, OVERWRITE IF EXISTING

f=open(sys.argv[2],'w')
f.close
f=open(sys.argv[2],'a')

s=open(sys.argv[3],'w')
s.close
s=open(sys.argv[3],'a')

#INITIALIZE MPI
nprocs =(MPI.COMM_WORLD.Get_size())
myid=MPI.Comm.Get_rank(MPI.COMM_WORLD)
fh=MPI.File.Open(MPI.COMM_WORLD, sys.argv[1], MPI.MODE_RDONLY)
size=MPI.File.Get_size(fh)

#DETERMINE SEGMENTS
segsize = int(size/nprocs)
seg_start = segsize*myid
buf=bytearray(100)

if myid==nprocs-1:
    seg_end=size
else:
    seg_end=seg_start+segsize-1;
    
if myid !=0:
    fh.Seek(seg_start)
    fh.Iread(buf)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
    offset=len(str(rows[0]))
    seg_start+=offset+1
    


if myid !=(nprocs-1):    
    fh.Seek(seg_end)
    fh.Iread(buf)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
    offset=len(str(rows[0]))
    seg_end+=offset
splitend=dt.datetime.now()
#DETERMINE BLOCKS PER SEGMENT

print myid, 'Time to split file into segments', splitend-Starttime
blocksize = 10000000
block_starts=[seg_start]
block_start=seg_start+blocksize
while block_start<seg_end:
        buf=bytearray(100)
        fh.Seek(block_start)
        fh.Iread(buf)
        rows=buf.decode('utf-8')
        rows=rows.split('\n')
        offset=len(str(rows[0]))+1
        adjblock_start=block_start+offset
        block_starts.append(adjblock_start)
        block_start+=blocksize
block_starts.append(seg_end)    
blockendtime=dt.datetime.now()
print myid, 'Time to split segment into blocks', blockendtime-Starttime
Proc+=(blockendtime-Starttime)


i=0
#COMPILE REGULAR EXPRESSION
#THIS ONE EXPRESSION FILTERS OUT NEGATIVE VALUES, MALFORMED LINES, and EXTREME OUTLIERS 
date_exp = re.compile("[0-9]{8}:[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6},[^-0][0-9]{2,4}\.[0-9]{0,2},[^-0][0-9]*")
for block in block_starts [0:-1]:
    begblock=dt.datetime.now()
    buffersize=int(block_starts[(i+1)]-block-1)
    print(buffersize, myid)
    begreadtime=dt.datetime.now() 
    i=i+1
    print(myid,'reading block %d' %(i))      
    buf=bytearray(buffersize)
    fh.Open
    fh.Seek(int(block))
    fh.Read(buf)
    fh.Close
    endreadtime=dt.datetime.now()
    print myid,'Reading time', endreadtime-begreadtime
    Input+=(endreadtime-begreadtime)
    buf=buf.decode('utf-8')
    buf=buf.split('\n')
    
    #TEST EACH LINE
    noise=[]    
    signal=[]
    
    for index,row in enumerate(buf):         
             record = row             
             if date_exp.match(row):
                 signal.append(row)
             else:
                 noise.append(row)
             
                              
    print myid, 'Signal lenght: ', len(signal), 'Noise length',len(noise),'Percent lost', len(noise)/(len(noise)+len(signal))  
    #WRITE SIGNAL AND NOISE TO FILE
    signal=('%s' % os.linesep .join(map(str, signal))+os.linesep)
    noise=('%s' % os.linesep .join(map(str, noise))+os.linesep)
    endproctime=dt.datetime.now()
    print myid, 'Processing time', endproctime-endreadtime
    Proc+=(endproctime-endreadtime)
    n=bytearray(noise,encoding='utf-8')
    nh=MPI.File.Open(MPI.COMM_WORLD, sys.argv[2],MPI.MODE_WRONLY)
    #FILE POINTER SO NON BLOCKING WRITE CAN BE USED
    info=MPI.File.Get_size(nh)    
    nh.Seek_shared(info)
    nh.Iwrite_shared(n)
    nh.Close
    nwritetime=dt.datetime.now()
    print myid, 'To write Noise',nwritetime-endproctime
    Output+=(nwritetime-endproctime)
    signal=bytearray(signal,encoding='utf-8')
    sh=MPI.File.Open(MPI.COMM_WORLD, sys.argv[3],MPI.MODE_WRONLY)
    info=MPI.File.Get_size(sh)
    sh.Seek_shared(info)
    sh.Write_ordered(signal)
    endblock=dt.datetime.now()
    print myid, 'To write signal', endblock-nwritetime, 'Total Block Time', endblock-begblock
    Output+=(endblock-nwritetime)
f.close   
s.close
#PRINT PROFILER DATA
endtime=dt.datetime.now()
totaltime=endtime-Starttime
Total=MPI.COMM_WORLD.reduce(totaltime,MPI.MAX,root=0)  

if myid==0:
    print 'Total Time', Total
    print 'Input Time', Input
    print 'Output Time', Output
    print 'Processing Time', Proc
    pr.disable()
    pr.dump_stats('profile')
    ps=pstats.Stats('profile',stream=sys.stdout).sort_stats('cumulative')
    ps.print_stats()
