# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 15:57:31 2017

@author: wilso
"""

from mpi4py import MPI

#import bisect

f=open('C:/Users/wilso/Programming_analytics/noise.txt','w')
f.close
f=open('C:/Users/wilso/Programming_analytics/signal.txt','w')
f.close

nprocs =(MPI.COMM_WORLD.Get_size())
myid=MPI.Comm.Get_rank(MPI.COMM_WORLD)
fh=MPI.File.Open(MPI.COMM_WORLD, 'C:/Users/wilso/Programming_analytics/data-big.txt', MPI.MODE_RDONLY)
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
    fh.Iread(buf)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
    offset=len(str(rows[0]))
    seg_start+=offset
    


if myid !=(nprocs-1):    
    fh.Seek(seg_end)
    fh.Iread(buf)
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
        fh.Iread(buf)
        rows=buf.decode('utf-8')
        rows=rows.split('\n')
        offset=len(str(rows[0]))
        adjblock_start=block_start+offset
        block_starts.append(adjblock_start)
        block_start+=blocksize

block_starts.append(seg_end)    
if myid==0:
    print(len(block_starts))


import datetime as dt

def date_parse(string):
        return dt.datetime.strptime(string,'%Y%m%d:%H:%M:%S.%f')
noise=[]
i=0
for block in block_starts [0:-1]:
    buffersize=int(block_starts[(i+1)]-block)
    buf=bytearray(buffersize)   
    
    packages=[]
    fh.Open
    fh.Seek(int(block))
    fh.Iread(buf)
    fh.Close
    print(myid,'reading block %d' %(i), flush=True)
    rows=buf.decode('utf-8')
    rows=rows.split('\n')
   
    noise=[]
    signaldates=[]
    a=0
    #print(myid, rows[0])
    while a<1000:
        signaldates.append(date_parse('19000101:12:12:12.000000'))
        a=a+1
       

    for row in rows:         
            
            cols=row.split(",")        
            try:
                cols[0]=date_parse(cols[0])
                cols[1]=float(cols[1])
                cols[2]=int(cols[2])            
            except:
                cols[0]=cols[0]
                noise.append(cols[0])
                
                continue        
            if cols[0] in signaldates:
                noise.append(cols[0].strftime('%Y%m%d:%H:%M:%S.%f'))
                
                continue
            if cols[2] <= 0:
                noise.append(cols[0].strftime('%Y%m%d:%H:%M:%S.%f'))
                
                continue
            if cols[1]<= 0:
                noise.append(cols[0].strftime('%Y%m%d:%H:%M:%S.%f'))
               
                continue            
            signaldates.append(cols[0])
    #        insertion=bisect.bisect_left(signaldates,cols[0])
     #       signaldates.insert(insertion,cols[0])
            del signaldates[0]
       #     cols[0]=cols[0].strftime('%Y%m%d:%H:%M:%S.%f')
         #   insertion=len(signal)-(len(signaldates)-insertion)
         #   s=cols[0]+','+str(cols[1])+','+str(cols[2])        
         #   signal.insert(insertion,s) 
    #signal=('%s' % '\n '.join(map(str, signal)))
    #s=bytearray(signal,encoding='utf-8')
    i=i+1
    noise=signal=('%s' % '\n '.join(map(str, noise)))
    n=bytearray(noise,encoding='utf-8')
    nh=MPI.File.Open(MPI.COMM_WORLD, 'C:/Users/wilso/Programming_analytics/noise.txt',MPI.MODE_WRONLY)
    #sh=MPI.File.Open(MPI.COMM_WORLD, 'C:/Users/wilso/Programming_analytics/signal.txt',MPI.MODE_WRONLY)
    info=MPI.File.Get_size(nh)
    nh.Seek_shared(info)
    nh.Write_ordered(n)
    
    
    
    
    if myid==0:
        print('size: ',info,flush=True)
    print (myid,"write block %d" %(i-1), flush=True)
    #print(myid,'writing block %d' %(i))
    #sh.Write_ordered(s)
    #nh.Close
    #sh.Close
    
#fh.Close"""