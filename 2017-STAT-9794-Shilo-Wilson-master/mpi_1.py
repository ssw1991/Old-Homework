# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 15:57:31 2017

@author: wilso
"""

from mpi4py import MPI
import sys



nprocs = (MPI.COMM_WORLD.Get_size())
myid=MPI.Comm.Get_rank(MPI.COMM_WORLD)

fh=MPI.File.Open(MPI.COMM_WORLD, 'C:/Users/wilso/Programming_analytics/small.txt', MPI.MODE_RDONLY)
size=MPI.File.Get_size(fh)
MPI.File.Close

blocksize = size/nprocs
block_start = blocksize*myid
buf=MPI.Alloc_mem(100)
if myid==nprocs-1:
    block_end=size
else:
    block_end=block_start+blocksize-1;
    
print("beginning", myid,block_start)

if myid !=0:
    fh=open('C:/Users/wilso/Programming_analytics/small.txt')
    fh.seek(block_start)
    buffer=fh.readlines(100)
    block_start+=sys.getsizeof(buffer[0])-48
    print("ending", myid,block_start)
    
 
   