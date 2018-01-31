# -*- coding: utf-8 -*-
"""
Created on Sat Mar  4 14:16:53 2017

@author: wilso
"""
import math
from mpi4py import MPI


nprocs =(MPI.COMM_WORLD.Get_size())
myid=MPI.Comm.Get_rank(MPI.COMM_WORLD)
fh=MPI.File.Open(MPI.COMM_WORLD, 'C:/Users/wilso/Programming_analytics/data-big.txt', MPI.MODE_RDONLY)
size=MPI.File.Get_size(fh)
print (size)

blocksize = 130000000


block_starts=[0]

block_start=blocksize
if myid ==0:
    while block_start<size:
        buf=bytearray(100)
        fh.Seek(block_start)
        fh.Iread(buf)
        rows=buf.decode('utf-8')
        rows=rows.split('\n')
        offset=len(str(rows[0]))
        adjblock_start=block_start+offset
        block_starts.append(adjblock_start)
        block_start+=blocksize
        if block_start > size:
            block_starts.append(block_start)
print (block_starts)       
