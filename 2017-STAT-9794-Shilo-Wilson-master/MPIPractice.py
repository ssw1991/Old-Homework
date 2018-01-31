# -*- coding: utf-8 -*-
"""
Created on Sat Feb 25 23:20:08 2017

@author: wilso
"""

from mpi4py import MPI
f=open('small.txt')

f=MPI.File.Open(MPI.COMM_WORLD,'small.txt',MPI.MODE_RDONLY)

size=MPI.File.Get_size(f)
print(size)

