# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 21:49:52 2017

@author: wilso
"""

from mpi4py import MPI 
comm = MPI.COMM_WORLD 
rank = comm.Get_rank() 
print ("hello world from process ", rank) 
