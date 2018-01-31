# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 18:20:20 2017

@author: wilso
"""

def slidingwindow(sequence,winsize,step=1):
    numofchunks=int(((len(sequence)-winsize)/step))
    for i in range (0,numofchunks,step):
        yield sequence[i:i+winsize]
chunks = slidingwindow(list,3,1)

    

        
    
    