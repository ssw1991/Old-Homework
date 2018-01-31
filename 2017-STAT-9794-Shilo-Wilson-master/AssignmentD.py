# -*- coding: utf-8 -*-
"""

"""
"""
If a token has been identified to contain
non-alphanumeric characters, such as punctuation,
assume it is leading or trailing punctuation
and trim them off. Other internal punctuation
is left intact. - Micheal Cvet

I moved this from its orginal place in the model, so that I 
call it in the Mapping Function.  I also removed the Tuple_sort function,
as it used decapricated language from python 2.7.  Instead I used a key to
sort.
"""
def sanitize(w):
 
  # Strip punctuation from the front
  while len(w) > 0 and not w[0].isalnum():
    w = w[1:]
 
  # String punctuation from the back
  while len(w) > 0 and not w[-1].isalnum():
    w = w[:-1]
   
  return w.lower()


def Map(L):
  results=[]
  for w in L:
    # True if w contains non-alphanumeric characters
    if not w.isalnum():
      w = sanitize(w)  
    w=w.lower()
    results.append ([w, 1])
 
  return results
 
"""
Group the sublists of (token, 1) pairs into a term-frequency-list
map, so that the Reduce operation later can work on sorted
term counts. The returned result is a dictionary with the structure
{token : [(token, 1), ...] .. } -Micheal Cvet

Here, I sanitized the text as it went into the tuple.  Additionally,
I ensured each string was all lower case, the way "the" and "The" would be
mapped the same.
"""
def Partition(L):
  tf = {}
  for sublist in L:
    for p in sublist:
      # Append the tuple to the list in the map
      try:
        tf[p[0]].append (p)
      except KeyError:
        tf[p[0]] = [p]
  return tf
 
"""
Given a (token, [(token, 1) ...]) tuple, collapse all the
count tuples from the Map operation into a single term frequency
number for this token, and return a final tuple (token, frequency).
"""
def Reduce(Mapping):
  return (Mapping[0], sum(pair[1] for pair in Mapping[1]))

#import sys
from multiprocessing import Pool
import math

"""
Load the contents the file at the given
path into a big string and return it.
"""
def load(path):
 
  word_list = []
  f = open(path, "r")
  for line in f:
    word_list.append (line)
 
  # Efficiently concatenate Python string objects
  return (''.join(word_list)).split ()
 
"""
A generator function for chopping up a given list into chunks of
length n.
"""
def chunks(l, n):
  for i in range(0, len(l), n):
      yield l[i:i+n]
 

 
if __name__ == '__main__':
 

 text = load ('C:/Users/wilso/Programming_analytics/shakespeare.txt')
 
  # Build a pool of 8 processes
 pool = Pool(processes=8,)
 
  # Fragment the string data into 8 chunks, the ceiling function ensures that the division
  #happens on an integer,  the last list will be shorter.
 partitioned_text = list(chunks(text, math.ceil(len(text) / 8)))
  
  # Generate count tuples for title-cased tokens
 single_count_tuples = pool.map(Map, partitioned_text)
 
  # Organize the count tuples; lists of tuples by token key
 token_to_tuples = Partition(single_count_tuples)
 
  # Collapse the lists of tuples into total term frequencies
 term_frequencies = pool.map(Reduce, token_to_tuples.items())
 
  # Sort the term frequencies in nonincreasing order
 term_frequencies.sort(key=lambda tup: tup[1], reverse=True)
 final_list = []
 fh=open('something4.txt', 'a')
 for pair in term_frequencies[0:]:
    print(pair)
    final_list=list(pair) 
    fh.write(str(final_list))
    fh.write('\n')
    
     
  