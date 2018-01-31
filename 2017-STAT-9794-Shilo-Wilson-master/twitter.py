# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 15:36:22 2017

@author: wilso
"""

import nltk
import json

path=('C:/Users/wilso/Programming_analytics/twitter.json')
tknzr=nltk.tokenize.casual.TweetTokenizer(preserve_case=False)

records=[json.loads(line) for line in open(path)]
search=['#msft', 'msft', 'microsoft','starbucks','sbux','#sbux']
for record in records:
    try:      
        record['text']= tknzr.tokenize(str(record['text']))
        for word in search:
            if word in record['text']:
                print(record['text'])
           
    except:
        continue

    