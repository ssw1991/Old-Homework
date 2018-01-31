# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 18:38:53 2017

@author: wilso
"""

import nltk
import json

path=('C:/Users/wilso/Programming_analytics/twitter.json')
tknzr=nltk.tokenize.casual.TweetTokenizer(preserve_case=False)

records=[json.loads(line) for line in open(path)]



clean=[]   
for record in records:
    
    try:
        record['entities']=record['entities']
        clean.append(record)
    except:
        continue
      

def extract_tweet_entities(statuses):       
    
    if len(statuses) == 0:        
        return [], [], [], [], [] 
           
    screen_names = [ user_mention['screen_name']                          
                        for status in statuses   
                             for user_mention in status['entities']['user_mentions'] ]        
    hashtags = [ hashtag['text']                      
                    for status in statuses                    
                        for hashtag in status['entities']['hashtags'] ]
    urls = [ url['expanded_url']                      
                for status in statuses                        
                    for url in status['entities']['urls'] ]    
    symbols = [ symbol['text']                   
                for status in statuses                      
                    for symbol in status['entities']['symbols'] ]
    
    return screen_names, hashtags, urls, symbols

screen_names, hashtags, urls, symbols = extract_tweet_entities(clean)
print(symbols) 