# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 18:38:53 2017

@author: wilso
"""

import nltk
import json
import bz2

import glob
path=('C:/Users/wilso/Programming_analytics/twitter/tweets/sample/01')


    
tknzr=nltk.tokenize.casual.TweetTokenizer(preserve_case=False)

class Tweet:
    created_at=''
    id_str=''
    text=''
    source=''
    truncated=''
    user=''
    retweet_count=''  
    entities=''
    retweeted=''
    timestamp_ms=''

def createtweet(tweet):
    if 'created_at' in tweet:
        t=Tweet()
        t.created_at=tweet['created_at']
        t.id_str=tweet['id_str']
        t.text=tweet['text']
        source=tweet['source']
        truncated=tweet['truncated']
        t.user=tweet['user']
        t.retweet_count=tweet['retweet_count']
        t.entities=tweet['entities']
        t.symbols = [symbol['text'] for symbol in tweet['entities']['symbols']]
        t.retweeted=tweet['retweeted']
        t.timestamp_ms=tweet['timestamp_ms']
        return t
        
      

        
def clean(tweet):
    if tweet.get('entities')!=None:
        if tweet['entities']['symbols']!=[]:
            return(tweet)
        
def withticker(lst):
     i=0
     tweets = list(map(lambda x: createtweet(x),lst))
     if tweets:
        return [tweet for tweet in tweets]
              
                 
def ref_comp(lst,base):
       b=open('C:/Users/wilso/Programming_analytics/twitter/completejson1.json','a')
       for record in lst:
            tokenized= tknzr.tokenize(str(record['text']))
            for word in base:
                if word in tokenized:
                    json.dump(record,b)
                    
    

def read_files(file):
    a=bz2.BZ2File(file,'rb')  
    data=a.readlines()
    json_list=[json.loads(line) for line in data]
    return (list(filter(lambda x: clean(x)!=None,json_list)))
    
#screen_names, hashtags, urls, symbols = extract_tweet_entities(clean)

fls=glob.glob('**/*.bz2',recursive=True)
search=['#msft', 'msft', 'microsoft','starbucks','sbux','nividia', 'ibm', 'yahoo','#sbux']

print (len(fls))
i=0

for fl in fls[1:100]:
    records=read_files(fl)
    tickers=withticker(records)
    i+=1
    comp_ref = ref_comp(records,search)
    if comp_ref!=None:
        print(comp_ref,i)
        
    
        
       
    
        