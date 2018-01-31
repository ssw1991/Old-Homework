# -*- coding: utf-8 -*-
"""
Created on Sat Feb 18 16:15:48 2017

@author: wilso
"""
import bisect
import datetime as dt
def date_parse(string):
    return dt.datetime.strptime(string,'%Y%m%d:%H:%M:%S.%f')
packages=[]
f=open('data-big.txt')
rows = f.readlines(1000000)

noise=[]
signaldates=[]
a=0
while a<1000:
    signaldates.append(dt.datetime(1900,1,1,1,1,1,000000))
    a=a+1

signal=[]
for row in rows:
        package ={}
        
        cols=row.split(",")
        try:
            package["date"]=date_parse(cols[0])
            package['price']=float(cols[1])
            package['volume']=int(cols[2].strip('\n'))
        except:
            package['date']=cols[0]
            noise.append(package['date'])
            continue
        if package['volume'] < 0:
            noise.append(package['volume'])
            continue
        if package['price']<0:
            noise.append(package['price'])
            continue
        insertion=bisect.bisect_left(signaldates,package['date'])
        signaldates.insert(insertion,package['date'])
        del signaldates[0]
        
        insertion=len(signal)-(len(signaldates)-insertion)
        signal.insert(insertion,package)
        
       
        
    
            

            
f=open('signal.txt','w')
for i in signal:
    f.write("%s,%.2f,%.d\n" %(str(i['date']),i['price'],i['volume']))  
f.close()

print(len(signaldates))
         
    
    
        #print(package['date'])
        
    
        
    #except:
     #   package['date']=cols[0]
      #  noise.append(package['date'])
#print(noise)
#print(signal)
#print("number of noise ", len(noise), "number of rows", len(rows), "percent lost", len(noise)/len(rows))
#print("number of signal ", len(signal), "number of rows", len(rows), "total", len(signal)+len(noise))

#for package in packages:
   # print (package["date"])
"""for package in packages:
    try:
        package["date"]= date_parse(package["date"]) 
    except:
        package['date']=package['date']
        noise.append(package['date'])
        continue
print(noise)"""
