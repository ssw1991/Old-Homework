# -*- coding: utf-8 -*-
"""
Created on Sun Mar 12 14:44:30 2017

@author: wilso
"""
import datetime as dt
f=open('C:/Users/wilso/Programming_analytics/data-big.txt')

rows=f.readlines(10000000)
dates=[]
for row in rows:
    cols=row.split(',')
    dates.append(cols[0])

def date_parse(string):
    return dt.datetime.strptime(string,'%Y%m%d:%H:%M:%S.%f')
begin=dt.datetime.now()

datesparsed = [date_parse(x) for x in dates]

end=dt.datetime.now()
print('parsing date' ,end-begin)




def dt_to_sec(strdate):
    monthdaysdict={'01':0,'02':31,'03':59,'04':89,'05':119,'06':150,'07':180,'08':211,'09':242,'10':273,'11':304,'12':334}
    monthdaysdictleap={'01':0,'02':31,'03':60,'04':90,'05':120,'06':151,'07':181,'08':212,'09':243,'10':274,'11':305,'12':335}
    if int(strdate[0:4])%4 ==0 and int(strdate[0:4])%100!=0:
        leap=True
    if int(strdate[0:4])%400==0:
        leap=True
    else:
        leap=False
    
    
    if leap != True:
        month_to_day=monthdaysdict[strdate[4:6]]
    else:
        month_to_day=monthdaysdictleap[strdate[4:6]]
    year_tosec=31536000*float(strdate[0:4])
    days_tosec=month_to_day+float(strdate[6:8])*24*60*60
    hrtosec=float(strdate[9:11])*60*60
    mintosec=float(strdate[12:14])*60
    sec=float(strdate[15:])
    return year_tosec+days_tosec+hrtosec+mintosec+sec

begin=dt.datetime.now()

datesparsed = [dt_to_sec(x) for x in dates]

end=dt.datetime.now()
print('calculating seconds', end-begin)