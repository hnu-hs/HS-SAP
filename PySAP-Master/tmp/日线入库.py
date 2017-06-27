# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 16:13:59 2017

@author: Administrator
"""

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np



flag=1 #0为股票，1为板块

if flag==0:
    jysjpth=jysjpth=u'E:\\股票数据\\测试分钟线\\'
    names=['hq_date','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
    table='hstockquotationday'
    #table='stockday'
    
else:
    jysjpth=u'E:\\股票数据\\测试板块\\'
    table='hindexquotationday'
    names=['hq_date','hq_close','hq_vol']
engine = create_engine(r"mysql+mysqldb://root:lzg000@127.0.0.1/stocksystem?charset=utf8")
i=0
s2=pd.DataFrame()
filelist=os.listdir(jysjpth)
lastfile=filelist[-1]
#s=filelist.index('SZ300617.txt')
#filelist=filelist[s+1:]
for f in  filelist:
    filename=jysjpth+f
    fin=open(filename,'r')
    codename=fin.readline().strip()
    codename=codename.split() 
    if len(codename)>4:
        endpos=codename.index('日线')
        if endpos==3:
            name=codename[1]+codename[2]
        elif endpos==4:
            name=codename[1]+codename[2]+codename[3]
            
    else:  
        name=codename[1]
    code=f[:-4]
    fin.close()
    
    names=['hq_date','hq_close','hq_vol']
    s1=pd.read_table(filename,header=1,usecols=[0,4,5],names=names,dtype={'hq_time':str,'hq_close':np.float32,'hq_vol':np.float32}) 
    #s1=pd.read_table(filename,header=1,names=names)    
    s1=s1.iloc[:-1] 
    #s1["hq_date"]=s1["hq_date"].apply(lambda x: x.strip().replace('/',''))
    s1['hq_code']=code
    #s1['hq_name']=name
#    tlist=[]
#    for time in s1['hq_time']:
#        time=time[0:2]+':'+time[2:4]#+':00'
#        tlist.append(time)
#    
#    s1["hq_time"]=tlist

#    index=s1['hq_date']+' '+s1['hq_time']  
#    index=pd.to_datetime(index)
#    s1.set_index(index,inplace=True)     
    
    i+=1
    print i
    s2=s2.append(s1)

    if i%30==0:  
        index=s2['hq_date'].tolist()
        index=pd.to_datetime(index)
        s2.set_index(index,inplace=True)   
        s2.to_sql(table,con=engine,if_exists='append')
        s2=pd.DataFrame()
        print '入库'
        
    elif f==lastfile:   
        index=s2['hq_date'].tolist()
        index=pd.to_datetime(index)
        s2.set_index(index,inplace=True) 
        s2.to_sql(table,con=engine,if_exists='append')
        s2=pd.DataFrame()
        print '入库完成' 