# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 16:13:59 2017

@author: Administrator
"""

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

ohlc_dict = {           
    'hq_date':'last',
    'hq_time':'last',
    'hq_code':'first',                                                                                                      
    'hq_close': 'last',                                                                                                    
    'hq_vol': 'sum' ,        
    }
#导入股票还是板块
sFlag=0
#导入几分钟的
mFlag=5

if sFlag==0:
    jysjpth=u'E:\\股票数据\\测试分钟线\\'
    if mFlag==1:
        table='hstockquotationone'
        indexName='1分钟线'
    elif mFlag==5:  
        table='hstockquotationfive'
        indexName='5分钟线'        
else:
    jysjpth=u'E:\\股票数据\\测试板块\\'
    if mFlag==1:
        table='hindexquotationone'
        indexName='1分钟线'        
    elif mFlag==5:
        table='hindexquotationfive'
        indexName='5分钟线'        
        
engine = create_engine(r"mysql+mysqldb://root:lzg000@127.0.0.1/stocksystem?charset=utf8")
i=0
s2=pd.DataFrame()
filelist=os.listdir(jysjpth)
lastfile=filelist[-1]
for f in  filelist:
    filename=jysjpth+f
    fin=open(filename,'r')
    codename=fin.readline().strip()
    codename=codename.split() 
    if len(codename)>4:
        endpos=codename.index(indexName)
        if endpos==3:
            name=codename[1]+codename[2]
        elif endpos==4:
            name=codename[1]+codename[2]+codename[3]            
    else:  
        name=codename[1]
    code=f[:-4]
    fin.close()
    #names=['hq_date','hq_time','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
    names=['hq_date','hq_time','hq_close','hq_vol']
    s1=pd.read_table(filename,header=1,usecols=[0,1,5,6],names=names,dtype={'hq_time':str,'hq_close':np.float32,'hq_vol':np.float32}) 
    #s1=pd.read_table(filename,header=1,names=names,dtype={'hq_time':str,'hq_close':np.float32,'hq_vol':np.float32}) 
    s1=s1.iloc[:-1] 
    #s1["hq_date"]=s1["hq_date"].apply(lambda x: x.strip().replace('/',''))
    s1['hq_code']=code
    #s1['hq_name']=name
    tlist=[]
    for time in s1['hq_time']:
        time=time[0:2]+':'+time[2:4]
        tlist.append(time)

    s1["hq_time"]=tlist
    index=pd.to_datetime(s1['hq_date']+' '+s1['hq_time'])       
    s1.set_index(index,inplace=True)        
    s60=s1.resample('30T', label='right',closed='left').apply(ohlc_dict)  

    i+=1
    print i
    s2=s2.append(s1)
    if i%30==0:   
        index=pd.to_datetime(s2['hq_date']+' '+s2['hq_time'])       
        s2.set_index(index,inplace=True)     
        #s2.dropna(axis=0,inplace=True)          
        s2.to_sql(table,con=engine,if_exists='append')
        s2=pd.DataFrame()
        print '入库'
    elif f==lastfile: 
        index=s2['hq_date']+' '+s2['hq_time']  
        s2.set_index(index,inplace=True) 
        #s2.dropna(axis=0,inplace=True)
        s2.to_sql(table,con=engine,if_exists='append')
        s2=pd.DataFrame()
        print '入库完成' 
    
