# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 16:13:59 2017
@author: Administrator
"""

import pandas as pd
import os
from sqlalchemy import create_engine
import sys

reload(sys)
          
sys.setdefaultencoding('utf-8')


host='127.0.0.1'
user='root'
passwd='lzg000'
db='stocksystem'
charset='utf8'

engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (user, passwd, host, db))

start='20000104'
jysjpth=u'E:\\股票数据\\股票交易数据补齐\\'
i=0
for f in  os.listdir(jysjpth):
    filename=jysjpth+f
    fin=open(filename,'r')
    codename=fin.readline().strip()
    codename=codename.split()
    if codename[-3]=='':
        name=codename[-2]
    else:
        name=codename[-3]+codename[-2]    
    code=f[:-4]
    fin.close()
    s1=pd.read_table(filename,header=1)   
    s1=s1.iloc[:-1,:1] 
    s1['name']=name
    s1['code']=code
    s1.columns=['date','name','code']
    s1["date"]=s1["date"].apply(lambda x: x.strip().replace('/',''))
    s1=s1[s1.date>=start]
    s1.to_sql('stocktrading',con=engine,if_exists='append')
    i+=1
    print i



    
