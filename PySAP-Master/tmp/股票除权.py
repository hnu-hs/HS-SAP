# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 16:13:59 2017

@author: Administrator
"""

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np
import time


#import sys
#
#reload(sys)
#          
#sys.setdefaultencoding('utf-8')

def stockCq():
    turn=0
    #数据库引擎
    engine = create_engine(str(r"mysql+mysqldb://root:lzg000@127.0.0.1/stocksystem?charset=utf8"))
    #筛选2000年之后的数据
    sql="SELECT * FROM stockdivideinfo where 权息日 >= '2000-01-01'"   
    data=pd.read_sql(sql,con=engine)  
    #把除权的股票代码放入List
    codelist=data[u'股票代码'].groupby(data[u'股票代码']).head(1).tolist()
    #遍历所有除权除息的股票
    for code in codelist:
        time1=time.time()
        #获取本股票的所有除权信息
        tmpdata=data[data[u'股票代码']==code]        
        #每只股票的除权次数
        nrow=len(tmpdata)
        #获取该股票的所有收盘价，开盘价
        priceData=pd.read_sql('SELECT date,close FROM stockday where code= '+code,con=engine)
        #复权股价变化比率列表初始化
        ratiolist=[]
        #错误日期列表
        #mistakedate=[]
        #创建数据列表，每处理100支股票入一次库
        tmplist=pd.DataFrame()
        #选取这个code的股票开始复权，遍历每一个复权节点
        for i in xrange(nrow):
            s1=tmpdata.iloc[i]
            date=s1[3].strftime('%Y%m%d')
            szg=s1[5]
            fh=s1[6]
            pg=s1[7]
            pgj=s1[8]
            qgb=s1[-2]
            hgb=s1[-1]
            
            #date,szg,fh,pg,pgj,qgb,hgb=s1[3].strftime('%Y%m%d'),s1[5],s1[6],s1[7],s1[8],s1[-2],s1[-1]
                        
            #获得除权之前的收盘价
            close=priceData[priceData.date<date].close.values
           # close=close.tolist()
            close=close[-1]            
            
            
            
            #根据除权信息分类操作，得到除权价格
            if szg and fh==0 and pg==0:
                #print '只有送转股'
                newPrice=close*qgb/hgb
            elif fh and szg==0 and pg==0:
                #print '只有分红'
                newPrice=close-fh/10
            elif pg and szg==0 and fh==0:
               # print '只有配股'
                newPrice=(close*qgb+(hgb-qgb)*pgj)/hgb
            elif szg and fh and pg==0:
               # print '送转股和分红'
                newPrice=qgb*(close-fh/10)/hgb      
            elif fh and pg and szg==0:
              #  print '分红和配股'
                newPrice=((close-fh/10)*qgb+(hgb-qgb)*pgj)/hgb
            elif szg and pg and fh==0:
             #   print '送转股和配股'
                newPrice=(close*qgb+(pg/10)*qgb*pgj)/hgb
            elif szg and fh and pg:
             #   print '送转股、分红、配股'
                newPrice=((close-fh/10)*qgb+(pg/10)*qgb*pgj)/hgb
                
            
            #得到除权后的开盘价，用于比对是否异动
#            openPrice=priceData[priceData.date>=date].open.values
#            openPrice=openPrice[0]
#            if abs(openPrice-newPrice)<=0.1:
            ratio=newPrice/close               
#            else:
#                ratio=np.nan
#                mistakedate.append(code+date)
            ratiolist.append(ratio)
        tmpdata['ratio']=ratiolist
        tmplist=tmplist.append(tmpdata)
        turn+=1
        print time.time()-time1
        print turn
        
        if turn%10==0:
            tmplist.to_sql('stockratio',con=engine,if_exists='append')
            tmplist=pd.DataFrame()

if __name__=='__main__':
    stockCq()
            









    
