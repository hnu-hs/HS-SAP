# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 10:28:00 2017

@author: Administrator
"""
import sys
sys.path.append("..")
reload(sys)      
sys.setdefaultencoding('utf-8')

from DataHanle.MktDataHandle import MktIndexHandle

from DataHanle.MktDataHandle import MktStockHandle

from sqlalchemy import create_engine 

import datetime

import pandas as pd

import numpy as np

import time

import glob

from PlotData.plotpic.picZH import  *
from PlotData.plotpic.picZqZH import  *
from PlotData.plotexcel.plotToExcel import *
import ZJ
import rftrading
import tushare as ts


class ZH():
    
    def __init__(self,sdate,edate):
        
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
      
        #定义时间周期   
#        if ' ' not in sdate:     
#            self.tstartdate=datetime.datetime.strptime(sdate, "%Y-%m-%d")  
#        else:
#            self.tstartdate=datetime.datetime.strptime(sdate, "%Y-%m-%d %H:%M")
#
#        if ' 'not in edate:   
#            self.tenddate=datetime.datetime.strptime(edate, "%Y-%m-%d")  
#        else:
#            self.tenddate=datetime.datetime.strptime(edate, "%Y-%m-%d %H:%M")
      
        if ' ' in sdate: 
            self.tstartdate=sdate
            self.tenddate=edate
        else:
            self.tstartdate=sdate+' 00:00'
            self.tenddate=edate+' 15:00'
        
        self.mktindex = MktIndexHandle()
        
        self.mktstock = MktStockHandle()
        
        self.rf30file=u'E:\\工作\\标的\\30.txt'
        
        self.rf200file=u'E:\\工作\\标的\\200.txt'
        
        self.rf30=self.getCode(self.rf30file)

        self.rf200=self.getCode(self.rf200file)
        
        self.stocBoardFile=u'E:\\股票数据\\股票细分板块\\allboard.txt'
        
        self.stockNames=pd.read_sql_table('stockinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])    
        
        self.stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        
        self.indexNames=pd.read_sql_table('boardindexbaseinfo',con=self.engine,schema='stocksystem',index_col='board_code',columns=['board_code','board_name'])
                          
    #apply函数，计算相对涨跌幅
    def xdChg(self,x,indexchg):   
        try:
            indexchg.index=x.index
            x=x-indexchg
            return x    
        except:
            print x.name
            print '数据长度有误'   
            
    #特立独行处理函数，用于apply
    def tldx(self,x,series):
        if not x.any():
            xishu=np.nan
        else:
            nx=x.shift()
            bl=nx/x
            x=np.log(bl).dropna() 
            x.index = np.arange(len(x))  
            series.index=np.arange(len(series))
            try:
                xishu=(np.mean(series*x)-np.mean(series)*np.mean(x))/(np.std(series)*np.std(x))
            except:
                xishu=10
        return np.abs(xishu)  
    
    #计算聪明因子,apply
    def jsS(self,x):
        x['smartS']=np.abs(x['chgper'])/np.sqrt(x['hq_vol'])*100000
        x.sort_values(by=['smartS'], ascending=[0],inplace=True) 
        x['accumVolPct']=x['hq_vol'].cumsum()/x['hq_vol'].sum()
        return x   
           
    #计算Q因子
    def jsQIndex(self,x):
        totalvol=x['hq_vol'].sum()
        try:           
            VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
            x=x[x.accumVolPct<=0.2]
            smartvol=x['hq_vol'].sum()
            VWAPsmart=(x['hq_close']*x['hq_vol']).sum()/smartvol 
            x['Q']=VWAPsmart/VWAPall
            x=x[['hq_name','Q','hq_code']].head(1)
            return x                   
        except ZeroDivisionError:
            pass     
        
    def jsQStock(self,x):
        totalvol=x['hq_vol'].sum()
        try:           
           # VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
            VWAPall=(x['hq_amo']).sum()/totalvol       
            x=x[x.accumVolPct<=0.2]
            smartvol=x['hq_vol'].sum()
            VWAPsmart=(x['hq_amo']).sum()/smartvol 
            x['Q']=VWAPsmart/VWAPall
            x=x[['hq_name','Q','hq_code','board_name']].head(1)
            return x                   
        except ZeroDivisionError:
            pass               
             
    #获取大盘涨幅和收盘价
    def getDapanChg(self,df_dapan):
        #df_dapan = self.mktindex.MktIndexBarHistDataGet('=399317',self.tstartdate,self.tenddate,"D")
        df_dapan.index=df_dapan['index']   
        #计算大盘每日涨幅   
        daPanChg = df_dapan['hq_close'].diff()
        daPanPreCLose=df_dapan['hq_close'].shift()
        daPanChgper=daPanChg/daPanPreCLose
        return daPanChgper,df_dapan['hq_close']    
    
    #获取指数代码对应名称
    def getIndexNames(self,df_index,indexNames):
        df_index.index=df_index['hq_code']
        df_index['hq_name']=indexNames['board_name']
        df_index['hq_name']=df_index['hq_name'].apply(lambda x:x.replace(u'通达信行业-',''))  
        df_index.index=np.arange(len(df_index))
        return df_index['hq_name']
    
    #获取股票代码对应名称
    def getStockNames(self,df_stock,stockNames):
        
        if 'hq_code' in df_stock.columns:     
            df_stock.index=df_stock['hq_code']
        df_stock['hq_name']=stockNames['stock_name']
        return df_stock['hq_name']
    
    #获取股票关联板块
    def getStockRelated(self,df_stock,stockrelated):
        #stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        
        stockrelated['cFlag']=stockrelated['board_name'].str.contains(u'通达信行业-')
        
        stockrelated=stockrelated[stockrelated['cFlag']]
        
        stockrelated['board_name']=stockrelated['board_name'].apply(lambda x:x.replace(u'通达信行业-',''))     
        
        df_stock.index=df_stock.hq_code
        stockrelated.index=stockrelated.stock_id
        stockrelated['board_name']
        return  stockrelated['board_name']
    
    #得到标的代码
    def getCode(self,fname):
        try:
            s1=pd.read_table(fname,usecols=[0],dtype=str,encoding='utf-8')
        except:
            s1=pd.read_table(fname,usecols=[0],dtype=str,encoding='gbk')
        
        s1.columns=['code']
                
        codelist=s1['code'].astype('int')
        
        return codelist 
    
    #得到标的细分行业
    def getBoard(self,rfname,df_stock):
        if rfname==200:
            s1=pd.read_table(self.rf200file,index_col=0,usecols=[0,18],dtype=str,encoding='gbk')
        elif rfname==30:
            s1=pd.read_table(self.rf30file,index_col=0,usecols=[0,18],dtype=str,encoding='gbk')
        elif rfname==3000:
            s1=pd.read_table(self.stocBoardFile,usecols=[0,18],dtype={'代码':int,'细分行业':str},index_col=0,encoding='gbk')
        
        s1.columns=['board_name']
        
        #如果数据含有列'hq_code',就把code设为索引，否则需要索引为code
        if 'hq_code' in df_stock.columns:
            df_stock.index=df_stock.hq_code
            
        df_stock['board_name']=s1['board_name']
        
        return df_stock
        
    #得到标的数据,要穿一个hq_code列
    def getRF(self,name,df_stock,boardFlag=0):
        if name==30:      
            codelist=self.rf30
        else:
            codelist=self.rf200
        
        if 'hq_code' in df_stock.columns:        
            df_stock['cFlag']=df_stock['hq_code'].isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]  
        else:
            df_stock['cFlag']=df_stock.index.isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]             
  
        if boardFlag!=0:
            if 'hq_code' in df_stock.columns: 
                df_stock200.set_index('hq_code',inplace=True)

            df_stock200=self.getBoard(name,df_stock200)
            
        del df_stock200['cFlag'],df_stock['cFlag']   
        
        return df_stock200       
    
    #复权
    def FQ(self,df_stock):
        sdate=self.tstartdate.strftime('%Y-%m-%d')
        edate=self.tenddate.strftime('%Y-%m-%d')
        #对股票进行复权
        #获得股票除权信息
        cqsql="select * from stockratio where date>='"+sdate+"' and date<='"+edate+"'"      
        stockRatio=pd.read_sql(cqsql,con=self.engine)   
        for code in stockRatio['code']:
            #取出对应code的股票，进行复权
            stockData=df_stock[df_stock.hq_code==code]
            ratioData=stockRatio[stockRatio.code==code]
            ratioDates=ratioData['date'][::-1]
            for date in ratioDates:
                ratio=(ratioData[ratioData.date==date])['ratio'].iat[0]
                tmpStock=stockData[stockData.hq_date<date]
                tmpStock['hq_close']=tmpStock['hq_close']*ratio
                stockData[stockData.hq_date<date]=tmpStock
            #用复权后的数据覆盖原数据             
            df_stock[df_stock.hq_code==code]=stockData  
        return df_stock

    def getZqChg(self,x):
        _open=x['hq_close'].iat[0]
        _close=x['hq_close'].iat[-1]
        diff=_close-_open
        try:
            chgper=diff/_open
            x['chgper']=chgper
            return x.tail(1)
        except Exception as e:
            print e       
            print x.name        
    
    def getDayData(self):
        #获取每日数据
        df_stock = self.mktstock.MkStockBarHistOneDayGet('',self.tstartdate,self.tenddate)
                      
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>400000',self.tstartdate,self.tenddate,"D")
        
        df_dapan = self.mktindex.MktIndexBarHistDataGet('=399317',self.tstartdate,self.tenddate,"D")
        
        df_dapan.index=df_dapan['index']
        df_stock.index=df_stock['index']
        length=len(df_dapan)
        #按交易日期补全数据，停牌涨幅设为-100
        stockGrouped=df_stock[['hq_code','hq_close','hq_open','hq_vol','hq_date']].groupby(df_stock['hq_code'])        
        df_stock=pd.DataFrame()
        datelist=df_dapan.hq_date.tolist()
        
        for code,data in stockGrouped:
            if len(data)!=length:       
                firstopen=data['hq_open'].iat[0]
                data=data.reindex(df_dapan.index)
                data['hq_code']=code     
                data['hq_close'].fillna(method='ffill',inplace=True)
                data['hq_close'].fillna(firstopen,inplace=True)
                data['hq_vol'].fillna(0,inplace=True)  
                data['hq_date']=datelist
            df_stock=df_stock.append(data)        
            
        df_stock.index=np.arange(len(df_stock))
        df_dapan.index=np.arange(length)
        
        return df_stock,df_index,df_dapan
        
    
    def get5minData(self):
        
        tstartdate=self.tstartdate
        
        #取得5分钟级别数据
        df_dapan =self.mktindex.MktIndexBarHistDataGet('=399317',sDate_Start=tstartdate,sDate_End=self.tenddate,speriod='5M') 
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>400000',tstartdate,self.tenddate,"5M")
        
        df_stock= self.mktstock.MktStockBarHistDataGet('>=000001',tstartdate,self.tenddate,"5M")
        
        return df_stock,df_index,df_dapan
        
        
    def get1minData(self):
        
        df_dapan =self.mktindex.MktIndexBarHistDataGet('=399317',sDate_Start=self.tstartdate,sDate_End=self.tenddate,speriod='1M') 
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>400000',self.tstartdate,self.tenddate,"1M")
        
        df_stock= self.mktstock.MktStockBarHistDataGet('>=000001',self.tstartdate,self.tenddate,"1M")
        
        return df_stock,df_index,df_dapan        
            
            
    #计算指数每个最小周期涨幅
    def indexEveryChg(self,df_index):
        #计算指数每日涨幅
        df_index['chg'] = df_index['hq_close'].groupby(df_index['hq_code']).diff()
        df_index['preclose']=df_index['hq_close'].groupby(df_index['hq_code']).shift()
        df_index['chgper']=df_index['chg']/df_index['preclose']
        #df_index.index=df_index['index']           
   
        return df_index
      
    #计算股票每个最小周期涨幅
    def stockEveryChg(self,df_stock):
        #计算股票每个周期涨幅
        df_stock['chg'] = df_stock['hq_close'].groupby(df_stock['hq_code']).diff()
        df_stock['preclose']=df_stock['hq_close'].groupby(df_stock['hq_code']).shift()
        df_stock['chgper']=df_stock['chg']/df_stock['preclose']                
        #修复索引，防止出错
        df_stock.index=np.arange(len(df_stock)) 

        return df_stock        
        
            
    #计算每日涨幅,相对涨幅，排名
    def calculateChg(self,df_stock,df_index,df_dapan):
             
        
        def paixu(x):
            x.sort_values(by=['chgper'], ascending=[0],inplace=True)
            index=np.arange(1,len(x)+1)
            x['paiming']=index
            return x

        #计算大盘每日涨幅   
        df_dapan.index=df_dapan['index']
        daPanChg = df_dapan['hq_close'].diff()
        daPanPreCLose=df_dapan['hq_close'].shift()
        daPanChgper=daPanChg/daPanPreCLose
        dateList=df_dapan['hq_date'].tolist()
        del dateList[0]                               
        #length=len(daPanChg)
        
#        #按交易日期补全数据，停牌涨幅设为-100
#        stockGrouped=df_stock[['hq_code','chgper','hq_date']].groupby(df_stock['hq_code'])
#        
#        df_stock=pd.DataFrame()
#        
##        for code,data in stockGrouped:
##            if len(data)!=length:       
##                data=data.reindex(df_dapan.index)
##                data['hq_code'].fillna(method='bfill',inplace=True)     
##                data['hq_code'].fillna(method='ffill',inplace=True)   
##                data['chgper'].fillna(0,inplace=True)       
##            df_stock=df_stock.append(data)
#       
        #得到指数每日涨幅
        df_index=self.indexEveryChg(df_index)
        #拼接板块的代码与名称              
        df_index['hq_name']=self.getIndexNames(df_index,self.indexNames) 
        #修复索引，防止出错
        #df_index.index=np.arange(len(df_index))    
        df_index['xdchgper']=df_index['chgper'].groupby(df_index['hq_code']).apply(lambda x: self.xdChg(x,daPanChgper))        
        
        df_index=df_index[['hq_name','hq_date','chgper','xdchgper']].dropna() 

        indexChg=[]        
               
        #计算每日排名，加入数据列表
        for date in dateList:
            data=df_index[df_index.hq_date==date]    
            chg=data.sort_values(by=['chgper'], ascending=[0])
            indexChg.append(chg)        
           
        #计算股票每日涨幅           
        df_stock=self.stockEveryChg(df_stock)
        
        #计算每日相对涨幅
        df_stock['xdchgper']=df_stock['chgper'].groupby(df_stock['hq_code']).apply(lambda x: self.xdChg(x,daPanChgper))
        
        #按股票涨幅排序
        df_stock=df_stock.groupby(df_stock['hq_date']).apply(paixu)

        #获取代码对应股票名称
        df_stock['hq_name']=self.getStockNames(df_stock,self.stockNames)
        
        #得到股票所属板块
        df_stock['board_name']=self.getStockRelated(df_stock,self.stockrelated)    
                                         
        #筛选数据
        df_stock=df_stock[['hq_name','hq_date','chgper','xdchgper','board_name','paiming','hq_code']].dropna()
        
        daPanChgper.dropna(inplace=True)        
        
        #找到RF200   
        df_stock200=self.getRF(200,df_stock)               

        #定义涨幅与相对涨幅列表
        stockChg=[]
        stock200Chg=[]
        
        for date in dateList:
            #涨幅的添加
            stockData=df_stock[df_stock.hq_date==date]          
            stock200Data=df_stock200[df_stock200.hq_date==date]
            stockChg.append(stockData)
            stock200Chg.append(stock200Data)   
        
        return indexChg,stockChg,stock200Chg
    


    def stockZqChg(self,df_stock):
        
        zq_stock=df_stock.groupby('hq_code').apply(self.getZqChg)   
        return zq_stock      
        
    def indexZqChg(self,df_index):
        zq_index = df_index.groupby('hq_code').apply(self.getZqChg)
        return zq_index
        
    
    def dapanZqChg(self,df_dapan):
        daPanCloseList=df_dapan['hq_close'].tolist()
        daPanDiff=daPanCloseList[-1]-daPanCloseList[0]
        daPanZqChgper=daPanDiff/daPanCloseList[0] 
        return daPanZqChgper
        
      
    def calculateZqChg(self,df_stock,df_index,df_dapan):

        #计算大盘周期涨幅
        daPanZqChgper=self.dapanZqChg(df_dapan)
       
        
        #计算股票周期涨幅
        zq_stock=self.stockZqChg(df_stock)
        #计算周期相对涨幅
        zq_stock['xdchgper']=zq_stock['chgper']-daPanZqChgper
        
        zq_stock['hq_name']=self.getStockNames(zq_stock,self.stockNames) 
        
        #得到股票所属板块        
        zq_stock['board_name']=self.getStockRelated(zq_stock,self.stockrelated)      
        
        #计算周期涨幅排序，并筛选数据 
        zq_stock=zq_stock.sort_values(by=['chgper'], ascending=[0])
        index=np.arange(1,len(zq_stock)+1)
        zq_stock['paiming']=index         

        zq_stock200=self.getRF(200,zq_stock)        
        
        zq_stock30=self.getRF(30,zq_stock)
                        
        #计算周期排名
        zq_stock=zq_stock.loc[:,['hq_name','chgper','xdchgper','board_name','paiming']].dropna() 
        
        zq_stock200=zq_stock200.loc[:,['hq_name','chgper','xdchgper','board_name','paiming']].dropna()        
        
        zq_stock30=zq_stock30.loc[:,['hq_name','chgper','xdchgper','board_name','paiming']]
                
        #计算指数周期涨幅
        zq_index = self.indexZqChg(df_index)
        zq_index['xdchgper']=zq_index['chgper']-daPanZqChgper         
        zq_index['hq_name']=self.getIndexNames(zq_index,self.indexNames)                            
        zq_index=zq_index[['hq_name','chgper','xdchgper']].sort_values(by=['chgper'], ascending=[0])             
        zq_index=zq_index.loc[:,['hq_name','chgper','xdchgper']].sort_values(by=['chgper'], ascending=[0]) 

        return zq_index,zq_stock,zq_stock200,zq_stock30
        
    def calculateMinZqChg(self):
        df_stock= self.mktstock.MktStockBarHistDataGet('>=000001',self.tstartdate,self.tenddate,"5M")
        df_stock=df_stock.groupby('hq_code').apply(self.getZqChg)
        df_stock.set_index('hq_code',inplace=True)
        return df_stock['chgper']
        
        
    #计算Q因子，特立独行
    def calculateFactor(self,df_stock,df_index,df_dapan):
                          
        #拼接板块的代码与名称              
        df_index['hq_name']=self.getIndexNames(df_index,self.indexNames)
        
        #拼接股票的代码与名称
        df_stock['hq_name']=self.getStockNames(df_stock,self.stockNames)
        
        #拼接股票关联板块
        df_stock['board_name']=self.getStockRelated(df_stock,self.stockrelated)
        
        #设置索引，防止groupby出错
        df_stock.index=np.arange(len(df_stock))
               
        #定义存储序列
        iTLDXList=[]
        sTLDXList=[[],[],[]]
        iQList=[]
        sQList=[[],[],[]]
        #sQList200=[]
                      
        dates=df_dapan['hq_date'].drop_duplicates().drop(0)
        for tstartdate in dates:
        #for tstartdate in df_index['hq_date']:            
            #tstartdate=datetime.datetime.strptime(tstartdate,'%Y-%m-%d').date()
            
            dapan_price=df_dapan[df_dapan.hq_date==tstartdate]['hq_close']
            
            indexDay=df_index[df_index.hq_date==tstartdate]#[['hq_name','hq_close','hq_vol','hq_code']]
            
            stockDay=df_stock[df_stock.hq_date==tstartdate]#[['hq_name','hq_close','hq_vol','board_name','hq_code','hq_amo']]
                            
            indexTLDX=indexDay.loc[:,['hq_name','hq_close']].groupby(indexDay['hq_name']).agg({'hq_name':'first','hq_close':lambda x:self.tldx(x,dapan_price)}).sort_values('hq_close')                   
            iTLDXList.append(indexTLDX)
            
            stockTLDX=stockDay.loc[:,['hq_name','hq_close','board_name','hq_code']].groupby(stockDay['hq_name']).agg({'hq_name':'first','hq_close':lambda x:self.tldx(x,dapan_price),'board_name':'first','hq_code':'first'}).sort_values('hq_close')
            sTLDXList[0].append(stockTLDX)
            
            #筛选RF200
            stockTLDX200=self.getRF(200,stockTLDX)        
            stockTLDX30=self.getRF(30,stockTLDX)        
            sTLDXList[1].append(stockTLDX200)
            sTLDXList[2].append(stockTLDX30)
                            
            #计算指数当日涨幅
#            chg = indexDay['hq_close'].groupby(indexDay['hq_name']).diff()
#            preclose=indexDay['hq_close'].groupby(indexDay['hq_name']).shift()
#            indexDay['chgper']=chg/preclose            
            indexDay=self.indexEveryChg(indexDay)
                    
            #计算股票当日涨幅  
#            chg = stockDay['hq_close'].groupby(stockDay['hq_name']).diff()
#            preclose=stockDay['hq_close'].groupby(stockDay['hq_name']).shift()
#            stockDay['chgper']=chg/preclose    
            stockDay=self.stockEveryChg(stockDay)
            
            indexDay   = indexDay.groupby(indexDay['hq_code']).apply(self.jsS)
            stockDay   = stockDay.groupby(stockDay['hq_code']).apply(self.jsS)
            
            indexQ = indexDay.groupby(indexDay['hq_code']).apply(self.jsQIndex).sort_values('Q')
            stockQ = stockDay.groupby(stockDay['hq_code']).apply(self.jsQStock).sort_values('Q')
            
            #筛选RF200    
            stockQ200=self.getRF(200,stockQ)      
            stockQ30=self.getRF(30,stockQ)              
            iQList.append(indexQ)
            sQList[0].append(stockQ)
            sQList[1].append(stockQ200)
            sQList[2].append(stockQ30)
            
            print tstartdate
            
        return iTLDXList,sTLDXList,iQList,sQList
            
            
            
    def indexZqTldx(self,df_index,df_dapan):
        indexTLDX=df_index[['hq_code','hq_close']].groupby(df_index['hq_code']).agg({'hq_code':'first','hq_close':lambda x:self.tldx(x,df_dapan['hq_close'])}).sort_values('hq_close')
        return indexTLDX
        
        
    def indexZqQ(self,df_index):
        def jsQindex(x):
            totalvol=x['hq_vol'].sum()
            try:           
                VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
                x=x[x.accumVolPct<=0.2]
                smartvol=x['hq_vol'].sum()
                VWAPsmart=(x['hq_close']*x['hq_vol']).sum()/smartvol 
                x['Q']=VWAPsmart/VWAPall
                x=x.loc[:,['Q','hq_code']].head(1)
                return x                   
            except ZeroDivisionError:
                pass           
     
        #计算指数涨幅
        df_index=self.indexEveryChg(df_index)
        #计算指数聪明因子                 
        df_index   = df_index.groupby(df_index['hq_code']).apply(self.jsS)                                  
        #指数Q因子
        indexQ = df_index.groupby(df_index['hq_code']).apply(jsQindex).sort_values('Q') 

        return indexQ      
    
    def stockZqTldx(self,df_stock,df_dapan):
        stockTLDX=df_stock[['hq_close','hq_code']].groupby(df_stock['hq_code']).agg({'hq_close':lambda x:self.tldx(x,df_dapan['hq_close']),'hq_code':'first'}).sort_values('hq_close')        
        return stockTLDX        
        
    def stockZqQ(self,df_stock):
        def jsQstock(x):
            totalvol=x['hq_vol'].sum()
            try:           
                VWAPall=(x['hq_amo']).sum()/totalvol
                x=x[x.accumVolPct<=0.2]
                smartvol=x['hq_vol'].sum()
                VWAPsmart=(x['hq_amo']).sum()/smartvol 
                x['Q']=VWAPsmart/VWAPall
                x=x.loc[:,['Q','hq_code']].head(1)
                return x                   
            except ZeroDivisionError:
                pass          
        #计算股票涨幅  
        df_stock=self.stockEveryChg(df_stock)
        #先计算聪明因子
        df_stock   = df_stock.groupby(df_stock['hq_code']).apply(self.jsS)    
        #计算Q因子
        stockQ = df_stock.groupby(df_stock['hq_code']).apply(jsQstock).sort_values('Q')     
        
        return stockQ
        
    def calculateZqFactor(self,df_stock,df_index,df_dapan): 
                     
#        chg = df_index['hq_close'].groupby(df_index['hq_code']).diff()
#        preclose=df_index['hq_close'].groupby(df_index['hq_code']).shift()
#        df_index['chgper']=chg/preclose    
                

#        chg = df_stock['hq_close'].groupby(df_stock['hq_code']).diff()
#        preclose=df_stock['hq_close'].groupby(df_stock['hq_code']).shift()
#        df_stock['chgper']=chg/preclose   
                    
        #df_stock.fillna(0,inplace=True)
        
        #股票特立独行
        stockTLDX=self.stockZqTldx(df_stock,df_dapan)
       #计算股票Q因子
        stockQ=self.stockZqQ(df_stock)        
                  
        #计算指数特立独行
        indexTLDX=self.indexZqTldx(df_index,df_dapan)           
        #指数Q因子
        indexQ=self.indexZqQ(df_index)
        

        #拼接股票的代码与名称
        stockTLDX['hq_name']=self.getStockNames(stockTLDX,self.stockNames) 
        stockQ['hq_name']=self.getStockNames(stockQ,self.stockNames)
          
        #拼接股票关联板块
        stockTLDX['board_name']=self.getStockRelated(stockTLDX,self.stockrelated)  
        stockQ['board_name']=self.getStockRelated(stockQ,self.stockrelated)
        
        #删除未录入名字的新股
        stockTLDX.dropna(inplace=True)
        stockQ.dropna(inplace=True)
        
        #拼接板块的代码与名称              
        indexTLDX['hq_name']=self.getIndexNames(indexTLDX,self.indexNames)
        indexQ['hq_name']=self.getIndexNames(indexQ,self.indexNames)     
        
        #筛选RF200   
        stockQ200=self.getRF(200,stockQ)
        
        stockQ30=self.getRF(30,stockQ)
        
        stockTLDX200=self.getRF(200,stockTLDX)
        
        stockTLDX30=self.getRF(30,stockTLDX)
        
        return indexTLDX,stockTLDX,indexQ,stockQ,stockQ200,stockTLDX200,stockQ30,stockTLDX30
    
    def getAllChg(self,df_stock,df_dapan):
        
        #计算累计涨幅
        def allChg(data,bdFlag=0):
            if bdFlag==0:
                close0=data['hq_close'].iat[0]
                allChg=data['hq_close']/close0-1      
                return allChg 
            else:
                close0=data['hq_close'].iat[0]
                allChg=data['hq_close']/close0-1    
                data['allchg']=allChg
                
                return data  
                    
        mktindex = self.mktindex      
        
        #sdate=datetime.datetime.strptime(sdate, "%Y-%m-%d")#.date()
#        #获取每日数据
#          #市场股票
#        df_stock=df_stock[df_stock.hq_date>=sdate]
          #上证指数
        df_sz=mktindex.MktIndexBarHistDataGet('=000001',self.tstartdate,self.tenddate,"D")
          #创业板综
        df_cybz   = mktindex.MktIndexBarHistDataGet('=399102',self.tstartdate,self.tenddate,"D")
          #次新
        df_cx   = mktindex.MktIndexBarHistDataGet('=880529',self.tstartdate,self.tenddate,"D")          
          #上证50
        df_sz50  = mktindex.MktIndexBarHistDataGet('=000016',self.tstartdate,self.tenddate,"D")  
          #中证500
        df_zz500  = mktindex.MktIndexBarHistDataGet('=399905',self.tstartdate,self.tenddate,"D")       
          #沪深300
        df_hs300  = mktindex.MktIndexBarHistDataGet('=399300',self.tstartdate,self.tenddate,"D")         
        
        #找到RF200，用于计算累计涨幅   
        df_stock200=self.getRF(200,df_stock)[['hq_code','hq_date','hq_close','hq_open']] 
     
        #找到RF30  
        df_stock30=self.getRF(30,df_stock)[['hq_code','hq_date','hq_close','hq_open']]            
  
        #计算累计涨幅
        allChgdapan=allChg(df_dapan)
        allChgcybz=allChg(df_cybz)
        allChgcx=allChg(df_cx)
        allChgsz50=allChg(df_sz50)
        allChgzz500=allChg(df_zz500)
        allChghs300=allChg(df_hs300)
        allChgsz=allChg(df_sz)
        achg200=df_stock200[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:allChg(x,1))
        achg30=df_stock30[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:allChg(x,1))   
        timeList=[]
        allChg200=[]
        allChg30=[]
        for date in df_dapan['hq_date']:
            timeList.append(date.strftime("%Y-%m-%d") )
            tmp200=achg200[achg200.hq_date==date]
            tmpachg=tmp200['allchg'].mean()
            allChg200.append(tmpachg)
            tmp30=achg30[achg30.hq_date==date]
            tmpachg=tmp30['allchg'].mean()
            allChg30.append(tmpachg)            
              
        allChg200=pd.Series(allChg200)
        allChg30=pd.Series(allChg30)
        avgChg={'399317':[],'200':[],'30':[],'300':[]}   

        chg200to30=allChg200-allChg30
        chg200tohs300=allChg200-allChghs300        
        chg30tohs300=allChg30-allChghs300
        chg50to300=allChgsz50-allChghs300
        chg500to300=allChgzz500-allChghs300
        chgcybto300=allChgcybz-allChghs300
        chgcxto300=allChgcx-allChghs300       
        
        #得到净值数据
        avgDic={'日期':timeList,'RF200净值':allChg200,'RF30净值':allChg30,'国证A指':allChgdapan,'RF200相对RF30':chg200to30,'RF30相对沪深300':chg30tohs300,'上证50相对沪深300':chg50to300,'中证500相对沪深300':chg500to300,'创业板综相对沪深300':chgcybto300,'次新股相对沪深300':chgcxto300,'创业板综':allChgcybz,'次新股':allChgcx,'上证50':allChgsz50,'中证500':allChgzz500,'沪深300':allChghs300,'沪深300指数':df_hs300['hq_close'],'RF200相对沪深300':chg200tohs300,'上证指数':allChgsz}
        avg=pd.DataFrame(avgDic)   
        #计算周期净值变化
        chg200=allChg200.iat[-1]
        chg30=allChg30.iat[-1]
        chgi=allChgdapan.iat[-1]
        chg30to200=chg30-chg200
        chgito200=chgi-chg200
        chg30toi=chg30-chgi
        chg200toi=chg200-chgi
        chg30tohs300=chg30-allChghs300.iat[-1]
        chg200tohs300=chg200-allChghs300.iat[-1]
        chgitohs300=chgi-allChghs300.iat[-1]
           
        avgChg['200'].append(chg200)
        avgChg['200'].append(chg200toi)
        avgChg['200'].append(0)
        avgChg['200'].append(chg200tohs300)
        
        avgChg['30'].append(chg30)        
        avgChg['30'].append(chg30toi)
        avgChg['30'].append(chg30to200)
        avgChg['30'].append(chg30tohs300)
        
        avgChg['399317'].append(chgi)       
        avgChg['399317'].append(0)         
        avgChg['399317'].append(chgito200) 
        avgChg['399317'].append(chgitohs300)
        
        avgChg['300'].append(allChghs300.iat[-1])
        avgChg['300'].append(allChghs300.iat[-1]-chgi)
        avgChg['300'].append(allChghs300.iat[-1]-chg200)
        avgChg['300'].append(0) 
        return avg,avgChg  

    def getRFTop(self,stock200ZqChg,stock30ZqChg): 
        #规定数据长度为RF30的长度
        stock200ZqChg=stock200ZqChg.iloc[:,0:2].head(len(stock30ZqChg))
        stock30ZqChg=stock30ZqChg.iloc[:,0:2]
        
        stock200ZqChg=stock200ZqChg.reset_index()
        stock30ZqChg=stock30ZqChg.reset_index()        
      
#        stock200ZqChg.index=np.arange(len(stock200ZqChg))   
#        stock30ZqChg.index=np.arange(len(stock30ZqChg))   
#        RFtopdata=pd.concat([stock200ZqChg,stock30ZqChg],axis=1).head(15)
#        RFtopdata.columns=['RFTop30','RFTop30Chg','RF30','RF30Chg']  
        return stock200ZqChg,stock30ZqChg 
    
    def buildForms(self):
        stockd,indexd,dapand=self.getDayData()
        stock5,index5,dapan5=self.get5minData()        

        zfTimes=dapand['hq_date'].drop_duplicates().drop(0).apply(lambda x:x.strftime("%Y-%m-%d"))
        #生成每日报表
        indexChg,stockChg,stock200Chg=self.calculateChg(stockd,indexd,dapand)
        (iTLDX,sTLDX,iQList,sQList)=self.calculateFactor(stock5,index5,dapan5)
       
        num=0
        for t in zfTimes:
            p=picZH.picExcel(t,indexChg[num],stockChg[num],stock200Chg[num],iTLDX[num],sTLDX[0][num],sTLDX[1][num],sTLDX[2][num],iQList[num],sQList[0][num],sQList[1][num],sQList[2][num])
            p.picModel()
            #p.update()
            num+=1
            print num
#        
        #生成周期报表
        indexZqTLDX,stockZqTLDX,indexQ,stockQ,stockQ200,stockTLDX200,stockQ30,stockTLDX30=self.calculateZqFactor(stock5,index5,dapan5)  
        indexZqChg,stockZqChg,stock200ZqChg,stock30ZqChg=self.calculateZqChg(stockd,indexd,dapand)      
        sdate=zfTimes.iat[0]
        edate=zfTimes.iat[-1]   
        period=sdate+'至'+edate       
        p=picZqZH.picExcel(period,indexZqChg,stockZqChg,stock200ZqChg,stock30ZqChg,indexZqTLDX,stockZqTLDX,stockTLDX200,stockTLDX30,indexQ,stockQ,stockQ200,stockQ30)
        p.picModel()
        print '综合报表生成'   
        

    def factorCount(self):
        tldxdir=u'E:/工作/数据备份/tldx200/*.csv'
        qdir=u'E:/工作/数据备份/Q200/*.csv'
        
        tldxdata=pd.DataFrame()
        qdata=pd.DataFrame()
        tldxflist=glob.glob(tldxdir)
        qflist=glob.glob(qdir)
        
        for f in tldxflist:
            s1=pd.read_csv(f,encoding='gbk').head(10)
            s1['date']=f.split('\\')[-1][:-4]
            tldxdata=tldxdata.append(s1)
            
        for f in qflist:
            s1=pd.read_csv(f,encoding='gbk').head(10)
            s1['date']=f.split('\\')[-1][:-4]
            qdata=qdata.append(s1)         
        
        tldxdata=tldxdata.loc[:,['hq_code','hq_name','hq_close','date']]
        qdata=qdata.loc[:,['hq_code','hq_name','Q','date']]
        
        zh=tldxdata.append(qdata)
        
        #统计个股在综合中出现的次数
        tj=zh.groupby('hq_code').agg({'hq_name':'first','hq_code':'count'})
        #找到总出现次数大于1的
        tj=tj[tj.hq_code>1]
        
        #筛选出现次数大于1的票
        zh['cFlag']=zh['hq_name'].isin(tj.hq_name)
        zh=zh[zh['cFlag']].sort_values('hq_name')
       
        date=datetime.datetime.now().strftime('%Y-%m-%d')
 
        zh.to_csv(u'E:/工作/数据备份/因子统计结果/'+date+'.csv',encoding='gbk')
        
        return zh,tj
    
    #为数据排序
    def rank(self,data,gradename,maxgrade=5):
        
        #计算长度均值，然后依据这个值把数据划分为5等分
        
        dataLen=len(data)
        
        #获取列名，如果code列不存在，直接reindex，否则重新赋值给index
        
        columns=list(data.columns)
        if 'hq_code' not in columns:
            data.reset_index(inplace=True)
        else:         
            data.index=np.arange(dataLen)
        avg=float(dataLen)/maxgrade
                 
        n=0
        
        grade=pd.Series()
        
        gradename=gradename+'grade'
        #不知道为什么，这里用np.arange排序后不能用这个排序把数据筛选出来，只能用索引排序筛选了
        for i in xrange(maxgrade):
            tmp=data[(data.index>=n)&(data.index<(n+avg))] 
 
            tmp[gradename]=(maxgrade-i)

            grade=grade.append(tmp[gradename])

            n+=avg
            
        data[gradename]=grade
    
        data.set_index('hq_code',inplace=True)
    
        return data


    def zjRank(self,sdate,edate,allFlag=0):      
        z=ZJ.ZJ()
        zj=z.zqAmoMin(sdate,edate)
#        zj200=self.getRF(30,zj)
#        zj30=self.getRF(200,zj)
#        if allFlag==0:     
#            zj200=self.rank(zj200,'zj')
#            zj30=self.rank(zj30,'zj')
#            return zj200,zj30
#        else:
#            zj=self.rank(zj.loc[:,['hq_code','vbigper','bigD','vbigD','lbigD']] ,'zj')        

        zj200,zj30,zj=z.zqAmo(sdate,edate)        
        if allFlag==0:
            zj200=zj200.loc[:,['hq_code','vbigper']]
            zj30=zj30.loc[:,['hq_code','vbigper']]     
            zj200=self.rank(zj200,'zj')
            zj30=self.rank(zj30,'zj')
            return zj200,zj30
        else:
            zj=self.rank(zj.loc[:,['hq_code','vbigper','bigD','vbigD','lbigD']] ,'zj')
            return zj
               
    def factorRank(self,sdate,edate,allFlag=0):
        sdate=sdate.split()[0]
        edate=edate.split()[0]
        date=sdate+'至'+edate
        tldxfile200=u'E:/工作/数据备份/tldx200'+'/'+date+'.csv'
        tldxfile30=u'E:/工作/数据备份/tldx30'+'/'+date+'.csv'
        tldxfile=u'E:/工作/数据备份/tldx'+'/'+date+'.csv'
        qfile200=u'E:/工作/数据备份/Q200'+'/'+date+'.csv'
        qfile30=u'E:/工作/数据备份/Q30'+'/'+date+'.csv'
        qfile=u'E:/工作/数据备份/Q'+'/'+date+'.csv'
        
        if allFlag==0:
            tldx200=pd.read_csv(tldxfile200,encoding='gbk').loc[:,['hq_code','hq_name','hq_close']]
            tldx30=pd.read_csv(tldxfile30,encoding='gbk').loc[:,['hq_code','hq_name','hq_close']]
            q200=pd.read_csv(qfile200,encoding='gbk').loc[:,['hq_code','hq_name','Q']]
            q30=pd.read_csv(qfile30,encoding='gbk').loc[:,['hq_code','hq_name','Q']]
            
            tldx200=self.rank(tldx200,'tldx')
            tldx30=self.rank(tldx30,'tldx')
            q200=self.rank(q200,'q')
            q30=self.rank(q30,'q')
            
            q200['fgrade']=0.5*tldx200['tldxgrade']+0.5*q200['qgrade']
            q30['fgrade']=0.5*tldx30['tldxgrade']+0.5*q30['qgrade']
            
            return q200['fgrade'],q30['fgrade']
        else:
            tldx=pd.read_csv(tldxfile,encoding='gbk').loc[:,['hq_code','hq_name','hq_close']]
            q=pd.read_csv(qfile,encoding='gbk').loc[:,['hq_code','hq_name','Q']]
            tldx=self.rank(tldx,'tldx')
            q=self.rank(q,'q')    

            q['fgrade']=0.5*tldx['tldxgrade']+0.5*q['qgrade']   
            
            return q['fgrade']
    
    def chgRank(self,stock200chg,stock30chg,stockchg=pd.DataFrame(),maxgrade=5):
        if stockchg.empty:          
            stock200chg=self.rank(stock200chg,'chg',maxgrade)
            stock30chg=self.rank(stock30chg,'chg',maxgrade)
             
            return stock200chg.loc[:,['hq_name','chgper','chggrade']],stock30chg.loc[:,['hq_name','chgper','chggrade']]
        else:
            stockchg=self.rank(stockchg,'chg',maxgrade)
            return stockchg.loc[:,['hq_name','chgper','chggrade']]
            
    
    def tradingRank(self,sdate,edate,allFlag=0,update=False):
        
        r=rftrading.RF_TradingMonitor()
        rzye200_df,rzye30_df,atr_200,atr_30,rzye_df,atr_all=r.getTrading(sdate,edate,update)   
        
        if allFlag==0:
            rzye200_df=self.rank(rzye200_df,'rz')
            rzye30_df=self.rank(rzye30_df,'rz')
            atr_200=self.rank(atr_200,'atr')
            atr_30=self.rank(atr_30,'atr')
            
            return rzye200_df,rzye30_df,atr_200,atr_30
        else:
            rzye_df=self.rank(rzye_df,'rz')
            atr_all=self.rank(atr_all,'atr')        
            return rzye_df,atr_all
    
    def getRank(self,stock200ZqChg,stock30ZqChg,stockZqChg,sdate,edate,allFlag=0,update=False):
        
        if allFlag==0:
            stock200ZqChg,stock30ZqChg=self.chgRank(stock200ZqChg,stock30ZqChg)        
            frank200,frank30=self.factorRank(sdate,edate)
            zjdata200,zjdata30=self.zjRank(sdate,edate)   
            rzye200_df,rzye30_df,atr_200,atr_30=self.tradingRank(sdate,edate,allFlag,update)   
            
            df_rank=[]
            
            df_rank1=pd.concat([frank200,zjdata200,stock200ZqChg,rzye200_df,atr_200],axis=1)
            df_rank1.loc[:,['mt_rzye','rzgrade']]=df_rank1.loc[:,['mt_rzye','rzgrade']].fillna(0)
            df_rank1['grade']=df_rank1['chggrade']*0.3+df_rank1['zjgrade']*0.3+df_rank1['atrgrade']*0.1+df_rank1['rzgrade']*0.1+df_rank1['fgrade']*0.2
            df_rank1=df_rank1.sort_values('grade',ascending=False).dropna() 
            df_rank1=self.getBoard(200,df_rank1)
            df_rank1.index=np.arange(len(df_rank1))
            
            df_rank2=pd.concat([frank30,zjdata30,stock30ZqChg,rzye30_df,atr_30],axis=1)
            df_rank2.loc[:,['mt_rzye','rzgrade']]=df_rank2.loc[:,['mt_rzye','rzgrade']].fillna(0)
            df_rank2['grade']=df_rank2['chggrade']*0.3+df_rank2['zjgrade']*0.3+df_rank2['atrgrade']*0.1+df_rank2['rzgrade']*0.1+df_rank2['fgrade']*0.2
            df_rank2.sort_values('grade',ascending=False,inplace=True)   
            df_rank2=self.getBoard(30,df_rank2)
            df_rank2.index=np.arange(len(df_rank2))
            
            #标记200中的30，后面用不同的颜色区分开            
            df_rank1['cFlag']=df_rank1['hq_name'].isin(df_rank2['hq_name'])
            
            df_rank.append(df_rank1)
            df_rank.append(df_rank2)
            
            return df_rank1,df_rank2
        else:
            stockZqChg=self.chgRank(stock200ZqChg,stock30ZqChg,stockZqChg)
            frank=self.factorRank(sdate,edate,1)
            zjdata=self.zjRank(sdate,edate,1)
            rzye_df,atr=self.tradingRank(sdate,edate,1)
            
            df_rank=pd.concat([stockZqChg,frank,zjdata,rzye_df,atr],axis=1)
            df_rank.loc[:,['mt_rzye','rzgrade']]=df_rank.loc[:,['mt_rzye','rzgrade']].fillna(0)
            df_rank['grade']=df_rank['chggrade']*0.3+df_rank['zjgrade']*0.3+df_rank['atrgrade']*0.1+df_rank['rzgrade']*0.1+df_rank['fgrade']*0.2
            del df_rank['cFlag']
            df_rank=df_rank.sort_values('grade',ascending=False).dropna() 
            df_rank.index=np.arange(len(df_rank))       
            return df_rank

    def buildJzRankForm(self,allFlag=0,update=False,rankFlag=1):
        stockd,indexd,dapand=self.getDayData() 
        indexZqChg,stockZqChg,stock200ZqChg,stock30ZqChg=self.calculateZqChg(stockd,indexd,dapand) 
        
        sdate=dapand['hq_date'].iat[1].strftime("%Y-%m-%d")
        edate=dapand['hq_date'].iat[-1].strftime("%Y-%m-%d") 
        period=sdate+'至'+edate        
        
        #allFlag=0，则只取公司标的，否则取全市场       
        if rankFlag==0:
            df_rank=[]
        else:
            df_rank=self.getRank(stock200ZqChg,stock30ZqChg,stockZqChg,sdate,edate,allFlag,update)
        
        if allFlag==0:
            rf200zqchg,rf30zqchg=self.getRFTop(stock200ZqChg,stock30ZqChg)
            
            avg,avgChg=self.getAllChg(stockd,dapand)
            
            pte = plotToExcel.PlotToExcel()
            pte.bulidAvg(avg,avgChg,stock200ZqChg,stock30ZqChg,df_rank,period)        
        else:
            df_rank=df_rank.loc[:,['hq_name','chgper','vbigD','lbigD','bigD','vbigper','mt_rzye','atr','fgrade','grade']]
            df_rank.columns=['名称','涨幅','特大净额','大净额','总净额','特大占比','融资余额','ATR','异动级别','总评']
            df_rank.to_csv(u'E:\\工作\\数据备份\\全市场选股\\'+period+'.csv',encoding='gbk',float_format='%.2f')
        
    def buildMinRankForm(self,factor=False):

        
        z=ZJ.ZJ()
        amodata=z.zqAmoMin(self.tstartdate,self.tenddate)
        amodata.sort_values('bigper',inplace=True,ascending=False)
        

        zjRank=self.rank(amodata,'zj',10)
            
        if factor==True:
            stock5,index5,dapan5=self.get1minData()
            
            stockZqChg=self.stockZqChg(stock5).loc[:,['hq_code','chgper']]
            stockZqChg.sort_values('chgper',inplace=True,ascending=False)
            chgRank=self.rank(stockZqChg,'chg',10)            
            stockZqTLDX=self.stockZqTldx(stock5,dapan5)
            stockZqTLDX.sort_values('hq_close',inplace=True)
            stockQ=self.stockZqQ(stock5)
            stockQ.sort_values('Q',inplace=True)
            
            tldxRank=self.rank(stockZqTLDX,'tldx',10)
            qRank=self.rank(stockQ,'q',10)
            qRank['fgrade']=qRank['qgrade']*0.5+tldxRank['tldxgrade']*0.5          
            df_rank=pd.concat([chgRank,zjRank,qRank],axis=1).dropna()
            df_rank['grade']=df_rank['chggrade']*0.4+df_rank['zjgrade']*0.4+df_rank['fgrade']*0.2
            df_rank=self.getBoard(3000,df_rank)
            df_rank['hq_name']=self.getStockNames(df_rank,self.stockNames)
            df_rank=df_rank.loc[:,['hq_name','board_name','chgper','bigD','bigper','fgrade','grade']]
            
        else:   
            stock5,index5,dapan5=self.get5minData()
            
            stockZqChg=self.stockZqChg(stock5).loc[:,['hq_code','chgper']]
            stockZqChg.sort_values('chgper',inplace=True,ascending=False)
            chgRank=self.rank(stockZqChg,'chg',10)            
            df_rank=pd.concat([chgRank,zjRank],axis=1).dropna()
            df_rank['grade']=df_rank['chggrade']*0.5+df_rank['zjgrade']*0.5
            df_rank=self.getBoard(3000,df_rank)
            df_rank['hq_name']=self.getStockNames(df_rank,self.stockNames)
            df_rank=df_rank.loc[:,['hq_name','board_name','chgper','bigD','bigper','grade']]
               
        df_rank['chgper']=df_rank['chgper']*100
        df_rank['bigper']=df_rank['bigper']*100
        df_rank.sort_values(['grade','board_name'],inplace=True,ascending=False)
        
        df_rank200=self.getRF(200,df_rank,1)
        df_rank30=self.getRF(30,df_rank,1)
#        df_rank200.reset_index(inplace=True)    
#        df_rank30.reset_index(inplace=True)
        
        if factor==True:
            df_rank200.columns=df_rank30.columns=df_rank.columns=['名称','板块','涨幅','大单净额','净额占比','异动级别','综合评分']
        else:
            df_rank200.columns=df_rank30.columns=df_rank.columns=['名称','板块','涨幅','大单净额','净额占比','综合评分']
            
        
        df_rank200['']=''
        df_rank200[' ']=''
        df_rank200['  ']=''
        
        df_rank200.index=np.arange(len(df_rank200))
        df_rank30.index=np.arange(len(df_rank30))
        df_rankRF=pd.concat([df_rank200,df_rank30],axis=1)
        
        if factor==False:       
            df_rank.to_csv(u'E:\\工作\\数据备份\\全市场选股\\'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.2f')
            df_rankRF.to_csv(u'E:\\工作\\数据备份\\全市场选股\\RF'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.3f')
        else:
            df_rank.to_csv(u'E:\\工作\\数据备份\\全市场选股\\F'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.2f')
            df_rankRF.to_csv(u'E:\\工作\\数据备份\\全市场选股\\FRF'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.3f')            

        
  
if __name__=='__main__':
#    t1=time.time()
    #注意 纯日期不要打空格

    print sys.path
    
    c=ZH('2017-06-20','2017-07-21')
    #amo=c.indexCashflow('2017-07-17 14:30','2017-07-17 15:00')
    #c.buildMinRankForm(1)
    c.buildJzRankForm(update=True,rankFlag=0)
#    stock,index,dapan=c.getDayData()
#    a,b=c.getAllChg(stock,dapan)

    
    
    

                            

    