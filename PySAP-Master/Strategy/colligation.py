# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 10:28:00 2017

@author: Administrator
"""
import sys
sys.path.append("..")
reload(sys) 

from DataHanle.MktDataHandle import MktIndexHandle

from DataHanle.MktDataHandle import MktStockHandle

from sqlalchemy import create_engine 

import datetime

import pandas as pd

import numpy as np

import time


from PlotData.plotpic import picZH
from PlotData.plotpic import picZqZH
from PlotData.plotexcel import plotToExcel


class ZH():
    
    def __init__(self):
        
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
      
        #定义时间周期        
        self.tstartdate=datetime.datetime(2017,6,16)

        self.tenddate=datetime.datetime(2017,6,22)  
        
        self.mktindex = MktIndexHandle()
        
        self.mktstock = MktStockHandle()
        
        self.rf30=self.getCode(u'E:\\工作\\标的\\30.txt')

        self.rf200=self.getCode(u'E:\\工作\\标的\\200.txt')
                          
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
            y=x[x.accumVolPct<=0.2]
            smartvol=y['hq_vol'].sum()
            VWAPsmart=(y['hq_close']*y['hq_vol']).sum()/smartvol 
            x['Q']=VWAPsmart/VWAPall
            x=x[['hq_name','Q','hq_code']].head(1)
            return x                   
        except ZeroDivisionError:
            pass     
        
    def jsQStock(self,x):
        totalvol=x['hq_vol'].sum()
        try:           
            VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
            y=x[x.accumVolPct<=0.2]
            smartvol=y['hq_vol'].sum()
            VWAPsmart=(y['hq_close']*y['hq_vol']).sum()/smartvol 
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
    def getIndexNames(self,df_index):
        indexNames=pd.read_sql_table('boardindexbaseinfo',con=self.engine,schema='stocksystem',index_col='board_code',columns=['board_code','board_name'])    
        df_index.index=df_index['hq_code']
        df_index['hq_name']=indexNames['board_name']
        df_index['hq_name']=df_index['hq_name'].apply(lambda x:x.replace(u'通达信行业-',''))  
        df_index.index=np.arange(len(df_index))
        return df_index['hq_name']
    
    #获取股票代码对应名称
    def getStockNames(self,df_stock,stockNames):
        #stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name']) 
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
        df_stock['board_name']=stockrelated['board_name']
        return  df_stock['board_name']
    
    #得到标的代码
    def getCode(self,fname):
        s1=pd.read_table(fname,usecols=[0],dtype=str)
                
        codelist=s1['code'].astype('int')
        
        return codelist 
    
    #得到标的数据
    def getRF(self,name,df_stock):
        if name==30:      
            codelist=self.rf30
        else:
            codelist=self.rf200
            
        df_stock['cFlag']=df_stock['hq_code'].isin(codelist)        
        df_stock200=df_stock[df_stock['cFlag']]               
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
        
    
    def getDayData(self):
        #获取每日数据
        df_stock = self.mktstock.MkStockBarHistOneDayGet('',self.tstartdate,self.tenddate)
                      
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>400000',self.tstartdate,self.tenddate,"D")
        
        df_dapan = self.mktindex.MktIndexBarHistDataGet('=399317',self.tstartdate,self.tenddate,"D")
        
        return df_stock,df_index,df_dapan
        
    
    def get5minData(self):
        
        #因为涨幅只能从起始日的后一天开始算，为了对齐数据，把特立独行的起始日延后一天
        tstartdate=self.tstartdate+datetime.timedelta(1)        
        #取得5分钟级别数据
        df_dapan =self.mktindex.MktIndexBarHistDataGet('=399317',sDate_Start=tstartdate,sDate_End=self.tenddate,speriod='5M') 
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>400000',tstartdate,self.tenddate,"5M")
        
        df_stock= self.mktstock.MktStockBarHistDataGet('>=000001',tstartdate,self.tenddate,"5M")
        
        return df_stock,df_index,df_dapan
            
    def calculateChg(self,df_stock,df_index,df_dapan):
             
        def zhouqi(x):
            closeList=x['hq_close'].tolist()
            _open=closeList[0]
            _close=closeList[-1]
            diff=_close-_open
            try:
                chgper=diff/_open
                x['chgper']=chgper
                return x.tail(1)
            except:
                print x
        
        def paixu(x):
            x.sort_values(by=['chgper'], ascending=[0],inplace=True)
            index=np.arange(1,len(x)+1)
            x['paiming']=index
            return x
     
        #计算股票每日涨幅
        df_stock['chg'] = df_stock['hq_close'].groupby(df_stock['hq_code']).diff()
        df_stock['preclose']=df_stock['hq_close'].groupby(df_stock['hq_code']).shift()
        df_stock['chgper']=df_stock['chg']/df_stock['preclose']
        #剔除未复权的错误数据
        errorIndex=df_stock[df_stock.chgper<-0.12].index
        df_stock.drop(errorIndex,inplace=True)
        df_stock.index=df_stock['index']
               
        #计算指数每日涨幅
        df_index['chg'] = df_index['hq_close'].groupby(df_index['hq_code']).diff()
        df_index['preclose']=df_index['hq_close'].groupby(df_index['hq_code']).shift()
        df_index['chgper']=df_index['chg']/df_index['preclose']
        #df_index.index=df_index['index']

        #计算大盘每日涨幅   
        df_dapan.index=df_dapan['index']
        daPanChg = df_dapan['hq_close'].diff()
        daPanPreCLose=df_dapan['hq_close'].shift()
        daPanChgper=daPanChg/daPanPreCLose
        
         
        #拼接板块的代码与名称              
        df_index['hq_name']=self.getIndexNames(df_index)           
        
        #计算股票周期涨幅
        zq_stock=df_stock.groupby('hq_code').apply(zhouqi)
        
        #计算指数周期涨幅
        zq_index = df_index.groupby('hq_code').apply(zhouqi)
        
        #计算大盘周期涨幅
        daPanCloseList=df_dapan['hq_close'].tolist()
        daPanDiff=daPanCloseList[-1]-daPanCloseList[0]
        daPanZqChgper=daPanDiff/daPanCloseList[0]
        
        length=len(daPanChg)
        
        #按交易日期补全数据，停牌涨幅设为-100
        stockGrouped=df_stock[['hq_code','chgper','hq_date']].groupby(df_stock['hq_code'])
        
        df_stock=pd.DataFrame()
        
        for code,data in stockGrouped:
            if len(data)!=length:       
                data=data.reindex(df_dapan.index)
                data['hq_code'].fillna(method='bfill',inplace=True)     
                data['hq_code'].fillna(method='ffill',inplace=True)   
                data['chgper'].fillna(0,inplace=True)       
            df_stock=df_stock.append(data)
       
        #修复索引，防止出错
        df_stock.index=np.arange(len(df_stock))   
                
        #计算每日相对涨幅
        df_stock['xdchgper']=df_stock['chgper'].groupby(df_stock['hq_code']).apply(lambda x: self.xdChg(x,daPanChgper))
        
        #按股票涨幅排序
        df_stock=df_stock.groupby(df_stock['hq_date']).apply(paixu)
               
        #修复索引，防止出错
        #df_index.index=np.arange(len(df_index))
        
        df_index['xdchgper']=df_index['chgper'].groupby(df_index['hq_code']).apply(lambda x: self.xdChg(x,daPanChgper))
        
        #计算周期相对涨幅
        zq_stock['xdchgper']=zq_stock['chgper']-daPanZqChgper
        
        zq_index['xdchgper']=zq_index['chgper']-daPanZqChgper    
                
        #删除部分股价未复权的股票
        #剔除未复权的错误数据
#        errorIndex=zq_stock[zq_stock.chgper<-0.12].index
#        zq_stock.drop(errorIndex,inplace=True)               
                  
        #获取代码对应股票名称
        stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])         
        df_stock['hq_name']=self.getStockNames(df_stock,stockNames)
        zq_stock['hq_name']=self.getStockNames(zq_stock,stockNames)
        
        #得到股票所属板块
        stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')        
        df_stock['board_name']=self.getStockRelated(df_stock,stockrelated)
        zq_stock['board_name']=self.getStockRelated(zq_stock,stockrelated)
            
        #计算周期涨幅排序，并筛选数据 
        zq_stock=zq_stock.sort_values(by=['chgper'], ascending=[0])
        index=np.arange(1,len(zq_stock)+1)
        zq_stock['paiming']=index        
                                         
        #筛选数据
        df_stock=df_stock[['hq_name','hq_date','chgper','xdchgper','board_name','paiming','hq_code']].dropna()
        df_index=df_index[['hq_name','hq_date','chgper','xdchgper']].dropna() 
        daPanChgper.dropna(inplace=True)        
        
        #找到RF200   
        df_stock200=self.getRF(200,df_stock)           
        zq_stock200=self.getRF(200,zq_stock)     

        #定义涨幅与相对涨幅列表
        indexChg=[]
        stockChg=[]
        stock200Chg=[]
        timeList=[]
        dateList=[]
               
        #计算每日排名，加入数据列表
        indexGrouped=df_index.groupby('hq_date')
        for date,data in indexGrouped:
            timeList.append(date.strftime('%Y-%m-%d'))
            dateList.append(date)
            chg=data.sort_values(by=['chgper'], ascending=[0])
            indexChg.append(chg)


        num=0
        for date in dateList:
            #涨幅的添加
            stockData=df_stock[df_stock.hq_date==date]          
            stock200Data=df_stock200[df_stock200.hq_date==date]
            stockChg.append(stockData)
            stock200Chg.append(stock200Data)   
            num+=1             
             
        #计算周期排名
        zq_stock=zq_stock[['hq_name','chgper','xdchgper','board_name','paiming']].dropna() 
        
        zq_stock200=zq_stock200[['hq_name','chgper','xdchgper','board_name','paiming']].dropna()  
          
        zq_index=zq_index[['hq_name','chgper','xdchgper']].sort_values(by=['chgper'], ascending=[0])  
        
        return timeList,indexChg,stockChg,stock200Chg,zq_index,zq_stock,zq_stock200


    #计算Q因子，特立独行
    def calculateFactor(self,dates,df_stock,df_index,df_dapan):
                          
        #拼接板块的代码与名称              
        df_index['hq_name']=self.getIndexNames(df_index)
        
        #拼接股票的代码与名称
        stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name']) 
        df_stock['hq_name']=self.getStockNames(df_stock,stockNames)
        
        #拼接股票关联板块
        stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        df_stock['board_name']=self.getStockRelated(df_stock,stockrelated)
        
        #设置索引，防止groupby出错
        df_stock.index=np.arange(len(df_stock))
               
        #定义存储序列
        iTLDXList=[]
        sTLDXList=[[],[]]
        iQList=[]
        sQList=[]
        sQList200=[]
                      
        for tstartdate in dates:
        #for tstartdate in df_index['hq_date']:            
            tstartdate=datetime.datetime.strptime(tstartdate,'%Y-%m-%d').date()
            
            dapan_price=df_dapan[df_dapan.hq_date==tstartdate]['hq_close']
            
            indexDay=df_index[df_index.hq_date==tstartdate][['hq_name','hq_close','hq_vol','hq_code']]
            
            stockDay=df_stock[df_stock.hq_date==tstartdate][['hq_name','hq_close','hq_vol','board_name','hq_code']]
                            
            indexTLDX=indexDay[['hq_name','hq_close']].groupby(indexDay['hq_name']).agg({'hq_name':'first','hq_close':lambda x:self.tldx(x,dapan_price)}).sort_values('hq_close')                   
            iTLDXList.append(indexTLDX)
            
            stockTLDX=stockDay[['hq_name','hq_close','board_name','hq_code']].groupby(stockDay['hq_name']).agg({'hq_name':'first','hq_close':lambda x:self.tldx(x,dapan_price),'board_name':'first','hq_code':'first'}).sort_values('hq_close')
            sTLDXList[0].append(stockTLDX)
            
            #筛选RF200
            stockTLDX200=self.getRF(200,stockTLDX)        
            sTLDXList[1].append(stockTLDX200)
                            
            #计算指数当日涨幅
            chg = indexDay['hq_close'].groupby(indexDay['hq_name']).diff()
            preclose=indexDay['hq_close'].groupby(indexDay['hq_name']).shift()
            indexDay['chgper']=chg/preclose    
                    
            #计算股票当日涨幅  
            chg = stockDay['hq_close'].groupby(stockDay['hq_name']).diff()
            preclose=stockDay['hq_close'].groupby(stockDay['hq_name']).shift()
            stockDay['chgper']=chg/preclose             
            
            indexDay   = indexDay.groupby(indexDay['hq_name']).apply(self.jsS)
            stockDay   = stockDay.groupby(stockDay['hq_name']).apply(self.jsS)
            
            indexQ = indexDay.groupby(indexDay['hq_name']).apply(self.jsQIndex).sort_values(by=['Q'])
            stockQ = stockDay.groupby(stockDay['hq_name']).apply(self.jsQStock).sort_values(by=['Q'])
            
            #筛选RF200    
            stockQ200=self.getRF(200,stockQ)              
                      
            iQList.append(indexQ)
            sQList.append(stockQ)
            sQList200.append(stockQ200)
            
        return iTLDXList,sTLDXList,iQList,sQList,sQList200
            

    def calculateZq(self,df_stock,df_index,df_dapan): 
               
        def jsQ(x):
            totalvol=x['hq_vol'].sum()
            try:           
                VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
                y=x[x.accumVolPct<=0.2]
                smartvol=y['hq_vol'].sum()
                VWAPsmart=(y['hq_close']*y['hq_vol']).sum()/smartvol 
                x['Q']=VWAPsmart/VWAPall
                x=x[['Q','hq_code']].head(1)
                return x                   
            except ZeroDivisionError:
                pass     
                             
        indexTLDX=df_index[['hq_code','hq_close']].groupby(df_index['hq_code']).agg({'hq_code':'first','hq_close':lambda x:self.tldx(x,df_dapan['hq_close'])}).sort_values('hq_close')  
        stockTLDX=df_stock[['hq_close','hq_code']].groupby(df_stock['hq_code']).agg({'hq_close':lambda x:self.tldx(x,df_dapan['hq_close']),'hq_code':'first'}).sort_values('hq_close')        
       
        #计算指数涨幅
        chg = df_index['hq_close'].groupby(df_index['hq_code']).diff()
        preclose=df_index['hq_close'].groupby(df_index['hq_code']).shift()
        df_index['chgper']=chg/preclose    
                
        #计算股票涨幅  
        chg = df_stock['hq_close'].groupby(df_stock['hq_code']).diff()
        preclose=df_stock['hq_close'].groupby(df_stock['hq_code']).shift()
        df_stock['chgper']=chg/preclose   
                    
#        计算Q因子
#        先计算聪明因子
        df_index   = df_index.groupby(df_index['hq_code']).apply(self.jsS)
        df_stock   = df_stock.groupby(df_stock['hq_code']).apply(self.jsS)     
        
        #指数
        indexQ = df_index[['hq_close','hq_vol','hq_code','accumVolPct']].groupby(df_index['hq_code']).apply(jsQ).sort_values(by=['Q'])
        #股票
        stockQ = df_stock[['hq_close','hq_vol','hq_code','accumVolPct']].groupby(df_stock['hq_code']).apply(jsQ).sort_values(by=['Q'])

        #拼接股票的代码与名称
        stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])
        stockTLDX['hq_name']=self.getStockNames(stockTLDX,stockNames) 
        stockQ['hq_name']=self.getStockNames(stockQ,stockNames)
          
        #拼接股票关联板块
        stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        stockTLDX['board_name']=self.getStockRelated(stockTLDX,stockrelated)  
        stockQ['board_name']=self.getStockRelated(stockQ,stockrelated)
        
        #删除未录入名字的新股
        stockTLDX.dropna(inplace=True)
        stockQ.dropna(inplace=True)
        
        #拼接板块的代码与名称              
        indexTLDX['hq_name']=self.getIndexNames(indexTLDX)
        indexQ['hq_name']=self.getIndexNames(indexQ)     
        
        #筛选RF200   
        stockQ200=self.getRF(200,stockQ)
        
        stockTLDX200=self.getRF(200,stockTLDX)
        
        return indexTLDX,stockTLDX,indexQ,stockQ,stockQ200,stockTLDX200
    
    
    def getAllChg(self,sdate,df_stock,df_dapan):
        
        #计算累计涨幅
        def allChg(data,bdFlag=0):
            if bdFlag==0:
                close0=data['hq_open'].iat[0]
                allChg=data['hq_close']/close0-1      
                return allChg 
            else:
                close0=data['hq_open'].iat[0]
                allChg=data['hq_close']/close0-1    
                data['allchg']=allChg
                return data  
                    
        mktindex = self.mktindex      
        
        sdate=datetime.datetime.strptime(sdate, "%Y-%m-%d")
        #获取每日数据
          #创业板综
        df_cybz   = mktindex.MktIndexBarHistDataGet('=399102',sdate,self.tenddate,"D")
          #上证
        df_sz  = mktindex.MktIndexBarHistDataGet('=000001',sdate,self.tenddate,"D")   
          #深证
        df_cz  = mktindex.MktIndexBarHistDataGet('=399001',sdate,self.tenddate,"D")   
          #中小
        df_zx = mktindex.MktIndexBarHistDataGet('=399101',sdate,self.tenddate,"D")   
          #上证50
        df_sz50  = mktindex.MktIndexBarHistDataGet('=000016',sdate,self.tenddate,"D")  
          #中证500
        df_zz500  = mktindex.MktIndexBarHistDataGet('=399905',sdate,self.tenddate,"D")       
          #沪深300
        df_hs300  = mktindex.MktIndexBarHistDataGet('=399300',sdate,self.tenddate,"D")         
        
      
        #找到RF200，用于计算累计涨幅   
        df_stock200=self.getRF(200,df_stock)[['hq_code','hq_date','hq_close','hq_open']] 
     
        #找到RF30  
        df_stock30=self.getRF(30,df_stock200)[['hq_code','hq_date','hq_close','hq_open']]            
  
        #计算累计涨幅
        allChgdapan=allChg(df_dapan).tolist()
        allChgcybz=allChg(df_cybz).tolist()
        allChgsz=allChg(df_sz).tolist()
        allChgcz=allChg(df_cz).tolist()
        allChgzx=allChg(df_zx).tolist()   
        allChgsz50=allChg(df_sz50).tolist() 
        allChgzz500=allChg(df_zz500).tolist()   
        allChghs300=allChg(df_hs300).tolist() 
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
        avgChg={'399317':[],'200':[],'30':[]}   

        chg30to200=(allChg30-allChg200).tolist()
        chg30toi=(allChg30-allChgdapan).tolist()
        chg30tosz50=(allChg30-allChgsz50).tolist()
        chg30tohs300=(allChg30-allChghs300).tolist()
        chg30tozz500=(allChg30-allChgzz500).tolist()        
        allChg200=allChg200.tolist()
        allChg30=allChg30.tolist()
        
        #得到国证A指收盘价
        daPanCloseList=df_dapan['hq_close']
        
        #得到净值数据
        avgDic={'日期':timeList,'RF200净值':allChg200,'RF30净值':allChg30,'国证A指净值':allChgdapan,'RF30相对RF200':chg30to200,'RF30相对全市场':chg30toi,'RF30相对沪深300':chg30tohs300,'RF30相对上证50':chg30tosz50,'RF30相对中证500':chg30tozz500,'创业板综':allChgcybz,'上证指数':allChgsz,'深证成指':allChgcz,'中小板综':allChgzx,'国证A指':daPanCloseList,'上证50':allChgsz50,'中证500':allChgzz500,'沪深300':allChghs300}
        avg=pd.DataFrame(avgDic)   
        #计算周期净值变化
        chg200=allChg200[-1]
        chg30=allChg30[-1]
        chgi=allChgdapan[-1]
        chg30to200=chg30-chg200
        chgito200=chgi-chg200
        chg30toi=chg30-chgi
        chg200toi=chg200-chgi
        chg30tohs300=chg30-allChghs300[-1]
        chg200tohs300=chg200-allChghs300[-1]
        chgitohs300=chgi-allChghs300[-1]
        
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
        
        return avg,avgChg        
        
        
if __name__=='__main__':
    c=ZH()
    stockd,indexd,dapand=c.getDayData()
    #stockday,dapanday=stockd.copy(),dapand.copy()
    stock5,index5,dapan5=c.get5minData()
    indexZqTLDX,stockZqTLDX,indexQ,stockQ,stockQ200,stockTLDX200=c.calculateZq(stock5,index5,dapan5)
    (zfTimes,indexChg,stockChg,stock200Chg,indexZqChg,stockZqChg,stock200ZqChg)=c.calculateChg(stockd,indexd,dapand)
    (iTLDX,sTLDX,iQList,sQList,sQList200)=c.calculateFactor(zfTimes,stock5,index5,dapan5)
    
    num=0
    for t in zfTimes:
        p=picZH.picExcel(t,indexChg[num],stockChg[num],stock200Chg[num],iTLDX[num],sTLDX[0][num],sTLDX[1][num],iQList[num],sQList[num],sQList200[num])
        p.picModel()
        #p.update()
        num+=1
        print num
        
    sdate=zfTimes[0]
    edate=zfTimes[-1]   
    period=sdate+'至'+edate            
    p=picZqZH.picExcel(period,indexZqChg,stockZqChg,stock200ZqChg,indexZqTLDX,stockZqTLDX,stockTLDX200,indexQ,stockQ,stockQ200)
    wbk=p.picModel(1)


    sdate=c.tstartdate.strftime("%Y-%m-%d")
    #edate=c.tenddate.strftime("%Y-%m-%d")
    period=sdate+'至'+edate
    avg,avgChg=c.getAllChg(sdate,stockd,dapand)
    pte = plotToExcel.PlotToExcel()
    pte.bulidAvg(avg,avgChg,period,wbk)
    #p.update()
# 
    

