# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 13:56:58 2017

@author: rf_hs
"""

from sqlalchemy import create_engine 

from pandas import Series

import tushare as ts

import os

import pandas as pd
import numpy as np

import datetime,time

import sys

reload(sys)
          
sys.setdefaultencoding('utf-8')

class MktStockHandle:
    
    def __init__(self):
        
        #数据库引擎
        self.engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        self.tradingtimePath = "\\Data\\History\\Stock\\股票时间数据\\股票交易日期\\"
        
        #获取股票上市时间
        self.queryInMarketSql  = "select stock_code,stock_InMarketDate from stockbaseinfo"
        
        #获取股本信息
        self.queryGbinfoSql  = "select 股票代码,权息日,前流通盘,后流通盘,前总股本,后总股本 from stockgbinfo"
        
        #获取所有股本股票代码
        self.queryGbcodeSql  = "select distinct 股票代码 from stockgbinfo"
        
        #获取指定时间股票
        self.querystocksql   = "select st.stock_code,st.stock_name from stockbaseinfo as st where UNIX_TIMESTAMP(st.stock_InMarketDate)<="
      
        self.testpath        = "d:\\test\\test.csv"
      
    #获取所有交易时间
    
    def MkStockAllGbData(self,gbtime):
        
        #取出股本表中的所有股票
        sgb_engine = self.engine
        
        sgbstockdata = pd.DataFrame()
        
        sqlstr = self.queryGbcodeSql
        
        # 获取所有股票上市时间，获取所有交易时间，获取所有股本信息
        
        # 获取所有股票上市时间
        stockInMarketdata = mkstcok.MkStockInMarketTime()
        
        #获取所有交易时间
        allTimedata = mkstcok.MkStockAllTradingTime()
        
        #获取所有股本信息
        allgbdata   = mkstcok.MkStockGbData()
    
        #获取股本表中的所有股票的代码
        
        with sgb_engine.connect() as conn, conn.begin():
            sgbstockdata = pd.read_sql_query(sqlstr, conn)
        
        # 读取所有股票代码
        print sgbstockdata.columns
        
        sgbcodes = sgbstockdata[u'股票代码']
        
        sgbcodelist = sgbcodes.tolist()
        
        count = 0 
        
        allgbdf = pd.DataFrame()
        
        gbcount = 0
        
        for sgblist in sgbcodelist:
            
            # 按股票来区分上市时间，通过对比上市时间与股本时间，获取2000年之后的股本数据
            
            count =  count +1 
            
            print sgblist
            
            print count
            
            # 股票上市时间
            smktime = stockInMarketdata.ix[stockInMarketdata.stock_code==sgblist,:]
            
            smkseries = smktime.values.flatten()
            
            # 股本时间
        
            sgbtime = allgbdata.ix[allgbdata['股票代码']==sgblist,:]
            
            sgbtime = sgbtime.drop_duplicates(['权息日'])
  
            
            sgbtime.sort_values(by=['权息日'],ascending=[0],inplace=True)
            
            sgbtime_more = sgbtime.ix[sgbtime['权息日']>=gbtime,:]
            
            sgbtime_less = sgbtime.ix[sgbtime['权息日']<gbtime,:]
            
            #取出最后一条 股本数据
              
            lastsgb      = sgbtime.tail(1)
            
            fristgb_more = sgbtime_more.head(1)
            
            lastsgb_more = sgbtime_more.tail(1)
            
            lastsgb_less = sgbtime_less.head(1)
            
            lastraw  = lastsgb.values.flatten()
            
            lastraw_more = lastsgb_more.values.flatten()
            
            lastraw_less = lastsgb_less.values.flatten()   
             
            allTimedata.sort_values(by=['Time'],ascending=[0],inplace=True)
            
            sjytime = allTimedata.ix[allTimedata['Time']>=gbtime,:]
            
            sjytime_less = allTimedata.ix[allTimedata['Time']<gbtime,:]
            
            
            firstjytime = sjytime_less.head(1)
            
            #补充第一条数据进行合并
            
            sjylesstimeseries = firstjytime.values.flatten()
            
            sjylesstimeseries = sjylesstimeseries + " 00:00:00"
            
            sjylesstimelist   = sjylesstimeseries.tolist()
            
            sjytimeseries = sjytime.values.flatten()
            
            sjytimeseries = sjytimeseries + " 00:00:00"
            
            sjytimelist   = sjytimeseries.tolist()
            
            sjytimelist.extend(sjylesstimelist)
            
            gbreindex = pd.to_datetime(sjytimelist)
            
        
            #重新设置sgbtime的index
            sgbtimeseries = sgbtime_more['权息日'].values.flatten()
            
            sgbtimelist   = sgbtimeseries.tolist()
            
            
            gbdata = sgbtime_more.values
            
            if count<=1:
                fristgb_more.to_csv(self.testpath,mode='w')
            else:
                fristgb_more.to_csv(self.testpath,mode='a',header=False)

            # 取出股票上市时间
            if len(smkseries)>=2 and len(lastraw)>=2:
                        
                smktime  = smkseries[1]
                
                lastrawtime = lastraw[1]
                
                
                if lastrawtime>smktime:
                    lastsgb['权息日'] = smktime
                                    
                #2000之前的一行
                if len(lastraw_less)>=2:
                    #最后一日的股本赋值过来
                    lastsgb_less['权息日'] =sjylesstimelist[0]
                    
                    gbdata = np.concatenate([gbdata,lastsgb_less.values])
                    
                else:
                                    
                    if lastrawtime>smktime:
                        
                        lastsgb['权息日'] = smktime
                        
                        sgbtimelist.append(str(smktime))
                        
                        gbdata = np.concatenate([gbdata,lastsgb.values])
                        
                        
                    lastsgb_more['权息日'] =sjylesstimelist[0]
                    
                    lastsgb_more.iloc[:,2:] = 0
                    
                    gbdata = np.concatenate([gbdata,lastsgb_more.values])
            
                gbcolumns  = sgbtime_more.columns
                
                
                sgbtimelist.extend(sjylesstimelist)
                
                sgbreindex = pd.to_datetime(sgbtimelist)
                    
                
                gbdf       = pd.DataFrame(gbdata,columns=gbcolumns,index=sgbreindex)
                        
                #gbdf.sort_values(by=['权息日'],ascending=[0],inplace=True)
                
                gbdf       = gbdf.reindex(gbreindex)    
                
                print gbdf.columns
                
                gbdf.fillna(method='bfill',inplace=True)
                
                if gbcount==0:
                    allgbdf = gbdf.loc[:,[u'前流通盘',u'后流通盘',u'前总股本',u'后总股本']]
                else:
                    allgbdf = allgbdf + gbdf.loc[:,[u'前流通盘',u'后流通盘',u'前总股本',u'后总股本']]
                  #  allgbdf = allgbdf  + gbdf[u'前流通盘',u'后流通盘',u'前总股本',u'后总股本']
                    
                gbcount = gbcount + 1
                
        return allgbdf
        
    
    def MkStockAllTradingTime(self):
        
        testpath = "C:\\Users\\rf_hs\\Desktop\\PySAP-Master\\Pycode\\SAP\\Data\\History\\Stock\\股票时间数据\\股票交易日期\\"
        
        testpath = testpath+"交易时间.txt"
        
        tradingtime_path = os.getcwd()+self.tradingtimePath +"交易时间.txt"
        
        timedf =  pd.read_csv(testpath.decode())
    
        return timedf
        
    #获取所有股票上市时间
    def MkStockInMarketTime(self):
        
        stock_engine = self.engine
        
        sInMarketdata = pd.DataFrame()
        
        with stock_engine.connect() as conn, conn.begin():
            stockInMarketdata = pd.read_sql_table('stockbaseinfo', conn,columns=['stock_code','stock_InMarketDate'])
        
        sInMarketdata= stockInMarketdata
        
        return sInMarketdata
    
    #获取所有股本数据
    def MkStockGbData(self):
        
        stock_engine = self.engine
        
        sgbdata = pd.DataFrame()
        
        sqlstr = self.queryGbinfoSql
        
        with stock_engine.connect() as conn, conn.begin():
            sgbdata = pd.read_sql_query(sqlstr, conn)
        
        return sgbdata    
      
    #获取指定时间所有股票
        
    def MkStockOneDayGet(self,sDate):
        
        sDate = sDate.strftime( '%Y-%m-%d' )
        
        sTime = '00:00:00'
        
        strstart = ' UNIX_TIMESTAMP(\''+sDate +' ' + sTime+'\')'
        
        sgb_engine = self.engine
        
        sqlstr = self.querystocksql + strstart
        
        with sgb_engine.connect() as conn, conn.begin():
            stockOneDaydata = pd.read_sql_query(sqlstr, conn)
        
        print sqlstr
        
        return stockOneDaydata
        
        
    def MkStockBarHistOneDayGet(self,securityID,sDate,sEndDate):
        
        if securityID=='':
            securityID='>=000001'
            
        
        sDate = sDate.strftime( '%Y-%m-%d' )
        
        sEndDate = sEndDate.strftime( '%Y-%m-%d' )
        
        sTime_Start = '00:00:00'
        
        sTime_End   = '15:00:00'
            
        strstart = ' UNIX_TIMESTAMP(\''+sDate +' ' + sTime_Start+'\')'
        
        strend = 'UNIX_TIMESTAMP(\''+sEndDate +' '+sTime_End+'\')'
        
        if securityID=="":
            mkquerysql = "select * from hstockquotationday as hiq where "
        else:
            mkquerysql = "select * from hstockquotationday as hiq where hiq.hq_code " + securityID + " and "
                  
        strsql = " UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
       
        mkquerysql = mkquerysql + strsql
       
        print mkquerysql
        
        mkdf = pd.DataFrame()     
        
        if mkquerysql:
           mkdf = pd.read_sql(mkquerysql,self.engine)
        
        return  mkdf     
        
    # 统一获取股票历史交易数据接口
    def MktStockBarHistDataGet(self,securityID,sDate_Start,sDate_End,speriod):
        
        t1=time.time()
        
        sDate_Start = sDate_Start.strftime( '%Y-%m-%d' )
        
        sDate_End = sDate_End.strftime( '%Y-%m-%d' )        
                
        sTime_Start = '00:00:00'
               
        if speriod=="D":                 
           sTime_End = '00:00:00'
        else:
           sTime_End = '15:00:00'
            
        strstart = ' UNIX_TIMESTAMP(\''+sDate_Start +' ' + sTime_Start+'\')'
        
        strend = 'UNIX_TIMESTAMP(\''+sDate_End +' '+sTime_End+'\')'
        
        # 处理1分钟数据       
        if speriod=="1M":
                        
           mkquerysql = "select * from hstockquotationone as hiq where hiq.hq_code " + securityID
                                       
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
           
        # 处理5分钟数据       
        if speriod=="5M":
            
           mkquerysql = "select * from hstockquotationfive as hiq where hiq.hq_code " + securityID
           
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
         
        # 处理15分钟数据       
        if speriod=="15M":
            
           mkquerysql = "select * from hstockquotationfifteen as hiq where hiq.hq_code " + securityID
      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
           
        # 处理30分钟数据       
        if speriod=="30M":
            
           mkquerysql = "select * from hstockquotationthirty as hiq where hiq.hq_code " + securityID
           
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
        # 处理60分钟数据       
        if speriod=="60M":
            
           mkquerysql = "select * from hstockquotationsixty as hiq where hiq.hq_code " + securityID
           
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
        
        # 处理日线数据       
        if speriod=="D":
            
           mkquerysql = "select * from hstockquotationday_frights as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
                  
        
        mkdf = pd.DataFrame()     
        
        if mkquerysql:
           mkdf = pd.read_sql(mkquerysql,self.engine)
           
        return  mkdf   
        
        print time.time()-t1
        
class MktIndexHandle:
    
    #所有指数代码列表
    '''    
        Bz_Name	       Bz_Code	Bz_IndexCode	Bz_Lft	Bz_Rgt	Bz_Pid	Bz_IsIndex
        板块指数	        800	       0	    1	   28	    0	0
        风格	             801	       0	    2	    5	    1	0
        通达信风格     	80101	   0    3	    4	    2	0
        行业	             802	       0	    6	    13	    1	0
        通达信行业    	    80201       0	    7	    10	    4	0
        通达信细分行业	    80202	   0	    8	    9	    5	0
        申万二级行业	    80203	   0	    11	   12	    4	0
        概念	             803	       0	    14	   17	    1	0
        通达信概念   	    80301       0	    15	   16	    8	0
        地域	             804	       0	    18	   21	    1	0
        通达信地域	        80401	       0	    19	   20	    10	0
        规模	             805	       0	    22	   23	    1	0
        综合	             806	       0	    24	   25	    1	0
        中证（股指）	    807	       0	    26	   27	    1	0
    '''
       
    def __init__(self):
        
        #数据库引擎
        self.engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        
        self.tradingtimePath = "\\Data\\History\\Stock\\股票时间数据\\股票交易日期\\"
        
        #求出指数下属，指数
        
        self.boardNodesql ='''select boardnode.bz_name,boardnode.bz_indexcode from bzcategory as boardnode,bzcategory as parentnode
                                     where boardnode.bz_lft between parentnode.bz_lft and parentnode.bz_rgt and boardnode.Bz_IsIndex=1
                                     and boardnode.bz_pid=parentnode.bz_id and parentnode.bz_code=
                           '''
        #求出指数关联股票
        self.rbsql        = "select board_id,board_name,stock_id,stock_name from boardstock_related where " 
    
        #获取指定板块Id,4位数以上均为二级行业
        
        self.boardIdsql        = "select bz_id from bzcategory where bz_code>1000 and bz_code= " 
        
        #股票关联与板块进行内连接
        self.rbsinnersql       = '''select bsr.board_id,bsr.board_name,bsr.stock_id,bsr.stock_name from boardstock_related as bsr inner join bzcategory as bzg 
                                       on bzg.bz_indexcode = bsr.board_id and bzg.bz_pid=
                                 '''
        self.mktstock          = MktStockHandle()
     # 股票倒推到指定的板块指数
    def MktStocksToIndexClassify(self,stockList,indexID):
        
        spidsql  = self.boardIdsql  + indexID
        
        print spidsql
        
        sclassdf = pd.DataFrame()     
        
        spidf    = pd.DataFrame()   
        
        stockdf  = pd.DataFrame()  
        
        if spidsql:
            
           spidf = pd.read_sql(spidsql,self.engine)
           
           # df 不为空时，取指数对应股票,如果为空 要么没数据，要么是叶子节点
           if not spidf.empty:
               
               pids = spidf['bz_id']
               
               pidlist = pids.tolist()
               
               if len(pidlist)>=1:
                  #获取父节点id
                  pid = pidlist[0]
                  
                  sclassql = self.rbsinnersql  + str(pid)
                  
                  sclassdf = pd.read_sql(sclassql,self.engine)
                  
                  sids  = sclassdf['stock_id']
                  
                  sidflags = sids.isin(stockList)
                  
                  sclassdf['sid_flag'] = sidflags
                  
                  stockdf =  sclassdf[sclassdf['sid_flag']]
                
        return  stockdf     
        
     # 指数分类，得到指数下的股票
    def MktIndexToStocksClassify(self,indexID):
        
        # 获取指数下所有指数分类
        iclassSql = self.boardNodesql + indexID +" order by boardnode.bz_lft"
        
        print iclassSql
        
        iclassdf = pd.DataFrame()     
        
        rbsdf    = pd.DataFrame()     
        
        if iclassSql:
            
           iclassdf = pd.read_sql(iclassSql,self.engine)
           
           # df 不为空时，取指数对应股票,如果为空 要么没数据，要么是叶子节点
           if not iclassdf.empty:
               
               #获取板块下属指数代码
               icodes   = iclassdf['bz_indexcode']
               
               icodeslist = icodes.tolist()
               
               # list计数
               icount = 0   
               
               tmpstr ="board_id in ("
               
               #遍历所有指数，求出关联股票
               for iclist in icodeslist:
                   
                   icount = icount+1
                   
                   if icount==1: 
                      tmpstr = tmpstr + str(iclist)
                   else:
                      tmpstr = tmpstr + ", " + str(iclist)
               
               tmpstr = tmpstr +')'
               
               if icodeslist:
                   rbsql = self.rbsql + tmpstr
                   
                   print rbsql
                   
                   rbsdf = pd.read_sql(rbsql,self.engine)
           else:
                tmpstr = "board_id= " + indexID
                
                rbsql = self.rbsql + tmpstr
                
                rbsdf = pd.read_sql(rbsql,self.engine)
                 
        return  iclassdf,rbsdf
     
    def MkIndexAllTradingTime(self):
        
        testpath = u"E:\\SVN\\PySAP-Master\\Data\\History\\Stock\\股票时间数据\\股票交易日期\\"
        
        testpath = testpath+"交易时间.txt"
        
        tradingtime_path = os.getcwd()+self.tradingtimePath +"交易时间.txt"
        
        timedf =  pd.read_csv(testpath.decode())
    
        return timedf
        
       
    def MktIndexBarHistOneDayGet(self,securityID,sDate,sEndDate):
        
        sDate = sDate.strftime( '%Y-%m-%d' )
        
        sEndDate= sEndDate.strftime( '%Y-%m-%d' )
        
        sTime_Start = '00:00:00'
        
        sTime_End   = '15:00:00'
            
        strstart = ' UNIX_TIMESTAMP(\''+sDate +' ' + sTime_Start+'\')'
        
        strend = 'UNIX_TIMESTAMP(\''+sEndDate +' '+sTime_End+'\')'
        
        mkquerysql = "select * from hindexquotationday as hiq where hiq.hq_code =" + securityID
                  
        strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
       
        mkquerysql = mkquerysql + strsql
       
        print mkquerysql
        
        mkdf = pd.DataFrame()     
        
        if mkquerysql:
           mkdf = pd.read_sql(mkquerysql,self.engine)
        
        return  mkdf
        
    # 统一获取股票历史交易数据接口
    def MktIndexBarHistDataGet(self,securityID,sDate_Start,sDate_End,speriod,sTime_Start='00:00:00',sTime_End='15:00:00'):
        
        sDate_Start= sDate_Start.strftime( '%Y-%m-%d' )
        
        sDate_End = sDate_End.strftime( '%Y-%m-%d' )  
        
        if sTime_Start=='':
            sTime_Start = '00:00:00'
        
        if sTime_End=='':        
            if speriod=="D":                 
               sTime_End = '00:00:00'
            else:
               sTime_End = '15:00:00'
            
        strstart = ' UNIX_TIMESTAMP(\''+sDate_Start +' ' + sTime_Start+'\')'
        
        strend = 'UNIX_TIMESTAMP(\''+sDate_End +' '+sTime_End+'\')'
        
        
        # 处理1分钟数据       
        if speriod=="1M":
            
           mkquerysql = "select * from hindexquotationone as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
           
        # 处理5分钟数据       
        if speriod=="5M":
            
           mkquerysql = "select * from hindexquotationfive as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
         
        # 处理15分钟数据       
        if speriod=="15M":
            
           mkquerysql = "select * from hindexquotationfifteen as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
        # 处理30分钟数据       
        if speriod=="30M":
            
           mkquerysql = "select * from hstockquotationthirty as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
           
        # 处理60分钟数据       
        if speriod=="60M":
            
           mkquerysql = "select * from hstockquotationsixty as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           print mkquerysql
        
        # 处理日线数据       
        if speriod=="D":
            
           mkquerysql = "select * from hindexquotationday as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           print mkquerysql
                  
        
        mkdf = pd.DataFrame()     
        
        if mkquerysql:
           mkdf = pd.read_sql(mkquerysql,self.engine)
           
        return  mkdf 
      
    # 统一获取规模指数历史交易数据接口
    def MktScaleIndexBarHistDataGet(self,securityID,sDate_Start,sDate_End,speriod,sTime_Start='00:00:00',sTime_End='15:00:00'):
        
        sDate_Start= sDate_Start.strftime( '%Y-%m-%d' )
        
        sDate_End = sDate_End.strftime( '%Y-%m-%d' )  
        
        if sTime_Start=='':
            sTime_Start = '00:00:00'
        
        if sTime_End=='':        
            if speriod=="D":                 
               sTime_End = '00:00:00'
            else:
               sTime_End = '15:00:00'
            
        strstart = ' UNIX_TIMESTAMP(\''+sDate_Start +' ' + sTime_Start+'\')'
        
        strend = 'UNIX_TIMESTAMP(\''+sDate_End +' '+sTime_End+'\')'
        
        
        # 处理1分钟数据       
        if speriod=="1M":
            
           mkquerysql = "select * from hscaleindexquotationone as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
           
        # 处理5分钟数据       
        if speriod=="5M":
            
           mkquerysql = "select * from hscaleindexquotationfive as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
         
        # 处理15分钟数据       
        if speriod=="15M":
            
           mkquerysql = "select * from hscaleindexquotationfifteen as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
        # 处理30分钟数据       
        if speriod=="30M":
            
           mkquerysql = "select * from hscaleindexquotationthirty as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           
           print mkquerysql
           
        # 处理60分钟数据       
        if speriod=="60M":
            
           mkquerysql = "select * from hscaleindexquotationsixty as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           print mkquerysql
        
        # 处理日线数据       
        if speriod=="D":
            
           mkquerysql = "select * from hscaleindexquotationday as hiq where hiq.hq_code " + securityID
                      
           strsql = " and UNIX_TIMESTAMP(hiq.index)>="+strstart +" and UNIX_TIMESTAMP(hiq.index)<=" +strend 
           
           mkquerysql = mkquerysql + strsql
           print mkquerysql
                  
        
        mkdf = pd.DataFrame()     
        
        if mkquerysql:
           mkdf = pd.read_sql(mkquerysql,self.engine)
           
        return  mkdf 


#聪明钱，特立独行，涨跌幅 同步进行
if '__main__'==__name__:  
    import cmq240
       
#    apply函数，计算smartS
    def jsS(x):
        x['smartS']=x['chgper']/np.sqrt(x['hq_vol'])*100000
        x.sort_values(by=['smartS'], ascending=[0],inplace=True) 
        x['accumVolPct']=x['hq_vol'].cumsum()/x['hq_vol'].sum()
        return x         
        
    #计算Q因子
    def jsQ(x):
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
    
    #特立独行处理函数，用于apply
    def tldx(x,series):
        if not x.any():
            xishu=np.nan
        else:
            nx=x.shift()
            bl=nx/x
            y=np.log(bl).dropna() 
            y.index = np.arange(len(y))   
            try:
                xishu=(np.mean(series*y)-np.mean(series)*np.mean(y))/(np.std(series)*np.std(y))
            except:
                xishu=10
        return np.abs(xishu)      

    def getCode():
#        def inttostr(x):
#            if len(x)<6:
#                if 6-len(x)==3:
#                    x='000'+x
#                elif 6-len(x)==2:
#                    x='00'+x
#                elif 6-len(x)==1:
#                    x='0'+x
#                elif 6-len(x)==4:
#                    x='0000'+x
#                else:
#                    x='00000'+x
#            return x
        #代码文件目录
        fname=r'E:\test\code\code.txt'
        
        s1=pd.read_table(fname,usecols=[0],names=['code'],dtype=str)
                
        codelist=s1['code'].astype('int')
        
        return codelist
           
    #定义apply函数，获得版块每天的涨幅比
    def zf(x):
        nx=x.shift()
        bl=nx/x
        index_list=np.log(bl).dropna().tolist()             
        return index_list
    
    def xdchg(x,indexchg):
        if len(x)!=240:  
            print x
            return x
        else:
            indexchg.index=x.index
            x=x-indexchg
            return x
    
    t=time.time()
    
    engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
    
    mkstock = MktStockHandle()
    
   # mkindex = MktIndexHandle()   
    mkindex = MktIndexHandle()
           
    #定义起始终止日期
    
    tstartdate=datetime.datetime(2017,5,20)
    
    tenddate=datetime.datetime(2017,5,22)
    
    #取全市场股票的where条件
    allcode='>=000001'
    
    #获得股票名称
    #stockNames=pd.read_sql_table('stockbaseinfo',con=engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])
    
    #取200支标的股票代码列表，用于在dataframe中筛选
    code200=getCode()
    
    #取得起始日至终止日的所有版块数据
    index =mkindex.MktIndexBarHistDataGet('399317',sDate_Start=tstartdate,sDate_End=tenddate,speriod='1M')
    
    #按日期groupby,计算每天的涨幅比
    index['chg'] = index['hq_close'].groupby(index['hq_date']).diff()
    index['preclose']=index['hq_close'].groupby(index['hq_date']).shift()
    index['chgper']=index['chg']*100/index['preclose']
    index=index[['hq_date','hq_close','chgper']].groupby(index['hq_date']).agg({'hq_date':'first','chgper':lambda x:x.tolist(),'hq_close':lambda x:zf(x)})
    
    #得到日期序列，用于EXCEL作图
    dateList=index['hq_date'].apply(lambda x:x.strftime( '%Y-%m-%d' ))
        
    #遍历每一天，处理数据   
    dataList=[[],[]]
    QList=[[],[]]
    for tstartdate in index['hq_date']:     
        t1=time.time()             
        SList=[[],[]]
        chgList=[[],[]]
        timeList=[]
        #获取当天板块的涨幅序列
        index_price=index[index.hq_date==tstartdate]['hq_close']
        index_price=pd.Series(index_price[0])                      
        #得到全市场股票的dataframe
        stockList        = mkstock.MktStockBarHistDataGet(allcode,tstartdate,tstartdate,"1M")
        #拼接股票代码与名称
#        stockList['hq_code']=stockList['hq_code'].astype('int')
#        stockList.index=stockList.hq_code
#        stockList['hq_name']=stockNames['stock_name']
        
        #stockList.dropna(inplace=True)
        stockList['chg'] = stockList['hq_close'].groupby(stockList['hq_code']).diff()
        stockList['preclose']=stockList['hq_close'].groupby(stockList['hq_code']).shift()
        stockList['chgper']=stockList['chg']*100/stockList['preclose']
        index_chgper=index[index.hq_date==tstartdate]['chgper']
        index_chgper=pd.Series(index_chgper[0])  
        stockList['xdchgper']=stockList['chgper'].groupby(stockList['hq_code']).apply(lambda x:xdchg(x,index_chgper))
        
        stockList        = (stockList.groupby(stockList['hq_code']).apply(jsS)) 
        
        #分钟级别监控部分
        
        sdata=stockList[(stockList.hq_vol>1000)&(stockList.smartS!=0)]
        #(stockList.hq_vol>1000)&
        
        #筛选出200支标的
        cFlag=sdata['hq_code'].isin(code200)        
        sdata['cFlag']=cFlag
        data200=sdata[sdata['cFlag']]        
        
        data200=data200[['hq_name','smartS','chgper','xdchgper']].groupby(data200['hq_time'])
        sdata=sdata[['hq_name','smartS','chgper','xdchgper']].groupby(sdata['hq_time'])
        
        #添加全市场数据
        num=0
        for time1,data in sdata:
            #因为通达信数据延迟一分钟，此处平移数据对齐正确时间
            num+=1
            if num==1:
                timeList.append(str(time1).split()[-1])                
                continue
            if num==len(sdata):
                chgPer=data.sort_values(by=['chgper'], ascending=[0])   
                chgList[0].append(chgPer)           
                data.sort_values(by=['smartS'], ascending=[0],inplace=True)           
                SList[0].append(data)
                break
            timeList.append(str(time1).split()[-1]) 
            chgPer=data.sort_values(by=['chgper'], ascending=[0])   
            chgList[0].append(chgPer)           
            data.sort_values(by=['smartS'], ascending=[0],inplace=True)           
            SList[0].append(data)
            if num==238:
                print num
            
            
        #添加200标的数据
        num=0
        for time1,data in data200:
            num+=1
            if num==1:
                continue            
            chgper200=data.sort_values(by=['chgper'], ascending=[0])   
            chgList[1].append(chgper200)         
            data.sort_values(by=['smartS'], ascending=[0],inplace=True)
            SList[1].append(data)
  
        
        tstartdate=tstartdate.strftime( '%Y-%m-%d' )
        p=cmq240.picExcel(timeList,SList,tstartdate,chgList)#,chgList
        p.picModel()
        #p.update()


        #日级别监控部分
        #计算Q因子
        Qdata = stockList.groupby(stockList['hq_code']).apply(jsQ)
        
        Qdata.sort_values(by=['Q'],inplace=True)
        
        cFlag=Qdata.hq_code.isin(code200)       
        Qdata['cFlag']=cFlag
        Qdata200=Qdata[Qdata['cFlag']]  
        
        QList[0].append(Qdata)
        QList[1].append(Qdata200)
             
        #处理全市场数据                     
        dayData=stockList[['hq_name','hq_close','hq_code']].groupby(stockList['hq_code']).agg({'hq_name':'first','hq_close':lambda x:tldx(x,index_price),'hq_code':'first'})
        dayData.sort('hq_close',inplace=True)
              
        dayData.dropna(inplace=True)
    
        cFlag=dayData.index.isin(code200)       
        dayData['cFlag']=cFlag
        dayData200=dayData[dayData['cFlag']]

        #dayData=dayData.head(300)
        dataList[0].append(dayData)
        dataList[1].append(dayData200)
        
        print '一个周期'
        print time.time()-t1
     
    #画EXCEL
    from tldxday import picExcel
  
    p=picExcel(dataList,dateList,QList)
    p.picModel()
    p.update()
    
    print 'OVER'
    print time.time()-t



