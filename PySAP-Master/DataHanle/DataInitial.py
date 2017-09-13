# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 11:46:55 2017

@author: rf_hs
"""
#!/usr/bin/env Python
# coding=utf-8
"""
Created on Mon Feb 27 12:21:33 2017

@author: rf_hs
"""

from sqlalchemy import create_engine 


import sys
sys.path.append("..")
reload(sys)
sys.setdefaultencoding('utf8')

from DataHanle.MktDataHandle import MktIndexHandle

import os

import pandas as pd

import numpy as np

class DataInitial:
      
      def __init__(self):
                          
        #通达信概念个股
        self.TdxConceptDir = '\\BoardIndex\\config\\通达信（行业、概念）\\通达信概念\\'  
        
        #通达信行业个股
        self.TdxLineDir = '\\BoardIndex\\config\\通达信（行业、概念）\\通达信行业\\'   
        
        #通达信股票，细分行业
        self.TdxStock = '\\BoardIndex\\config\\通达信（行业、概念）\\沪深Ａ股.txt'
        
        #通达信股票，细分行业
        self.TdxConcept = '\\BoardIndex\\config\\通达信（行业、概念）\\通达信概念.txt'
        
        #通达信股票，细分行业
        self.TdxLine = '\\BoardIndex\\config\\通达信（行业、概念）\\通达信行业.txt'
        
        #通达信股票，细分行业
        self.TdxXfLine = '\\BoardIndex\\config\\通达信（行业、概念）\\通达信细分行业.txt'
                
        # 板块节点路径
        self.BInit_FILENAME = '\\BoardIndex\\config\\板块节点配置.txt' 
        
        #数据库名称
        self.bzcategory     ='bzcategory'
        
        self.stockbaseinfo  = 'stockbaseinfo'
        
        self.boardstock_related = 'boardstock_related'
        
        #数据库引擎
        self.engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        self.mktindex = MktIndexHandle()
        
      def setDirFiletoSql(self,fdir,tdxdict):
          
          flist = os.listdir(fdir)
          
          fcount = 0
          
          bsrdf  = pd.DataFrame()
        
             
          for fl in  flist:
              
            fnamepos  = fl.find('20')
            
            fcount += 1
            
            print fl
             
            print fcount
            
            if fnamepos>0:
                
               board_name =  fl[:fnamepos] 
                
               fname  = fdir + str(fl)
               
               if tdxdict.has_key(board_name): 
                   
                   tdxitem = tdxdict[board_name]
                   
                   board_id = tdxitem['Code'].tolist()[0]
                                        
                   try:                        
                      fdata=pd.read_table(fname,header=0,encoding='utf-8')
                   except:
                      fdata=pd.read_table(fname,header=0,encoding='gbk')
                    
                   fdata = fdata[:-1]
                   
                   tmpdf = fdata[[u'代码',u'名称']]
                   
                   tmpdf['board_id'] = board_id
                   
                   tmpdf['board_name'] = board_name
                   
                   tmpdf.rename(columns={u'代码': 'stock_id', u'名称': 'stock_name'}, inplace=True) 
                   
                   bsrdf  = bsrdf.append(tmpdf)
          
          #test = dict(list(bsrdf.groupby('board_id')))
          
          if len(bsrdf)>0:                  
             bsrdf.to_sql(self.boardstock_related,con=self.engine,index=False,if_exists='append')
          
              
      
      def stockLineDeal(self,dictTuple):
          
          
          tdxLine_dict = dictTuple[0]
            
          #通达信细分行业字典
          tdxXfline_dict = dictTuple[1]
            
          #通达信概念字典
          tdxConcept_dict = dictTuple[2]
          
          tmpdf  = pd.DataFrame()
          
          bsrdf  = pd.DataFrame()
          
          #通达信共用头，更新个股
          sheader =['stock_code','stock_name','stock_xfline']  
                    
          pwd   =  os.getcwd()
        
          fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep)
          
          # 通达信所有股票
          allstockfname = fpwd + self.TdxStock
          
          #通达信行业目录
          tdxLineDir  =  fpwd + self.TdxLineDir
          
          tdxLineDir  =  tdxLineDir.decode('utf-8')
          
          #通达信行业目录
          tdxConceptDir  = fpwd + self.TdxConceptDir
          
          tdxConceptDir  = tdxConceptDir.decode('utf-8')
            
          #插入通达信行业
          try:                        
             stockLineData=pd.read_table(allstockfname.decode('utf-8'),header=0,names=sheader,delimiter=',',encoding='utf-8')
          except:
             stockLineData=pd.read_table(allstockfname.decode('utf-8'),header=0,names=sheader,delimiter=',',encoding='gbk')
          
          stockDatadf = stockLineData[['stock_code','stock_name']]
          
          stockDatadf['stock_InMarketDate'] ='1990-01-01'
          
          stockDatadf['stock_BelongMarket'] ='SH-SZ'
          
          stockDatadf['stock_MarketSymbol'] ='SH-SZ'
          
          stockDatadf['stock_MarketBlocks'] ='SH-SZ'
          
          delstr_sbinfo = "truncate table  " + self.stockbaseinfo
 
          try:
             pd.read_sql_query(delstr_sbinfo,con=self.engine)
        
          except:
             print delstr_sbinfo
        
          stockDatadf.to_sql(self.stockbaseinfo,con=self.engine,index=False,if_exists='append')
          
          #更新行业，细分行业以及概念       
          #处理细分行业
          stockDatadf = stockLineData[['stock_code','stock_name','stock_xfline']]
          
          stockData_group = stockDatadf.groupby('stock_xfline')
          
          sxfline_dict    = dict(list(stockData_group))                   
          
          for tdxfd in tdxXfline_dict:
              
              tdxitem  = tdxXfline_dict[tdxfd]
             
              if sxfline_dict.has_key(tdxfd):
                                     
                 board_id = tdxitem['Code'].tolist()[0]
                 
                 tmpdf  = sxfline_dict[tdxfd]
              
                 tmpdf['board_id'] = board_id
                 
                 bsrdf = bsrdf.append(tmpdf)
           
          if len(bsrdf)>0:
             bsrdf.rename(columns={'stock_code': 'stock_id', 'stock_xfline': 'board_name'}, inplace=True) 
          
          
          delstr_bsr = "truncate table  " + self.boardstock_related
 
          try:
             pd.read_sql_query(delstr_bsr,con=self.engine)
        
          except:
             print delstr_bsr
          
          bsrdf.to_sql(self.boardstock_related,con=self.engine,index=False,if_exists='append')
          
          #通达信行业关联股票          
          self.setDirFiletoSql(tdxLineDir,tdxLine_dict)
          
          #通达信概念关联股票
          self.setDirFiletoSql(tdxConceptDir,tdxConcept_dict)
                                  
          m = 1
       
      def boardLayerDeal(self,boardData,tdxLinefname,boardNam,boardheader):
                        
            Bz_Id  = 0 
            
            Bz_Name = ''
            
            Bz_code = 0
            
            Bz_IndexCode = 0
            
            Bz_lft       = 0
            
            Bz_rgt       = 0
            
            Bz_pid       = 0
            
            Bz_isinde    = 1
                      
            #通达信共用头
            commonheader =['Code','Name']
            
            Bz_Id = len(boardData)
            #插入通达信行业
            try:                        
               tdxLineData=pd.read_table(tdxLinefname.decode('utf-8'),header=0,names=commonheader,delimiter=',',encoding='utf-8')
            except:
               tdxLineData=pd.read_table(tdxLinefname.decode('utf-8'),header=0,names=commonheader,delimiter=',',encoding='gbk')
            
            tdxline_group = tdxLineData.groupby('Name')
            
            tdxline_dict  = dict(list(tdxline_group))
            
            tmpdf  = boardData[boardData['Bz_Name']==boardNam]
            
            Bz_pid       = tmpdf['Bz_Id'].values.tolist()[0]
                
            Tmp_Bz_Rgt   = tmpdf['Bz_Rgt'].values.tolist()[0]
            
            Bz_lft       = Tmp_Bz_Rgt
            
            Bz_rgt       = Tmp_Bz_Rgt +1
            
            tdxLine_list  = tdxLineData.values.tolist()
            
            boardData['Bz_Rgt']  =  boardData['Bz_Rgt'].astype(int)
            
            boardData['Bz_Lft']  =  boardData['Bz_Lft'].astype(int)
            
            
            bkcount       = 1 
            
            for tdxlist in tdxLine_list:
                
                Bz_Id = Bz_Id +1
                
                Bz_IndexCode = tdxlist[0]
                
                Bz_Name      = boardNam +'-' + tdxlist[1]
                
                
                boardData['Bz_Rgt']  =  boardData['Bz_Rgt'].astype(int)
                
                boardData['Bz_Lft']  =  boardData['Bz_Lft'].astype(int)
                            
                boardData['Bz_Rgt'] = np.where(boardData['Bz_Rgt']>=Tmp_Bz_Rgt,boardData['Bz_Rgt']+2,boardData['Bz_Rgt'])
                
                boardData['Bz_Lft'] = np.where(boardData['Bz_Lft']>=Tmp_Bz_Rgt,boardData['Bz_Lft']+2,boardData['Bz_Lft'])
                
                if bkcount>1:
                    
                    Bz_lft = Bz_lft +2
                    
                    Bz_rgt = Bz_rgt +2
                    
                
                
                Line_array  = np.array([Bz_Id,Bz_Name,Bz_code,Bz_IndexCode,Bz_lft,Bz_rgt,Bz_pid,Bz_isinde])
                
                line_df     = pd.DataFrame(Line_array.ravel(),boardheader)
                
                line_df     = line_df.T
             
                
                boardData    = boardData.append(line_df)
                
            return  boardData,tdxline_dict
            
     
      def boardLayerInsert(self):
          #读取板块节点  
          pwd   =  os.getcwd()
            
          fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep)
            
          #通达信行业字典
          tdxLine_dict = {}
            
          #通达信细分行业字典
          tdxXfline_dict = {}
            
          #通达信概念字典
          tdxConcept_dict = {}
            
          #节点配置文件路径
          dbfname  = fpwd + self.BInit_FILENAME
            
          #通达信行业文件路径
          tdxLinefname =  fpwd + self.TdxLine
            
          #通达信细分行业文件路径
          tdxXflinefname =  fpwd + self.TdxXfLine
              
          #通达信概念文件路径
          tdxConceptfname =  fpwd + self.TdxConcept 
            
          #通达信板块
          boardheader = ['Bz_Id','Bz_Name','Bz_Code','Bz_IndexCode','Bz_Lft','Bz_Rgt','Bz_Pid','Bz_IsIndex']
          #生成目录字典
    
          try:                        
             boardData=pd.read_table(dbfname.decode('utf-8'),header=0,names=boardheader,delimiter=',',encoding='utf-8')
          except:
             boardData=pd.read_table(dbfname.decode('utf-8'),header=0,names=boardheader,delimiter=',',encoding='gbk')
               
          #通达信行业
          boardNam =u'通达信行业' 
            
          (boardData,tdxLine_dict)  = self.boardLayerDeal(boardData,tdxLinefname,boardNam,boardheader)
            
          #通达信细分行业
          boardNam =u'通达信细分行业' 
            
          (boardData,tdxXfline_dict)  = self.boardLayerDeal(boardData,tdxXflinefname,boardNam,boardheader)
            
          #通达信细分行业
          boardNam =u'通达信概念' 
            
          (boardData,tdxConcept_dict)  = self.boardLayerDeal(boardData,tdxConceptfname,boardNam,boardheader)
       
    
          delstr = "truncate table  " + self.bzcategory
     
          try:
              pd.read_sql_query(delstr,con=self.engine)
            
          except:
              print delstr
            
          boardData.to_sql(self.bzcategory,con=self.engine,index=False,if_exists='append')
           
          rtuple = (tdxLine_dict,tdxXfline_dict,tdxConcept_dict)
        
          return rtuple
        
if '__main__'==__name__: 
    
             
            
    datainit =  DataInitial()
     
    #更新板块
    dictTuple = datainit.boardLayerInsert() 
    
    #细分行业，个股信息,行业，概念入库
    datainit.stockLineDeal(dictTuple)
    
    
    mktindex  = datainit.mktindex
    
    #获取板块与下属关联股票
    
    #通达信行业
    (bkLinedf,bkxfdf) = mktindex.MktIndexToStocksClassify('80201')
    
    #通达信细分行业
    (bkLinedf_xf,bkxfdf_xf) = mktindex.MktIndexToStocksClassify('80202')
    
    #通达信概念
    (cfLinedf_xf,cfdf_xf) = mktindex.MktIndexToStocksClassify('80301')
        
    #通达信所有板块
    AbkLinedf = bkLinedf.append(bkLinedf_xf, ignore_index=True)
        
    Abkxfdf = bkxfdf.append(bkxfdf_xf, ignore_index=True)
           
    m = 1
     