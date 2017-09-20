# -*- coding: utf-8 -*-
"""
Created on Mon Sep 04 10:24:24 2017

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 12:09:46 2017

@author: Administrator
"""

from sqlalchemy import create_engine 
import xlsxwriter
import os
import gc

import pandas as pd
import numpy as np
import copy

import sys
sys.path.append("..")
reload(sys)
sys.setdefaultencoding('utf8')

from DataHanle.MktDataHandle import MktIndexHandle

from datetime import datetime

 
class AmoStrategy():
    
    #初始化程序
    def __init__(self):
        
        self.mktindex = MktIndexHandle()
                
        self.lineDir = '\\BoardIndex\\config\\通达信行业.txt'
        
        self.tdxLinefname = '\\PlotData\\export\\excel\\通达信行业'
        
        self.tdxConceptfname = '\\PlotData\\export\\excel\\通达信概念'
    
    #获取指数数据
    def getPlotIndexData(self,indexID,startDate,endDate,KlineType,indexType):
        
        mktindex =  self.mktindex
        
        mktdf = pd.DataFrame()
                
        if indexID!='':
            
            indexID = 'in(' +indexID+')'
            
            if indexType=='BoardIndex':
                mktdf = mktindex.MktIndexBarHistDataGet(indexID,startDate,endDate,KlineType)
             
            if indexType=='ScaleIndex':
                mktdf = mktindex.MktScaleIndexBarHistDataGet(indexID,startDate,endDate,KlineType)
                
        
        return mktdf
    
    #获取所有股票数据
    def getPlotStockData(self,startDate,endDate,KlineType,securityID):
        
        mktindex =  self.mktindex
        
        mktdf = pd.DataFrame()
        
        mktstock  = mktindex.mktstock
                
        mktdf = mktstock.MktStockBarHistDataGet(securityID,startDate,endDate,KlineType)
                
        return mktdf
        
    #获取涨跌幅
    def getIndexOrStockChg(self,idf):
        
        if len(idf)>1:
            
            idf_item   = idf.head(1)
            
            idf_tmp    = idf_item['hq_close'].values
            
            idf_close  = idf_tmp[0]
        
            idf['hq_preclose'] = idf['hq_close'].shift(1)

            preidf = idf['hq_preclose']
            
            preidf.fillna(method='bfill')
            
            
            idf['hq_chg']= ((idf['hq_close']/idf['hq_preclose'] -1)*100).round(2)
            
            idf['hq_allchg']= ((idf['hq_close']/idf_close -1)*100).round(2)
            
            
            idf['hq_allchg'] = np.where(idf['hq_allchg']==np.inf, -1, idf['hq_allchg'])
                        
            idf['hq_chg'] = np.where(idf['hq_chg']==np.inf, -1, idf['hq_chg'])
                    
        idf_ret = idf
        
        return idf_ret
    
    def getBmiStockChg(self,istockdf,bkidf,bkstock_item):
        
        bkidf_tmp  = bkidf.copy()
        
        bkidf_tmp.rename(columns={'hq_code': 'bhq_code','hq_close': 'bhq_close', 'hq_date': 'bhq_date', 'hq_time': 'bhq_time', 'hq_chg': 'bhq_chg', 'hq_allchg': 'bhq_allchg', 'hq_vol': 'bhq_vol', 'hq_amo': 'bhq_amo'}, inplace=True) 
                      
        xdidf_ret = pd.DataFrame()
        
        bkstock_df = bkstock_item[['stock_id','stock_name']]
                
        if len(istockdf)>1:
                        
           idf_group  = istockdf.groupby('hq_code')
           
           idf_dict   = dict(list(idf_group))
                             
           for idict in idf_dict:
                              
               dictItem = idf_dict[idict]
               
               #开始分析各板块异动                              
               dictItem  = self.getIndexOrStockChg(dictItem)
               
               if len(dictItem)>2:
                                                      
                   idf_ret  = pd.merge(dictItem,bkidf_tmp,left_on='index',right_on='index')
#                   
#                   print idf_ret
                                                                     
                   idf_ret['hq_xdamo']  = (idf_ret['hq_amo']/idf_ret['hq_allamo']*100).round(4)
                   
                   idf_ret['hq_xdamo']  = np.where(idf_ret['hq_xdamo']==np.inf, -1, idf_ret['hq_xdamo'])
                   
                   stock_df  = bkstock_df[bkstock_df['stock_id']==idict]
                   
                   stock_name = stock_df['stock_name'].tolist()[0]
                   
                   idf_ret['hq_name']   = stock_name
                   
                   idf_tmp = idf_ret [['hq_code','hq_name','hq_date','hq_time','hq_chg','hq_allchg','hq_xdamo']]
                   
                   idf_tmp = idf_tmp[1:]
                   
                   xdidf_ret = xdidf_ret.append(idf_tmp)
        
        del  bkidf_tmp        

        gc.collect()         
               
        return xdidf_ret    
        
    def getAllStockChg(self,istockdf,bkidf_tmp):
                
        xdidf_ret = pd.DataFrame()
        
        stockChgDict ={}
        
        if len(istockdf)>1:
                        
           idf_group  = istockdf.groupby('hq_code')
           
           idf_dict   = dict(list(idf_group))
           
           for idict in idf_dict:
                              
               dictItem = idf_dict[idict]
               
               if len(dictItem)>2:
                
                   idf_ret  = self.getIndexOrStockChg(dictItem)
                                                         
                   idf_ret  = pd.merge(idf_ret,bkidf_tmp,left_on='index',right_on='index')

                   idf_ret['hq_xdallchg'] = idf_ret['hq_allchg']
                                  
                   idf_ret.loc[:,['hq_xdallchg']]  = idf_ret['hq_allchg']- idf_ret['bhq_allchg']
                                                  
                   idf_ret['hq_xdamo']  = (idf_ret['hq_amo']/idf_ret['bhq_amo']*100).round(3)
                   
                   idf_ret['hq_xdamo'] = np.where(idf_ret['hq_xdamo']==np.inf, -1, idf_ret['hq_xdamo'])
                    
                   xdidf_ret = xdidf_ret.append(idf_ret)
                   
                   #获取所有股票的涨跌幅
                   stockChgList  = idf_ret['hq_allchg'].tolist()
                   
                   stockChgDict[idict] =  stockChgList.pop()
                                      
               
        return xdidf_ret,stockChgDict
        
    #获取指数相对强弱，相对量能
    def getIndexStockXdQr(self,bmidf,bkidf,bkdict,bkxfdf,stockdf):
        
        xdidf_ret = pd.DataFrame()
        
        dict_ret  ={}  
        
        sortdict = {}
            
        bkStockdict ={}           
            
        bkStockChgdict ={}
        
        bkidf_dict={}
        
        stockdf_len = len(stockdf)
        
        if stockdf_len>0:
            
            Mstockdf = pd.merge(stockdf,bkxfdf,left_on='hq_code',right_on='stock_id')
            
            stockdf  = pd.DataFrame()
            #对板块个股进行分类
                    
            Mistockdf_group = Mstockdf.groupby('board_id')
            
                    
            #板块个股关联        
            bkidf_dict = dict(list(Mistockdf_group))
            
            Mstockdf  = pd.DataFrame()
            
            gc.collect()
        
        #如果板块有数据，计算处理
        
        if len(bkidf)>0 and len(bmidf)>0  :
            
            
            #指数数据分组        
            hkidf_group = bkidf.groupby('hq_code')
            
#            #股票数据分组
#            stockdf_group = stockdf.groupby('hq_code')
#                        
#            #生成指数dict        
            hkidf_dict = dict(list(hkidf_group))
#            
#            #生成股票dict            
#            stockdf_dict = dict(list(stockdf_group))
#                                              
            #生成基准指数累计涨跌幅度
            xdhidf   = self.getIndexOrStockChg(bmidf)
            
            bmi_len   = len(bmidf)
            
            xdhindex  = xdhidf.index
            
            dictcount = 0 
            
                
            #取出排名指数
            for dfdict in  hkidf_dict:
                
                print dfdict
                                
                dictcount  = dictcount +1
                
                #取出每个指数的行情数据
                hidf_item  = hkidf_dict[dfdict]
                
                
                tmpidf     = xdhidf.copy()
                
                hidf_ret   = self.getIndexOrStockChg(hidf_item)
                
                hidf_len   = len(hidf_item)
                
                if hidf_len!=bmi_len:
                    hidf_item = hidf_item.reindex(xdhindex)
               
                tmpdf      = hidf_item.copy()
               
                tmpidf.set_index(['index'], inplace = True)
                
                tmpdf.set_index(['index'], inplace = True)
                
                tmpdf.loc[:,['hq_code']]  = str(dfdict)
                
                tmpdf['hq_bmcode']  = benchmarkIndex 
                
                hq_bmname =''
                
                if bkdict.has_key(str(benchmarkIndex)):
                   hq_bmname = bkdict[str(benchmarkIndex)] 
                
                tmpdf['hq_bmname']  = hq_bmname
                
                
                hq_name =''
                
                if bkdict.has_key(dfdict):
                   hq_name = bkdict[dfdict] 
                   
                   hq_name = hq_name.replace(u'通达信行业-','').replace(u'通达信细分行业-','')
                
                tmpdf['hq_name']  = hq_name
                
                
                tmpdf.loc[:,['hq_chg']]   = tmpdf['hq_allchg']
                
                tmpdf.loc[:,['hq_allchg']]  = tmpdf['hq_allchg']- tmpidf['hq_allchg']
                
                tmpidf.loc[:,['hq_vol']] = np.where(tmpidf['hq_vol']==0,np.where(tmpdf['hq_vol']==0,-1,-tmpdf['hq_vol']),tmpidf['hq_vol'])
                
                tmpdf['hq_xdvol']  = (tmpdf['hq_vol']/tmpidf['hq_allvol']*100).round(3)
                
                tmpdf['hq_xdvol'] = np.where(tmpdf['hq_xdvol']==np.inf, -1, tmpdf['hq_xdvol'])
                
                tmpcolums = tmpdf.columns
                
                if 'hq_amo' in tmpcolums:                    
                    tmpdf['hq_xdamo']  = (tmpdf['hq_amo']/tmpidf['hq_allamo']*100).round(3)
                    
                    tmpdf['hq_xdamo'] = np.where(tmpdf['hq_xdamo']==np.inf, -1, tmpdf['hq_xdamo'])
                
                
                if dictcount<=1:
                    xdhead_array = tmpdf.values
                else:
                    xdhead_array = np.concatenate([xdhead_array,tmpdf.values],axis=0)
                
                
                #加入涨跌幅的排名
                
                hichg = hidf_ret['hq_allchg'].tolist()
                
                if len(hichg)>0:
                   #得到最后一个数据 
                   sortdict[dfdict] =  hichg.pop()
                
                bkidf_tmp =  tmpdf[['hq_code','hq_date','hq_time','hq_chg','hq_allchg','hq_vol','hq_amo']]
                
                
                
                bkidf_tmp.rename(columns={'hq_code': 'bhq_code', 'hq_date': 'bhq_date', 'hq_time': 'bhq_time', 'hq_chg': 'bhq_chg', 'hq_allchg': 'bhq_allchg', 'hq_vol': 'bhq_vol', 'hq_amo': 'bhq_amo'}, inplace=True) 
                
                bkidf_tmp['index'] = bkidf_tmp.index
                
                #找到指数对应股票的行情数据
                if bkidf_dict.has_key(str(dfdict)) and stockdf_len>0:
                    
                   bkitem  = bkidf_dict[str(dfdict)]
                   
                   (tmpstockdf,stockChgDict) = self.getAllStockChg(bkitem,bkidf_tmp)
                   
                   bksrdf     = tmpstockdf[['board_id','board_name','hq_date','hq_time','stock_id','stock_name','hq_close','hq_preclose','hq_vol','hq_amo','hq_allchg','hq_xdallchg','hq_xdamo']]
                    
                   bkStockdict[dfdict] = bksrdf
                   
                   bkStockChgdict[dfdict] = stockChgDict
                 
                   gc.collect()
                   
                   m = 1
                
            xdhead_columnus = tmpdf.columns     
           
            xdhead_idf = pd.DataFrame(xdhead_array,columns=xdhead_columnus)
            
            if 'hq_xdamo' in xdhead_columnus:
                xdidf_ret  = xdhead_idf[['hq_code','hq_name','hq_date','hq_time','hq_bmcode','hq_bmname','hq_close','hq_preclose','hq_vol','hq_amo','hq_chg','hq_allchg','hq_xdvol','hq_xdamo']]
            
            #对涨跌幅字典进行排序
            dict_ret= sorted(sortdict.items(), key=lambda d:d[1], reverse = True)
            
            
            gc.collect()
        
        
        return xdidf_ret,dict_ret,bkStockdict,bkStockChgdict
      
    #获取指数相对强弱，相对量能
    def getIndexXdQr(self,bmidf,bkidf,bkdict):
        
        xdidf_ret = pd.DataFrame()
        
        dict_ret  ={}  
        
        sortdict = {}
            
        #如果板块有数据，计算处理
        
        if len(bkidf)>0 and len(bmidf)>0  :
            
            #指数数据分组        
            hkidf_group = bkidf.groupby('hq_code')
                 
            hkidf_dict = dict(list(hkidf_group))
          
            #生成基准指数累计涨跌幅度
            xdhidf   = self.getIndexOrStockChg(bmidf)
            
            bmi_len   = len(bmidf)
            
            xdhindex  = xdhidf.index
            
            dictcount = 0 
            
            #取出排名指数
            for dfdict in  hkidf_dict:
                
                print dfdict
                                
                dictcount  = dictcount +1
                
                #取出每个指数的行情数据
                hidf_item  = hkidf_dict[dfdict]
                
                
                tmpidf     = xdhidf.copy()
                
                hidf_ret   = self.getIndexOrStockChg(hidf_item)
                
                hidf_ret   = hidf_ret[1:]
                
                hidf_len   = len(hidf_item)
                
                if hidf_len!=bmi_len:
                    hidf_item = hidf_item.reindex(xdhindex)
               
                tmpdf      = hidf_item.copy()
               
                tmpidf.set_index(['index'], inplace = True)
                
                tmpdf.set_index(['index'], inplace = True)
                
                tmpdf.loc[:,['hq_code']]  = str(dfdict)
                
                tmpdf['hq_bmcode']  = benchmarkIndex 
                
                hq_bmname =''
                
                if bkdict.has_key(int(benchmarkIndex)):
                   hq_bmname = bkdict[int(benchmarkIndex)] 
                
                tmpdf['hq_bmname']  = hq_bmname
                
                
                hq_name =''
                
                if bkdict.has_key(dfdict)  :
                   
                   hq_name = bkdict[dfdict] 
                   
                   hq_name = hq_name.replace(u'通达信行业-','').replace(u'通达信细分行业-','').replace(u'通达信概念-','')
                
                
                tmpdf['hq_name']  = hq_name
                
                
                tmpdf.loc[:,['hq_chg']]   = tmpdf['hq_allchg']
                
                tmpdf.loc[:,['hq_allchg']]  = tmpdf['hq_allchg']- tmpidf['hq_allchg']
                
                tmpidf.loc[:,['hq_vol']] = np.where(tmpidf['hq_vol']==0,np.where(tmpdf['hq_vol']==0,-1,-tmpdf['hq_vol']),tmpidf['hq_vol'])
                
                tmpdf['hq_xdvol']  = (tmpdf['hq_vol']/tmpidf['hq_allvol']*100).round(3)
                
                tmpdf['hq_xdvol'] = np.where(tmpdf['hq_xdvol']==np.inf, -1, tmpdf['hq_xdvol'])
                
                tmpcolums = tmpdf.columns
                
                if 'hq_amo' in tmpcolums:                    
                    tmpdf['hq_xdamo']  = (tmpdf['hq_amo']/tmpidf['hq_allamo']*100).round(3)
                    
                    tmpdf['hq_xdamo'] = np.where(tmpdf['hq_xdamo']==np.inf, -1, tmpdf['hq_xdamo'])
                
                tmpdf = tmpdf[1:]
                
                if dictcount<=1:
                    xdhead_array = tmpdf.values
                else:
                    xdhead_array = np.concatenate([xdhead_array,tmpdf.values],axis=0)
                
                
                #加入涨跌幅的排名
                
                hichg = hidf_ret['hq_allchg'].tolist()
                
                if len(hichg)>0:
                   #得到最后一个数据 
                   sortdict[dfdict] =  hichg.pop()
                
                bkidf_tmp =  tmpdf[['hq_code','hq_date','hq_time','hq_chg','hq_allchg','hq_vol','hq_amo']]
                
                
                
                bkidf_tmp.rename(columns={'hq_code': 'bhq_code', 'hq_date': 'bhq_date', 'hq_time': 'bhq_time', 'hq_chg': 'bhq_chg', 'hq_allchg': 'bhq_allchg', 'hq_vol': 'bhq_vol', 'hq_amo': 'bhq_amo'}, inplace=True) 
                
                bkidf_tmp['index'] = bkidf_tmp.index
                
                
            xdhead_columnus = tmpdf.columns     
           
            xdhead_idf = pd.DataFrame(xdhead_array,columns=xdhead_columnus)
            
            if 'hq_xdamo' in xdhead_columnus:
                xdidf_ret  = xdhead_idf[['hq_code','hq_name','hq_date','hq_time','hq_bmcode','hq_bmname','hq_close','hq_preclose','hq_vol','hq_amo','hq_chg','hq_allchg','hq_xdvol','hq_xdamo']]
            
            #对涨跌幅字典进行排序
            dict_ret= sorted(sortdict.items(), key=lambda d:d[1], reverse = True)
            
            del bkidf,bkdict,tmpdf,xdhead_idf,hidf_ret
                        
            gc.collect()
        
        return xdidf_ret,dict_ret
            
    
    
    def bulidChart(self,wbk,bkchar_tuple,bktiles,idxstr,KlineType):
        
        bk_chart = wbk.add_chart({'type': 'line'})
               
        bk_chart.set_style(4)
        
        
        bench_XY   = bkchar_tuple[0]
        
        bkXY_list  = bkchar_tuple[1]
        
        shift      = bkchar_tuple[2]
        
        shift2     = bkchar_tuple[3]
        
        style      = bkchar_tuple[4]
        
        
        if KlineType=='5M':
           interval_unit = 48
        else:           
           interval_unit = 7 
       
       
        data_top2   = bench_XY[1]
        
        data_left2  = bench_XY[2]
        
        bkidf_len2  = bench_XY[3]        
    
        bkXY = bkXY_list[2]
        
        data_top   = bkXY[1]
        
        data_left  = bkXY[2]
        
        bkidf_len  = bkXY[3]
                   
        
        #向图表添加数据 
        
        if style==1:
            
        
            
            bk_chart.add_series({
             'name':[idxstr, data_top+1, data_left+1],
             'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
             'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
             'line':{'color':'red'},
                    
             })    
        
            bk_chart.add_series({
             'name':[idxstr, data_top2+1, data_left2+1],
             'categories':[idxstr, data_top2+1, data_left2+2, data_top2+bkidf_len2, data_left2+2],
             'values':[idxstr, data_top2+1, data_left2+shift2, data_top2+bkidf_len2, data_left2+shift2],
             'line':{'color':'blue'}, 
             'y2_axis': True, 

                      
             })
             
        if style==2:     
                 
             bk_chart.add_series({
             'name':[idxstr, data_top, data_left+shift],
             'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
             'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
             'line':{'color':'FF6347'},
            
             }) 
                          
             bk_chart.add_series({
             'name':[idxstr, data_top2, data_left2+shift2],
             'categories':[idxstr, data_top2+1, data_left2+2, data_top2+bkidf_len2, data_left2+2],
             'values':[idxstr, data_top2+1, data_left2+shift2, data_top2+bkidf_len2, data_left2+shift2],
             'line':{'color':'708090'},  
             'y2_axis': True, 
          
             })
                 
        if style==3:
            
            bk_chart.add_series({
             'name':[idxstr, data_top+1, data_left+1],
             'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
             'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
             'line':{'color':'red'},
             
                    
             })            
            
            bk_chart.add_series({
             'name':[idxstr, data_top2+1, data_left2+4],
             'categories':[idxstr, data_top2+1, data_left2+2, data_top2+bkidf_len2, data_left2+2],
             'values':[idxstr, data_top2+1, data_left2+shift2, data_top2+bkidf_len2, data_left2+shift2],
             'line':{'color':'black'}, 
             'y2_axis': True,  
                      
             })   
             
        #bold = wbk.add_format({'bold': 1})
     
     
        bk_chart.set_title({'name':bktiles,
                           'name_font': {'size': 10, 'bold': True}
                          })
                                   
        bk_chart.set_x_axis({'name_font': {'size': 10, 'bold': True},
                            'label_position': 'low',
                            'interval_unit': interval_unit
                    
                          })
                        
        bk_chart.set_y_axis({'name':u'日期',
                            'name_font': {'size': 10, 'bold': True}
                       })
   
        bk_chart.set_y_axis({'name':'',
                        'name_font': {'size': 10, 'bold': True}
                        })
                       
        bk_chart.set_size({'width':1600,'height':450})
           
        
        return bk_chart
     
    
    def bulidALLChart(self,wbk,bkchar_tuple,bktiles,idxstr,KlineType):
        
        bk_chart = wbk.add_chart({'type': 'line'})
               
        bk_chart.set_style(4)   
        
        bench_XY   = bkchar_tuple[0]
        
        bkXY_list  = bkchar_tuple[1]
        
        shift      = bkchar_tuple[2]
        
        shift2     = bkchar_tuple[3]
        
        style      = bkchar_tuple[4]
        
        #bkchar_tuple =(bk_XY,bkXY_list,shift,shift2,style)
        
        # CD6600 橙色，C1C1C1紫色，556B2F草绿色，696969灰色，
        colorlist =['CD6600','0F0F0F','FF0000','556B2F','FFD700','C1C1C1','556B2F','696969']
        
        
        if KlineType=='5M':
           interval_unit = 48
        else:           
           interval_unit = 7 
        
        #基准无需变动
        
        if style==1:
            
            data_top2 = bench_XY[1]
            
            data_left2 = bench_XY[2]
            
            bkidf_len  = bench_XY[3]
                                   
            bk_chart.add_series({
             'name':[idxstr, data_top2+1, data_left2+1],
             'categories':[idxstr, data_top2+1, data_left2+2, data_top2+bkidf_len, data_left2+2],
             'values':[idxstr, data_top2+1, data_left2+shift2, data_top2+bkidf_len, data_left2+shift2],
             'line':{'color':'blue'},  
             'y2_axis': True
                      
             })
             
            data_top2 = bench_XY[1]
            
            data_left2 = bench_XY[2]
            
            bkidf_len  = bench_XY[3]
            
            icount = 0
            
            #向图表添加数据 
            for bkl in bkXY_list:
                
                bk_XY = bkl[2]
                   
                data_top   = bk_XY[1]
                
                data_left  = bk_XY[2]
                
                bkidf_len  = bk_XY[3]
                    
                bk_chart.add_series({
                 'name':[idxstr, data_top+1, data_left+1],
                 'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
                 'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
                 'line':{'color':colorlist[icount]},
                 
                        
                 })                 
                
                icount  =icount+1
        else:
            
            icount = 0
            
            #向图表添加数据 
            for bkl in bkXY_list:
                
                bk_XY = bkl[2]
                   
                data_top   = bk_XY[1]
                
                data_left  = bk_XY[2]
                
                bkidf_len  = bk_XY[3]
                    
                bk_chart.add_series({
                 'name':[idxstr, data_top+1, data_left+1],
                 'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
                 'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
                 'line':{'color':colorlist[icount]},
                 
                        
                 })                 
                
                icount  =icount+1
            
        #bold = wbk.add_format({'bold': 1})
     
     
        bk_chart.set_title({'name':bktiles,
                           'name_font': {'size': 10, 'bold': True}
                          })
                                   
        bk_chart.set_x_axis({'name_font': {'size': 10, 'bold': True},
                            'label_position': 'low',
                            'interval_unit': interval_unit
                    
                          })
                        
        bk_chart.set_y_axis({'name':u'日期',
                            'name_font': {'size': 10, 'bold': True}
                       })
   
        bk_chart.set_y_axis({'name':'',
                        'name_font': {'size': 10, 'bold': True}
                        })
                       
        bk_chart.set_size({'width':1600,'height':450})
           
        
        return bk_chart    
        
    def bulidALLChart_XF(self,wbk,bkXY_dict,flcode,fxflinecodes,bktile,idxstr,shift,shift2,KlineType):
        
        
        bk_chart = wbk.add_chart({'type': 'line'})
               
        bk_chart.set_style(4)
        
        # CD6600 橙色，C1C1C1紫色，556B2F草绿色，696969灰色，
        colorlist =['CD6600','0F0F0F','FF0000','556B2F','FFD700','C1C1C1','556B2F','696969']
        
        if KlineType=='5M':
           interval_unit = 48
        else:           
           interval_unit = 7  
        
        icount  = 0 
        
        if bkXY_dict.has_key(str(flcode)):
             
            tmplist = bkXY_dict[str(flcode)]
                         
            if len(tmplist)==4:
                    
               bkcode  = tmplist[0]
                    
               bk_x    = tmplist[1]
                    
               bk_y    = tmplist[2]
                    
               bk_len  = tmplist[3]
                    
               data_top  = bk_x
                    
               data_left  = bk_y
                    
               bkidf_len = bk_len
            
               bk_chart.add_series({
                          'name':[idxstr, data_top+1, data_left+1],
                          'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
                          'values':[idxstr, data_top+1, data_left+shift2, data_top+bkidf_len, data_left+shift2],
                          'line':{'color':colorlist[icount]},
                          'y2_axis': True,                         
                        })  
               
               icount +=1      
        
        
        for fxfcode in fxflinecodes:
            
            if bkXY_dict.has_key(str(fxfcode)):
                
                tmplist = bkXY_dict[str(fxfcode)]
                
                
                if len(tmplist)==4:
                    
                    bkcode  = tmplist[0]
                    
                    bk_x    = tmplist[1]
                    
                    bk_y    = tmplist[2]
                    
                    bk_len  = tmplist[3]
                    
                    data_top  = bk_x
                    
                    data_left  = bk_y
                    
                    bkidf_len = bk_len
                    
                    bk_chart.add_series({
                          'name':[idxstr, data_top+1, data_left+1],
                          'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
                          'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
                          'line':{'color':colorlist[icount]},
                                                  
                        })  
                    
                    icount += 1
            
        bk_chart.set_title({'name':bktile,
                           'name_font': {'size': 10, 'bold': True}
                          })
                                   
        bk_chart.set_x_axis({'name_font': {'size': 10, 'bold': True},
                            'label_position': 'low',
                            'interval_unit': interval_unit
                    
                          })
                        
        bk_chart.set_y_axis({'name':u'日期',
                            'name_font': {'size': 10, 'bold': True}
                       })
   
        bk_chart.set_y_axis({'name':'',
                        'name_font': {'size': 10, 'bold': True}
                        })
                       
        bk_chart.set_size({'width':1600,'height':450})
           
        
        return bk_chart     
      
    
    def bulidBoardExcelFrame(self,ebmidf,ramolist,Abkdict,bkcodes,linedf,stockdf,KlineType,boardType):
        
        #处理指数excel数据
        data_left = 0    #数据起始列

        data_top  = 0    #数据起始行
        
        Adate = datetime.now().strftime('%Y-%m-%d')
    
        pwd   =  os.getcwd()
        
        fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep)
        
        #处理行业与个股
        if boardType==1:
                    
           execlfname  = fpwd + self.tdxLinefname+ Adate +'-('+KlineType+').xlsx'
        
           execlfname  = execlfname.decode()
        
           wbk =xlsxwriter.Workbook(execlfname) 
                   
           wbk.add_worksheet(u'异动行业')
            
           IData_Sheet = wbk.add_worksheet(u'指数数据')
                
        if boardType==2:
           
           execlfname  = fpwd + self.tdxConceptfname+ Adate +'-('+KlineType+').xlsx'
        
           execlfname  = execlfname.decode()
        
           wbk =xlsxwriter.Workbook(execlfname) 
                   
           wbk.add_worksheet(u'异动概念')
           
           IData_Sheet = wbk.add_worksheet(u'指数数据')
                           
        #IData_Sheet.hide()
                
        xdiColumns= list([u'板块代码', u'板块名称', u'日期',u'时间' ,u'基准板块代码', u'基准板块名称', u'收盘价', u'前收盘价', u'成交量',u'成交额' ,u'日相对涨跌幅', u'累计相对涨跌幅', u'相对量比', u'相对金额比'])
        
        stockColumns = list([u'股票代码', u'股票名称', u'日期',u'时间' ,u'相对涨跌幅', u'累计相对涨跌幅',u'相对金额比'])
        
        ebmidf.fillna(method='bfill',inplace=True)
        
        #基准数据写入数据sheet中
        if len(ebmidf)>0:
            
            bmidf_group = ebmidf.groupby('hq_code')
    
            bmi_list = list(bmidf_group)
            
            bmiColumns = list([u'基准指数代码', u'基准指数名称', u'日期',u'时间', u'收盘价', u'前收盘价', u'成交量', u'涨跌幅', u'累涨跌幅',u'总成交量',u'总成交额'])
            
            #未处理多个基准标的比较问题，以及标的指数与板块数据不一致的问题
                  
            (IData_Sheet,idataXY_dict) = self.bulidIndexDataToExcel(bmi_list,IData_Sheet,bmiColumns,data_left,data_top)
            
            data_left = data_left +len(bmiColumns) +2
           
        #行业指数，概念指数 数据写入数据sheet中
        if len(ramolist)>0 :
                        
            axdtmp_group = linedf.groupby('hq_code',sort=False)
    
            abkidf_list = list(axdtmp_group)
            
            #abkColumns = list([u'规模指数代码', u'规模指数名称', u'日期',u'时间', u'收盘价', u'前收盘价', u'成交量', u'涨跌幅', u'累涨跌幅',u'总成交量',u'总成交额'])
            
            #未处理多个基准标的比较问题，以及标的指数与板块数据不一致的问题
                  
            (IData_Sheet,AbkXY_dict) = self.bulidIndexDataToExcel(abkidf_list,IData_Sheet,xdiColumns,data_left,data_top)
            
            data_left = data_left +len(xdiColumns) +2
            
            
            stocktmp_group = stockdf.groupby('hq_code',sort=False)
    
            stockidf_list = list(stocktmp_group)
            
            #abkColumns = list([u'规模指数代码', u'规模指数名称', u'日期',u'时间', u'收盘价', u'前收盘价', u'成交量', u'涨跌幅', u'累涨跌幅',u'总成交量',u'总成交额'])
            
            #未处理多个基准标的比较问题，以及标的指数与板块数据不一致的问题
                  
            (IData_Sheet,stockXY_dict) = self.bulidIndexDataToExcel(stockidf_list,IData_Sheet,stockColumns,data_left,data_top)
            
            data_left = data_left +len(xdiColumns) +2
            
            #处理规模指数
            XY_Tuple = (idataXY_dict,AbkXY_dict,stockXY_dict)
            
            ColumnsTuple =(bmiColumns,xdiColumns,stockColumns)
        
            #基准指数坐标   
            wbk  = self.bulidAllExcelPic(wbk,IData_Sheet,XY_Tuple,ColumnsTuple,ramolist,Abkdict,bkcodes,boardType)
            
            
        
        IData_Sheet.hide() 
        
        wbk.close()            
        
        return wbk
        
    #统一画图模块
    def bulidSigleExcelPic(self,wbk,IData_Sheet,xdiColumns,bkp_tuple,SheetName):    
            
       #bkp_tuple =(bkcodestr,Abkdict,idataXY_dict,bench_XY)
        sheetflag = False
        
        lastflag  = False        
        
        bench_key = ''
        
        bkcodestr       = bkp_tuple[0]
        
        Abkdict         = bkp_tuple[1]
        
        idataXY_dict    = bkp_tuple[2]
        
        AbkXY_dict      = bkp_tuple[3]
        
        bench_XY        = bkp_tuple[4]
        
        bench_key       = bkp_tuple[5]
        
        for qrsheet in wbk.worksheets_objs:
            
            if SheetName==qrsheet.name:
               sheetflag   = True  
               QR_Sheet    = qrsheet
               
        
        if not sheetflag:
            
            QR_Sheet = wbk.add_worksheet(SheetName)
        
                   
        xdiColumnlens  = len(xdiColumns)
        
        #画模块
        headStr='强弱排名（涨幅）'
        
        headvol= '量比（金额）'
        
         
        red = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'C0504D','font_size':16,'font_color':'white'})
        
        blue = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'8064A2','font_size':16,'font_color':'white'})
        #间隔格式
        JG = wbk.add_format({'bg_color':'CCC0DA'})
        
        #处理第一行excel格式
        QR_Sheet.merge_range(0,0,1,xdiColumnlens+10,headStr,red)         
        QR_Sheet.set_column(xdiColumnlens+1+10,xdiColumnlens+1+10,0.3,JG)

        QR_Sheet.merge_range(0,xdiColumnlens+2+10,1,2*(xdiColumnlens+10)+2,headvol,blue)               
        QR_Sheet.set_column(2*xdiColumnlens+3+20,2*xdiColumnlens+3+20,0.3,JG)
               
           
        pic_left  = 0    #图像起始列 
                
        pic_top   = 2    #图像起始行
                           
   
        bkidf_list =bkcodestr.split(',')
       
        if len(bkidf_list)>0:
            lastfile = bkidf_list[-1]
           
        for dflist in  bkidf_list:
                       
           bkidf_code  = dflist
           
           bkname = ''
           
           if lastfile==dflist: 
               lastflag = True
           
           test1 = Abkdict.has_key(int(dflist))
           
           test2 = AbkXY_dict.has_key(int(dflist))
           
           test3 = idataXY_dict.has_key(int(bench_key))
           
           if Abkdict.has_key(int(dflist)) and AbkXY_dict.has_key(int(dflist)) and idataXY_dict.has_key(int(bench_key)):
               
              
              bkname = Abkdict[int(dflist)]
              
              bkname = bkname.replace(u'通达信行业-','').replace(u'通达信细分行业-','').replace(u'通达信概念-','')
              
              bktiles = bkname +'('+bkidf_code+')'
              
              bk_XY = AbkXY_dict[int(dflist)] 
              
              
              bkXY_Tuple =(bkidf_code,bkname,bk_XY)
              
            
              idxstr  = u'指数数据'
               
              #指数参数设置
              shift =10
               
              shift2  = 4
               
              style   = 1
             
             
              bkchar_tuple =(bench_XY,bkXY_Tuple,shift,shift2,style)
           
              bk_chart = self.bulidChart(wbk,bkchar_tuple,bktiles,idxstr,KlineType)
              #画出双轴对比图
           
              QR_Sheet.insert_chart( pic_top, pic_left,bk_chart)
              #bg+=19       
         
              pic_left+=len(xdiColumns)+2+10 
           
              #画成交量与成交金额相对相对量比
           
              shift =12
                          
              shift2  = 13
           
              style  =2
             
              bkchar_tuple2 =(bench_XY,bkXY_Tuple,shift,shift2,style)
             
             
              bk_chart = self.bulidChart(wbk,bkchar_tuple2,bktiles,idxstr,KlineType)
                
              #画出双轴对比图
           
              QR_Sheet.insert_chart( pic_top, pic_left,bk_chart)
             #bg+=19       
           
              pic_top+=23
           
              if lastfile!=dflist: 
                 pic_left-=len(xdiColumns) +2+10 
           
              bktiles =''
                 
        
        return wbk
        
        
    #统一画图模块
    def bulidUniformExcelPic(self,wbk,IData_Sheet,xdiColumns,bkp_tuple,SheetName):
        
        #bkp_tuple =(bkcodestr,Abkdict,idataXY_dict,bench_XY)
        sheetflag = False
        
        lastflag  = False
        
        for qrsheet in wbk.worksheets_objs:
            
            if SheetName==qrsheet.name:
               sheetflag   = True  
               QR_Sheet    = qrsheet
        
        if sheetflag:
                
            xdiColumnlens  = len(xdiColumns)
            
            #画模块
            headStr='强弱排名（涨幅）'
            
            headvol= '量比（金额）'
            
             
            red = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'C0504D','font_size':16,'font_color':'white'})
            
            blue = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'8064A2','font_size':16,'font_color':'white'})
            #间隔格式
            JG = wbk.add_format({'bg_color':'CCC0DA'})
            
            #处理第一行excel格式
            QR_Sheet.merge_range(0,0,1,xdiColumnlens+10,headStr,red)         
            QR_Sheet.set_column(xdiColumnlens+1+10,xdiColumnlens+1+10,0.3,JG)
    
            QR_Sheet.merge_range(0,xdiColumnlens+2+10,1,2*(xdiColumnlens+10)+2,headvol,blue)               
            QR_Sheet.set_column(2*xdiColumnlens+3+20,2*xdiColumnlens+3+20,0.3,JG)
                   
            bench_key = ''
            
            bkcodestr       = bkp_tuple[0]
            
            Abkdict         = bkp_tuple[1]
            
            idataXY_dict    = bkp_tuple[2]
            
            AbkXY_dict      = bkp_tuple[3]
            
            bench_XY        = bkp_tuple[4]
            
            bench_key       = bkp_tuple[5]
               
            pic_left  = 0    #图像起始列 
                    
            pic_top   = 2    #图像起始行
            
            bkXY_list = []
            
            bkcount = 0
            
            bkNum   = 8 
            
            
            bkidf_list =bkcodestr.split(',')
            
            if len(bkidf_list)>0:
                lastfile = bkidf_list[-1]
               
            for dflist in  bkidf_list:
                           
               bkidf_code  = dflist
               
               bkname = ''
               
               if lastfile==dflist: 
                   lastflag = True
               
               test1 = Abkdict.has_key(int(dflist))
               
               test2 = AbkXY_dict.has_key(int(dflist))
               
               test3 = idataXY_dict.has_key(int(bench_key))
               
               if Abkdict.has_key(int(dflist)) and AbkXY_dict.has_key(int(dflist)) and idataXY_dict.has_key(int(bench_key)):
                   
                  bkcount  = bkcount +1   
                  
                  bkname = Abkdict[int(dflist)]
                  
                  bkname = bkname.replace(u'通达信行业-','').replace(u'通达信细分行业-','')
                  
                  #bktiles = bkname +'('+bkidf_code+')'
                  
                  bk_XY = AbkXY_dict[int(dflist)] 
                  
                  
                  bkXY_Tuple =(bkidf_code,bkname,bk_XY)
                  
                  bkXY_list.append(bkXY_Tuple)
                  
                   
                  if bkcount==1 :
                     bktiles =  bkname                  
                  else:
                     bktiles = bktiles + ',' +bkname  
                                      
                  #每隔固定几个值进行读取
                  if bkcount%bkNum ==0 or lastflag :
                     
                     idxstr  = u'指数数据'
                     
                     bktiles = '(' + bktiles +')' 
                     
                     #指数参数设置
                     shift =10
                   
                     shift2  = 4
                   
                     style   = 1
                     
                     
                     bkchar_tuple =(bench_XY,bkXY_list,shift,shift2,style)
                   
                     bk_chart = self.bulidALLChart(wbk,bkchar_tuple,bktiles,idxstr,KlineType)
                     #画出双轴对比图
                   
                     QR_Sheet.insert_chart( pic_top, pic_left,bk_chart)
                     #bg+=19       
                 
                     pic_left+=len(xdiColumns)+2+10 
                   
                     #画成交量与成交金额相对相对量比
                   
                     shift =12
                                  
                     shift2  = 13
                   
                     style  =2
                     
                     bkchar_tuple2 =(bench_XY,bkXY_list,shift,shift2,style)
                     
                     
                     bk_chart = self.bulidALLChart(wbk,bkchar_tuple2,bktiles,idxstr,KlineType)
                        
                     #画出双轴对比图
                   
                     QR_Sheet.insert_chart( pic_top, pic_left,bk_chart)
                     #bg+=19       
                   
                     pic_top+=23
                   
                     if lastfile!=dflist: 
                        pic_left-=len(xdiColumns) +2+10 
                   
                     bktiles =''
                     
                     bkcount = 0
                     
                     bkXY_list = []
        
        return wbk
           
        
    def bulidAllExcelPic(self,wbk,IData_Sheet,XY_Tuple,ColumnsTuple,ramolist,Abkdict,bkcodes,boardType):
        
        idataXY_dict  = XY_Tuple[0]        
        
        AbkXY_dict    = XY_Tuple[1]
        
        stockXY_dict  = XY_Tuple[2]
                    
        xdiColumns    = ColumnsTuple[1]
                        
        stockColumns  = ColumnsTuple[2]        
        
        #处理基准指数数据（以399317为基准）
        if len(idataXY_dict)>0:
           
           ikeys = idataXY_dict.keys()
        
           bench_key = ikeys[0] 
           
           bench_XY = idataXY_dict[bench_key]
           
        
        if boardType==1:
            #画通达信板块指数
            SheetName =u'异动行业'
            
            bkp_tuple =(bkcodes,Abkdict,idataXY_dict,AbkXY_dict,bench_XY,bench_key)
                 
            wbk = self.bulidSigleExcelPic(wbk,IData_Sheet,xdiColumns,bkp_tuple,SheetName)
            
            #循环生成每个异动行业个股
            for rl in ramolist:
                
                bkinfodf      = rl[1]
                
                bkstockdf     = rl[2]
                
                bkcodes       = rl[3]
                
                stockdf  = bkstockdf[['hq_code','hq_name']]
                
                bkstockdict = stockdf.to_dict(orient='list')
                                                
                bkcode      = bkinfodf['hq_code'].tolist()[0]
                
                bkname      = bkinfodf['hq_name'].tolist()[0]
                
                SheetName   = bkname +'('+bkcode +')'                               
                
                bkp_tuple =(bkcodes,bkstockdict,idataXY_dict,stockXY_dict,bench_XY,bench_key)
                
                wbk = self.bulidSigleExcelPic(wbk,IData_Sheet,stockColumns,bkp_tuple,SheetName)
            

                        
            
#            #画规模指数  
#            SheetName =u'异动行业（个股）' 
#            gm_tuple =(scalestr,scaleDict,idataXY_dict,stockXY_dict,bench_XY,bench_key)                
#            wbk = self.bulidUniformExcelPic(wbk,IData_Sheet,stockColumns,gm_tuple,SheetName)
#        
#        if boardType==2:
#            #画通达信板块指数
#            SheetName =u'异动概念'
#            bkp_tuple =(bkcodes,Abkdict,idataXY_dict,AbkXY_dict,bench_XY,bench_key)
#            wbk = self.bulidUniformExcelPic(wbk,IData_Sheet,xdiColumns,bkp_tuple,SheetName)
#            
#            
#            #画规模指数  
#            SheetName =u'异动概念（个股）' 
#            gm_tuple =(scalestr,scaleDict,idataXY_dict,stockXY_dict,bench_XY,bench_key)                
#            wbk = self.bulidUniformExcelPic(wbk,IData_Sheet,stockColumns,gm_tuple,SheetName)
##        
#        #画通达信细分板块指数        
#        SheetName =u'行业细分指数'
#        xfbkp_tuple =(xfbkcodestr,Abkdict,idataXY_dict,AbkXY_dict,bench_XY,bench_key)        
#        wbk = self.bulidUniformExcelPic(wbk,IData_Sheet,xdiColumns,xfbkp_tuple,SheetName)
#        
#              
        return wbk
        
    # 在excel中插入基准指数的数据，lef，top 分别代表 x，y坐标
        
    def bulidIndexDataToExcel(self,bmi_list,IData_Sheet,bmiColumns,left,top):
        
        #写入指数数据头
        
        idataXY_dict ={}
        
                
        for dflist in  bmi_list:
            
           if len(dflist)==2:
               
               bkidf_code  = dflist[0]
                                        
               bkidf_item  = dflist[1]
               
               bkidf_item['hq_date'] = bkidf_item['hq_date'].astype('str')
               
               bkidf_len   = len(bkidf_item)
               
               tmpdatalist = [bkidf_code,top,left,bkidf_len]
                              
               idataXY_dict[int(bkidf_code)] = tmpdatalist
               
               #写入头
               IData_Sheet.write_row(top, left,bmiColumns)
               
                   
               for row in range(0,bkidf_len):   
                  #for col in range(left,len(fields)+left):  
                  
                  tmplist  = bkidf_item[row:row+1].values.tolist()
                                              
                  datalist = tmplist[0]
                                                  
                  IData_Sheet.write_row(top+row+1, left,datalist)
               
               top = top   + bkidf_len + 2
               
        return  IData_Sheet,idataXY_dict
        
    #构建指数excel构架    
    def bulidIndexExcelFrame(self,bmidf,xdhead_idf,bkStockdict,bkStockChgdict,KlineType):
              
        bkdNum = 12  # 按12个行业来区分
        
        bklen  = len(bkStockdict)     
        
        excelCount =  bklen/bkdNum  
           
        pwd   =  os.getcwd()
        
        fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep)
        
         
        if len(xdhead_idf)>0:
                
         #数据分组        
            xdhead_group = xdhead_idf.groupby('hq_code',sort=False)
                
            bkidf_list= list(xdhead_group)
          
        #按板块个数来生成excel文件
        
        for exlnum in range(excelCount):
            
            data_left = 0    #数据起始列
        
            pic_left  = 0    #图像起始列 
            
            data_top  = 0    #数据起始行
            
            pic_top   = 2    #图像起始行
                            
            execlfname  = fpwd + self.BCfname +'-'+ str(exlnum)+'('+KlineType+').xlsx'
            
            execlfname  = execlfname.decode()
            
            wbk =xlsxwriter.Workbook(execlfname)  
                    
            #newwbk = copy(wbk)
            QR_Sheet   = wbk.add_worksheet(u'指数相对强弱')
        
            IData_Sheet = wbk.add_worksheet(u'指数数据')
            
            
            headStr='强弱排名（涨幅）'
            
            headvol= '量比（金额）'
                        
            xdiColumns= list([u'板块代码', u'板块名称', u'日期',u'时间', u'基准板块代码', u'基准板块名称', u'收盘价', u'前收盘价', u'成交量',u'成交额' ,u'日相对涨跌幅', u'累计相对涨跌幅', u'相对量比', u'相对金额比'])
                  
            xdiColumnlens = len(xdiColumns) 
            
             
            red = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'C0504D','font_size':16,'font_color':'white'})
            
            blue = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'8064A2','font_size':16,'font_color':'white'})
            #间隔格式
            JG = wbk.add_format({'bg_color':'CCC0DA'})
            
            #处理第一行excel格式
            QR_Sheet.merge_range(0,0,1,xdiColumnlens+10,headStr,red)         
            QR_Sheet.set_column(xdiColumnlens+1+10,xdiColumnlens+1+10,0.3,JG)
    
            QR_Sheet.merge_range(0,xdiColumnlens+2+10,1,2*(xdiColumnlens+10)+2,headvol,blue)               
            QR_Sheet.set_column(2*xdiColumnlens+3+20,2*xdiColumnlens+3+20,0.3,JG)
            
                    
            #基准数据写入数据sheet中
            if len(bmidf)>0:
                bmidf_group = bmidf.groupby('hq_code')
        
                bmi_list = list(bmidf_group)
                
                bmiColumns = list([u'基准指数代码', u'基准指数名称', u'日期',u'时间', u'收盘价', u'前收盘价', u'成交量', u'涨跌幅', u'累涨跌幅',u'总成交量',u'总成交额'])
                
                #未处理多个基准标的比较问题，以及标的指数与板块数据不一致的问题
                (IData_Sheet,idataXY_dict) = self.bulidIndexDataToExcel(bmi_list,IData_Sheet,bmiColumns,data_left,data_top)
                
                data_left = data_left +len(bmiColumns) +2
              
            if len(xdhead_idf)>0:
                #数据分组        
                                
                (wbk,pic_left,bkdataXY_dict,bkXY_dict)  = self.bulidExcelPic(bkidf_list,wbk,QR_Sheet,IData_Sheet,xdiColumns,data_left,pic_left,data_top,pic_top,bkStockdict,xdsColumns,bkStockChgdict,exlnum,bkdNum,KlineType)
                
                data_left = data_left+xdiColumnlens+2
      
            wbk.close()
        
    #处理指数数据
    def getExcelIndexData(self,bmidf,benchmarkName):
        
        retdata  = pd.DataFrame()
        
        if len(bmidf)>0:
            
            tmpdata = bmidf[1:]
#                                                   
            tmpdata['hq_name'] =benchmarkName        
#            
            retdata = tmpdata[['hq_code','hq_name','hq_date','hq_time','hq_close','hq_preclose','hq_vol','hq_chg','hq_allchg','hq_allvol','hq_allamo']]
        
        return retdata
        
     # 获取前几，后几排名数据 
    def getSortedIndexdf(self,xd_idf,sortlist):
        
        idf_len = len(sortlist)
        
        tmplist = []
        
        bkidf_group = xd_idf.groupby('hq_code')
            
        #生成排名dict        
        bkidf_dict = dict(list(bkidf_group))
        
        
        xdtmp_idf = pd.DataFrame()
        
        xdhead_idf = pd.DataFrame()
    
        #先整体df进行排序，然后再进行分类
    
        for sloc in range(idf_len):
            
            tmpitem  = sortlist[sloc]
                       
            tmplist.append(tmpitem[0])
       
      
        for tlist in tmplist:
        
          dictkey = str(tlist) 
       
          if(bkidf_dict.has_key(dictkey)):
           
            bkitem = bkidf_dict[dictkey]
           
            xdtmp_idf=  xdtmp_idf.append(bkitem) 
        
        #如果个数大于，指定显示数量
        if idf_len>0 :
            
             #获取前几后几 
            for sloc in range(idf_len):
                
                headitem  = sortlist[sloc]
                
                hstr      = headitem[0]
                                
                dictkey = str(hstr) 
               
                if(bkidf_dict.has_key(dictkey)):
                   
                   bkitem = bkidf_dict[dictkey]
                   
                   xdhead_idf=  xdhead_idf.append(bkitem)
            
        else:
            
            xdhead_idf = xdtmp_idf
           
            
        return xdtmp_idf,xdhead_idf

        
    #excel中plot相对指数强弱图形
    def PlotIndexPicToExcel(self,benchmarkIndex,allMarketIndex,bkcodestr,bkxfdf,startDate,endDate,KlineType,bkdict,fxflinedict):
        
        
        indexType = 'BoardIndex'
        
        #获取指数排名数据        
        hidf  =  self.getPlotIndexData(bkcodestr,startDate,endDate,KlineType,indexType)
        
        #获取股票排名数据 
        stockdf  =  self.getPlotStockData(startDate,endDate,KlineType)
          
       
        #test     =  self.getAllIndexOrStockChg(stockdf)
        
        indexType = 'ScaleIndex'
        
        #获取基准指数数据
        bmidf =  self.getPlotIndexData(benchmarkIndex,startDate,endDate,KlineType,indexType)
        
        #获取所有指数成交量，成交额数据        
        allrawdf  =  self.getPlotIndexData(allMarketIndex,startDate,endDate,KlineType,indexType)
        
                
        #计算所有
        if KlineType =='D':
                
            Salldf     =   allrawdf.groupby('hq_date').agg(
                          {'hq_vol':'sum',
                           'hq_amo':'sum'                       
                          })
            
            Salldf['hq_date'] = Salldf.index
            
            Salldf = Salldf.set_index(bmidf.index)
            
        else:
            
            Salldf     =   allrawdf.groupby('index').agg(
                          {'hq_vol':'sum',
                           'hq_amo':'sum'                       
                          })
            
            Salldf['hq_date'] = Salldf.index
            
            Salldf = Salldf.set_index(bmidf.index)
        
                        
        bmidf['hq_allvol'] = Salldf['hq_vol']
        
        bmidf['hq_allamo'] = Salldf['hq_amo']
        
        
        if 'hq_time' not in hidf.columns:
            hidf['hq_time']  = '00:00:00'
            
        
        if 'hq_time' not in stockdf.columns:
            stockdf['hq_time']  = '00:00:00'
            
            
        
        if 'hq_time' not in bmidf.columns:
            bmidf['hq_time']  = '00:00:00'
                
        
        if 'hq_time' not in allrawdf.columns:
            allrawdf['hq_time']  = '00:00:00'
                
        
        bmidf['hq_time'] = bmidf['hq_time'].astype('str')
        
        #获取所有指数排名数据 
        (xd_idf,sortlist,bkStockdict,bkStockChgdict) = self.getIndexXdQr(bmidf,hidf,bkdict,bkxfdf,stockdf)        
                
        #处理excel中的指数数据
        ebmidf  = self.getExcelIndexData(bmidf,bkdict)
        
        # 获取前几，后几排名数据     
        (xdtmp_idf,xdhead_idf) =self.getSortedIndexdf(xd_idf,sortlist)
                      
        #画出指数排名图形（所有图形）
        self.bulidAllIndexExcelFrame(ebmidf,xdtmp_idf,KlineType,fxflinedict)
        
        #画出指数排名图形
        self.bulidIndexExcelFrame(ebmidf,xdhead_idf,bkStockdict,bkStockChgdict,KlineType)
          
 
    #生成所有报表
    def plotDataToReport(self,baseTuple,tdxlineTuple,tdxconceptTuple,KlineType,timeTuple):
                
        startDate = timeTuple[0]        
        endDate   = timeTuple[1]      
        
        #获取所有基本数据
        benchmarkIndex = baseTuple[0]        
      
        benchmarkName  = baseTuple[1]
      
        allMarketIndex = baseTuple[2]   
        
        #获取通达信行业数据
        Abkdict     = tdxlineTuple[0]
        
        Abkcodestr  = tdxlineTuple[1]
        
        Abkxfdf     = tdxlineTuple[2]
        
        bkcodestr   = tdxlineTuple[3]   
        
        xfbkcodestr = tdxlineTuple[4]
        
        fxflinedict = tdxlineTuple[5]
        
        #获取通达信概念数据        
        cfdict      = tdxconceptTuple[0]
        
        cfcodestr   = tdxconceptTuple[1]   
        
        cfdf_xf     = tdxconceptTuple[2]
                     
        #获取基准指数数         
        indexType = 'ScaleIndex'
        
        bmidf =  self.getPlotIndexData(benchmarkIndex,startDate,endDate,KlineType,indexType)

        #获取所有指数成交量，成交额数据        
        allrawdf  =  self.getPlotIndexData(allMarketIndex,startDate,endDate,KlineType,indexType)
                    
        #计算所有
        if KlineType =='D':
                
            Salldf     =   allrawdf.groupby('hq_date').agg(
                          {'hq_vol':'sum',
                           'hq_amo':'sum'                       
                          })
            
            Salldf['hq_date'] = Salldf.index
            
            Salldf = Salldf.set_index(bmidf.index)
            
        else:
            
            Salldf     =   allrawdf.groupby('index').agg(
                          {'hq_vol':'sum',
                           'hq_amo':'sum'                       
                          })
            
            Salldf['hq_date'] = Salldf.index
            
            Salldf = Salldf.set_index(bmidf.index)
        
                        
        bmidf['hq_allvol'] = Salldf['hq_vol']
        
        bmidf['hq_allamo'] = Salldf['hq_amo']
        
        if 'hq_time' not in bmidf.columns:
            bmidf['hq_time']  = '00:00:00'
                        
        if 'hq_time' not in allrawdf.columns:
            allrawdf['hq_time']  = '00:00:00'
       
        bmidf['hq_time'] = bmidf['hq_time'].astype('str')
        
        #获取通达信各指数             
        indexType = 'BoardIndex'
        
        isXf  = True
        
        boardType =1 
        
        indexDataTuple = (Abkcodestr,startDate,endDate,KlineType,indexType,fxflinedict)
        
        amolinedf = self.plotTdxBoardData(indexDataTuple,bmidf,Abkdict,Abkxfdf,isXf,boardType)
        
      
    #excel中plot相对指数强弱图形
    def dealBoardInfo(self,bkLinedf,bkxfdf,benchmarkName):
        #通达信行业代码    
        bkLinedf['bz_indexcode'] = bkLinedf['bz_indexcode'].astype('int')
        
        bkdict = bkLinedf.set_index('bz_indexcode')['bz_name'].to_dict()
            
        bkdict[int(benchmarkIndex)]=benchmarkName
                
        bkcodes = bkLinedf['bz_indexcode'].astype('str')
                    
        bkcodestr = ','.join(bkcodes)
        
        bkxfdf[['board_id']]= bkxfdf[['board_id']].astype(str)
        
        bkxfdf['board_name'] = ((bkxfdf['board_name'].str.replace(u'通达信行业-','')).str.replace(u'通达信细分行业-','')).str.replace(u'通达信概念-','')
        
        
        return bkdict,bkcodestr,bkxfdf
    
    def getTdxXfLine(self):         
             
        #配置文件目录
            
        pwd   =  os.getcwd()
            
        fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep)
            
        dbfname  = fpwd + self.lineDir
                
        fheader = ['Lcode','LName','Line','Linexf']
            
        #生成目录字典
        fdata=pd.read_table(dbfname.decode('utf-8'),header=0,names=fheader,delimiter=',')
                        
        fxflinegroup = fdata.groupby('Linexf')
            
        fxflinedict  = dict(list(fxflinegroup))
        
        del fxflinegroup,fdata    
        
        gc.collect()
        
        return fxflinedict
    
    def dealTdxLine(self,AlineSortlist,fxflinedict):

        xflinesortlist = copy.deepcopy(AlineSortlist)
                
        linesortlist   = []
        
        tdxdict        = {}
        
        tdxXfdict      = {}
        
        #处理 fxflinedict 中的数据，key为0 代表 有细分行业指数，key为1 代表无细分行业指数
        if fxflinedict.has_key(0) and fxflinedict.has_key(1):
            
            lineitem    = fxflinedict[0]
            
            xflineitem  = fxflinedict[1]
            
            #通达信行业
            tdxlineItem = lineitem.append(xflineitem)
            
            del fxflinedict[0]
            
            del fxflinedict[1]
            
            tdxgroup     = tdxlineItem.groupby('Lcode')
                
            tdxdict      = dict(list(tdxgroup))
            
            tdxXfdict    = fxflinedict
            
            #只做一级行业排名                      
            for lsl in AlineSortlist:
                
                linecode = lsl[0]     
                                 
                if tdxdict.has_key(linecode):
                    
                   linesortlist.append(lsl) 
                   
                   xflinesortlist.remove(lsl) 
                    
        return tdxdict,linesortlist,tdxXfdict,xflinesortlist
    
    def dealBoardAmo(self,AxdLine_idf,KlineNum,KlineParam,threshold):
        
       axdline_group = AxdLine_idf.groupby('hq_code')
        
       axdline_dict  = dict(list(axdline_group))
       
       ramolinedf    = pd.DataFrame()
       
       amolinedf     = pd.DataFrame()
       
       linedf        = pd.DataFrame()
       
       for ldict in axdline_dict:
           
           lineitem = axdline_dict[ldict]
           
           amolinedf = lineitem[['hq_code','hq_name','hq_date','hq_time','hq_chg','hq_xdamo']]
                  
           dataLen=len(amolinedf)
                      
           try:
               amolinedf.index=xrange(dataLen)
           except:
               print amolinedf.iat[0,0]
               print len(amolinedf)
              
           abnormalNum=0
       
           abnormalValues=[]
       
           abnormalSum=[]
              
           for i in range(dataLen-KlineNum,dataLen):
               
               print i 
           
               xdamo_Mean= amolinedf[i-KlineParam:i]['hq_xdamo'].mean()
               
               xdamo_Std =  amolinedf[i-KlineParam:i]['hq_xdamo'].std()
           
               if xdamo_Std != 0:                   
                   amo_Std=amolinedf.loc[i,'hq_xdamostd']=(amolinedf.iloc[i]['hq_xdamo']-xdamo_Mean)/xdamo_Std
               
               else:
                   amo_Std=0
           
               if amo_Std>=threshold:
                  abnormalNum+=1
                  abnormalValues.append(amo_Std)
                  abnormalSum.append(amolinedf.iloc[i]['hq_xdamo'])
            
               abnormalValueSeries =pd.Series(abnormalValues)
               abnormalSumSeries =pd.Series(abnormalSum)
       
           amolinedf['hq_abnNum']=abnormalNum
           amolinedf['hq_abnavg']=abnormalValueSeries.mean()
           amolinedf['hq_amoavg']=abnormalSumSeries.mean()
           
           amolinedf.dropna(inplace=True)
           
           if len(amolinedf)>0:
               
               amolinedf = amolinedf[amolinedf['hq_xdamostd']>threshold]
                                     
               ramolinedf = ramolinedf.append(amolinedf)
               
               linedf     = linedf.append(lineitem)
           
       #test = dict(list(ramolinedf.groupby('hq_code')))
           
       del amolinedf,axdline_group,axdline_dict

       gc.collect()
                 
       return ramolinedf,linedf        
        
     
    def plotTdxBoardData(self,indexDataTuple,bmidf,Abkdict,Abkxfdf,isXf,boardType):
               
        #indexDataTuple = (Abkcodestr,startDate,endDate,KlineType,indexType)
        
        Abkcodestr    = indexDataTuple[0]
        
        startDate     = indexDataTuple[1]
        
        endDate       = indexDataTuple[2]
        
        KlineType     = indexDataTuple[3]
        
        indexType     = indexDataTuple[4]
        
        fxflinedict   = indexDataTuple[5]
        
        ramolist      = []
        
        stockdf     = pd.DataFrame()
        
        tdxline_df    = self.getPlotIndexData(Abkcodestr,startDate,endDate,KlineType,indexType)
        
        
        if 'hq_time' not in tdxline_df.columns:
            tdxline_df['hq_time']  = '00:00:00'
            
         #获取所有指数排名数据(所有通达信行业) 
        (AxdLine_idf,AlineSortlist) = self.getIndexXdQr(bmidf,tdxline_df,Abkdict)
        
                       
        #删除多余数据
        del tdxline_df
        
        gc.collect()
        
        #取几日数据
        KlineNum = 5
        
        #算多少日标准差
        KlineParam =20
        
        #标准差阈值
        threshold = 3
        
        #开始分析各板块异动
        (ramolinedf,linedf) = self.dealBoardAmo(AxdLine_idf, KlineNum ,KlineParam,threshold)
        
        ramolinedf=ramolinedf.sort_values(['hq_abnNum','hq_abnavg'],ascending=False)
        
        ramoline_group = ramolinedf.groupby('hq_code',sort=False)
        
        ramolinelist  = list(ramoline_group)
        
        ramolen       = len(ramolinelist)
        
        ramoRange     = 5
        
        bkstock_group = Abkxfdf.groupby('board_id')
        
        bkstock_dict  = dict(list(bkstock_group))
        
        bkcodes  =''
        
        
        if int(ramolen*0.3)>=5:
            ramoRange = int(ramolen*0.3)
        
        if  ramolen<=5:
            ramoRange = ramolen
   
        for rlen in range(ramoRange):
            
            ramoitem = ramolinelist[rlen]
            
            ramocode = ramoitem[0]
            
            if bkcodes=='':
               bkcodes  = ramocode
            else:
               bkcodes  = bkcodes +','+ramocode 

            ramodf   = ramoitem[1]
            
            if bkstock_dict.has_key(ramocode):
                
                bkstock_item = bkstock_dict[ramocode]
                
                bkstock_id   = bkstock_item['stock_id'].astype('str')
                
                bkcodestr = ','.join(bkstock_id)
                
                bkcodestrs = 'in('+bkcodestr+')'
                
                #获取股票排名数据 
                stock_df     =  self.getPlotStockData(startDate,endDate,KlineType,bkcodestrs)
                               
                xdidf_ret   =  self.getBmiStockChg(stock_df,bmidf,bkstock_item)
                
                (stocklinedf,slinedf) =  self.dealBoardAmo(xdidf_ret, KlineNum ,KlineParam,threshold)
                
                stocklinedf =  stocklinedf.sort_values(['hq_abnNum','hq_abnavg'],ascending=False)
                
                scodes =  stocklinedf[['hq_code']]
                
                scodes = scodes.drop_duplicates() 
                
                bkstock_id   = scodes['hq_code'].astype('str')
                
                bkscodestr  = ','.join(bkstock_id)
                                
                ramotuple   =(ramocode,ramodf,stocklinedf,bkscodestr)
                
                ramolist.append(ramotuple)
                
                stockdf = stockdf.append(slinedf)
                        
        #处理excel中的指数数据
        ebmidf  = self.getExcelIndexData(bmidf,benchmarkName)    
                
        if isXf:
            #对sortlist进行分离，求出行业与细分 
           (tdxdict,linesortlist,tdxXfdict,xflinesortlist) = self.dealTdxLine(AlineSortlist,fxflinedict) 
        
            
        self.bulidBoardExcelFrame(ebmidf,ramolist,Abkdict,bkcodes,linedf,stockdf,KlineType,boardType)
        
        return 1     
        
if '__main__'==__name__:  
        
    amosty = AmoStrategy()
        
    Adate = datetime.now().strftime('%Y-%m-%d')    
        
    #全市场指数（000002 A股指数，399107深圳A指）
    allMarketIndex = '000002,399107'
    
    #基准对比指数
    benchmarkIndex = '399317'
        
    benchmarkName  =u'国证A指'
           
    KlineDict ={}
        
    #Adate='2017-09-18'    
    #K线时间类型    
    KlineType ='5M'
        
    #获取数据起始时间
    start_date = datetime.strptime("2017-09-01", "%Y-%m-%d")
    
    end_date   = datetime.strptime(Adate, "%Y-%m-%d")
    
    timetuple   =(start_date,end_date)
    
    KlineDict[KlineType] = timetuple
#        
    #K线时间类型   
    KlineType ='D'
        
    #获取数据起始时间
    start_date = datetime.strptime("2017-01-01", "%Y-%m-%d")
    
    end_date   = datetime.strptime(Adate, "%Y-%m-%d")
    
    timetuple   =(start_date,end_date)    
    
    KlineDict[KlineType] = timetuple
    
    #数据库接口    
    mktindex = amosty.mktindex
             
    #通达信行业
    (bkLinedf,bkxfdf) = mktindex.MktIndexToStocksClassify('80201')
    
    #通达信细分行业
    (bkLinedf_xf,bkxfdf_xf) = mktindex.MktIndexToStocksClassify('80202')
    
     #通达信概念
    (cfLinedf_xf,cfxfdf_xf) = mktindex.MktIndexToStocksClassify('80301')  

    fxflinedict  =  amosty.getTdxXfLine()    
    
    #通达信所有板块
    AbkLinedf = bkLinedf.append(bkLinedf_xf, ignore_index=True)
        
    Abkxfdf = bkxfdf.append(bkxfdf_xf, ignore_index=True)
    
    (Abkdict,Abkcodestr,Abkxfdf) = amosty.dealBoardInfo(AbkLinedf,Abkxfdf,benchmarkName)
    
    (bkdict,bkcodestr,bkxfdf) = amosty.dealBoardInfo(bkLinedf,bkxfdf,benchmarkName)
    
    (xfbkdict,xfbkcodestr,bkxfdf_xf) = amosty.dealBoardInfo(bkLinedf_xf,bkxfdf_xf,benchmarkName)
    
    (cfdict,cfcodestr,cfdf_xf) = amosty.dealBoardInfo(cfLinedf_xf,cfxfdf_xf,benchmarkName)
        
    #基础指数    
    baseTuple  =(benchmarkIndex,benchmarkName,allMarketIndex)
    
    #通达信行业
    tdxlineTuple =(Abkdict,Abkcodestr,Abkxfdf,bkcodestr,xfbkcodestr,fxflinedict)
    
    #通达信概念
    tdxconceptTuple =(cfdict,cfcodestr,cfdf_xf)
    
    for kdict in KlineDict:
       #指数分类对比图
       KlineType = kdict
       
       timeTuple = KlineDict[KlineType] 
    
       amosty.plotDataToReport(baseTuple,tdxlineTuple,tdxconceptTuple,KlineType,timeTuple)
    
    
    #所有指数强弱，量比图
    
    #获取板块与下属关联股票
    
    
    
    
    #所有指数强弱，量比图
    
    
    
    
    