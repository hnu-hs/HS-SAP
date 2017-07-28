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

import sys
sys.path.append("..\..")
reload(sys)
sys.setdefaultencoding('utf8')

from DataHanle.MktDataHandle import MktIndexHandle

from datetime import datetime


class TmpDealData():
    #初始化程序
    def __init__(self):
        
        #将补齐数据配置到指定文件夹
        self.dbdataDir = '\\BoardIndex\\config\\通达信数据补齐目录\\Tdxdb.txt'       
        
        #一次生成文件个数
        self.fbulidNum = 30
        
         #数据库引擎
        self.engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
#        
#        #通达信行业目录
#        self.linedir_Day = u'F:\\股票数据\\通达信板块\\行业\\日线'
#        
#        
#        self.linedir_5Min = u'F:\\股票数据\\通达信板块\\行业\\5分钟线'
#        
#        
#        self.linedir_1Min = u'F:\\股票数据\\通达信板块\\行业\\1分钟线'
#        
#        #通达信规模指数目录
#        self.scaleindexdir_Day = u'F:\\股票数据\\通达信板块\\规模指数\\日线'
#        
#        self.scaleindexdir_5Min = u'F:\\股票数据\\通达信板块\\规模指数\\5分钟线'
#        
#        self.scaleindexdir_1Min = u'F:\\股票数据\\通达信板块\\规模指数\\1分钟线'
#        
##        self.scaleindexdir = u'F:\\股票数据\\通达信板块\\规模指数\\test\\'
#        
#        #通达信个股目录
#        self.stockdir_Day = u'F:\股票数据\\个股数据\\日线\\复权\\'
#        
#        self.stockdir_5Min = u'F:\股票数据\\个股数据\\5分钟线\\复权\\'
#        
#        self.stockdir_1Min = u'F:\股票数据\\个股数据\\1分钟线\\复权\\'
    
    #采取读取配置文件方式    
    def getAllTypeDir(self):
        
        #配置文件目录
        
        pwd   =  os.getcwd()
        
        fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep+"..")
        
        dbfname  = fpwd + self.dbdataDir
        
        fheader = ['MktType','DbTable','DataPath']
        #生成目录字典
        fdata=pd.read_table(dbfname.decode('utf-8'),header=1,names=fheader,delimiter=',')
               
        fdatagroup  = fdata.groupby('MktType')
        
        fdict = dict(list(fdatagroup))
        
        #遍历所要补齐的目录
        for fkey in fdict:
                            
            fitemdf = fdict[fkey]
            
            dbtables = fitemdf['DbTable'].tolist()
            
            datapaths = fitemdf['DataPath'].tolist()
            
            #合并成一个dict
            dbtPathdict =dict(zip(dbtables,datapaths)) 
            
            #遍历所有目录
            
            for dbtkey in dbtPathdict:
                
                dtable  = str(dbtkey)
                
                dbInfo = dbtPathdict[dbtkey]
                
                #利用#号来区分时间级别
                dbPos  = dbInfo.find('#')
                
                if  dbPos!=-1:
                    
                    dataPath  = dbInfo[0:dbPos]
                    
                    dataPath  = str(dataPath).decode('unicode_escape')
                    
                    klinetype = dbInfo[dbPos+1:]
                    
                    self.setTdxData(fkey,dataPath,klinetype,self.fbulidNum,dtable,self.engine,2)
                
        m =11
        
    #构建通达信通用数据录入函数
    def setTdxData(self,fkey,fdir,klinetype,fnum,dtable,engine,lastrow):
        
        
        flist = os.listdir(fdir)
        
        sheader_Day =['hq_date','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
        
        sheader_Min =['hq_date','hq_time','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
        
        Afdata = pd.DataFrame()
        
        fcount = 0
        
        if flist:
           lastfile=flist[-1]
              
        for fl in  flist:
            
            fname  = fdir + str(fl)   
            
            fcount = fcount +1 
            
            #文件读取第一行信息
            try:
                
                rfile  = open(fname,'r') 
                
                fline = rfile.readline()
                
                flinelist = fline.split()
                
                if len(flinelist)==4 :
                    
                    # 品种代码
                    flcode = flinelist[0]
                    
                    # 品种名称
                    flname = flinelist[1]
                    
                    # 数据类型，通达信支持三种类型数据：1.日线，2. 5分钟， 3. 1分钟
                    flklinetype = flinelist[2]
                     
                    # 数据是否已除权 
                    flrights   = flinelist[3] 
                    
                    flklinetype = flklinetype.strip() 
                    
                    fdataflag = False
                                                            
                    #使用pandas格式读取数据
                                                            
                    print fname
                                                        
                    if klinetype=="Day":
                                                
                        fdata=pd.read_table(fname,header=1,names=sheader_Day)
                        
                        if len(fdata)>lastrow and lastrow>=1:
                           
                           fdata=fdata.iloc[:-lastrow] 
                                                                              
                           fdatelist =fdata['hq_date'].tolist()
                           
                           fdataflag = True
                           
                        
                    else:
                        
                        fdata=pd.read_table(fname,header=1,names=sheader_Min,dtype={'hq_time':str})
                             
                        if len(fdata)>lastrow and lastrow>=1:
                            
                           fdata=fdata.iloc[:-lastrow] 
                                               
                           fdatedf  =  fdata['hq_date'] + " " + fdata['hq_time']
                                                
                           fdatelist = fdatedf.tolist()
                           
                           fdataflag = True
                    
                    if fdataflag:
                        
                        if '\xef\xbb\xbf'  in flcode:
                            flcode = flcode.replace('\xef\xbb\xbf','')
                            
                        fdcolumnsIndex = fdata.columns
                        
                        # 保留小数点后2位
                        
                        if 'hq_amo' in fdcolumnsIndex:
                            
                            tmp  = fdata['hq_amo'] /100000000
                                                    
                            fdata['hq_amo'] = tmp.round(2)
                            
                        if 'hq_vol' in fdcolumnsIndex:
                            
                            if fkey!='Stock':                       
                                tmp  = fdata['hq_vol'] /10000
                            else:
                                tmp  = fdata['hq_vol'] /1000000
                            
                            fdata['hq_vol'] = tmp.round(2)
                            
                        
                        
                        fdata['hq_code'] = int(flcode)   
                        
                        findex=pd.to_datetime(fdatelist)
                        
                        fdata.set_index(findex,inplace=True)
                        
                        Afdata=Afdata.append(fdata)
                        
                        if fnum<0:
                            fnum  = 1
                        
                        if fcount % int(fnum)==0:
                            
                                Afdata.to_sql(dtable,con=engine,if_exists='append')
                                
                                Afdata=pd.DataFrame()
                        
                        elif fl==lastfile:   
                                            
                            Afdata.to_sql(dtable,con=engine,if_exists='append')
                            
                            Afdata=pd.DataFrame()
                                          
                    m =11
                    
                    print fcount
             
            finally:
                        
                if rfile:            
                    rfile.close()
 
 
class PlotToExcel():
    
    #初始化程序
    def __init__(self):
        
        self.mktindex = MktIndexHandle()
                
        self.BCfname = '\\PlotData\\export\\excel\\板块强弱分类数据.xlsx'
        
        self.BAfname = '\\PlotData\\export\\excel\\板块强弱数据.xlsx'
        
        pass;
          
    
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
    def getPlotStockData(self,startDate,endDate,KlineType):
        
        mktindex =  self.mktindex
        
        mktdf = pd.DataFrame()
        
        mktstock  = mktindex.mktstock
        
        securityID='>=000001 ' 
        
        mktdf = mktstock.MktStockBarHistDataGet(securityID,startDate,endDate,KlineType)
                
        return mktdf
        
    #获取涨跌幅
    def getIndexOrStockChg(self,idf):
        
        if len(idf)>1:
            
            idf_item   = idf.head(1)
            
            idf_tmp    = idf_item['hq_close'].values
            
            idf_close  = idf_tmp[0]
        
            idf['hq_preclose'] = idf['hq_close'].shift(1)
            
            idf['hq_chg']= ((idf['hq_close']/idf['hq_preclose'] -1)*100).round(2)
            
            idf['hq_allchg']= ((idf['hq_close']/idf_close -1)*100).round(2)
        
        idf_ret = idf
        
        return idf_ret
            
        
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
#                   
#                   print idf_ret
                   
                   idf_ret['hq_xdallchg'] = idf_ret['hq_allchg']
                                  
                   idf_ret.loc[:,['hq_xdallchg']]  = idf_ret['hq_allchg']- idf_ret['bhq_allchg']
                                                  
                   idf_ret['hq_xdamo']  = (idf_ret['hq_amo']/idf_ret['bhq_amo']*100).round(2)
                    
                   xdidf_ret = xdidf_ret.append(idf_ret)
                   
                   #获取所有股票的涨跌幅
                   stockChgList  = idf_ret['hq_allchg'].tolist()
                   
                   stockChgDict[idict] =  stockChgList.pop()
                                      
               
        return xdidf_ret,stockChgDict
        
    #获取指数相对强弱，相对量能
    def getIndexXdQr(self,bmidf,bkidf,bkdict,bkxfdf,stockdf):
        
        xdidf_ret = pd.DataFrame()
        
        dict_ret  ={}  
        
        stockdf_len = len(stockdf)
        
        Mstockdf = pd.merge(stockdf,bkxfdf,left_on='hq_code',right_on='stock_id')
        
        stockdf  = pd.DataFrame()
        #对板块个股进行分类
                
        Mistockdf_group = Mstockdf.groupby('board_id')
        
                
        #板块个股关联        
        bkidf_dict = dict(list(Mistockdf_group))
        
        Mstockdf  = pd.DataFrame()
        
        gc.collect()
        
        #如果板块有数据，计算处理
        
        if len(bkidf)>0 and len(bmidf)>0 and stockdf_len>0 :
            
            
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
            
            sortdict = {}
            
            bkStockdict ={}           
            
            bkStockChgdict ={}
                
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
                
                tmpdf['hq_name']  = hq_name
                
                
                tmpdf.loc[:,['hq_chg']]   = tmpdf['hq_chg']- tmpidf['hq_chg']
                
                tmpdf.loc[:,['hq_allchg']]  = tmpdf['hq_allchg']- tmpidf['hq_allchg']
                
                tmpidf.loc[:,['hq_vol']] = np.where(tmpidf['hq_vol']==0,np.where(tmpdf['hq_vol']==0,-1,-tmpdf['hq_vol']),tmpidf['hq_vol'])
                
                tmpdf['hq_xdvol']  = (tmpdf['hq_vol']/tmpidf['hq_allvol']*100).round(2)
                
                
                tmpcolums = tmpdf.columns
                
                if 'hq_amo' in tmpcolums:                    
                    tmpdf['hq_xdamo']  = (tmpdf['hq_amo']/tmpidf['hq_allamo']*100).round(2)
                
                
                if dictcount<=1:
                    xdhead_array = tmpdf.values
                else:
                    xdhead_array = np.concatenate([xdhead_array,tmpdf.values],axis=0)
                
                
                #加入涨跌幅的排名
                
                hichg = hidf_ret['hq_allchg'].tolist()
                
                if len(hichg)>0:
                   #得到最后一个数据 
                   sortdict[dfdict] =  hichg.pop()
                
                bkidf_tmp =  tmpdf[['hq_code','hq_date','hq_chg','hq_allchg','hq_vol','hq_amo']]
                
                
                
                bkidf_tmp.rename(columns={'hq_code': 'bhq_code', 'hq_date': 'bhq_date', 'hq_chg': 'bhq_chg', 'hq_allchg': 'bhq_allchg', 'hq_vol': 'bhq_vol', 'hq_amo': 'bhq_amo'}, inplace=True) 
                
                bkidf_tmp['index'] = bkidf_tmp.index
                
                #找到指数对应股票的行情数据
                if bkidf_dict.has_key(str(dfdict)):
                    
                   bkitem  = bkidf_dict[str(dfdict)]
                   
                   (tmpstockdf,stockChgDict) = self.getAllStockChg(bkitem,bkidf_tmp)
                   
                   bksrdf     = tmpstockdf[['board_id','board_name','hq_date','stock_id','stock_name','hq_close','hq_preclose','hq_vol','hq_amo','hq_chg','hq_xdallchg','hq_xdamo']]
                    
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
        
        return xdidf_ret,dict_ret,bkStockdict,bkStockChgdict
        
    
    def bulidChart(self,wbk,data_top,data_left,bkidf_len,bktile,idxstr,shift,data_top2,data_left2,bkidf_len2,shift2,style):
        
        bk_chart = wbk.add_chart({'type': 'line'})
               
        bk_chart.set_style(4)
       
        #向图表添加数据 
        
        if style==1:
            bk_chart.add_series({
             'name':[idxstr, data_top+1, data_left+1],
             'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
             'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
             'line':{'color':'red'},
             'y2_axis': True, 
                    
             })            
            
            bk_chart.add_series({
             'name':[idxstr, data_top2+1, data_left2+1],
             'categories':[idxstr, data_top2+1, data_left2+2, data_top2+bkidf_len2, data_left2+2],
             'values':[idxstr, data_top2+1, data_left2+shift2, data_top2+bkidf_len2, data_left2+shift2],
             'line':{'color':'blue'},  
                      
             })
             
        if style==2:     
                 
             bk_chart.add_series({
             'name':[idxstr, data_top, data_left+shift],
             'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
             'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
             'line':{'color':'FF6347'},
             'y2_axis': True,      
             }) 
                          
             bk_chart.add_series({
             'name':[idxstr, data_top2, data_left2+shift2],
             'categories':[idxstr, data_top2+1, data_left2+2, data_top2+bkidf_len2, data_left2+2],
             'values':[idxstr, data_top2+1, data_left2+shift2, data_top2+bkidf_len2, data_left2+shift2],
             'line':{'color':'708090'},  
                       
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
     
     
        bk_chart.set_title({'name':bktile,
                           'name_font': {'size': 10, 'bold': True}
                          })
                                   
        bk_chart.set_x_axis({'name_font': {'size': 10, 'bold': True},
                            'label_position': 'low',
                            'interval_unit': 2
                    
                          })
                        
        bk_chart.set_y_axis({'name':u'日期',
                            'name_font': {'size': 10, 'bold': True}
                       })
   
        bk_chart.set_y_axis({'name':'',
                        'name_font': {'size': 10, 'bold': True}
                        })
                       
        bk_chart.set_size({'width':897,'height':300})
           
        
        return bk_chart
    
     
    
    def bulidALLChart(self,wbk,data_top,data_left,bkidf_len,bktile,idxstr,shift,data_top2,data_left2,shift2,bkCount,style):
        
        bk_chart = wbk.add_chart({'type': 'line'})
               
        bk_chart.set_style(4)
        
        # CD6600 橙色，C1C1C1紫色，556B2F草绿色，696969灰色，
        colorlist =['CD6600','0F0F0F','FF0000','556B2F','FFD700']
        
        
        #基准无需变动
        
        if style==1:
            
            bk_chart.add_series({
             'name':[idxstr, data_top2+1, data_left2+1],
             'categories':[idxstr, data_top2+1, data_left2+2, data_top2+bkidf_len, data_left2+2],
             'values':[idxstr, data_top2+1, data_left2+shift2, data_top2+bkidf_len, data_left2+shift2],
             'line':{'color':'blue'},  
                      
             })
             
               #向图表添加数据 
            for icount in range(bkCount):
                    
                bk_chart.add_series({
                 'name':[idxstr, data_top+1, data_left+1],
                 'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
                 'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
                 'line':{'color':colorlist[icount]},
                 'y2_axis': True, 
                        
                 })                 
                
                data_top+=bkidf_len +2
             
        else:
            
            #向图表添加数据 
            for icount in range(bkCount):
                    
                bk_chart.add_series({
                 'name':[idxstr, data_top+1, data_left+1],
                 'categories':[idxstr, data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
                 'values':[idxstr, data_top+1, data_left+shift, data_top+bkidf_len, data_left+shift],
                 'line':{'color':colorlist[icount]},
                  
                 })                 
                
                data_top+=bkidf_len +2
            
        #bold = wbk.add_format({'bold': 1})
     
     
        bk_chart.set_title({'name':bktile,
                           'name_font': {'size': 10, 'bold': True}
                          })
                                   
        bk_chart.set_x_axis({'name_font': {'size': 10, 'bold': True},
                            'label_position': 'low',
                            'interval_unit': 2
                    
                          })
                        
        bk_chart.set_y_axis({'name':u'日期',
                            'name_font': {'size': 10, 'bold': True}
                       })
   
        bk_chart.set_y_axis({'name':'',
                        'name_font': {'size': 10, 'bold': True}
                        })
                       
        bk_chart.set_size({'width':897,'height':300})
           
        
        return bk_chart    
    #构建指数excel构架，,data_left,pic_lef,data_top,pic_top 分别代表数据，图像的 x，y坐标
    
    def bulidExcelPic(self,bkidf_list,wbk,QR_Sheet,IData_Sheet,xdiColumns,data_left,pic_lef,data_top,pic_top,bkStockdict,xdsColumns,bkStockChgdict):      
           
        #取出排名指数,写入到excel文件中
        sdata_left   = data_left + len(xdiColumns)+2
        
        sdata_top    = data_top
        
        #汇总板块名称
        bknamelist = []    
                    
        bkcodelist = []    
        
        bkdict ={}

        #股票指数 坐标轴字典
        sdataXY_list = []
        
        bkdataXY_dict ={}
        
        bkXY_dict={}
        
        bkcount  = 0 
                          
        if len(bkidf_list)>0:
            lastfile = bkidf_list[-1]
           
        for dflist in  bkidf_list:
           
           print dflist[0]
           
           sdataXY_list = []
                      
           bkcount += 1
            
           if len(dflist)==2:
                              
               bkidf_code  = dflist[0]
               
               bkidf_item  = dflist[1]
               
               bkidf_item = bkidf_item.dropna(how='any')
               
               bkhead = bkidf_item.head(1)
               
               bkname = bkhead['hq_name'].values
               
               bkname = bkname[0]
               
               bktile = bkname +'('+bkidf_code+')'
               
               bkidf_item['hq_date'] = bkidf_item['hq_date'].astype('str')
               
               bkidf_len   = len(bkidf_item)
               
               
               
               tmpbkist = [bkidf_code,data_top,data_left,bkidf_len]
               
               bkXY_dict[bkidf_code] = tmpbkist
               #写入头
               IData_Sheet.write_row(data_top, data_left,xdiColumns)
               
               #写入指数内容
                   
               for row in range(0,bkidf_len):   
                  #for col in range(left,len(fields)+left):  
                  
                  tmplist  = bkidf_item[row:row+1].values.tolist()
                                              
                  datalist = tmplist[0]
                                                  
                  IData_Sheet.write_row(data_top+row+1, data_left,datalist)
               
               #写入股票内容
               if bkStockdict.has_key(int(bkidf_code)):
                   
                   #对板块中的股票进行排序
                   if bkStockChgdict.has_key(int(bkidf_code)):
                       
                       stockchgdict = bkStockChgdict[int(bkidf_code)]
                               
                       stocklist= sorted(stockchgdict.items(), key=lambda d:d[1], reverse = True)                   
                   
                   
                   bkStockItem = bkStockdict[int(bkidf_code)] 
                                      
                   bkStockItem = bkStockItem.dropna(how='any')
                   
                   #按股票代码进行分类
                   bkStockGroup = bkStockItem.groupby('stock_id')
                   
                   #生成股票dict
                   tStockdict  = dict(list(bkStockGroup))
                                      
                   for slist in stocklist: 
                          
                       scode     = slist[0]
                       
                       if tStockdict.has_key(scode):
                       
                           stockitem = tStockdict[scode]
                           
                           stockitem['hq_date'] = stockitem['hq_date'].astype(str)
                           
                           stocklen  = len(stockitem)
                                                  
                           snamelist = stockitem['stock_name'].tolist()
                                                  
                           stockname =  snamelist[0]
                           
                           tmpdatalist = [scode,sdata_top,sdata_left,stocklen,stockname]
                                              
                           IData_Sheet.write_row(sdata_top, sdata_left,xdsColumns)
                           
                           for srow in range(0,stocklen):   
                              #for col in range(left,len(fields)+left):  
                              
                              tmplist  = stockitem[srow:srow+1].values.tolist()
                                                          
                              datalist = tmplist[0]
                                                              
                              IData_Sheet.write_row(sdata_top+srow+1, sdata_left,datalist)
                                   
                           
                           sdataXY_list.append(tmpdatalist)
                           
                           sdata_top  =    sdata_top +   stocklen +2
                   
                   sdata_left = sdata_left + len(xdsColumns) + 1 
               
               bkdataXY_dict[bkidf_code] = sdataXY_list
               
               sdata_top = 0
               
               idxstr  = u'指数数据'
               
               #指数参数设置
               shift =10
               
               data_top2 = 0
               
               data_left2 = 0
               
               shift2  = 3
               
               style   = 1
               
               bk_chart = self.bulidChart(wbk,data_top,data_left,bkidf_len,bktile,idxstr,shift,data_top2,data_left2,bkidf_len,shift2,style)
                    
               #画出双轴对比图
               
               QR_Sheet.insert_chart( pic_top, pic_lef,bk_chart)
                #bg+=19       
             
               pic_lef+=len(xdiColumns)+2
               
               #画成交量与成交金额相对相对量比
               
               shift =11
               
               data_top2 = data_top
               
               data_left2 = data_left
               
               shift2  = 12
               
               style   = 2
               
               bk_chart = self.bulidChart(wbk,data_top,data_left,bkidf_len,bktile,idxstr,shift,data_top2,data_left2,bkidf_len,shift2,style)
                    
               #画出双轴对比图
               
               QR_Sheet.insert_chart( pic_top, pic_lef,bk_chart)
                #bg+=19       
                      
               data_top+=bkidf_len +2
               
               pic_top+=15
               
               if lastfile!=dflist: 
                   pic_lef-=len(xdiColumns) +2
           
               #此处开始处理excel分类显示指数组，每N个指数一起显示
                         
               bkNum = 4
               
               bkpos  = bkname.find('-')
              
               if bkpos!=-1:
                  
                  tmpname = bkname[bkpos+1:]
               
                  bknamelist.append(tmpname)
                  
                  bkcodelist.append(dflist[0])
                  
                  bkdict[dflist[0]] = bkname
                  
               else:
                  
                  bknamelist.append(bkname)     
               
               #如果不为0，加入名称       
               
               if bkcount % bkNum ==0 :
                   
                   bknames   = ','.join(bknamelist)
                   
                   bknames   ='(' + bknames +')'
                   
                   bknamelist = []
                   
                   tmp_Sheet = wbk.add_worksheet(bknames)
                  
                   idxstr  = u'指数数据'
                                       
                   #指数参数设置
                   shift =10
                   
                   bkdata_top = 0
                   
                   bkdata_top2 = 0
                                      
                   bkdata_left = data_left
                   
                   bkdata_left2 = 0
                                      
                   shift2  = 3
                   
                   style   = 1
                   
                   picbk_top = 1
                   
                   picbk_lef = 1
                   
                   #先画指数，后画股票                                        
                   for bkcl in bkcodelist:
                       
                       if bkXY_dict.has_key(bkcl):
                           
                           picbk_top = 1
                           
                           #取出板块指数                             
                           bkitem  = bkXY_dict[bkcl]
                           
                           #获取X坐标                           
                           bkdata_top = bkitem[1]
                           
                           #获取Y坐标
                           bkdata_left = bkitem[2]
                           
                           #获取数据长度
                           bkdata_len  = bkitem[3]
                           
                           bkdata_top2 = 0
                           
                           bktiles = bkdict[bkcl]
                           
                           shift =10
                           
                           style   = 1
                                
                           shift2  = 3
                           
                           bkdata_left2 = 0 
                                                                              
                           bk_chart = self.bulidChart(wbk,bkdata_top,bkdata_left,bkdata_len,bktiles,idxstr,shift,bkdata_top2,bkdata_left2,bkidf_len,shift2,style)
                           
                           #画出双轴对比图
                                                      
                           tmp_Sheet.insert_chart( picbk_top, picbk_lef,bk_chart)
                            #bg+=19       
                           
                           picbk_top+=15
                           
                           #画成交量与成交金额相对相对量比
                           
                           shift =11
                           
                           bkdata_top2 = bkdata_top
                           
                           bkdata_left2 = data_left
                           
                           shift2  = 12
                           
                           style   = 2
                                                                              
                           bk_chart = self.bulidChart(wbk,bkdata_top,bkdata_left,bkdata_len,bktiles,idxstr,shift,bkdata_top2,bkdata_left2,bkidf_len,shift2,style)
                                
                           #画出双轴对比图
                           
                           tmp_Sheet.insert_chart( picbk_top, picbk_lef,bk_chart)
                           
                           
                           picbk_top+=15
                                                  
                           #picbk_lef += len(xdiColumns)+2
                           
                           #获取指数中的股票
                           if bkdataXY_dict.has_key(bkcl) :
                               
                               bkdataItem = bkdataXY_dict[bkcl]                                                            
                                                              
                               for bksl in bkdataItem:
                                   
                                   if len(bksl)==5:              
                                      #获取股票代码 
                                      bkstockcode  = bksl[0] 
                                      
                                      #获取股票X坐标
                                      bkstock_X    = bksl[1] 
                                
                                      #获取股票Y坐标
                                      bkstock_Y    = bksl[2]
                                      
                                      #获取股票长度
                                      bkstock_len  = bksl[3]
                                      
                                      bkstockname  = bksl[4]
                                      
                                      
                                
                                      bktiles = str(bkstockcode)+'('+bkstockname+')'
                                       
                                      shift =10
                                       
                                      style   = 3
                                            
                                      shift2  = 10
                                       
                                      bkdata_left2 = 0 
                                      
                                      bk_chart = self.bulidChart(wbk,bkdata_top,bkdata_left,bkdata_len,bktiles,idxstr,shift,bkstock_X,bkstock_Y,bkstock_len,shift2,style)
                                                                 
                                      tmp_Sheet.insert_chart( picbk_top, picbk_lef,bk_chart)
                                      
                                      picbk_top+=15
                                                                            
                                      #画成交量与成交金额相对相对量比
                                      shift =11
                                       
                                      style   = 2
                                            
                                      shift2  = 11
                                                                                          
                                      bk_chart = self.bulidChart(wbk,bkstock_X,bkstock_Y,bkstock_len,bktiles,idxstr,shift,bkstock_X,bkstock_Y,bkstock_len,shift2,style)
                                            
                                      #画出双轴对比图
                                       
                                      tmp_Sheet.insert_chart( picbk_top, picbk_lef,bk_chart)
                                        
                                      picbk_top+=15
                                      
                           picbk_lef += len(xdiColumns)+2                                      
                   
                   bkcodelist = []                                  
        
        
        m =1 
        
        return wbk,pic_lef,bkdataXY_dict,bkXY_dict
        
        
    def bulidAllExcelPic(self,bkidf_list,wbk,QR_Sheet,IData_Sheet,xdiColumns,data_left,pic_lef,data_top,pic_top):      
           
        #取出排名指数,写入到excel文件中
           
        bkcount = 0
           
        if len(bkidf_list)>0:
            lastfile = bkidf_list[-1]
           
        for dflist in  bkidf_list:
            
           if len(dflist)==2:
               
               bkcount  = bkcount +1            
               
               bkidf_code  = dflist[0]
               bkidf_item  = dflist[1]
               
               bkidf_item = bkidf_item.dropna(how='any')
               
               bkhead = bkidf_item.head(1)
               
               bkname = bkhead['hq_name'].values
               
               bkname = bkname[0]
               
               bkpos  = bkname.find("-")
               
               bkname = bkname[bkpos+1:]
               
               if bkcount==1 :
                   
                  bktiles =  bkname
                  
               else:
                  
                  if bktiles=='':
                      
                     bktiles = bkname
                     
                  else:
                     bktiles = bktiles + ',' +bkname  
               
               bkidf_item['hq_date'] = bkidf_item['hq_date'].astype('str')
               
               bkidf_len   = len(bkidf_item)
               
               #写入头
               IData_Sheet.write_row(data_top, data_left,xdiColumns)
               
               #写入内容
                   
               for row in range(0,bkidf_len):   
                  #for col in range(left,len(fields)+left):  
                  
                  tmplist  = bkidf_item[row:row+1].values.tolist()
                                              
                  datalist = tmplist[0]
                                                  
                  IData_Sheet.write_row(data_top+row+1, data_left,datalist)
               
               data_top+=bkidf_len +2
               
               #每隔固定几个值进行读取
               if bkcount%5 ==0:
                     
                   idxstr  = u'指数数据'
                   
                   #指数参数设置
                   shift =10
                   
                   bktiles = '(' + bktiles +')' 
                   
                   #第一次赋值为0 
                   if bkcount==5:
                       dstart_top = 0
                   
                   data_top2 = 0
                   
                   data_left2 = 0
                   
                   shift2  = 3
                   
                   style   = 1
                   
                   bk_chart = self.bulidALLChart(wbk,dstart_top,data_left,bkidf_len,bktiles,idxstr,shift,data_top2,data_left2,shift2,5,style)
                        
                   #画出双轴对比图
                   
                   QR_Sheet.insert_chart( pic_top, pic_lef,bk_chart)
                    #bg+=19       
                 
                   pic_lef+=len(xdiColumns)+2
                   
                   #画成交量与成交金额相对相对量比
                   
                   shift =11
                   
                   data_top2 = dstart_top
                   
                   data_left2 = data_left
                   
                   shift2  = 12
                   
                   style  =2
                                      
                   bk_chart = self.bulidALLChart(wbk,dstart_top,data_left,bkidf_len,bktiles,idxstr,shift,data_top2,data_left2,shift2,5,style)
                        
                   #画出双轴对比图
                   
                   QR_Sheet.insert_chart( pic_top, pic_lef,bk_chart)
                    #bg+=19       
                          
                   dstart_top = data_top
                   
                   pic_top+=15
                   
                   if lastfile!=dflist: 
                       pic_lef-=len(xdiColumns) +2
                   
                   bktiles =''
        
        pic_lef+=len(xdiColumns) +2   
        
        return wbk,pic_lef
        
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
               
               
               idataXY_dict[bkidf_code] = tmpdatalist
               
               #写入头
               IData_Sheet.write_row(top, left,bmiColumns)
               
               #tmplist = []
               
               #写入内容
                   
               for row in range(0,bkidf_len):   
                  #for col in range(left,len(fields)+left):  
                  
                  tmplist  = bkidf_item[row:row+1].values.tolist()
                                              
                  datalist = tmplist[0]
                                                  
                  IData_Sheet.write_row(top+row+1, left,datalist)
               
               top = top   + bkidf_len + 2
               
        return  IData_Sheet,idataXY_dict
        
    #构建指数excel构架    
    def bulidIndexExcelFrame(self,bmidf,xdhead_idf,bkStockdict,bkStockChgdict):
                
        data_left = 0    #数据起始列
        
        pic_left  = 0    #图像起始列 
        
        data_top  = 0    #数据起始行
        
        pic_top   = 2    #图像起始行
        
        bkdNum = 12  # 按12个行业来区分
        
        bklen  = len(bkStockdict)     
        
        excelCount =  bklen/bkdNum  
           
        pwd   =  os.getcwd()
        
        fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep+"..")
        
         
        if len(xdhead_idf)>0:
                
         #数据分组        
            xdhead_group = xdhead_idf.groupby('hq_code',sort=False)
                
            bkidf_list= list(xdhead_group)
        
        #按板块个数来生成excel文件
        
        for exlnum in range(excelCount):
                            
            execlfname  = fpwd + self.BCfname+'-'+ str(exlnum)
            
            execlfname  = execlfname.decode()
            
            wbk =xlsxwriter.Workbook(execlfname)  
                    
            #newwbk = copy(wbk)
            QR_Sheet   = wbk.add_worksheet(u'指数相对强弱')
        
            IData_Sheet = wbk.add_worksheet(u'指数数据')
            
            #画模块
            headStr='指数强弱排名（涨幅）'
            
            headvol= '指数量比（金额）'
            
            tailStr='指数强弱排名（跌幅）'
            
            tailvol= '指数量比（金额）'
            
            
            xdiColumns= list([u'板块代码', u'板块名称', u'日期', u'基准板块代码', u'基准板块名称', u'收盘价', u'前收盘价', u'成交量',u'成交额' ,u'日相对涨跌幅', u'累计相对涨跌幅', u'相对量比', u'相对金额比'])
                  
            xdiColumnlens = len(xdiColumns) 
            
            
            xdsColumns= list([u'板块代码', u'板块名称', u'股票代码', u'股票名称',  u'日期',u'收盘价', u'前收盘价', u'成交量',u'成交额' ,u'日相对涨跌幅', u'累计相对涨跌幅', u'相对金额比'])
                  
            xdsColumnlens = len(xdiColumns)   
            
            red = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'C0504D','font_size':16,'font_color':'white'})
            
            blue = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'8064A2','font_size':16,'font_color':'white'})
            
            
            #间隔格式
            JG = wbk.add_format({'bg_color':'CCC0DA'})
            
            #处理第一行excel格式
            QR_Sheet.merge_range(0,0,1,xdiColumnlens,headStr,red)         
            QR_Sheet.set_column(xdiColumnlens+1,xdiColumnlens+1,0.3,JG)
    
            QR_Sheet.merge_range(0,xdiColumnlens+2,1,2*xdiColumnlens+2,headvol,blue)               
            QR_Sheet.set_column(2*xdiColumnlens+3,2*xdiColumnlens+3,0.3,JG)
                    
            QR_Sheet.merge_range(0,2*xdiColumnlens+4,1,3*xdiColumnlens+4,tailStr,red)                    
            QR_Sheet.set_column(3*xdiColumnlens+5,3*xdiColumnlens+5,0.3,JG)
            
            QR_Sheet.merge_range(0,3*xdiColumnlens+6,1,4*xdiColumnlens+6,tailvol,blue)
        
            #基准数据写入数据sheet中
            if len(bmidf)>0:
                bmidf_group = bmidf.groupby('hq_code')
        
                bmi_list = list(bmidf_group)
                
                bmiColumns = list([u'基准指数代码', u'基准指数名称', u'日期', u'收盘价', u'前收盘价', u'成交量', u'涨跌幅', u'累涨跌幅',u'总成交量',u'总成交额'])
                
                #未处理多个基准标的比较问题，以及标的指数与板块数据不一致的问题
                      
                (IData_Sheet,idataXY_dict) = self.bulidIndexDataToExcel(bmi_list,IData_Sheet,bmiColumns,data_left,data_top)
                
                data_left = data_left +len(bmiColumns) +2
                
            
            
            if len(xdhead_idf)>0:
                
                #数据分组        
                            
                (wbk,pic_left,bkdataXY_dict,bkXY_dict)  = self.bulidExcelPic(bkidf_list,wbk,QR_Sheet,IData_Sheet,xdiColumns,data_left,pic_left,data_top,pic_top,bkStockdict,xdsColumns,bkStockChgdict)
                
                data_left = data_left+xdiColumnlens+2
      
            wbk.close()
        
     
    def bulidAllIndexExcelFrame(self,bmidf,xdtmp_idf):
                
        data_left = 0    #数据起始列
        
        pic_left  = 0    #图像起始列 
        
        data_top  = 0    #数据起始行
        
        pic_top   = 2    #图像起始行
        
        
        pwd   =  os.getcwd()
        
        fpwd  = os.path.abspath(os.path.dirname(pwd)+os.path.sep+"..")
        
        execlfname  = fpwd + self.BAfname
        
        execlfname  = execlfname.decode()
        
        wbk =xlsxwriter.Workbook(execlfname)  
        #newwbk = copy(wbk)
        QR_Sheet   = wbk.add_worksheet(u'指数相对强弱')
#                
#        HEQR_Sheet   = wbk.add_worksheet(u'指数混合强弱')
    
        IData_Sheet = wbk.add_worksheet(u'指数数据')
        
        #画模块
        headStr='指数强弱排名（涨幅）'
        
        headvol= '指数量比（金额）'
        
        
        xdiColumns= list([u'板块代码', u'板块名称', u'日期', u'基准板块代码', u'基准板块名称', u'收盘价', u'前收盘价', u'成交量',u'成交额' ,u'日相对涨跌幅', u'累计相对涨跌幅', u'相对量比', u'相对金额比'])
              
        xdiColumnlens = len(xdiColumns)   
        
        red = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'C0504D','font_size':16,'font_color':'white'})
        
        blue = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'8064A2','font_size':16,'font_color':'white'})
        
        
        #间隔格式
        JG = wbk.add_format({'bg_color':'CCC0DA'})
        
        #处理第一行excel格式
        QR_Sheet.merge_range(0,0,1,xdiColumnlens,headStr,red)         
        QR_Sheet.set_column(xdiColumnlens+1,xdiColumnlens+1,0.3,JG)

        QR_Sheet.merge_range(0,xdiColumnlens+2,1,2*xdiColumnlens+2,headvol,blue)               
        QR_Sheet.set_column(2*xdiColumnlens+3,2*xdiColumnlens+3,0.3,JG)
               
        
        #基准数据写入数据sheet中
        if len(bmidf)>0:
            bmidf_group = bmidf.groupby('hq_code')
    
            bmi_list = list(bmidf_group)
            
            bmiColumns = list([u'基准指数代码', u'基准指数名称', u'日期', u'收盘价', u'前收盘价', u'成交量', u'涨跌幅', u'累涨跌幅',u'总成交量',u'总成交额'])
            
            #未处理多个基准标的比较问题，以及标的指数与板块数据不一致的问题
                  
            (IData_Sheet,idataXY_dict) = self.bulidIndexDataToExcel(bmi_list,IData_Sheet,bmiColumns,data_left,data_top)
            
            data_left = data_left +len(bmiColumns) +2
        
        if len(xdtmp_idf)>0:
            
            #数据分组        
            xdtmp_group = xdtmp_idf.groupby('hq_code',sort=False)
            
            bkidf_list= list(xdtmp_group)
                        
            (wbk,pic_left)  = self.bulidAllExcelPic(bkidf_list,wbk,QR_Sheet,IData_Sheet,xdiColumns,data_left,pic_left,data_top,pic_top)
            
            data_left = data_left+xdiColumnlens+2
                                       
        wbk.close()

               
    #处理指数数据
    def getExcelIndexData(self,bmidf,bkdict):
        
        retdata  = pd.DataFrame()
        
        if len(bmidf)>0:
            
            tmpdata = bmidf[1:]
            
#           retdata = tmpdata[['hq_code','hq_date','hq_close','hq_preclose','hq_vol','hq_chg','hq_allchg']]
#             
            rethead   = bmidf['hq_code']
#            
            rethcode  = rethead.tolist()
            
            hq_name = ''
            
            if len(rethcode)>0:
                hq_code = rethcode[0]
                
                if(bkdict.has_key(str(hq_code))):                 
                   hq_name = bkdict[str(hq_code)]
                   
            tmpdata['hq_name'] =hq_name        
#            
            retdata = tmpdata[['hq_code','hq_name','hq_date','hq_close','hq_preclose','hq_vol','hq_chg','hq_allchg','hq_allvol','hq_allamo']]
                
        return retdata
        
     # 获取前几，后几排名数据 
    def getSortedIndexdf(self,xd_idf,sortlist,rankingnum):
        
        idf_len = len(sortlist)
        
        headlist = []
        
        taillist = []

        tmplist = []
        
        bkidf_group = xd_idf.groupby('hq_code')
            
        #生成排名dict        
        bkidf_dict = dict(list(bkidf_group))
        
        
        xdtmp_idf = pd.DataFrame()
        
        xdhead_idf = pd.DataFrame()
        
        xdtail_idf = pd.DataFrame()
    
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
        if idf_len>0 and rankingnum<idf_len:
            
             #获取前几后几 
            for sloc in range(rankingnum):
                
                headitem  = sortlist[sloc]
                 
                tailitem  = sortlist[0-sloc-1] 
                
                headlist.append(headitem[0])
                
                taillist.append(tailitem[0])
            
            #取出前几数据
            
            for hlist in headlist:
                
               dictkey = str(hlist) 
               
               if(bkidf_dict.has_key(dictkey)):
                   
                   bkitem = bkidf_dict[dictkey]
                   
                   xdhead_idf=  xdhead_idf.append(bkitem)
                               
            #取出后几数据              
            for hlist in taillist:
                
               dictkey = str(hlist) 
               
               if(bkidf_dict.has_key(dictkey)):
                   
                   bkitem = bkidf_dict[dictkey]
                   
                   xdtail_idf = xdtail_idf.append(bkitem)
                   
        else:
            
            xdhead_idf = xdtmp_idf
           
            
        return xdtmp_idf,xdhead_idf,xdtail_idf

        
    #excel中plot相对指数强弱图形
    def PlotIndexPicToExcel(self,benchmarkIndex,allMarketIndex,bkcodestr,bkxfdf,startDate,endDate,KlineType,rankingnum,bkdict):
        
        
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
        
        #获取所有指数排名数据 
        (xd_idf,sortlist,bkStockdict,bkStockChgdict) = self.getIndexXdQr(bmidf,hidf,bkdict,bkxfdf,stockdf)        
                
        #处理excel中的指数数据
        ebmidf  = self.getExcelIndexData(bmidf,bkdict)
        
        # 获取前几，后几排名数据     
        (xdtmp_idf,xdhead_idf,xdtail_idf) =self.getSortedIndexdf(xd_idf,sortlist,rankingnum)
        
        #画出指数排名图形
        self.bulidIndexExcelFrame(ebmidf,xdhead_idf,bkStockdict,bkStockChgdict)
                
        #画出指数排名图形（所有图形）
        self.bulidAllIndexExcelFrame(ebmidf,xdtmp_idf)
        
        kk =1 
        
        
if '__main__'==__name__:  
    
    pte = PlotToExcel()
    
    tdd = TmpDealData()
    
    dataFlag  = False
     
    #dataFlag  = True
    #调用临时入库程序，完成补齐日线数据   
    if dataFlag:
        tdd.getAllTypeDir()
    
    
    #基准对比指数
    benchmarkIndex = '399317'
        
    benchmarkName  =u'国证A指'
    
    #全市场指数（000002 A股指数，399107深圳A指数）
    allMarketIndex = '000002,399107'
    
    #获取数据起始时间
    
    start_date = datetime.strptime("2017-07-17", "%Y-%m-%d")
    
    end_date = datetime.strptime("2017-07-21", "%Y-%m-%d")
    
    #K线类型    
    KlineType ='5M'
        
    #获取所有板块数据
    mktindex  = pte.mktindex
    
    #获取板块与下属关联股票
    (bkLinedf,bkxfdf) = mktindex.MktIndexToStocksClassify('80201')
    
    
    bkcodes = bkLinedf['bz_indexcode'].astype('str')
    
    bkcodelist = bkcodes.tolist()
    
    bkcodestr = ','.join(bkcodes)
    
    tailIndexs=''
    
    rankingnum =56
    
    bkdict = bkLinedf.set_index('bz_indexcode')['bz_name'].to_dict()
        
    
    bkdict[benchmarkIndex]=benchmarkName
      
    
    bkxfdf[['board_id']]= bkxfdf[['board_id']].astype(str)
    
    print bkxfdf.dtypes
    
    #指数分类对比图
    pte.PlotIndexPicToExcel(benchmarkIndex,allMarketIndex,bkcodestr,bkxfdf,start_date,end_date,KlineType,rankingnum,bkdict)
    
    
    
    
    m = 1
    
    #所有指数强弱，量比图
    
    
    
    
    
    
    
    