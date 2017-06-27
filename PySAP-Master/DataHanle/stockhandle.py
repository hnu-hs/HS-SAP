# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 13:35:56 2017

@author: rf_hs
"""

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
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 13:57:44 2017

@author: rf_hs
"""


import os
import re

import pandas as pd
import numpy as np

from FileStringHandle import loop_file

from FileStringHandle import FileStringHandle

from sqlalchemy import create_engine  


import sys

reload(sys)
          
sys.setdefaultencoding('utf-8')

class StockHandle:
    
    def __init__(self):
        
       # 板块测试路径,分板块时间与板块数据两大块
       self.sdata_hqpath        = "\\Data\\History\\Stock\\股票tick数据\\2017\\"
       
       self.sdata_gdpath        = "\\Data\\History\\Stock\\股票股东数据\\"

       self.sdata_capitalpath   = "\\Data\\History\\Stock\\股票股本数据\\"
       
       self.sdata_tpath         = "\\Data\\History\\Stock\\股票时间数据\\"
       
       self.sdate_samplepath    ="\\Data\\History\\Stock\\股票时间数据\\股票样本日期\\"
       
       self.sdata_historypath   = "\\Data\\History\\Stock\\股票历史数据\\"
       
       # 板块起始时间
       self.stockdate       = "2015/1/1"
       
       
       # 处理数据库
       self.trunc_1min_sql  = "truncate table hstockquotationone"
          
       self.delete_1min_sql = "delete from hstockquotationone"
       
       
       self.trunc_5min_sql  = "truncate table hstockquotationfive"
          
       self.delete_5min_sql = "delete from hstockquotationfive"
       
       
       self.trunc_15min_sql  = "truncate table hstockquotationfifteen"
          
       self.delete_15min_sql = "delete from hstockquotationfifteen"
       
       
       self.trunc_30min_sql  = "truncate table hstockquotationthirty"
          
       self.delete_30min_sql = "delete from hstockquotationthirty"
       
       
       self.trunc_60min_sql  = "truncate table hstockquotationsixty"
          
       self.delete_60min_sql = "delete from hstockquotationsixty"
       
       
       self.trunc_day_sql  = "truncate table hstockquotationday"
          
       self.delete_day_sql = "delete from hstockquotationday"
       
       
       self.trunc_stockdivideinfo_sql  = "truncate table stockdivideinfo"
          
       self.delete_stockdivideinfo_sql = "delete from stockdivideinfo"
       
      
       self.trunc_stockgbinfo_sql  = "truncate table stockgbinfo"
          
       self.delete_stockgbinfo_sql = "delete from stockgbinfo"
       
       
   # 初始化数据库
       
    def initdatabase(self,mysqldb):

          pass;       
#         mysqldb.query(self.trunc_1min_sql)
#         mysqldb.update(self.delete_1min_sql)
#         
#         
#         mysqldb.query(self.trunc_5min_sql)
#         mysqldb.update(self.delete_5min_sql)
#         
#         
#         mysqldb.query(self.trunc_15min_sql)
#         mysqldb.update(self.delete_15min_sql)
#         
#         
#         
#         mysqldb.query(self.trunc_30min_sql)
#         mysqldb.update(self.delete_30min_sql)
#         
#         
#         
#         mysqldb.query(self.trunc_60min_sql)
#         mysqldb.update(self.delete_60min_sql)
#         
#         
#         mysqldb.query(self.trunc_day_sql)
#         mysqldb.update(self.delete_day_sql)
         
 
   #3层目录结构来读取所有股票的数据，目前先测试录入2015-01 到2017-04的数据
         
    def getAllStockPath(slef,stockpath):
       
       dirlist_all = []
       
       onelayerpath  = os.getcwd() + stockpath
       
#       onelayerpath = onelayerpath.replace("DataHanle","")
#           
       # 年目录
       dironelist = os.listdir(onelayerpath.decode('utf-8'))
       
       for dlist_one in dironelist :
      
           fstr = dlist_one
           
           twolayerpath  = onelayerpath +fstr
           
           #月目录
           dirtwolist = os.listdir(twolayerpath.decode('utf-8'))
          
           for dlist_two in dirtwolist:
               
               fstr = dlist_two
               
               Threelayerpath  = twolayerpath +'\\'+fstr
               
               dirlist_all.append(Threelayerpath)
               
               
#               dirthreelist    = os.listdir(Threelayerpath.decode('utf-8'))
#               
#               for dlist_three in dirthreelist:
#                   
#                   fstr = dlist_three
#                   
#                   tmpath  = Threelayerpath +'\\'+fstr
#                   
#                   dirlist_all.append(tmpath)
               
               
       return dirlist_all        
   
    
   # 板块数据从文本文件夹中读取，校正，入库
   # 获取板块时间数据             
                
    def getStockFile(self,stpath):
        
       root_dir         = os.getcwd()+stpath
       
       short_exclude    = []                            ###不包含检查的短目录、文件  
          
       long_exclude     = []                            ###不包含检查的长目录、文件  
    
       file_extend      = ['.txt']                      ###包含检查的文件类型  
    
       lf               = loop_file(root_dir.decode('utf-8'), short_exclude, long_exclude, file_extend)  
           
       # 求出板块时间目录下的文件
           
       return lf
       
    # 处理股票股本数据
       
    def getStockCapital(self,lf,engine): 
        
        columns = []
       
        raw_list =[]
        
        data_list =[]
        
        gb_list=[]
        
        gbdata_list =[]
        
        pregbdata_list =[]
        
        gbcount = 0
        
        datacount = 0
        
#         mysqldb.query(self.trunc_1min_sql)
#         mysqldb.update(self.delete_1min_sql)
#         
#         
#         mysqldb.query(self.trunc_5min_sql)
#         mysqldb.update(self.delete_5min_sql)
        #数据库引擎
#        
#        engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
#        
        #raw_data  =  pd.DataFrame(raw_dict,columns=['code','date','time','price','vol','amo'])
               
        # 求出板块时间目录下的文件
        for f in lf.start(lambda f: f): 
            
            if f=='F:\PySAP\Data\History\Stock\股票股本数据\1039.txt':
                k =1
                    
            try:
                
               rfile = open(f,'r')
                 
               # 数量不多一次性全部取出
               lines = rfile.readlines()
               
               
               splines = "|".join(lines)
               
               strpos_zl  = splines.find('总股本')
                 
               strpos_qx  = splines.find('权息资料')
               
               strpos_gm  = splines.find('更名信息')
               
               linelen =  len(splines)
               
               headflag = 0
               
               if strpos_zl!=-1:
                   
                   gbstr = splines[strpos_zl:]
                   
                   strposend = gbstr.find('|')
                   
                   gbstr     = gbstr[0:strposend]
                   
                   gblist    = gbstr.split()
                   
                   if len(gblist)==4:
                       
                       totalgb = gblist[1]
                       
                       totalgb = totalgb.replace("万股","")
                       
                       ltgb    = gblist[3]
                       
                       ltgb    = ltgb.replace("万股","")
                   
               if strpos_qx!=-1:
                  
                  psplines = splines[0:strpos_qx-1]        
                  
                  if strpos_gm!=-1:
                      splines =  splines[strpos_qx:strpos_gm]
                  else:
                      splines =  splines[strpos_qx:linelen]
                  
                  psplist = psplines.split('|')
                  
                  #求最后一个得到 股票代码和名称
                  tmpstr =psplist[len(psplist)-1] 
                  
                  tmplist  = tmpstr.split('(')
                  
                  if len(tmplist)==2:
                          
                      stockcode = tmplist[1].replace(" ","")
                      
                      stockname = tmplist[0].replace(" ","")
                      
                      splist  = splines.split('|')
                      
                      pregbdata_list =[]
                      
                      spcount = 0
                      
                      for spl in splist:
                                                 
                        raw_list =[]
                        
                        gb_list  =[]
                        
                        spl = spl.decode()
                                            
                        tmpspl =  spl.split()
                          
                        gbtype = -1
                        
                        if headflag and len(tmpspl)>2:
                            
                           
                           findstr = tmpspl[1] 
                           
                           tmpstr = spl.replace(" ","*")
                           
                           findpos = tmpstr.find(findstr)
                           
                           
                           if spcount==0 and findstr=="除权除息":                               
                               pregbdata_list =[ltgb,ltgb,totalgb,totalgb]
                           
                           spcount= spcount +1
                           
                           # 股票代码 名称
                           raw_list.append(stockcode)
                           
                           raw_list.append(stockname)
                           
                           '''
                             标识几种情况的list生成
                             1.除权除息（长度不定）；2.股本变化（长度为6）3.认沽去掉
                             case1:除权除息 送股 后面有1，3，0，1代表 送股分红，3代表 送股分红 配股
                             case2:除权除息 分红 后面有 2，0  2，代表分红，配股
                             case3: 除权除息 配股 
                             case4： 股本变化  长度为6
                             case5： 认沽去掉
                           
                             gbtype = 0 送股，分红，配股 都有
                             gbtype = 1 股本 数据
                             
                           '''
                           strcond = (findstr!="送认沽权证") and (findstr!="扩缩股")                          
                           
                           if findpos!=-1 and strcond :
                               
                               insectstr = tmpstr[findpos:len(tmpstr)]
                               
                               tmpos = insectstr.find(".")
                               
                               if tmpos!=-1 and len(tmpspl)>=2:
                                   
                                   countstr = insectstr[0:tmpos]
                                   
                                   count = countstr.count("*")
                                   
                                   #加入时间,信息
                                   raw_list.extend(tmpspl[0:2])
                                   
                                   #数*号的个数
                                   if len(tmpspl)==6:
                                       
                                      tmplist=['0','0','0','0']
                                      
                                      if(findstr!="除权除息"):
                                          
                                          gbtype = 0
                                          
                                          raw_list.extend(tmplist)
                                          
                                          raw_list.extend(tmpspl[2:6])
                                          
                                          pregbdata_list = tmpspl[2:6]
                                          
                                      else:
                                          
                                          gbtype = 1
                                          
                                          raw_list.extend(tmpspl[2:6])
                                          
                                          raw_list.extend(tmplist)
                                          
                                          gb_list.append(stockcode)
                                           
                                          gb_list.append(stockname)
                                          
                                          gb_list.extend(tmpspl[0:2])
                                         
                                          gb_list.extend(tmpspl[2:6])
                                      
                                          gb_list.extend(pregbdata_list)
                                          
                                          
                                   if len(tmpspl)<6 :  
                                   # 计算*的个数
                                     if count>=5 and count<10:
                                         
                                         if len(tmpspl)==3:
    
                                             gbtype = 2
                                             
                                             # gbtype =2 表示只送股
                                             
                                             tmplist=['0','0','0','0','0','0','0']
                                             
                                             gbtmplist=['0','0','0']
                                             
                                             raw_list.append(tmpspl[2])
                                             
                                             raw_list.extend(tmplist)
                                                
                                             gb_list.append(stockcode)
                                               
                                             gb_list.append(stockname)
                                             
                                             gb_list.extend(tmpspl[0:2])
                                         
                                             gb_list.append(tmpspl[2])
                                         
                                             gb_list.extend(gbtmplist)
                                              
                                             gb_list.extend(pregbdata_list)
                                                     
                                         
                                         if len(tmpspl)==4:
                                             
                                             gbtype = 3
                                             
                                             # gbtype =3 表示即送股又分红
                                                                                         
                                             tmplist=['0','0','0','0','0','0']
                                             
                                             gbtmplist=['0','0']
                                             
                                             raw_list.extend(tmpspl[2:4])
                                             
                                             raw_list.extend(tmplist)
                                                 
                                             gb_list.append(stockcode)
                                               
                                             gb_list.append(stockname)
                                                                                              
                                             gb_list.extend(tmpspl[0:2])
                                             
                                             gb_list.extend(tmpspl[2:4])
                                                 
                                             gb_list.extend(gbtmplist)
                                              
                                             gb_list.extend(pregbdata_list)
                                                     
                                             
                                         if len(tmpspl)==5:
                                             
                                             gbtype  =4
                                               
                                             tmplist=['0','0','0','0']
                                             
                                             raw_list.append(tmpspl[2])
                                             
                                             raw_list.append('0')
                                             
                                             raw_list.extend(tmpspl[3:5])
                                             
                                             raw_list.extend(tmplist)
                                     
                                             
                                             gb_list.append(stockcode)
                                               
                                             gb_list.append(stockname)
                                                                                              
                                             gb_list.extend(tmpspl[0:2])
                                         
                                             gb_list.append(tmpspl[2])
                                         
                                             gb_list.append('0')
                                         
                                             gb_list.extend(tmpspl[3:5])
                                                                                           
                                             gb_list.extend(pregbdata_list)
                                        
                                     
                                     if count>=10 and count<15:
                                         
                                         if len(tmpspl)==3:
                                             
                                             gbtype =5
                                             
                                             tmplist=['0','0','0','0','0','0']
                                             
                                             gbtmplist=['0','0']
                                             
                                             raw_list.append('0')
                                             
                                             raw_list.append(tmpspl[2])

                                             raw_list.extend(tmplist)
                                                                                         
                                             gb_list.append(stockcode)
                                               
                                             gb_list.append(stockname)
                                                                                              
                                             gb_list.extend(tmpspl[0:2])
                                                                                              
                                             gb_list.append('0')
                                             
                                             gb_list.append(tmpspl[2])                                                 
                                             
                                             gb_list.extend(gbtmplist)
                                              
                                             gb_list.extend(pregbdata_list)
                                     
                                 
                                         if len(tmpspl)==5:
                                             
                                             gbtype = 6
                                                                                          
                                             tmplist=['0','0','0','0']
                                             
                                             raw_list.append('0')
                                          
                                             raw_list.extend(tmpspl[2:5])
                                             
                                             raw_list.extend(tmplist)
                                     
                                             
                                             gb_list.append(stockcode)
                                               
                                             gb_list.append(stockname)
                                                                                              
                                             gb_list.extend(tmpspl[0:2])
                                         
                                             gb_list.append('0')
                                          
                                             gb_list.extend(tmpspl[2:5])
                                                                     
                                             gb_list.extend(pregbdata_list)
                                                                  
                                     if count>=15 :
                                         if len(tmpspl)==4:
                                             
                                             gbtype =7

                                             tmplist=['0','0','0','0']
                                             
                                             raw_list.append('0')
                                             
                                             raw_list.append('0')
                                          
                                             raw_list.extend(tmpspl[2:4])
                                             
                                             raw_list.extend(tmplist)
                                             
                                             
                                             gb_list.append(stockcode)
                                               
                                             gb_list.append(stockname)
                                             
                                             gb_list.extend(tmpspl[0:2])
                                         
                                             gb_list.append('0')
                                             
                                             gb_list.append('0')
                                                                                                                                
                                             gb_list.extend(tmpspl[2:4])
                                                                                          
                                             gb_list.extend(pregbdata_list)
                                             
                                             
                               if len(raw_list)==12:
                                   
                                  data_list.append(raw_list)
                                  
                                  if gb_list:
                                      gbdata_list.append(gb_list)
                               
                           
                               
                        
                        #建立头
                        if len(tmpspl)==10 and headflag==0:
                           #建立dataframe                            
                            columns.append('股票代码'.decode())
                            
                            columns.append('股票名称'.decode())
#                            
                            columns.extend(tmpspl)
                                                        
                            headflag =1
                            
                      
                      if len(data_list)>0:
                         
#                          if stockname=="青海春天":
#                             x =1
                          
                          data_series = np.array(data_list)
                          
                          raw_data  =  pd.DataFrame(data_series,columns=columns)
                          
                          if datacount==0:
                              raw_data.to_sql('stockgbinfo',engine,if_exists='replace')
                          else:
                              raw_data.to_sql('stockgbinfo',engine,if_exists='append')
                          
                          
                          datacount = datacount + 1
                          
                          
                          data_list =[]
#                      
                      
                      if len(gbdata_list)>0:
                                                 
                          gbdata_series = np.array(gbdata_list)
                          
                          gbraw_data  =  pd.DataFrame(gbdata_series,columns=columns)
                          
                          if gbcount==0:
                             gbraw_data.to_sql('stockdivideinfo',engine,if_exists='replace')
                          else:
                              
                             if len(gbraw_data)>0:
                                 gbraw_data.to_sql('stockdivideinfo',engine,if_exists='append')
                             
                             if len(gbraw_data)==0:
                                 m =1                                  
                             
                          gbcount =  gbcount + 1
                          
                          gbdata_list =[]
                          
                      
                      print f
                      
                      columns = []
                       
            finally:
                    
                if rfile:
                   rfile.close()
        m =1                  


    def getBoardIndexTradingTime(self,lf): 
        
        bktradingtimedict = {}
        
        # 求出板块时间目录下的文件
        for f in lf.start(lambda f: f): 
            
          lflist = f.split("\\")

          bklen  = len(lflist)
           
          if bklen>2:
             
             tbkinfo  = lflist[bklen-1]
             
             tbkinfo  = tbkinfo.replace(".txt","")
             
             tbkpos   = tbkinfo.find("_")
             
             if tbkpos!=-1:
                 
                tbktime  = tbkinfo[0:tbkpos]                 
                 
                tbkcode  = tbkinfo[tbkpos+1:len(tbkinfo)]
                
                bkstartdate = self.bkdate
                
                bkstartdate = bkstartdate.replace("/","")
                
                if long(tbktime)>=long(bkstartdate):                
                    
                    if bktradingtimedict.has_key(tbkcode):
                      
                       tmpset =  bktradingtimedict[tbkcode]
                       tmpset.add(tbktime)
                       bktradingtimedict[tbkcode] = tmpset 
                    else:
                        dateset = set()   
                        dateset.add(tbktime)
                        bktradingtimedict[tbkcode] = dateset
                               
        return bktradingtimedict

    def getBoardMissdata(self,dkey,misset): 
        
        filestr = self.missdata_path+"missingfile"+".txt"
        
        filestr = os.getcwd()+ filestr.decode()
            
        
        try:
            
          wfile  = open(filestr, 'a')
          
          wfile.writelines(dkey+'\n')
          
          mlist  = list(misset)
          
          mlist.sort()
          
          for ml in mlist:
             wfile.write(ml + "\n")
                      
          
          wfile.close()
                 
        finally:
          pass;
          

    def StockTickHandle(self,dirlist,engine):
        
        # 统计配置目录下文件夹个数
        
        # 读出样本数据，用于筛选数据
          
        #数据库引擎
#        
#        engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        sample_path = os.getcwd()+self.sdate_samplepath +"sample.csv"
        
        sampledf =  pd.read_csv(sample_path.decode())
        
        samplelist = ((sampledf.values).ravel()).tolist()
        
        raw_dict        ={}
        
        oneminutes_dict ={}
        
        ohlc_dict_day = {           
            'hq_date':'first',
            'hq_code':'first',
            'hq_open':'first',                                                                                                    
            'hq_high':'max',                                                                                                       
            'hq_low':'min',                                                                                                        
            'hq_close': 'last',                                                                                                    
            'hq_vol': 'sum', 
            'hq_amount': 'sum' 
            }

        ohlc_dict = {           
            'hq_date':'first',
            'hq_time':'first',
            'hq_code':'first',
            'hq_open':'first',                                                                                                    
            'hq_high':'max',                                                                                                       
            'hq_low':'min',                                                                                                        
            'hq_close': 'last',                                                                                                    
            'hq_vol': 'sum',    
            'hq_amount': 'sum' 
            }

        fbdatelist   =[]
        
        fbcodelist   =[]
        
        fbtimelist   =[]
        
        fbpricelist  =[]
        
        fbvolist     =[]
        
        FlagStr=['txt','csv']
        
        lastminrow = pd.DataFrame()
        
        dirlist.sort()
        
        for dl in dirlist:
            
           tmpstr  = dl.strip()
           
           fhl     = FileStringHandle()
           
           tstr    = tmpstr.decode()
           
           filelist = fhl.GetFileList(tstr,FlagStr)
           
           #filelist.reverse()
           
           filelist.sort()
           
           for f in filelist: 
               
               try:           
                   
                   rfile = open(f,'r')                     
                     
                   fpos  = f.find(".txt")
                   
                   fposc = f.find(".csv")
                     
                     
                   if fpos!=-1 or fposc!=-1:
                                                 
                      lflist = f.split("\\")
                         
                      if len(lflist)>=2:
                             
                         fstr = lflist[len(lflist)-1]
                             
                         fdate = lflist[len(lflist)-2]
                         if fposc ==-1:
                         
                             fstr = fstr.replace(".txt","")
                         else:
                             fstr = fstr.replace(".csv","")
                            
                         tbkclass_1min = "1min\\"+fstr+".csv"
                             
                         bkname_1min  = os.getcwd() + self.sdata_historypath + tbkclass_1min
                            
                             
                         lines      = rfile.readlines()
                         
                         splines = "|".join(lines)
                         
                         splines=splines.replace('B','').replace('S','').replace(',',' ')
                         
                         splist  = splines.split("|")                         
                             
                         tmpdate = fdate
                         
                         #处理SH，SZ
                         tmpcode = fstr.replace("SH","").replace("SZ","")
                         
                         for spl  in splist:
                                         
                            tmplist  = spl.split()                 
                             
                            if len(tmplist)==3 or len(tmplist)==4:
                                 
                                fbdatelist.append(tmpdate)
                                 
                                fbcodelist.append(tmpcode)
                                
                                tmpstr = tmplist[0]
#                                
                                if len(tmpstr)==5:
                                    if ':' in tmpstr:
                                        tmptime=tmpstr
                                    else:
                                        tmptime = '0'+tmpstr[0]+":"+ tmpstr[1:3]
                                
                                if len(tmpstr)==6 :
                                    tmptime = tmpstr[0:2]+":"+ tmpstr[2:4]
                                 
                                fbtimelist.append(tmptime) 
                                
                                if  fposc!=-1:                               
                                    fbpricelist.append(float(tmplist[1]))
                                    
                                else:                           
                                    fbpricelist.append(float(tmplist[1])/100)
                             
                                fbvolist.append(abs(int(tmplist[2])))
                           
                        #处理完一天数据，生成一天的1分钟基础数据，后面数据在1分钟基础数据上进行处理
                        #这里生成一个1分钟的dataframe确保数据一致 
                                 
                         fbcode_series = pd.Series(np.array(fbcodelist))                                     
                         raw_dict["code"] = fbcode_series
                                                             
                         fbdate_series = pd.Series(np.array(fbdatelist))
                         raw_dict["date"] = fbdate_series
                         
                         fbtime_series = pd.Series(np.array(fbtimelist))
                         raw_dict["time"] = fbtime_series
                         
                         fbprice_series = pd.Series(np.array(fbpricelist))
                         raw_dict["price"] = fbprice_series
                         
                         fbvol_series = pd.Series(np.array(fbvolist))
                         raw_dict["vol"] = fbvol_series
                                                      
                                
                         raw_data  =  pd.DataFrame(raw_dict,columns=['code','date','time','price','vol','amo'])
                                 
                         raw_data['price'] = raw_data['price'].astype('float64')
                         
                         raw_data['vol']   = raw_data['vol'].astype('float64')
                        
                         raw_data['amo']   = (raw_data['price']*raw_data['vol'])                             
                        
                         grouptime = raw_data['time'].groupby(raw_data['time'])
                         
                         groupcode = raw_data['code'].groupby(raw_data['time'])
                         
                         groupdate = raw_data['date'].groupby(raw_data['time'])
                                                         
                         groupvol  = raw_data['vol'].groupby(raw_data['time'])
                                                             
                         grouprice = raw_data['price'].groupby(raw_data['time'])
                        
                         groupamo  = raw_data['amo'].groupby(raw_data['time'])
                        
                         oneminutes_code  = groupcode.head(1)
                         
                         oneminutes_date  = groupdate.head(1)
                         
                         oneminutes_time  = grouptime.head(1)
                         
                         oneminutes_open  =  grouprice.head(1)
                         
                         oneminutes_close =  grouprice.tail(1)
                         
                         oneminutes_ceiling_price = grouprice.max()
                                                              
                         oneminutes_floor_price = grouprice.min()
                         
                         oneminutes_vol = groupvol.sum()
                         
                         oneminutes_vol = oneminutes_vol/100
                        
                         oneminutes_amo = groupamo.sum()/10000
             
                         #处理一分钟数据
                     
                         # 处理代码
                      
                         oneminutes_dict['hq_code'] = oneminutes_code.values                                         
                       
                         oneminutes_dict['hq_date'] = oneminutes_date.values 
                       
                         oneminutes_dict['hq_time'] = oneminutes_time.values 
                                   
                             # 处理开盘价
                         
                         oneminutes_dict['hq_open'] = oneminutes_open.values 
                        
                         # 处理最高价
                         
                         oneminutes_dict['hq_high'] = oneminutes_ceiling_price.values                                          
                         
                         # 处理最低价
                       
                         oneminutes_dict['hq_low'] = oneminutes_floor_price.values                                          
                         
                         # 处理收盘价
                       
                         oneminutes_dict['hq_close'] = oneminutes_close.values                                          
                        
                         # 处理成交量
                    
                         oneminutes_dict['hq_vol'] = oneminutes_vol.values   

                         # 处理成交额
                      
                         oneminutes_dict['hq_amo'] = oneminutes_amo.values                                              
                                     
                          
                         fbdatelist   =[]
                        
                         fbcodelist   =[]
                        
                         fbtimelist   =[]
                        
                         fbpricelist  =[]
                        
                         fbvolist     =[]
                                                          
                         #每来一个有效文件后，进行整体时间校验，缺失数据补齐，后存入指定文件，最后批量入库
                                                          
                         reindexlist = []
          
                         for saplist in samplelist:
                           
                            tmpstr = str(tmpdate) +" " +str(saplist) +":00"                                                  
                           
                            reindexlist.append(tmpstr)             
                               
                         # minutes_index = pd.to_datetime(oneminutes_date.values +'' + oneminutes_time.values)
                       
                         if oneminutes_dict.has_key('hq_date'):
                             
                             #测试代码
                             tdate=oneminutes_dict['hq_date']
                             
                             ttime=oneminutes_dict['hq_time']
                                   
                             minutes_index = pd.to_datetime(oneminutes_dict['hq_date']+' ' +oneminutes_dict['hq_time'] )
                           
                             oneminutes = pd.DataFrame(oneminutes_dict,index=minutes_index)
                             
                           
#                            test = oneminutes.resample('1T', label='left',closed='left',fill_method='ffill')
                             bkreindex = pd.to_datetime(reindexlist)
                           
                             reoneminutes = oneminutes.reindex(index=bkreindex)
                             
                             np.seterr(invalid='ignore')
                             
                             zvalues = reoneminutes.loc[~(reoneminutes.hq_vol > 0)].loc[:, ['hq_vol','hq_amo']]
                             
                             reoneminutes.update(zvalues.fillna(0))
                             
                             reheadrow = reoneminutes.head(1)
                             
                             firstisnull = reheadrow.isnull().any().any()
                             
                             if firstisnull :
                                 
                                 tmpindex  = reoneminutes.iloc[[0]].index
                                 
                                 nreoneminutes = lastminrow.set_index(tmpindex)
                                 
                                 nreoneminutes.iloc[0,:]=np.NaN
                                 
                                 nreoneminutes['hq_code'] = tmpcode
                                 
                                 nreoneminutes['hq_date'] = tmpdate
                                 
                                 nreoneminutes['hq_time'] = samplelist[0]
                                 
                                 reoneminutes =  reoneminutes[1:]
                                 
                                 reoneminutes = pd.concat([nreoneminutes,reoneminutes])
                                 
                             
                             reoneminutes.fillna(method='ffill', inplace=True)
                             
                             lastminrow = oneminutes.tail(1)
                             
                        
                             if os.path.exists(bkname_1min):                                 
                                reoneminutes.to_csv(bkname_1min,mode='a',header=False)
#                                reoneminutes.to_sql('hstockquotationone',engine,if_exists='append')                                 
                             else:                                
                                reoneminutes.to_csv(bkname_1min,mode='w')
#                                reoneminutes.to_sql('hstockquotationone',engine,if_exists='append')
                                
                             reindexlist =[]
                             
                             print f
                             
                             reoneminutes    =  pd.DataFrame()
                             oneminutes      =  reoneminutes
                             
                             oneminutes_dict ={}
                                       
                          
               finally:
                        
                  if rfile:
                     rfile.close()
           m = 1                   

#    def StockTickHandle(self,dirlist):
#        
#        # 统计配置目录下文件夹个数
#        
#        # 读出样本数据，用于筛选数据
#          
#        #数据库引擎
#        
#        engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
#        
#        sample_path = os.getcwd()+self.sdate_samplepath +"sample.csv"
#        
#        sampledf =  pd.read_csv(sample_path.decode())
#        
#        samplelist = ((sampledf.values).ravel()).tolist()
#        
#        raw_dict        ={}
#        
#        oneminutes_dict ={}
#                
#        def getOneMinutesData(rawgroup_data):
#            
#            tdf = pd.DataFrame()
#            
#            tdf['hq_code'] = rawgroup_data['code'].head(1)
#            
#            tdf['hq_time'] = rawgroup_data['time'].head(1)
#            
#            tdf['hq_date'] = rawgroup_data['date'].head(1)
#            
#            tdf['hq_open'] = rawgroup_data['price'].head(1)
#            
#            tdf['hq_high'] = rawgroup_data['price'].max()
#            
#            tdf['hq_low'] = rawgroup_data['price'].min()
#                        
#            tdf['hq_close'] = rawgroup_data['price'].head(-1)
#            
#            tdf['hq_vol'] = rawgroup_data['vol'].sum()
#            
#            tdf['hq_amo'] = rawgroup_data['amo'].sum()
#         
#            
#            return tdf
#        
#        ohlc_dict_day = {           
#            'hq_date':'first',
#            'hq_code':'first',
#            'hq_open':'first',                                                                                                    
#            'hq_high':'max',                                                                                                       
#            'hq_low':'min',                                                                                                        
#            'hq_close': 'last',                                                                                                    
#            'hq_vol': 'sum', 
#            'hq_amount': 'sum' 
#            }
#
#        ohlc_dict = {           
#            'hq_date':'first',
#            'hq_time':'first',
#            'hq_code':'first',
#            'hq_open':'first',                                                                                                    
#            'hq_high':'max',                                                                                                       
#            'hq_low':'min',                                                                                                        
#            'hq_close': 'last',                                                                                                    
#            'hq_vol': 'sum',    
#            'hq_amount': 'sum' 
#            }
#
#        fbtimelist   =[]
#        
#        fbpricelist  =[]
#        
#        fbvolist     =[]
#        
#        FlagStr=['txt','csv']
#        
#        lastminrow = pd.DataFrame()
#        
#        dirlist.sort()
#        
#        for dl in dirlist:
#            
#           tmpstr  = dl.strip()
#           
#           fhl     = FileStringHandle()
#           
#           tstr    = tmpstr.decode()
#           
#           filelist = fhl.GetFileList(tstr,FlagStr)
#           
#           #filelist.reverse()
#           
#           filelist.sort()
#           
#           for f in filelist: 
#               
#               try:           
#                   
#                   rfile = open(f,'r')                     
#                     
#                   fpos  = f.find(".txt")
#                   
#                   fposc = f.find(".csv")
#                     
#                     
#                   if fpos!=-1 or fposc!=-1:
#                                                 
#                      lflist = f.split("\\")
#                         
#                      if len(lflist)>=2:
#                             
#                         fstr = lflist[len(lflist)-1]
#                             
#                         fdate = lflist[len(lflist)-2]
#                         if fposc ==-1:
#                         
#                             fstr = fstr.replace(".txt","")
#                         else:
#                             fstr = fstr.replace(".csv","")
#                            
#                         tbkclass_1min = "1min\\"+fstr+".csv"
#                             
#                         bkname_1min  = os.getcwd() + self.sdata_historypath + tbkclass_1min
#                            
#                             
#                         lines      = rfile.readlines()
#                         
#                         splines = "|".join(lines)
#                         
#                         splines=splines.replace('B','').replace('S','').replace(',',' ')
#                         
#                         splist  = splines.split("|")                         
#                             
#                         tmpdate = fdate
#                         
#                         #处理SH，SZ
#                         tmpcode = fstr.replace("SH","").replace("SZ","")
#                         
#                         for spl  in splist:
#                                         
#                            tmplist  = spl.split()                 
#                             
#                            if len(tmplist)==3 or len(tmplist)==4:
##                                 
#                                
#                                tmpstr = tmplist[0]
##                                
#                                if len(tmpstr)==5:
#                                    if ':' in tmpstr:
#                                        tmptime=tmpstr
#                                    else:
#                                        tmptime = '0'+tmpstr[0]+":"+ tmpstr[1:3]
#                                
#                                if len(tmpstr)==6 :
#                                    tmptime = tmpstr[0:2]+":"+ tmpstr[2:4]
#                                 
#                                fbtimelist.append(tmptime) 
#                                
#                                if fposc!=-1:                            
#                                    fbpricelist.append(float(tmplist[1]))
#                                    
#                                else:                           
#                                    fbpricelist.append(float(tmplist[1])/100)
#                             
#                                fbvolist.append(abs(int(tmplist[2]))/100)
#                           
#                        #处理完一天数据，生成一天的1分钟基础数据，后面数据在1分钟基础数据上进行处理
#                        #这里生成一个1分钟的dataframe确保数据一致 
##                                 
##                         fbcode_series = pd.Series(np.array(fbcodelist))                                     
#                         raw_dict["code"] = tmpcode
##                                                             
##                         fbdate_series = pd.Series(np.array(fbdatelist))
#                         raw_dict["date"] = tmpdate
#                         
#                         fbtime_series = pd.Series(np.array(fbtimelist))
#                         raw_dict["time"] = fbtime_series
#                         
#                         fbprice_series = pd.Series(np.array(fbpricelist))
#                         raw_dict["price"] = fbprice_series
#                         
#                         fbvol_series = pd.Series(np.array(fbvolist))
#                         raw_dict["vol"] = fbvol_series
#                                
#                         raw_data  =  pd.DataFrame(raw_dict,columns=['code','date','time','price','vol','amo'])
#                         
##                         
##                         print raw_data.describe()                           
#                         
#                         raw_data['price'] = raw_data['price'].astype('float64')
#                         
#                         raw_data['vol']   = raw_data['vol'].astype('float64')
#                        
#                         raw_data['amo']   = (raw_data['price']*raw_data['vol'])/100   
##                         
##                         print raw_data.dtypes
#
#                         rawgroup_data = raw_data.groupby(raw_data['time'])
#                         
#                         oneminutesdf= rawgroup_data.apply(                                                                                                                            )
#             
#                         #处理一分钟数据
#                     
#                         # 处理代码
#                        
##                         fbdatelist   =[]
##                        
##                         fbcodelist   =[]
#                        
#                         fbtimelist   =[]
#                        
#                         fbpricelist  =[]
#                        
#                         fbvolist     =[]
#                                                          
#                         #每来一个有效文件后，进行整体时间校验，缺失数据补齐，后存入指定文件，最后批量入库
#                                                          
#                         reindexlist = []
#          
#                         for saplist in samplelist:
#                           
#                            tmpstr = str(tmpdate) +" " +str(saplist) +":00"                                                  
#                           
#                            reindexlist.append(tmpstr)             
#                               
#                         # minutes_index = pd.to_datetime(oneminutes_date.values +'' + oneminutes_time.values)
#                         minutes_index = pd.to_datetime(oneminutesdf['hq_date']+' ' +oneminutesdf['hq_time'] ) 
#                         
#                         bkreindex = pd.to_datetime(reindexlist)
#                         
#                         oneminutesdf = oneminutesdf.set_index(minutes_index)
#                         
##                         oneminutesdf.reset_index(level=0,inplace=True)
#                         reoneminutes = oneminutesdf.reindex(index=bkreindex)
#                        
#                         np.seterr(invalid='ignore')
#                         
#                         zvalues = reoneminutes.loc[~(reoneminutes.hq_vol > 0)].loc[:, ['hq_vol','hq_amo']]
#                         
#                         reoneminutes.update(zvalues.fillna(0))
#                         
#                         reheadrow = reoneminutes.head(1)
#                         
#                         firstisnull = reheadrow.isnull().any().any()
#                             
#                         if firstisnull :
#                             
#                             tmpindex  = reoneminutes.iloc[[0]].index
#                             
#                             nreoneminutes = lastminrow.set_index(tmpindex)
#                             
#                             nreoneminutes.iloc[0,:]=np.NaN
#                             
#                             nreoneminutes['hq_code'] = tmpcode
#                             
#                             nreoneminutes['hq_date'] = tmpdate
#                             
#                             nreoneminutes['hq_time'] = samplelist[0]
#                             
#                             reoneminutes =  reoneminutes[1:]
#                             
#                             reoneminutes = pd.concat([nreoneminutes,reoneminutes])
#                             
#                             
#                         reoneminutes.fillna(method='ffill', inplace=True)
#                             
#                         lastminrow = oneminutesdf.tail(1)
#                             
#                    
#                         if os.path.exists(bkname_1min):                                 
#                            reoneminutes.to_csv(bkname_1min,mode='a',header=False)
##                                reoneminutes.to_sql('hstockquotationone',engine,if_exists='append')                                 
#                         else:                                
#                            reoneminutes.to_csv(bkname_1min,mode='w')
##                                reoneminutes.to_sql('hstockquotationone',engine,if_exists='append')
#                            
#                         reindexlist =[]
#                         
#                         print f
#                         
#                         reoneminutes    =  pd.DataFrame()
#                         oneminutesdf    =  reoneminutes
#      
#               finally:
#                        
#                  if rfile:
#                     rfile.close()
#           m = 1   
'''
if '__main__'==__name__:  
    
    stockhandle = StockHandle()
    
    dirlist = []
    
    #获取所有股票目录
    dirlist = stockhandle.getallstockpath(stockhandle.sdata_hqpath)
    
    m =1   
'''        
          
          
          
                      
                  
                  
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
