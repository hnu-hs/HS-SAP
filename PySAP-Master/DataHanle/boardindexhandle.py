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

class BoardIndex:
    
    def __init__(self):
        
       # 板块测试路径,分板块时间与板块数据两大块
       self.bktime_path   = "\\Data\\History\\BoardIndex\\板块测试\\板块时间"
       
       self.bk_path       = "\\Data\\History\\BoardIndex\\板块测试\\板块数据\\通达信"

       self.bkdata_path   = "\\Data\\History\\BoardIndex\\板块测试\\板块数据"
       
       self.missdata_path = "\\Data\\History\\BoardIndex\\板块缺失数据\\"
       
       self.sample_path   =  "\\Data\\History\\BoardIndex\\板块测试\\tick样本数据\\"
       
       self.bkfzdata_path = "\\Data\\History\\BoardIndex\\板块测试\\板块分钟数据\\通达信\\"
       
       # 板块起始时间
       self.bkdate        = "2011/11/22"
       
       # 处理数据库
       self.trunc_1min_sql  = "truncate table hindexquotationone"
          
       self.delete_1min_sql = "delete from hindexquotationone"
       
       
       self.trunc_5min_sql  = "truncate table hindexquotationfive"
          
       self.delete_5min_sql = "delete from hindexquotationfive"
       
       
       self.trunc_15min_sql  = "truncate table hindexquotationfifteen"
          
       self.delete_15min_sql = "delete from hindexquotationfifteen"
       
       
       self.trunc_30min_sql  = "truncate table hindexquotationthirty"
          
       self.delete_30min_sql = "delete from hindexquotationthirty"
       
       
       self.trunc_60min_sql  = "truncate table hindexquotationsixty"
          
       self.delete_60min_sql = "delete from hindexquotationsixty"
       
       
       self.trunc_day_sql  = "truncate table hindexquotationday"
          
       self.delete_day_sql = "delete from hindexquotationday"
       
   # 初始化数据库
       
    def initdatabase(self,mysqldb):
       
         mysqldb.query(self.trunc_1min_sql)
         mysqldb.update(self.delete_1min_sql)
         
         
         mysqldb.query(self.trunc_5min_sql)
         mysqldb.update(self.delete_5min_sql)
         
         
         mysqldb.query(self.trunc_15min_sql)
         mysqldb.update(self.delete_15min_sql)
         
         
         
         mysqldb.query(self.trunc_30min_sql)
         mysqldb.update(self.delete_30min_sql)
         
         
         
         mysqldb.query(self.trunc_60min_sql)
         mysqldb.update(self.delete_60min_sql)
         
         
         mysqldb.query(self.trunc_day_sql)
         mysqldb.update(self.delete_day_sql)
         
         
    
   # 板块数据从文本文件夹中读取，校正，入库
   # 获取板块时间数据             
              
    def getBoardIndexFile(self,bkpath):
        
       root_dir         = os.getcwd()+bkpath
       
       short_exclude    = []                            ###不包含检查的短目录、文件  
          
       long_exclude     = []                            ###不包含检查的长目录、文件  
    
       file_extend      = ['.txt']                      ###包含检查的文件类型  
    
       lf               = loop_file(root_dir.decode('utf-8'), short_exclude, long_exclude, file_extend)  
           
       # 求出板块时间目录下的文件
       for f in lf.start(lambda f: f): 
           
          pos    = f.find("//")
       
       return lf
       
       
    def getBoardIndexTime(self,lf): 
        
        
        bktimedict = {}
               
        # 求出板块时间目录下的文件
        for f in lf.start(lambda f: f):
                
              lflist = f.split("\\")
    
              bklen  = len(lflist)
              
              if bklen>2:
                  
                 tbkcode  = lflist[bklen-1]
                 
                 tbkcode  = tbkcode.replace(".txt","") 
                 
                 #取每只股票时间序列的集合，加入字典
                 s1=pd.read_table(f,header=1)
                 s1=s1.iloc[:,:1]
                 s1.columns=['time']
                 s1['time']=s1['time'].apply(lambda x:x.replace('/','').strip())
                 s1=s1[s1.time>=self.bkdate]
                 dateset=set()
                 for i in s1['time']:
                     dateset.add(unicode(i))   
                                
                 bktimedict[tbkcode] =  dateset    

                      
        return bktimedict            


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
                 

    def boardIndexTickHandle(self,boardindex,bktimedict):
        
        # 统计配置目录下文件夹个数
        
        # 读出样本数据，用于筛选数据
        
        sample_path = os.getcwd()+self.sample_path +"sample.csv"
        
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
            'hq_vol': 'sum'           
            }

        ohlc_dict = {           
            'hq_date':'first',
            'hq_time':'first',
            'hq_code':'first',
            'hq_open':'first',                                                                                                    
            'hq_high':'max',                                                                                                       
            'hq_low':'min',                                                                                                        
            'hq_close': 'last',                                                                                                    
            'hq_vol': 'sum'           
            }

        strpath  = os.getcwd() + self.bk_path
        
        dirlist = os.listdir(strpath.decode('utf-8'))
        
        fbdatelist   =[]
        
        fbcodelist   =[]
        
        fbtimelist   =[]
        
        fbpricelist  =[]
        
        fbvolist     =[]
        
        
        FlagStr=['txt']
        
        
        dbset     = set()
        
        dblist    =[]
        
        lastminrow = pd.DataFrame()
        
        #数据库引擎
        
        engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        
        for dl in dirlist:
            
           tmpstr  = os.getcwd() +self.bk_path +"\\"+dl.strip()
           
           fhl     =FileStringHandle()
           
           tstr    = tmpstr.decode('utf-8')
           
           filelist = fhl.GetFileList(tstr,FlagStr)
           
           #filelist.reverse()
           
           filelist.sort()
              
           #lf_dir  = self.getBoardIndexFile(tmpstr)
#              
#           #由时间全集生成 index
#           if bktimedict.has_key(dl):
#               bktimelist = list(bktimedict[dl])
#               bktimelist.sort()
                  
           filecount      = 0
           
           indexcount     = 0

           preindexcount  = 0    
           
           firstflag      = False
           
           slicenum       = 0
  
           pslicenum      = 0
           
           dbkeypos     = dl.find("-")
           
           #生成全局list
               
           if dbkeypos!=-1:
               
              dbkey = dl[dbkeypos+1:len(dl)]
              
              if bktimedict.has_key(dbkey):
                  
                  dbset  = bktimedict[dbkey]
                  
                  dblist = list(dbset)
                  
                  dblist.sort()
                  
           dblist_str = dblist.__str__()        
                  
           
           for f in filelist: 
               
               try:           
                   
                    
                     rfile = open(f,'r')                     
                     
                     fpos  = f.find(".txt")
                     
                     
                     print filecount
                     
                     if fpos!=-1:
                                                 
                         lflist = f.split("\\")
                         
                         
                         if len(lflist)>=2:
                             
                             fstr = lflist[len(lflist)-1]
                             
                             
                             tbkclass = lflist[len(lflist)-2]
                             
                             
                             tbkclass_1min = "1min\\"+tbkclass+".csv"
                             
                             tbkclass_5min = "5min\\"+tbkclass+".csv"
                             
                             tbkclass_15min = "15min\\"+tbkclass+".csv"
                             
                             tbkclass_30min = "30min\\"+tbkclass+".csv"
                             
                             tbkclass_60min = "60min\\"+tbkclass+".csv"
                             
                             tbkclass_day   = "day\\"+tbkclass+".csv"
                       
                             bkname_1min  = os.getcwd() + self.bkfzdata_path + tbkclass_1min
                             
                             
                             bkname_5min  = os.getcwd() + self.bkfzdata_path + tbkclass_5min
                             
                             
                             bkname_15min  = os.getcwd() + self.bkfzdata_path + tbkclass_15min
                             
                             
                             
                             bkname_30min  = os.getcwd() + self.bkfzdata_path + tbkclass_30min
                             
                             
                             bkname_60min  = os.getcwd() + self.bkfzdata_path + tbkclass_60min
                             
                             
                             bkname_day   = os.getcwd() + self.bkfzdata_path + tbkclass_day
 
         
                             fstr = fstr.replace(".txt","")
                             
                             dpos = fstr.find("_")
                             
                             if dpos!=-1:
                                 
                                 tmpdate =fstr[0:dpos] 
                                 
                                 tmpcode =fstr[dpos+1:len(fstr)]
                                 
                                 #如果文件的日期在全集list中，取出

                                 if dblist_str.find(tmpdate)!=-1 and firstflag==0:
                                    firstflag =True
                                    
                                    
                                 if firstflag:
                                     
                                     filecount = filecount +1 
                                     
                                               
                                     lines = rfile.readlines()
                                 
                                     splines = "|".join(lines)
                                 
                                     splist  = splines.split("|")
                                 
                                     #导入100个有效的文件
                                 
                                     if len(lines)>5:
                                     
                                       slicenum  = filecount//100
                                     
                                 
                                       for spl  in splist:
                                         
                                          tmplist  = spl.split()
                                         
                                          spos = spl.find(":")                     
                                         
                                          if len(tmplist)==4 and spos!=-1:
                                             
                                             fbdatelist.append(tmpdate)
                                             
                                             fbcodelist.append(tmpcode)
                                             
                                             fbtimelist.append(tmplist[0]) 
                                         
                                             fbpricelist.append(tmplist[1])
                                         
                                             fbvolist.append(tmplist[2])
                                             
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
                                         
                                         # 板块无
                                         # raw_dict["amo"] = fbprice_series * fbvol_series
                                        
                                                                             
                                       raw_data  =  pd.DataFrame(raw_dict,columns=['code','date','time','price','vol'])
                                         
                                       raw_data['price'] = raw_data['price'].astype('float64')
                                         
                                       raw_data['vol'] = raw_data['vol'].astype('float64')
                                         
                                       grouptime = raw_data['time'].groupby(raw_data['time'])
                                         
                                       groupcode = raw_data['code'].groupby(raw_data['time'])
                                         
                                       groupdate = raw_data['date'].groupby(raw_data['time'])
                                                                         
                                       groupvol  = raw_data['vol'].groupby(raw_data['time'])
                                                                             
                                       grouprice = raw_data['price'].groupby(raw_data['time'])
                                   
                                         
                                       oneminutes_code  = groupcode.head(1)
                                         
                                       oneminutes_date  = groupdate.head(1)
                                         
                                       oneminutes_time  = grouptime.head(1)
                                         
                                       oneminutes_open  =  grouprice.head(1)
                                         
                                       oneminutes_close =  grouprice.tail(1)
                                         
                                       oneminutes_ceiling_price = grouprice.max()
                                                                              
                                       oneminutes_floor_price = grouprice.min()
                                         
                                       oneminutes_vol = groupvol.sum()
                                     
                                       #处理一分钟数据
                                     
                                       # 处理代码
                                       if oneminutes_dict.has_key('hq_code'):
                                           tmp = oneminutes_dict['hq_code']                                
                                           tmp = np.concatenate([tmp,oneminutes_code.values],axis=0)
                                                
                                           oneminutes_dict['hq_code'] = tmp
                                                
                                       else:
                                           oneminutes_dict['hq_code'] = oneminutes_code.values                                         
                                             
                                             # 处理日期
                                       if oneminutes_dict.has_key('hq_date'):
                                           tmp =  oneminutes_dict['hq_date']
                                           tmp =   np.concatenate([tmp,oneminutes_date.values],axis=0)
                                           oneminutes_dict['hq_date'] = tmp
                                       else:
                                           oneminutes_dict['hq_date'] = oneminutes_date.values 
                                                 
                                             # 处理时间
                                       if oneminutes_dict.has_key('hq_time'):
                                           tmp =  oneminutes_dict['hq_time']
                                           tmp =   np.concatenate([tmp,oneminutes_time.values],axis=0)
                                           oneminutes_dict['hq_time'] = tmp
                                       else:
                                           oneminutes_dict['hq_time'] = oneminutes_time.values 
                                                   
                                             # 处理开盘价
                                       if oneminutes_dict.has_key('hq_open'):
                                           tmp = oneminutes_dict['hq_open']
                                           tmp =  np.concatenate([tmp,oneminutes_open.values],axis=0)
                                           oneminutes_dict['hq_open'] = tmp
                                       else:
                                           oneminutes_dict['hq_open'] = oneminutes_open.values 
                                        
                                         # 处理最高价
                                       if oneminutes_dict.has_key('hq_high'):
                                            tmp = oneminutes_dict['hq_high']
                                            tmp =  np.concatenate([tmp,oneminutes_ceiling_price.values],axis=0)
                                            oneminutes_dict['hq_high'] = tmp
                                       else:
                                            oneminutes_dict['hq_high'] = oneminutes_ceiling_price.values                                          
                                         
                                         
                                         # 处理最低价
                                       if oneminutes_dict.has_key('hq_low'):
                                            tmp =  oneminutes_dict['hq_low']
                                            tmp =  np.concatenate([tmp,oneminutes_floor_price.values],axis=0)
                                            oneminutes_dict['hq_low'] = tmp
                                       else:
                                            oneminutes_dict['hq_low'] = oneminutes_floor_price.values                                          
                                         
                                         # 处理收盘价
                                       if oneminutes_dict.has_key('hq_close'):
                                            tmp =  oneminutes_dict['hq_close']
                                            tmp =   np.concatenate([tmp,oneminutes_close.values],axis=0)
                                            oneminutes_dict['hq_close'] = tmp
                                       else:
                                            oneminutes_dict['hq_close'] = oneminutes_close.values                                          
                                         
                                         
                                         # 处理收盘价
                                       if oneminutes_dict.has_key('hq_vol'):
                                            tmp =  oneminutes_dict['hq_vol']
                                            tmp = np.concatenate([tmp,oneminutes_vol.values],axis=0)
                                            oneminutes_dict['hq_vol'] = tmp
                                       else:
                                            oneminutes_dict['hq_vol'] = oneminutes_vol.values                                          
#                                             
                                         
                                       tmp          =[]
                                         
                                       fbdatelist   =[]
                                        
                                       fbcodelist   =[]
                                        
                                       fbtimelist   =[]
                                        
                                       fbpricelist  =[]
                                        
                                       fbvolist     =[]
#                                                           
                                         #100个有效文件后，进行整体时间校验，缺失数据补齐，后存入指定文件，最后批量入库
                                                 
                                       if(slicenum>pslicenum):
                                                 
                                            pslicenum = slicenum
                                               
                                            # 最新dict索引
                                            indexcount = dblist.index(tmpdate)                                           
                                                                                                                                     
                                            tmpdblist = dblist[preindexcount:indexcount+1]
                                               
                                            reindexlist = []
                                            
                                               
                                           # 第一次全部录入
                                            if pslicenum==1:
                                               preindexcount =0 
                                           
                                            preindexcount = indexcount+1
                                           
                                            for tlist in tmpdblist:
                                               
                                                for saplist in samplelist:
                                                   
                                                   tmpstr = str(tlist) +" " +str(saplist) +":00"                                                  
                                                   
                                                   reindexlist.append(tmpstr)
                                               
  #                                        test = pd.to_datetime(oneminutes_date) 
                                           
                                           
                                           # minutes_index = pd.to_datetime(oneminutes_date.values +'' + oneminutes_time.values)
                                           
                                            if oneminutes_dict.has_key('hq_date') and oneminutes_dict.has_key('hq_time'):
                                                       
                                                 minutes_index = pd.to_datetime(oneminutes_dict['hq_date']+' ' +oneminutes_dict['hq_time'] )
                                               
                                                 oneminutes = pd.DataFrame(oneminutes_dict,index=minutes_index)
                                                 
                                               
#                                                test = oneminutes.resample('1T', label='left',closed='left',fill_method='ffill')
                                                 bkreindex = pd.to_datetime(reindexlist )
                                               
                                                 reoneminutes = oneminutes.reindex(index=bkreindex)
                                                 
                                                 np.seterr(invalid='ignore')
                                                 
                                                 zvalues = reoneminutes.loc[~(reoneminutes.hq_vol > 0)].loc[:, ['hq_time','hq_vol']]
                                                 
                                                 reoneminutes.update(zvalues.fillna(0))
                                                 
                                                 reheadrow = reoneminutes.head(1)
                                                 
                                                 firstisnull = reheadrow.isnull().any().any()
                                                 
                                                 if firstisnull and pslicenum>1:
                                                     
                                                     tmpindex  = reoneminutes.iloc[[0]].index
                                                     
                                                     nreoneminutes = lastminrow.set_index(tmpindex)
                                                     
                                                     nreoneminutes['hq_time'] = 0
                                                     
                                                     nreoneminutes['hq_vol'] = 0 
                                                     
                                                     reoneminutes =  reoneminutes[1:]
                                                     
                                                     reoneminutes = pd.concat([nreoneminutes,reoneminutes])
                                                     
                                                 
                                                 reoneminutes.fillna(method='ffill', inplace=True)
                                                 
                                                 
                                                 tmpfiveminutes=reoneminutes.resample('5T', label='left',closed='left').apply(ohlc_dict)
                                                 
                                                 refiveminutes = tmpfiveminutes.dropna(axis=0)
                                                 
                                                 tmp15minutes=refiveminutes.resample('15T', label='left',closed='left').apply(ohlc_dict)
                                                 re15minutes= tmp15minutes.dropna(axis=0)
                                                 
                                                 tmp30minutes=re15minutes.resample('30T',label='left',closed='left').apply(ohlc_dict)
                                                 re30minutes= tmp30minutes.dropna(axis=0)
                                                 
                                                 tmp60minutes=re15minutes.resample('60T', label='left',closed='left').apply(ohlc_dict)
                                                 re60minutes= tmp60minutes.dropna(axis=0)
                                                 
                                                 tmpday=tmp60minutes.resample('D', label='left',closed='left').apply(ohlc_dict_day)
                                                 redays= tmpday.dropna(axis=0)
#                                                
                                                 
                                                 lastminrow = oneminutes.tail(1)
                                                 
                                            
                                                 if pslicenum<=1:
                                                     
                                                    reoneminutes.to_csv(bkname_1min,mode='w')
                                                    
                                                    refiveminutes.to_csv(bkname_5min,mode='w')
                                                                                                        
                                                    re15minutes.to_csv(bkname_15min,mode='w')
                                                    
                                                    re30minutes.to_csv(bkname_30min,mode='w')
                                                    
                                                    re60minutes.to_csv(bkname_60min,mode='w')
                                                    
                                                    redays.to_csv(bkname_day,mode='w')
                                                    
                                                    reoneminutes.to_sql('hindexquotationone',engine,if_exists='append')
                                                    
                                                    refiveminutes.to_sql('hindexquotationfive',engine,if_exists='append')
                                                    
                                                    re15minutes.to_sql('hindexquotationfifteen',engine,if_exists='append')
                                                    
                                                    re30minutes.to_sql('hindexquotationthirty',engine,if_exists='append')
                                                    
                                                    re60minutes.to_sql('hindexquotationsixty',engine,if_exists='append')
                                                    
                                                    redays.to_sql('hindexquotationday',engine,if_exists='append')
                                                    
                                                 else:
                                                    
                                                    reoneminutes.to_csv(bkname_1min,mode='a',header=False)
                                                    
                                                    refiveminutes.to_csv(bkname_5min,mode='a',header=False)
                                                                                                        
                                                    re15minutes.to_csv(bkname_15min,mode='a',header=False)
                                                    
                                                    re30minutes.to_csv(bkname_30min,mode='a',header=False)
                                                    
                                                    re60minutes.to_csv(bkname_60min,mode='a',header=False)
                                                    
                                                    redays.to_csv(bkname_day,mode='a',header=False)
                                                    
                                                    
                                                    reoneminutes.to_sql('hindexquotationone',engine,if_exists='append')
                                                    
                                                    refiveminutes.to_sql('hindexquotationfive',engine,if_exists='append')
                                                    
                                                    re15minutes.to_sql('hindexquotationfifteen',engine,if_exists='append')
                                                    
                                                    re30minutes.to_sql('hindexquotationthirty',engine,if_exists='append')
                                                    
                                                    re60minutes.to_sql('hindexquotationsixty',engine,if_exists='append')
                                                    
                                                    redays.to_sql('hindexquotationday',engine,if_exists='append')
                                                                                                     
                                                 
                                                 reindexlist =[]
                                                 
                                                 
                                                 reoneminutes    =  pd.DataFrame()
                                                 oneminutes      =  reoneminutes
                                                 
                                                 tmpfiveminutes  =  reoneminutes
                                                 refiveminutes   =  reoneminutes
                                                 
                                                 tmp15minutes    =  reoneminutes
                                                 refiveminutes   =  reoneminutes
                                                  
                                                 tmp30minutes    =  reoneminutes
                                                 re30minutes     =  reoneminutes
                                                  
                                                 tmp60minutes    =  reoneminutes
                                                 re60minutes     =  reoneminutes
                                                 
                                                 tmpday          =  reoneminutes
                                                 redays          =  reoneminutes
                                                 
                                                 oneminutes_dict ={}
#                                           
#                                           bkarray = np.array([fbcodelist,fbdatelist,fbtimelist,fbpricelist,fbvolist])
#                                    
#                                           bkdf    = pd.DataFrame(bkarray.T)
#                                          
                                           #满100个文件，所有list清零
                                      
                     
               finally:
                        
                  if rfile:
                     rfile.close()
               
           # 若还有剩余数据
           if oneminutes_dict:
                
             if oneminutes_dict.has_key('hq_date') and oneminutes_dict.has_key('hq_time'):
                                                                                          
                minutes_index = pd.to_datetime(oneminutes_dict['hq_date']+' ' +oneminutes_dict['hq_time'] )
                
                
                oneminutes = pd.DataFrame(oneminutes_dict,index=minutes_index)
                
                tmpdblist = dblist[preindexcount:]
                
                
                
                for tlist in tmpdblist:
                                               
                    for saplist in samplelist:
                                                   
                        tmpstr = str(tlist) +" " +str(saplist) +":00"                                                  
                                                   
                        reindexlist.append(tmpstr)
                        
                bkreindex = pd.to_datetime(reindexlist )
                
                
                reoneminutes = oneminutes.reindex(index=bkreindex)
                 
                np.seterr(invalid='ignore')
                 
                zvalues = reoneminutes.loc[~(reoneminutes.hq_vol > 0)].loc[:, ['hq_time','hq_vol']]
                 
                reoneminutes.update(zvalues.fillna(0))
                 
                
                firstisnull = reheadrow.isnull().any().any()
                
                if firstisnull and pslicenum>1:
                     
                     tmpindex  = reoneminutes.iloc[[0]].index
                     
                     nreoneminutes = lastminrow.set_index(tmpindex)
                     
                     nreoneminutes['hq_time'] = 0
                     
                     nreoneminutes['hq_vol'] = 0 
                     
                     reoneminutes =  reoneminutes[1:]
                     
                     reoneminutes = pd.concat([nreoneminutes,reoneminutes])

                reoneminutes.fillna(method='ffill', inplace=True)
               
                 
                tmpfiveminutes=reoneminutes.resample('5T', label='left',closed='left').apply(ohlc_dict)
                 
                refiveminutes = tmpfiveminutes.dropna(axis=0)
                 
                tmp15minutes=refiveminutes.resample('15T', label='left',closed='left').apply(ohlc_dict)
                re15minutes= tmp15minutes.dropna(axis=0)
                 
                tmp30minutes=re15minutes.resample('30T',label='left',closed='left').apply(ohlc_dict)
                re30minutes= tmp30minutes.dropna(axis=0)
                 
                tmp60minutes=re15minutes.resample('60T', label='left',closed='left').apply(ohlc_dict)
                re60minutes= tmp60minutes.dropna(axis=0)
                 
                tmpday=tmp60minutes.resample('D', label='left',closed='left').apply(ohlc_dict_day)
                redays= tmpday.dropna(axis=0)
                 
                reindexlist =[]        
                
                
                if pslicenum<=1:
                     
                    reoneminutes.to_csv(bkname_1min,mode='w')
                    
                    
                    refiveminutes.to_csv(bkname_5min,mode='w')
                                                                        
                    re15minutes.to_csv(bkname_15min,mode='w')
                    
                    re30minutes.to_csv(bkname_30min,mode='w')
                    
                    re60minutes.to_csv(bkname_60min,mode='w')
                    
                    redays.to_csv(bkname_day,mode='w')
                    
                    reoneminutes.to_sql('hindexquotationone',engine,if_exists='append')
                    
                    refiveminutes.to_sql('hindexquotationfive',engine,if_exists='append')
                    
                    re15minutes.to_sql('hindexquotationfifteen',engine,if_exists='append')
                    
                    re30minutes.to_sql('hindexquotationthirty',engine,if_exists='append')
                    
                    re60minutes.to_sql('hindexquotationsixty',engine,if_exists='append')
                    
                    redays.to_sql('hindexquotationday',engine,if_exists='append')
   
                else:
                    
                    reoneminutes.to_csv(bkname_1min,mode='a',header=False)
                    
                    refiveminutes.to_csv(bkname_5min,mode='a',header=False)
                                                                        
                    re15minutes.to_csv(bkname_15min,mode='a',header=False)
                    
                    re30minutes.to_csv(bkname_30min,mode='a',header=False)
                    
                    re60minutes.to_csv(bkname_60min,mode='a',header=False)
                    
                    redays.to_csv(bkname_day,mode='a',header=False)

                    reoneminutes.to_sql('hindexquotationone',engine,if_exists='append')
                    
                    refiveminutes.to_sql('hindexquotationfive',engine,if_exists='append')
                    
                    re15minutes.to_sql('hindexquotationfifteen',engine,if_exists='append')
                    
                    re30minutes.to_sql('hindexquotationthirty',engine,if_exists='append')
                    
                    re60minutes.to_sql('hindexquotationsixty',engine,if_exists='append')
                    
                    redays.to_sql('hindexquotationday',engine,if_exists='append')


                reoneminutes    =  pd.DataFrame()
                oneminutes      =  reoneminutes
             
                tmpfiveminutes  =  reoneminutes
                refiveminutes   =  reoneminutes
             
                tmp15minutes    =  reoneminutes
                refiveminutes   =  reoneminutes
              
                tmp30minutes    =  reoneminutes
                re30minutes     =  reoneminutes
              
                tmp60minutes    =  reoneminutes
                re60minutes     =  reoneminutes
             
                tmpday          =  reoneminutes
                redays          =  reoneminutes
             
                oneminutes_dict ={}





        
