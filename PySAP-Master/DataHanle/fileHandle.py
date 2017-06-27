# -*- coding: utf-8 -*-
"""
Created on Thu May 18 14:31:11 2017

@author: Administrator
"""

import pandas as pd
import os
from sqlalchemy import create_engine 



class checkFile():
    def __init__(self):
        self.rdir=u'F:\\PySAP\\Data\\History\\Stock\\股票历史数据\\1min\\'

        self.tdir=u'F:\\PySAP\\Data\\History\\Stock\\股票历史数据\\补全数据1min\\'
        
        self.rootdir=u'F:\\PySAP\\Data\\History\\Stock\\股票历史数据\\'

        self.engine = create_engine('mysql://root:1234@127.0.0.1/stocksystem?charset=utf8')
        
        self.tickdir=u'F:\\PySAP\\Data\\History\\Stock\\股票tick数据'
        
    #补齐1分钟数据   
    def fillFile(self):
        flist=os.listdir(self.rdir)
        num=0
        for f in flist:            
            fname=os.path.join(self.rdir,f)  
            tname=os.path.join(self.tdir,f)
            s1=pd.read_csv(fname,index_col=0)
            
            s1['hq_close'].fillna(method='ffill',inplace=True)
            s1['hq_open'].fillna(method='ffill',inplace=True)
            s1['hq_high'].fillna(method='ffill',inplace=True)
            s1['hq_low'].fillna(method='ffill',inplace=True)
            
            s1['hq_close'].fillna(method='bfill',inplace=True)
            s1['hq_open'].fillna(method='bfill',inplace=True)
            s1['hq_high'].fillna(method='bfill',inplace=True)
            s1['hq_low'].fillna(method='bfill',inplace=True)
            
            s1['hq_amo'].fillna(0,inplace=True)
            s1['hq_vol'].fillna(0,inplace=True)
            s1.to_csv(tname)
            num+=1
            print num    
    
    #聚合1分钟数据，生成CSV,入库        
    def groupFile(self):
        ohlc_dict = {           
            'hq_date':'last',
            'hq_time':'first',
            'hq_code':'first',
            'hq_open':'first',                                                                                                    
            'hq_high':'max',                                                                                                       
            'hq_low':'min',                                                                                                        
            'hq_close': 'last',                                                                                                    
            'hq_vol': 'sum' ,   
            'hq_amo': 'sum'        
            }
        
        
        ohlc_dict2 = {           
            'hq_date':'last',
            'hq_code':'first',
            'hq_open':'first',                                                                                                    
            'hq_high':'max',                                                                                                       
            'hq_low':'min',                                                                                                        
            'hq_close': 'last',                                                                                                    
            'hq_vol': 'sum' ,   
            'hq_amo': 'sum'        
            }    
        
        
        flist=os.listdir(self.rdir)

        def cl(x):
            x.ix[1:2,['hq_vol','hq_amo']]=x.ix[0:1,['hq_vol','hq_amo']].values+x.ix[1:2,['hq_vol','hq_amo']].values
            x=x[1:]
            return x
            
        def cl30(x):
            y=pd.DataFrame()
            x.ix[2:3,'hq_open']=x.ix[0:1,'hq_open'].values
            x.ix[2:3,'hq_high']=x.ix[0:3,'hq_high'].max()
            x.ix[2:3,'hq_low']=x.ix[0:3,'hq_low'].min()
            x.ix[2:3,'hq_amo']=x.ix[0:3,'hq_amo'].sum()
            x.ix[2:3,'hq_vol']=x.ix[0:3,'hq_vol'].sum()
            y=y.append(x.ix[2:3])
            
            x.ix[4:5,'hq_open']=x.ix[3:4,'hq_open'].values
            x.ix[4:5,'hq_high']=x.ix[3:5,'hq_high'].max()
            x.ix[4:5,'hq_low']=x.ix[3:5,'hq_low'].min()
            x.ix[4:5,'hq_amo']=x.ix[3:5,'hq_amo'].sum()
            x.ix[4:5,'hq_vol']=x.ix[3:5,'hq_vol'].sum()
            y=y.append(x.ix[4:5])
            
            x.ix[6:7,'hq_open']=x.ix[5:6,'hq_open'].values
            x.ix[6:7,'hq_high']=x.ix[5:7,'hq_high'].max()
            x.ix[6:7,'hq_low']=x.ix[5:7,'hq_low'].min()
            x.ix[6:7,'hq_amo']=x.ix[5:7,'hq_amo'].sum()
            x.ix[6:7,'hq_vol']=x.ix[5:7,'hq_vol'].sum()
            y=y.append(x.ix[6:7])
            
            x.ix[8:9,'hq_open']=x.ix[7:8,'hq_open'].values
            x.ix[8:9,'hq_high']=x.ix[7:9,'hq_high'].max()
            x.ix[8:9,'hq_low']=x.ix[7:9,'hq_low'].min()
            x.ix[8:9,'hq_amo']=x.ix[7:9,'hq_amo'].sum()
            x.ix[8:9,'hq_vol']=x.ix[7:9,'hq_vol'].sum()
            y=y.append(x.ix[8:9])
        
            return y
            
        num=0
                    
        for f in flist:
             
            fname=os.path.join(self.rdir,f)    
            s1=pd.read_csv(fname,index_col=0)
            s1.index=pd.to_datetime(s1.index)
            s5=s1.resample('5T', label='right',closed='left').apply(ohlc_dict)  
            s5.dropna(axis=0,inplace=True)    
            s5=s5.groupby(s5['hq_date']).apply(lambda x:cl(x))
            del s5['hq_date']
            s5.reset_index(level=0,inplace=True)
            s15=s1.resample('15T', label='right',closed='left').apply(ohlc_dict) 
            s15.dropna(axis=0,inplace=True)
            s30=s1.resample('30T', label='right',closed='left').apply(ohlc_dict) 
            s30.dropna(axis=0,inplace=True)
            s60=s30.groupby('hq_date').apply(lambda x:cl30(x))  
            del s60['hq_date']
            s60.reset_index(level=0,inplace=True)
        #    s60=s1.resample('60T', label='right',closed='left').apply(ohlc_dict) 
        #    s60.dropna(axis=0,inplace=True)
            sday=s60.resample('D', label='left',closed='left').apply(ohlc_dict2) 
            sday.dropna(axis=0,inplace=True)
            sweek=s60.resample('W-FRI').apply(ohlc_dict2)
            sweek.dropna(axis=0,inplace=True)
            pass
            if os.path.exists(u'F:\\PySAP\\Data\\History\\Stock\\股票历史数据\\5min\\'+f):                                 
                s5.to_csv(self.rootdir+'5min\\'+f,mode='a',header=False)
                s5.to_sql('hstockquotationfive',self.engine,if_exists='append')                                 
            else:                                
                s5.to_csv(self.rootdir+'5min\\'+f,mode='w')
                s5.to_sql('hstockquotationfive',self.engine,if_exists='append') 
            if os.path.exists(self.rootdir+'15min\\'+f):                                 
                s15.to_csv(self.rootdir+'15min\\'+f,mode='a',header=False)
                s15.to_sql('hstcokquotationfifteen',self.engine,if_exists='append')                                 
            else:                                
                s15.to_csv(self.rootdir+'15min\\'+f,mode='w')
                s15.to_sql('hstcokquotationfifteen',self.engine,if_exists='append') 
            if os.path.exists(self.rootdir+'30min\\'+f):                                 
                s30.to_csv(self.rootdir+'30min\\'+f,mode='a',header=False)
                s30.to_sql('hstockquotationthirty',self.engine,if_exists='append')                                 
            else:                                
                s30.to_csv(self.rootdir+'30min\\'+f,mode='w')
                s30.to_sql('hstockquotationthirty',self.engine,if_exists='append')
                
            if os.path.exists(self.rootdir+'60min\\'+f):                                 
                s60.to_csv(self.rootdir+'60min\\'+f,mode='a',header=False)
                s60.to_sql('hstockquotationsixty',self.engine,if_exists='append')                                 
            else:                                
                s60.to_csv(self.rootdir+'60min\\'+f,mode='w')
                s60.to_sql('hstockquotationsixty',self.engine,if_exists='append')
            if os.path.exists(self.rootdir+'day\\'+f):                                 
                sday.to_csv(self.rootdir+'day\\'+f,mode='a',header=False)
                sday.to_sql('hstockquotationday',self.engine,if_exists='append')                                 
            else:                                
                sday.to_csv(self.rootdir+'day\\'+f,mode='w')
                sday.to_sql('hstockquotationday',self.engine,if_exists='append')
            if os.path.exists(self.rootdir+'week\\'+f):                                 
                sweek.to_csv(self.rootdir+'week\\'+f,mode='a',header=False)
                sweek.to_sql('hstockquotationweek',self.engine,if_exists='append')                                 
            else:                                
                sweek.to_csv(self.rootdir+'week\\'+f,mode='w')
                sweek.to_sql('hstockquotationweek',self.engine,if_exists='append')
                
            num+=1
            print num        
            
    #对1分钟数据进行入库，入库目录为补全数据1min
    def toSql1min(self):
        i=0
        s2=pd.DataFrame()
        filelist=os.listdir(self.tdir)
        lastfile=filelist[-1]
        for f in  filelist:
            filename=os.path.join(self.tdir,f)
            s1=pd.read_csv(filename,index_col=0)
            s2=s2.append(s1)
            i+=1
            print i
            if i%1==0:
                s2.to_sql('hstockquotationone',con=self.engine,if_exists='append')
                s2=pd.DataFrame()
            elif f==lastfile:
                s2.to_sql('hstockquotationone',con=self.engine,if_exists='append')    
                
    #找到小于1KB的文件，输入参数：年份（字符串）    
    def find0kbFile(self,year):
        ydir=os.path.join(self.tickdir,year)
        mdirlist=[]
        mlist=os.listdir(ydir)
        for m in mlist:
            mdir=os.path.join(ydir,m)
            mdirlist.append(mdir)
            
        #获得日期目录列表
        ddirlist=[]
        for mdir in mdirlist:
            dlist=os.listdir(mdir)
            for d in dlist:
                ddir=os.path.join(mdir,d)
                ddirlist.append(ddir)
        
        
        #找到日期目录
        for ddir in ddirlist:
            for f in os.listdir(ddir):
                fname=os.path.join(ddir,f)
                #找到小于0.1K的文件，输出
                fsize=float(os.path.getsize(fname))
                fsize=fsize/1024
                if fsize<0.01:
                    print fname        
                    
    #找到缺失的文件，，输入参数：年份（字符串）                    
    def findMissFile(self,year):
        ydir=os.path.join(self.tickdir,year)
        missfile=os.path.join(self.tickdir,u'tick缺失','missingdata.txt')
        trdset=set()
        tickset=set()
        mdirlist=[]
        mlist=os.listdir(ydir)
        for m in mlist:
            mdir=os.path.join(ydir,m)
            mdirlist.append(mdir)
            
        #获得日期目录列表
        ddirlist=[]
        for mdir in mdirlist:
            dlist=os.listdir(mdir)
            for d in dlist:
                ddir=os.path.join(mdir,d)
                ddirlist.append(ddir)
       
        #找到日期目录
        num=0
        for ddir in ddirlist:
            day=(ddir.split('\\'))[-1]
            for f in os.listdir(ddir):
                code=f[2:-4]
                tickset.add(code)
                codeList=pd.read_sql("SELECT code FROM stockday where hq_date =str_to_date('"+day+"', '%Y-%m-%d %H')",con=self.engine)
                length=len(codeList)
                for i in xrange(length):
                        code=codeList.iat[i,0]
                        trdset.add(code)
                    
                difference=trdset-tickset
                if difference:
                    fin=open(missfile,'a')
                    fin.write(day+'\n')
                    for line in difference:
                        fin.write(line+'\n')
                    fin.write('--------------------------------------------\n')
                    fin.close()
                    print '有差'          
            num+=1
            print num
                    
                    

        

if __name__=='__main__':
    c=checkFile()
    c.findMissFile('2015')

    
    
    


    

    
    
    
    
    
    
    
    
    
    
    
    

