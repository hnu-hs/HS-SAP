# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 15:03:22 2017

@author: Administrator
"""
import sys
sys.path.append("..")
reload(sys)

import pandas as pd
import numpy as np
import os
import time
from sqlalchemy import create_engine
from PlotData.plotexcel import plotToExcel
from colligation import ZH

class ZJ():
    def __init__(self):
        self.dir=u'E:\\工作\\tick\\20170622'
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
    
    #按成交量划分10个类别
    def tradeType(self,flag=0):
        names=['time','close','vol']
        flist=os.listdir(self.dir)
        s2=pd.DataFrame()
        n=0
        for f in flist:
            fname=os.path.join(self.dir,f)
            s1=pd.read_table(fname,names=names,dtype=int)
            #if flag==0: 
            
            #计算amo,亿为单位
            s1['amo']=np.abs(s1['vol'])*s1['close']/10000000000
                
            #分类
            vbig=s1[s1.close*np.abs(s1.vol)/100>=1000000]
            vbigs=vbig[vbig.vol<0]
            vbigb=vbig[vbig.vol>0]
            
            lbig=s1[(s1.close*np.abs(s1.vol)/100<1000000)&(s1.close*np.abs(s1.vol)/100>=500000)]
            lbigs=lbig[lbig.vol<0]
            lbigb=lbig[lbig.vol>0]
            
            big=s1[(s1.close*np.abs(s1.vol)/100<500000)&(s1.close*np.abs(s1.vol)/100>=100000)]
            bigs=big[big.vol<0]            
            bigb=big[big.vol>0]
            
            mid=s1[(s1.close*np.abs(s1.vol)/100<100000)&(s1.close*np.abs(s1.vol)/100>=30000)]
            mids=mid[mid.vol<0]            
            midb=mid[mid.vol>0]
            
            small=s1[(s1.close*np.abs(s1.vol)/100<30000)]
            smalls=small[small.vol<0]
            smallb=small[small.vol>0]
            
            vbigs['type']='vbigS'            
            vbigb['type']='vbigB'  
                 
            lbigs['type']='lbigS'
            lbigb['type']='lbigB'
                       
            bigs['type']='bigS'
            bigb['type']='bigB' 
                
            mids['type']='midS'
            midb['type']='midB'
            
            smalls['type']='smallS'
            smallb['type']='smallB'
                     
            #拼接分类的series，赋值给总数据                         
            zjtype=pd.concat([vbigs['type'],vbigb['type'],lbigs['type'],lbigb['type'],bigs['type'],bigb['type'],mids['type'],midb['type'],smalls['type'],smallb['type']])
            s1['type']=zjtype
            
#            else:
#                s2=s1.shift()
#                sg=s2[s2.close<s1.close]
#                xj=s2[s2.close>s1.close]
#                sg['type']='up'
#                xj['type']='down'
#                s1['type']=pd.concat([sg['type'],xj['type']])
#                s1.sort_index(inplace=True)
#                s1.fillna(0,inplace=True) 
            
            #计算成交额占比    
            #amosum=s1['amo'].sum()
            s1=s1.groupby('type').agg({'amo':'sum'})
            #s1['per']=s1['amo']/amosum
            
            #reindex，后面索引相同才可以相加
            sindex=['bigB','bigS','lbigB','lbigS','midB','midS','smallB','smallS','vbigB','vbigS']              
            s1=s1.reindex(sindex)      
            
            
            code=f[2:-4]           
#            s1['code']=int(code)              
#            code=s1['code']
#            del s1['code']           
#            s1=s1.set_index([code,s1.index])        
#            s1=s1.unstack()['amo']

            s1=s1.unstack().unstack()
            s1['hq_code']=int(code)
            
            s2=s2.append(s1)  
            n+=1
            if n%10==0:            
                print n
            
            #给空值赋0，为了后面可以相加不为空
        s2.fillna(0,inplace=True)

        return s2
    
    #按成交额大小排序
    def sortAmo(self):
        s1=self.tradeType()
#        vbigs=s1.loc['vbigS'].sort_values('amo',ascending=False).set_index('code')
#        lbigs=s1.loc['lbigS'].sort_values('amo',ascending=False).set_index('code')
#        vbigb=s1.loc['vbigB'].sort_values('amo',ascending=False).set_index('code')
#        lbigb=s1.loc['lbigB'].sort_values('amo',ascending=False).set_index('code')
#        vbigs['name']=stockNames['stock_name']    
#        vbigb['name']=stockNames['stock_name']
#        lbigs['name']=stockNames['stock_name']    
#        lbigb['name']=stockNames['stock_name']    
#        bigs['name']=stockNames['stock_name']
#        bigb['name']=stockNames['stock_name']           
        
#        bigs=(vbigs+lbigs).sort_values('amo',ascending=False)
#        bigb=(vbigb+lbigb).sort_values('amo',ascending=False)
#        vbigs=vbigs.dropna().head(30) 
#        vbigb=vbigb.dropna().head(30)
#        lbigs=lbigs.dropna().head(30)
#        lbigb=lbigb.dropna().head(30)
#        bigs=bigs.dropna().head(30)
#        bigb=bigb.dropna().head(30)

        s1['bigD']=s1['vbigB']+s1['lbigB']-s1['vbigS']-s1['lbigS']
        s1['vbigD']=s1['vbigB']-s1['vbigS']
        s1['lbigD']=s1['lbigB']-s1['lbigS']   
        
        s1.index=s1['hq_code']
        stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])  
        s1['name']=stockNames['stock_name']   

        s1=s1.loc[:,['name','vbigB','vbigS','vbigD','lbigB','lbigS','lbigD','bigD','hq_code']].sort_values('vbigB',ascending=False).dropna()
        
        c=ZH()
        s2=c.getRF(30,s1)   
        s3=s1
        s1=s1[s1.vbigB>0.1]
            
        return s1,s2,s3

if __name__=='__main__':

    z=ZJ()
    s1,s2,s3=z.sortAmo()
    date=z.dir[-8:]    
    s3.to_csv(u'E:\\工作\\数据备份\\资金流\\'+date+'flow.csv',encoding='gbk')
    pte = plotToExcel.PlotToExcel()
    pte.bulidZJ(s1,s2,date)
    