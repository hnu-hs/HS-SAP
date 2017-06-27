# -*- coding: utf-8 -*-
"""
Created on Thu Apr 13 14:47:34 2017

@author: Administrator
"""

  
from Queue import Queue  
  
import random  
  
import threading  
  
import time  

import tushare as ts

import pandas as pd

import pymysql
   
  
#Producer thread  
  
class Producer(threading.Thread):  
  
    def __init__(self, t_name, queue):  
  
        threading.Thread.__init__(self, name=t_name)  
  
        self.data=queue  
  
    def run(self):
        
        self.getInfo()       
  
        print "%s: %s finished!" %(time.ctime(), self.getName())  
        
    def getInfo(self):
        
        for pronum in xrange(1,1000):   #抓多少次
            s1=ts.get_index()
            s1=s1.loc[:,['code','name','change','preclose','high','low','close','amount']]
            t1=str(pd.to_datetime(time.ctime()))
            t1=t1[:-3]
            #t1=time.time()  #t1转换成全秒数
            s1['time']=t1
            print "%s: %s is producing %d to the queue!/n" %(time.ctime(), self.getName(), pronum)  
            self.data.put(s1)  
            time.sleep(3)  
      
   
  
#Consumer thread  
  
class Consumer(threading.Thread):  
  
    def __init__(self, t_name, queue):  
  
        threading.Thread.__init__(self, name=t_name)  
  
        self.data=queue  
        
        
       
    def run(self):
        
        con1=self.conlzg()               
        self.inInfo(con1)
        print "%s: %s finished!" %(time.ctime(), self.getName())  
    
    def inInfo(self,con):        
        cosnum=0
        treattime=0
        for i in xrange(100):      #入库多少次
            repeat=0
            start=0
            s2 = self.data.get(timeout=15)
            datanum=len(s2)
            repeat+=1
            cosnum+=1
            print "%s: %s is consuming. %d in the queue is consumed!/n" %(time.ctime(), self.getName(),cosnum) 
            t1=s2.at[0,'time']
            open_=s2['preclose'] 
            while True:
                s3=self.data.get(timeout=15)
                cosnum+=1
                print "%s: %s is consuming. %d in the queue is consumed!/n" %(time.ctime(), self.getName(),cosnum)
                t2=s3.at[0,'time']
                if t2!=t1:
                #if (t2-t1)>=60:
                    t1=t2
                    #t2=time.ctime(t2)
                    #t2=t2[11:-8]
                    #s3['time']=t2
                    break
                else:
                    s2=s2.append(s3)
                    repeat+=1
                    
            
            close_=s3['close']
            del s2['close']
            del s3['close']
           
            high_=pd.Series()
            low_=pd.Series()
            s2.sort(columns='name',inplace=True)
            for j in xrange(datanum):                
                s4=s2[start:start+repeat]
                #s4.sort(columns='high',ascending=False,inplace=True)
                #high=s4[:1].high
                max_=s4.high.max()
                high=s4['high'][s4.high==max_]
                high=high[:1]
                high_=high_.append(high)
                #s4.sort(columns='low',inplace=True)
                #low=s4[:1].low
                min_=s4.low.min()
                low=s4['low'][s4.low==min_]
                low=low[:1]
                low_=low_.append(low)
                start+=repeat
               
            #print high_
            #print open_
            s3['preclose']=open_
            s3['high']=high_
            s3['low']=low_
            s3['close']=close_
            #s3.to_excel('E:\\test\\test2.xlsx')  
            pd.io.sql.to_sql(s3,'zs', con, schema='lzg', flavor='mysql',if_exists='append')                
            s2=pd.DataFrame()
            treattime+=1
            print '处理数据第%d次'%treattime
            time.sleep(random.randrange(10))  
        
    def conlzg(self):
        
        con=pymysql.connect(host='localhost',
        user='root',
        passwd='lzg000',
        db='lzg',
        port=3306,
        charset='utf8'
        )
        return con
            
            
  
   
  
#Main thread  
  
def main():  
  
    queue = Queue()  
  
    producer = Producer('Pro.', queue)  
  
    consumer = Consumer('Con.', queue)  
  
    producer.start()  
  
    consumer.start()  
  
    producer.join()  
  
    consumer.join()  
  
    print 'All threads terminate!'  
  
   
  
if __name__ == '__main__':  
  
    main()  