# -*- coding: utf-8 -*-
"""
Created on Fri Sep 01 13:53:36 2017

@author: Administrator
"""

import pandas as pd 

from sqlalchemy import create_engine 

import xlsxwriter

class amoStrategy:
    
    def __init__(self,sdate,edate,window):
        
        #不属于56个行业的板块数据文件
        self.sindexfile=u'E:\\工作\\标的\\sindex.txt'
        
        u'\\BoardIndex\\config\\通达信数据补齐目录\\Tdxdb.txt'      
        
        self.stocBoardFile=u'E:\\工作\\股票数据\\股票细分板块\\allboard.txt'
        
        self.powerIndexDir=u'E:/工作/报表/量能选股/'
        
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        self.sdate=sdate
        
        self.edate=edate
        
        #定义要筛选几天的票，给出对应天数的股票dataframe
        self.window=window


    #获取股票关联板块
    def getStockRelated(self,df_stock,stockrelated):
        
        stockrelated['cFlag']=stockrelated['board_name'].str.contains(u'通达信行业-')
        
        stockrelated=stockrelated[stockrelated['cFlag']]
        
        stockrelated['board_name']=stockrelated['board_name'].apply(lambda x:x.replace(u'通达信行业-',''))     
        
        df_stock.index=df_stock.hq_code
        
        stockrelated.index=stockrelated.stock_id
        
        stockrelated['board_name']
        
        return  stockrelated['board_name']
        
        
        
    #获取指数代码对应名称
    def getIndexNames(self,df_index,indexNames):
        
        if 'hq_code' in df_index:
            df_index.set_index('hq_code',inplace=True)
            
        df_index['hq_name']=indexNames['board_name']
        
        df_index['hq_name']=df_index['hq_name'].apply(lambda x:x.replace(u'通达信行业-',''))  
        
        df_index.index=range(len(df_index))
        
        return df_index['hq_name']
    
    
    
    #获取股票代码对应名称
    def getStockNames(self,df_stock,stockNames):
        
        if 'hq_code' in df_stock.columns:     
            df_stock.index=df_stock['hq_code']
            
        df_stock['hq_name']=stockNames['stock_name']
        
        return df_stock['hq_name']  
        
        
        
    #板块日线成交额效能计算,时间窗口为20天
    def amoPower(self,df,marketAmoSeries):
        
        
       dataLen=len(marketAmoSeries)
       
       try:
           df.index=xrange(dataLen)
       except:
           print df.iat[0,0]
           print len(df)
       
       df['amo_ratio']=df['hq_amo']/marketAmoSeries
       
       usepower=0
       
       highpower=[]
       
       amoRatioSum=[]
       
       for i in range(dataLen-self.window,dataLen):
           
           amoRatioMean=df[i-20:i]['amo_ratio'].mean()
           amoRatioStd=df[i-20:i]['amo_ratio'].std()
           
           if amoRatioStd != 0:
               amo_power=df.loc[i,'amo_power']=(df.iloc[i]['amo_ratio']-amoRatioMean)/amoRatioStd
               
           else:
               amo_power=None
           
           if amo_power>=3:
               usepower+=1
               highpower.append(amo_power)
               amoRatioSum.append(df.iloc[i]['amo_ratio'])
            
       highpower=pd.Series(highpower)
       amoRatioSum=pd.Series(amoRatioSum)
       
       df['usecount']=usepower
       df['usepower_avg']=highpower.mean()
       df['amoratio_avg']=amoRatioSum.mean()

       return df

    #得到标的数据,要穿一个hq_code列
    def getRF(self,name,df_stock):
                   
        if name==30:      
            codelist=self.getCode(u'E:\\工作\\标的\\30.txt')
        else:
            codelist=self.getCode(u'E:\\工作\\标的\\200.txt')
                   
        if 'hq_code' in df_stock.columns:        
            df_stock['cFlag']=df_stock['hq_code'].isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]  
        else:
            df_stock['cFlag']=df_stock.index.isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]             
            
        del df_stock200['cFlag'],df_stock['cFlag']   
        
        return df_stock200 
        
    #得到标的代码
    def getCode(self,fname):
        try:
            s1=pd.read_table(fname,usecols=[0],dtype=str,encoding='utf-8')
        except:
            s1=pd.read_table(fname,usecols=[0],dtype=str,encoding='gbk')
        
        s1.columns=['code']
                
        codelist=s1['code'].astype('int')
        
        return codelist 
        
    def plotPowerIndexs(self,index_amopower,enddate):
        
        #获取头以及对应列
        header=list(index_amopower.columns)
        
        datecol=header.index('hq_date')
        
        powercol=header.index('amo_power')
        
        namecol=header.index('hq_code')
        
        ratiocol=header.index('amo_ratio')
        
        #数据之间的间隔
        interval=len(header)+1
        
        wbk =xlsxwriter.Workbook(self.powerIndexDir+enddate+' powerindex.xlsx') 
        
        picSheet=wbk.add_worksheet('pic')
        
        dataSheet=wbk.add_worksheet('data')
        
        codelist=index_amopower['hq_code'].drop_duplicates()
        
        left=0
        
        for code in codelist: 
            
            top=0
            
            amoratio_test=index_amopower[index_amopower.hq_code==code]
            
            dataLen=len(amoratio_test)
            
            dataSheet.write_row(0,left,header)
            
            for i in xrange(dataLen):
                
                dataSheet.write_row(1+top,left,amoratio_test.iloc[i])
                
                top+=1
            
            left+=interval
            
        #画图
        data_top=1
        
        width=800
        
        height=400
        
        for loopindex in xrange(len(codelist)): 
                
            bk_chart=wbk.add_chart({'type': 'line'})
            
            bk_chart.set_style(4)
               
               #向图表添加数据 
            bk_chart.add_series({
                'name':['data', 1, namecol+loopindex*interval],
                'categories':['data', data_top, datecol+loopindex*interval, data_top+dataLen, datecol+loopindex*interval],
                'values':['data', data_top, powercol+loopindex*interval, data_top++dataLen,powercol+loopindex*interval],
                'line':{'color':'#336699'},
                })
            
        
        #   bk_chart.set_title({'name':self.sdate+'-'+self.edate,
        #                       'name_font': {'size': 10, 'bold': True}
        #                       })
                                   
            bk_chart.set_x_axis({#'name':u'日期',
                                    #'name_font': {'size': 10, 'bold': True},
                                    'label_position': 'low',
                                    'interval_unit': 2                          
                                    })
        
            bk_chart.set_size({'width':width,'height':height})     
            
            
            picSheet.insert_chart(0+loopindex*(height/20),0,bk_chart)
        
        wbk.close()    
  
    def plotPowerStocks(self,wbk,date,powerindexs,day_powerstock,day_stock):
        
        PER=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'num_format':'0.00%'})
        
        powerSheet=wbk.add_worksheet(str(date))
  
        powerSheet.write_row(0,0,[u'板块排名',u'效能'])
      
        for i in xrange(len(powerindexs)):
            powerSheet.write_row(i+1,0,powerindexs.iloc[i])
        
        for i in xrange(len(day_stock)):
            if i == 0:
                powerSheet.write_row(0,3,[u'名称',u'板块',u'占比'])                  
            powerSheet.write_row(i+1,3,day_stock.iloc[i],PER)
        
        top=0
        for powerindex in powerindexs['hq_name']:
            
            tmp_powerstock=day_powerstock[day_powerstock.board_name==powerindex]
            
            powerSheet.write_row(top,7,[u'名称',u'板块',u'占比'])
            
            datalen=len(tmp_powerstock)
            
            for i in xrange(datalen):
                
                powerSheet.write_row(top+1+i,7,tmp_powerstock.iloc[i],PER)
            
            top+=datalen+2
               
        return wbk
                
    
    def powerStocks(self,threshold,RF=False):
        
        #得到不属于56个板块的股票代码
        try:
            df_sindex=pd.read_table(self.sindexfile,usecols=[0],dtype=str,encoding='utf-8')
        except:
            df_sindex=pd.read_table(self.sindexfile,usecols=[0],dtype=str,encoding='gbk')
        
        df_sindex.columns=['code']
                
        sindexCodes=df_sindex['code'].astype('int')   
        
        #所有板块数据
        df_index=pd.read_sql('select hq_code,hq_date,hq_amo from hindexquotationday where hq_code < 880500 and hq_date >="'+self.sdate+'" and hq_date <="'+self.edate+'"',con=self.engine)
        
        #获得所有日期
        dates=df_index['hq_date'].drop_duplicates()[-self.window:]
        
            
        #得到所有日线级别的全市场成交额
        df_scaleindex=pd.read_sql('select hq_code,hq_date,hq_amo from hscaleindexquotationday where hq_date >="'+self.sdate+'" and hq_date <="'+self.edate+'"',con=self.engine)
        df_000002=df_scaleindex[df_scaleindex.hq_code==2]
        df_399107=df_scaleindex[df_scaleindex.hq_code==399107]
        marketAmoSeries=(df_000002.reset_index())['hq_amo']+(df_399107.reset_index())['hq_amo']
        
        #筛选掉不属于56个板块的数据
        cflag=df_index['hq_code'].isin(sindexCodes)
        
        df_index=df_index[cflag==False]
        
        #计算强板块
        index_amopower=df_index.groupby('hq_code').apply(lambda x: self.amoPower(x,marketAmoSeries))
        index_amopower=index_amopower.sort_values(['usecount','usepower_avg'],ascending=False)
        
        #关联强板块名称
        indexNames=pd.read_sql_table('boardindexbaseinfo',con=self.engine,schema='stocksystem',index_col='board_code',columns=['board_code','board_name'])  
        index_amopower['hq_name']=self.getIndexNames(index_amopower,indexNames)         
        
        #关联板块数据
        stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        
        #取出该时间段股票数据，并关联板块
        stocksql='select hq_code,hq_date,hq_amo from hstockquotationday_frights where hq_date >="'+self.sdate+'" and hq_date <="'+self.edate+'"'        
        df_stock=pd.read_sql(stocksql,con=self.engine)        
        print stocksql
        #根据日期获得文件名
        if RF == True:
            df_stock=self.getRF(200,df_stock)
            fname=self.powerIndexDir+str(dates.iat[-self.window])+'-'+str(dates.iat[-1])+' RFpowerStock.xlsx'
        
        else:
            fname=self.powerIndexDir+str(dates.iat[-self.window])+'-'+str(dates.iat[-1])+' powerStock.xlsx'          
        
        #补齐股票数据
        df_000002.set_index('hq_date',inplace=True)
        df_stock.set_index('hq_date',inplace=True)
        tradingDays=len(df_000002)
        
        stockGroup=df_stock.groupby('hq_code')
        df_stock=pd.DataFrame()
        
        for code,tmp_stock in stockGroup:
            
            if len(tmp_stock) != tradingDays:
                
                tmp_stock=tmp_stock.reindex(df_000002.index)
                
                tmp_stock.fillna(method='ffill',inplace=True)
                
                tmp_stock.fillna(method='bfill',inplace=True)             
            
            df_stock=df_stock.append(tmp_stock)
            
        df_stock.reset_index(inplace=True)

               
        df_stock['board_name']=self.getStockRelated(df_stock,stockrelated)
        
        #获得股票名称
        stockNames=pd.read_table(self.stocBoardFile,usecols=[0,1],index_col=0,encoding='utf-8')
        
        stockNames.columns=['stock_name']
        
        df_stock['hq_name']=self.getStockNames(df_stock,stockNames)
        
        #计算强票        
        stock_amopower=df_stock.groupby('hq_code').apply(lambda x: self.amoPower(x,marketAmoSeries))
        stock_amopower=stock_amopower[stock_amopower.amo_power>=3]
        stock_amopower['hq_name']=stock_amopower['hq_name'].drop_duplicates()
        stock_amopower.dropna(inplace=True)
        stock_amoratio=stock_amopower.loc[:,['hq_name','board_name','amoratio_avg']].sort_values('amoratio_avg',ascending=False)
        stock_amopower=stock_amopower.loc[:,['hq_name','board_name','usecount','usepower_avg']].sort_values(['usecount','usepower_avg'],ascending=False)[:300]
        
   
        #建立EXCEL
        wbk=xlsxwriter.Workbook(fname)
        PER=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'num_format':'0.00%'})
        
        zqSheet=wbk.add_worksheet(u'周期统计')
        
        use_indexAmoPower=index_amopower[index_amopower.usecount>=1].loc[:,['hq_name','usecount','usepower_avg','amoratio_avg']]
        use_indexAmoPower['hq_name']=use_indexAmoPower['hq_name'].drop_duplicates()
        use_indexAmoPower.dropna(inplace=True)
        
        #写周期数据
          #板块周期数据
        indexHead=[u'板块',u'异动次数',u'异动均值',u'占比均值']
        indexWidth=len(indexHead)
        zqSheet.write_row(0,0,indexHead)
        for i in xrange(len(use_indexAmoPower)):
            zqSheet.write_row(i+1,0,use_indexAmoPower.iloc[i])
        
        stockAmoHead=[u'股票',u'板块',u'异动次数',u'异动均值']
        zqSheet.write_row(0,0+indexWidth+1,stockAmoHead)    
        for i in xrange(len(stock_amopower)):
            zqSheet.write_row(i+1,0+indexWidth+1,stock_amopower.iloc[i])
            
        stockRatioHead=[u'股票',u'板块',u'占比均值']
        stockRatioWidth=len(stockRatioHead)
        zqSheet.write_row(0,0+2*(indexWidth+1),stockRatioHead)    
        for i in xrange(len(stock_amoratio)):
            zqSheet.write_row(i+1,0+2*(indexWidth+1),stock_amoratio.iloc[i],PER)
            
        top=0
        for index in use_indexAmoPower['hq_name']:
            
            df_tmp=stock_amopower[stock_amopower.board_name==index]
            tmpLen=len(df_tmp)
            zqSheet.write_row(top,2*(indexWidth+1)+stockRatioWidth+1,stockAmoHead)
            
            for i in xrange(tmpLen):
                zqSheet.write_row(top+1+i,2*(indexWidth+1)+stockRatioWidth+1,df_tmp.iloc[i])
            
            top+=tmpLen+2
        
     
        #写每日数据
        for i in xrange(self.window):
            
            #提取每日数据
            date=dates.iat[i]                
    
            day_indexpower=index_amopower[index_amopower.hq_date==date]
            
            day_stock=df_stock[df_stock.hq_date==date]                    
            
            day_indexpower['hq_date']=day_indexpower['hq_date'].astype(str)
            
            #按照异动次数，异动均值排序,获得异动板块排名
            day_indexpower.sort_values('amo_power',inplace=True,ascending=False)
                                                              
            powerindexs=day_indexpower.iloc[:5][['hq_name','amo_power']]
               
            #获得每天的市场总成交额
            dayMarketAmo=marketAmoSeries.iat[i-self.window]
            
            #获得股票的成交额占比,按照成交额占比排序
            day_stock['amo_ratio']=day_stock['hq_amo']/dayMarketAmo
            
            day_stock.sort_values('amo_ratio',inplace=True,ascending=False)
            
            #按照板块的排序，取出关联股票进行排名
            day_powerstock=pd.DataFrame()
            for powerindex in powerindexs['hq_name']:
                
                tmp_stock=day_stock[day_stock.board_name==powerindex]
                
                stocknum=len(tmp_stock)
                
                usenum=stocknum/10
                
                if usenum<=5:
                    
                    usenum=5                
                          
                day_powerstock=day_powerstock.append(tmp_stock.iloc[:usenum])
                
                                 
            day_powerstock=day_powerstock.loc[:,['hq_name','board_name','amo_ratio']]
            
            day_stock=day_stock.iloc[:len(day_stock)/10][['hq_name','board_name','amo_ratio']].dropna()
                       
            wbk=self.plotPowerStocks(wbk,date,powerindexs,day_powerstock,day_stock)
            
            print date
                                      
        wbk.close()
        
        return index_amopower,stock_amopower,stock_amoratio
    

if __name__ == '__main__':
    
    a=amoStrategy(sdate='2017-07-01',edate='2017-09-01',window=5)
    
    index_amopower,stock_amopower,stock_amoratio=a.powerStocks(threshold=3,RF=False)
