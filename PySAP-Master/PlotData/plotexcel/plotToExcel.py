# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 12:09:46 2017

@author: Administrator
"""

import xlsxwriter

import pandas as pd
import numpy as np

import sys
sys.path.append("..\..")
reload(sys)
sys.setdefaultencoding('utf8')

from DataHanle.MktDataHandle import MktIndexHandle

from datetime import datetime


class PlotToExcel():
    
    #初始化程序
    def __init__(self):
        
        self.mktindex = MktIndexHandle()     
        
        self.filename = 'E:\\RF-SAP\\PySAP-Master\\PlotData\\export\\excel\\test.xlsx'
        
        pass;
          
    #获取股票数据
    def getPlotStockData(self):
        pass;
    
    #获取指数数据
    def getPlotIndexData(self,indexName,startDate,endDate,KlineType):
        
        mktindex =  self.mktindex
        
        mktdf = pd.DataFrame()
        
        if indexName!='':
            mktdf = mktindex.MktIndexBarHistDataGet(indexName,startDate,endDate,KlineType)
        
        return mktdf
    
    #获取涨跌幅
    def getIndexChg(self,idf):
        
        idf_item   = idf.head(1)
        
        idf_tmp    = idf_item['hq_close'].values
        
        idf_close  = idf_tmp[0]
    
        idf['hq_preclose'] = idf['hq_close'].shift(1)
        
        idf['hq_chg']= (idf['hq_close']/idf['hq_preclose'] -1)*100
        
        idf['hq_allchg']= (idf['hq_close']/idf_close -1)*100
        
        idf_ret = idf
        
        return idf_ret
    
    #获取指数相对强弱，相对量能
    def getIndexXdQr(self,bmidf,bkidf,bkdict):
        
        xdidf_ret = pd.DataFrame()
        
        dict_ret  ={}    
        
        #如果板块有数据，计算处理
        
        if len(bkidf)>0:
            #数据分组        
            hkidf_group = bkidf.groupby('hq_code')
            
            #生成排名dict        
            hkidf_dict = dict(list(hkidf_group))
            
            #生成基准指数累计涨跌幅度
            xdhidf   = self.getIndexChg(bmidf)
            
            bmi_len   = len(bmidf)
            
            xdhindex  = xdhidf.index
            
            dictcount = 0 
            
            sortdict = {}
    
            #取出排名指数
            for dfdict in  hkidf_dict:
                
                dictcount  = dictcount +1
                
                hidf_item  = hkidf_dict[dfdict]
                
                tmpidf     = xdhidf.copy()
                
                hidf_ret   = self.getIndexChg(hidf_item)
                
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
                
                if bkdict.has_key(str(dfdict)):
                   hq_name = bkdict[str(dfdict)] 
                
                tmpdf['hq_name']  = hq_name
                
                
                tmpdf.loc[:,['hq_chg']]   = tmpdf['hq_chg']- tmpidf['hq_chg']
                
                tmpdf.loc[:,['hq_allchg']]  = tmpdf['hq_allchg']- tmpidf['hq_allchg']
                
                tmpidf.loc[:,['hq_vol']] = np.where(tmpidf['hq_vol']==0,np.where(tmpdf['hq_vol']==0,-1,-tmpdf['hq_vol']),tmpidf['hq_vol'])
                
                tmpdf['hq_xdvol']  = tmpdf['hq_vol']/tmpidf['hq_vol']
                
                
                if dictcount<=1:
                    xdhead_array = tmpdf.values
                else:
                    xdhead_array = np.concatenate([xdhead_array,tmpdf.values],axis=0)
                
                
                #加入涨跌幅的排名
                
                hichg = hidf_ret['hq_allchg'].tolist()
                
                if len(hichg)>0:
                   #得到最后一个数据 
                   sortdict[dfdict] =  hichg.pop()
                
                    
                
            xdhead_columnus = tmpdf.columns     
           
            xdhead_idf = pd.DataFrame(xdhead_array,columns=xdhead_columnus)
            
            xdidf_ret  = xdhead_idf[['hq_code','hq_name','hq_date','hq_bmcode','hq_bmname','hq_close','hq_preclose','hq_vol','hq_chg','hq_allchg','hq_xdvol']]
            
            #对涨跌幅字典进行排序
            dict_ret= sorted(sortdict.items(), key=lambda d:d[1], reverse = True)
        
        return xdidf_ret,dict_ret
    
    
    #构建指数excel构架，,data_left,pic_lef,data_top,pic_top 分别代表数据，图像的 x，y坐标
    
    def bulidExcelPic(self,bkidf_list,wbk,QR_Sheet,Data_Sheet,xdiColumns,data_left,pic_lef,data_top,pic_top):      
           
        #取出排名指数,写入到excel文件中
           
        for dflist in  bkidf_list:
            
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
               
               #写入头
               Data_Sheet.write_row(data_top, data_left,xdiColumns)
               
               #写入内容
                   
               for row in range(0,bkidf_len):   
                  #for col in range(left,len(fields)+left):  
                  
                  tmplist  = bkidf_item[row:row+1].values.tolist()
                                              
                  datalist = tmplist[0]
                                                  
                  Data_Sheet.write_row(data_top+row+1, data_left,datalist)
               
               
               bk_chart = wbk.add_chart({'type': 'line'})
               bk_chart.set_style(4)
               
               #向图表添加数据 
               bk_chart.add_series({
                'name':[u'指数数据', data_top+1, data_left+1],
                'categories':[u'指数数据', data_top+1, data_left+2, data_top+bkidf_len, data_left+2],
                'values':[u'指数数据', data_top+1, data_left+9, data_top+bkidf_len, data_left+9],
                'line':{'color':'red'},
                        
                })
                
               bk_chart.add_series({
                'name':[u'指数数据', 1, 1],
                'categories':[u'指数数据', 1, 2, bkidf_len, 2],
                'values':[u'指数数据', 1, 3, bkidf_len, 3],
                'line':{'color':'black'},  
                'y2_axis': True,            
                })
                #bold = wbk.add_format({'bold': 1})
                
                
               bk_chart.set_title({'name':bktile,
                                   'name_font': {'size': 10, 'bold': True}
                                   })
                                   
               bk_chart.set_x_axis({'name':u'日期',
                                    'name_font': {'size': 10, 'bold': True},
                                    'label_position': 'low',
                                    'interval_unit': 2
                                
                                    })
                                    
               bk_chart.set_y_axis({'name':'',
                                   'name_font': {'size': 10, 'bold': True}
                                   })
               
               bk_chart.set_y_axis({'name':'',
                                    'name_font': {'size': 10, 'bold': True}
                                    })
                                   
               bk_chart.set_size({'width':770,'height':300})
               
               QR_Sheet.insert_chart( pic_top, pic_lef,bk_chart)
                #bg+=19       
                      
               data_top+=bkidf_len +2
               
               pic_top+=15
           
        return wbk  
        
    # 在excel中插入基准指数的数据，lef，top 分别代表 x，y坐标
        
    def bulidIndexDataToExcel(self,bmi_list,Data_Sheet,bmiColumns,left,top):
        
        #写入指数数据头
        Data_Sheet.write_row(top, left,bmiColumns)
                
        for dflist in  bmi_list:
            
           if len(dflist)==2:
                                        
               bkidf_item  = dflist[1]
               
               bkidf_item['hq_date'] = bkidf_item['hq_date'].astype('str')
               
               bkidf_len   = len(bkidf_item)
               
               #写入头
               Data_Sheet.write_row(top, left,bmiColumns)
               
               #写入内容
                   
               for row in range(0,bkidf_len):   
                  #for col in range(left,len(fields)+left):  
                  
                  tmplist  = bkidf_item[row:row+1].values.tolist()
                                              
                  datalist = tmplist[0]
                                                  
                  Data_Sheet.write_row(top+row+1, left,datalist)
        
        return  Data_Sheet  
        
    #构建指数excel构架    
    def bulidIndexExcelFrame(self,bmidf,xdhead_idf,xdtail_idf):
                
        data_left = 0    #数据起始列
        
        pic_left  = 0    #图像起始列 
        
        date_top  = 0    #数据起始行
        
        pic_top   = 2    #图像起始行
        
        wbk =xlsxwriter.Workbook(self.filename)  
        #newwbk = copy(wbk)
        QR_Sheet   = wbk.add_worksheet(u'指数相对强弱')
    
        Data_Sheet = wbk.add_worksheet(u'指数数据')
        
        #画模块
        headStr='指数强弱排名（涨幅）'
        
        tailStr='指数强弱排名（跌幅）'
        
        
        xdiColumns= list([u'板块代码', u'板块名称', u'日期', u'基准板块代码', u'基准板块名称', u'收盘价', u'前收盘价', u'成交量', u'日相对涨跌幅', u'累计相对涨跌幅', u'相对量比'])
              
        xdiColumnlens = len(xdiColumns)   
        
        red = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'C0504D','font_size':16,'font_color':'white'})
        
        blue = wbk.add_format({'border':4,'align':'center','valign': 'vcenter','bg_color':'8064A2','font_size':16,'font_color':'white'})
        
        QR_Sheet.merge_range(0,0,1,xdiColumnlens,headStr,red) 
        
        QR_Sheet.merge_range(0,xdiColumnlens+2,1,2*xdiColumnlens+2,tailStr,blue)
        
        #间隔格式
        JG = wbk.add_format({'bg_color':'CCC0DA'})
        QR_Sheet.set_column(xdiColumnlens+1,xdiColumnlens+1,0.3,JG)

        #基准数据写入数据sheet中
        if len(bmidf)>0:
            bmidf_group = bmidf.groupby('hq_code')
    
            bmi_list = list(bmidf_group)
            
            bmiColumns = list([u'基准指数代码', u'基准指数名称', u'日期', u'收盘价', u'前收盘价', u'成交量', u'涨跌幅', u'累涨跌幅'])
            
            #未处理多个基准标的比较问题，以及标的指数与板块数据不一致的问题
                  
            Data_Sheet = self.bulidIndexDataToExcel(bmi_list,Data_Sheet,bmiColumns,data_left,date_top)
            
            data_left = data_left +len(bmiColumns) +2
        
        if len(xdhead_idf)>0:
            
            #数据分组        
            xdhead_group = xdhead_idf.groupby('hq_code',sort=False)
            
            bkidf_list= list(xdhead_group)
            
            print bkidf_list
                        
            wbk  = self.bulidExcelPic(bkidf_list,wbk,QR_Sheet,Data_Sheet,xdiColumns,data_left,pic_left,date_top,pic_top)
            
            data_left = data_left+xdiColumnlens+2
            
            pic_left  = pic_left +xdiColumnlens+2
                
        if len(xdtail_idf)>0:            
                       
            #数据分组        
            xdtail_group = xdtail_idf.groupby('hq_code',sort=False)                                           
            #生成dict        
            bkidf_list = list(xdtail_group)
            
            wbk  = self.bulidExcelPic(bkidf_list,wbk,QR_Sheet,Data_Sheet,xdiColumns,data_left,pic_left,date_top,pic_top)
          
        wbk.close()
                  
    #处理指数数据
    def getExcelIndexData(self,bmidf,bkdict):
        
        retdata  = pd.DataFrame()
        
        if len(bmidf)>0:
            
            tmpdata = bmidf[1:]
            
#            retdata = tmpdata[['hq_code','hq_date','hq_close','hq_preclose','hq_vol','hq_chg','hq_allchg']]
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
            retdata = tmpdata[['hq_code','hq_name','hq_date','hq_close','hq_preclose','hq_vol','hq_chg','hq_allchg']]
                
            
        return retdata
        
     # 获取前几，后几排名数据 
    def getSortedIndexdf(self,xd_idf,sortlist,rankingnum):
        
        idf_len = len(sortlist)
        
        headlist = []
        
        taillist = []
        
        bkidf_group = xd_idf.groupby('hq_code')
            
        #生成排名dict        
        bkidf_dict = dict(list(bkidf_group))
        
        xdhead_idf = pd.DataFrame()
        
        xdtail_idf = pd.DataFrame()
        
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
            
           xdhead_idf = xd_idf   
            
        return xdhead_idf,xdtail_idf

        
    def bulidAvg(self,avg,avgChg,period,wbk=0):
        
        def addChart(data_left,bktile,bkidf_len,width,height,xdFlag=0):
         
           data_top=0   
           date_left=15  
           bk_chart = wbk.add_chart({'type': 'line'})   
           
           if xdFlag==0:    
               bk_chart.set_style(4)
               #向图表添加数据 
                   #RF30
               bk_chart.add_series({
                'name':[u'data', 0, 2],
                'categories':[u'data', data_top+1, date_left, data_top+bkidf_len, date_left],
                'values':[u'data', data_top+1, 2, data_top+bkidf_len, 2],
                'line':{'color':'red'},
                })

                    #RF200
               bk_chart.add_series({
                'name':[u'data', 0, 1],
                'categories':[u'data', 1, 2, bkidf_len, 2],
                'values':[u'data', 1, 1, bkidf_len, 1],
                'line':{'color':'yellow'},           
                })
    
                    #所选数据
               bk_chart.add_series({
                'name':[u'data', 0, data_left],
                'categories':[u'data', data_top+1, date_left, data_top+bkidf_len, date_left],
                'values':[u'data', data_top+1, data_left, data_top+bkidf_len, data_left],
                'line':{'color':'blue'},
                })        
           else:
               bk_chart.set_style(49)    
                #所选数据
               if data_left==1:
                   axisFlag=False  
                   linecolor='FF9900'
                   left30=3
               else:
                   axisFlag=False
                   linecolor='333333'
                   left30=6
                                     
                #RF30相对净值
               bk_chart.add_series({
                'name':[u'data', 0, left30],
                'categories':[u'data', data_top+1, date_left, data_top+bkidf_len, date_left],
                'values':[u'data', data_top+1, left30, data_top+bkidf_len, left30],
                'line':{'color':'FF0000'},
                })     
                               
               bk_chart.add_series({
                'name':[u'data', 0, data_left],
                'categories':[u'data', data_top+1, date_left, data_top+bkidf_len, date_left],
                'values':[u'data', data_top+1, data_left, data_top+bkidf_len, data_left],
                'line':{'color':linecolor},
                'y2_axis': axisFlag
                })   
    
                              
           bk_chart.set_title({'name':bktile,
                               'name_font': {'size': 10, 'bold': True}
                               })
                               
           bk_chart.set_x_axis({'name':u'日期',
                                'name_font': {'size': 10, 'bold': True},
                                'label_position': 'low',
                                'interval_unit': 2                           
                                })
                                
           bk_chart.set_y_axis({'name':'',
                               'name_font': {'size': 10, 'bold': True}
                               })
           
           bk_chart.set_size({'width':width,'height':height})  
        
 
           return bk_chart
        
        
        def writecolor(tmplist,top,left,sheet):
            
            for i in xrange(len(tmplist)):
                if tmplist[i]>0:
                    sheet.write(top,left+i,tmplist[i],red)
                elif tmplist[i]<0:
                    sheet.write(top,left+i,tmplist[i],green)
                else:
                    sheet.write(top,left+i,tmplist[i],green)
            return sheet
            
            
        if wbk==0:         
            wbk =xlsxwriter.Workbook('E:\\工作\\报表\\净值\\'+unicode(period)+'avg.xlsx') 
        JZ_Sheet   = wbk.add_worksheet(u'净值曲线')
        Data_Sheet = wbk.add_worksheet(u'data')
        Data_Sheet.hide()
        
        #定义格式
        PER=wbk.add_format({'num_format':'0.00%'})
        red=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'font_color':'red','num_format':'0.00%'})
        green=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'font_color':'green','num_format':'0.00%'})
        blue = wbk.add_format({'font_name':'微软雅黑','border':1,'align':'center','bg_color':'515F85','font_size':11,'font_color':'white'})
        title=wbk.add_format({'font_name':'微软雅黑','align':'center','valign':'vcenter','font_size':10})        
        #Data_Sheet.hide()
        datatop=0
        columns=avg.columns
        dataLen=len(avg)
        #写入头
        Data_Sheet.write_row(0, 1,columns)   
        #写入数据内容         
        for row in xrange(dataLen):   
          
           tmplist  = avg[row:row+1].values.tolist()
                                      
           datalist = tmplist[0]
                                          
           Data_Sheet.write_row(datatop+row+1, 1,datalist,PER) 
        
        #覆盖掉百分比显示的国证A指收盘价
        for row in range(0,len(avg)):                                      
           Data_Sheet.write(datatop+row+1, 8,avg.iat[row,7]) 
           
        #在净值sheet上写统计分析
        #写时间周期
        JZ_Sheet.merge_range(0,0,0,4,period,blue)
        #写分析表格头
        JZ_Sheet.write_row(1,1,['净值变化','相对全市场','相对RF200','相对沪深300'],title)
        JZ_Sheet.set_column(1,4,12)
        JZ_Sheet.write(2,0,'RF30',title)
        JZ_Sheet.write(3,0,'RF200',title)
        JZ_Sheet.write(4,0,'国证A指',title)
        #写入分析数据
        JZ_Sheet=writecolor(avgChg['30'],2,1,JZ_Sheet)
        JZ_Sheet=writecolor(avgChg['200'],3,1,JZ_Sheet)            
        JZ_Sheet=writecolor(avgChg['399317'],4,1,JZ_Sheet) 

        #开始作图
            #净值曲线
        jz_chart1=addChart(16,'净值曲线（对比沪深300）',dataLen,750,320)
        jz_chart2=addChart(8,'净值曲线（对比上证50）',dataLen,750,320)        
        jz_chart3=addChart(11,'净值曲线（对比中证500）',dataLen,750,320)
        jz_chart4=addChart(14,'净值曲线（对比国证A指）',dataLen,750,320)        
        jz_chart5=addChart(12,'净值曲线（对比创业板综）',dataLen,750,320)         
        
        JZ_Sheet.insert_chart(0,6,jz_chart1)
        JZ_Sheet.insert_chart(16,6,jz_chart2)
        JZ_Sheet.insert_chart(32,6,jz_chart3)        
        JZ_Sheet.insert_chart(48,6,jz_chart4)        
        JZ_Sheet.insert_chart(64,6,jz_chart5) 

            #相对强弱图
        xd_chart1=addChart(1,'相对强弱（对比RF200）',dataLen,600,300,1)
        xd_chart2=addChart(14,'相对强弱（对比全市场）',dataLen,600,300,1)            
        JZ_Sheet.insert_chart(0,18,xd_chart1)
        JZ_Sheet.insert_chart(15,18,xd_chart2)          
           
        wbk.close()
           
    def bulidZJ(self,buysort,buysort30,date):
        
        def writercolor(sheet,data,top,left):
            
            if top<8:
                rf=redtop
                gf=greentop
            else:
                rf=red
                gf=green
            if data>0:
                sheet.write(top,left,data,rf)
            elif data<0:
                sheet.write(top,left,data,gf)
            else:
                sheet.write(top,left,data,zwf)                  
            return sheet
            
        wbk =xlsxwriter.Workbook(u'E:\\工作\\报表\\资金流\\'+unicode(date)+'flow.xlsx') 
           
        ZJ_Sheet   = wbk.add_worksheet(u'资金统计') 
        ZJ_Sheet.set_column(0,7,12)
        ZJ_Sheet.set_column(9,16,12)   
        ZJ_Sheet.set_column(8,8,0.5)
        #写格式
          #日期格式
        datef = wbk.add_format({'font_name':'微软雅黑','border':1,'align':'center','font_size':11,'bg_color':'F2F2F2','font_color':'F56E00'})
          #大于零的净值为红色，小于零为绿色
        red=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'font_color':'red','num_format':'0.00'})
        redtop=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'font_color':'red','num_format':'0.00','bg_color':'FFFF66'})
        green=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'font_color':'green','num_format':'0.00'}) 
        greentop=wbk.add_format({'align':'center','valign':'vcenter','font_size':11,'font_color':'green','num_format':'0.00','bg_color':'FFFF66'})
        
          #大标题
        btitleall = wbk.add_format({'border':1,'align':'center','bg_color':'3E7CB6','font_size':12,'font_color':'white'})  
        btitle30  = wbk.add_format({'border':1,'align':'center','bg_color':'FF5050','font_size':12,'font_color':'white'})         
          #小标题
        titlef=wbk.add_format({'font_name':'微软雅黑','align':'center','valign':'vcenter','font_size':11})      
          #正文
        zwf=wbk.add_format({'align':'center','valign':'vcenter','font_size':10,'num_format':'0.00'})
        zwtopf=wbk.add_format({'align':'center','valign':'vcenter','font_size':10,'bg_color':'FFFF66','num_format':'0.00'})        
                          
        #写时间周期
        ZJ_Sheet.merge_range(0,0,0,16,date,datef)   
        #写大标题
        ZJ_Sheet.merge_range(1,0,1,7,'      大买单排名(全市场)         单位(亿)',btitleall)
        ZJ_Sheet.merge_range(1,9,1,16,'     大买单排名(RF30)          单位(亿)',btitle30)
        
        #写小标题
        ZJ_Sheet.write_row(2,0,['股票名称','特大买入','特大卖出','特大单净额','大单买入','大单卖出','大单净额','总净额'],titlef)
        ZJ_Sheet.write_row(2,9,['股票名称','特大买入','特大卖出','特大单净额','大单买入','大单卖出','大单净额','总净额'],titlef)
        #写内容
        for row in xrange(len(buysort)):
            try:
                if row<5:                   
                    ZJ_Sheet.write_row(row+3,0,[buysort.iat[row,0],buysort.iat[row,1],buysort.iat[row,2],'',buysort.iat[row,4],buysort.iat[row,5]],zwtopf)                   
                else:
                    ZJ_Sheet.write_row(row+3,0,[buysort.iat[row,0],buysort.iat[row,1],buysort.iat[row,2],'',buysort.iat[row,4],buysort.iat[row,5]],zwf)                  
            except Exception as e:
                print e
            
            #对于净值为正的，用红色表示，反之用绿色
            try:
                ZJ_Sheet=writercolor(ZJ_Sheet,buysort.iat[row,3],row+3,3)             
                ZJ_Sheet=writercolor(ZJ_Sheet,buysort.iat[row,6],row+3,6)  
                ZJ_Sheet=writercolor(ZJ_Sheet,buysort.iat[row,7],row+3,7)  
            except Exception as e:
                print e
       
        for row in xrange(len(buysort30)):
            try:
                if row<5:                   
                    ZJ_Sheet.write_row(row+3,9,[buysort30.iat[row,0],buysort30.iat[row,1],buysort30.iat[row,2],'',buysort30.iat[row,4],buysort30.iat[row,5]],zwtopf)                    
                else:
                    ZJ_Sheet.write_row(row+3,9,[buysort30.iat[row,0],buysort30.iat[row,1],buysort30.iat[row,2],'',buysort30.iat[row,4],buysort30.iat[row,5]],zwf)                    
            except Exception as e:
                print e            
              
            try:
                ZJ_Sheet=writercolor(ZJ_Sheet,buysort30.iat[row,3],row+3,12)             
                ZJ_Sheet=writercolor(ZJ_Sheet,buysort30.iat[row,6],row+3,15)  
                ZJ_Sheet=writercolor(ZJ_Sheet,buysort30.iat[row,7],row+3,16)  
            except Exception as e:
                print e                  
                
        wbk.close()
    
    
    #excel中plot相对指数强弱图形
    def PlotIndexPicToExcel(self,benchmarkIndex,bkcodestr,startDate,endDate,KlineType,rankingnum,bkdict):
        
        #获取基准指数数据
        bmidf =  self.getPlotIndexData(benchmarkIndex,startDate,endDate,KlineType)
        
        #获取指数排名数据        
        hidf  =  self.getPlotIndexData(bkcodestr,startDate,endDate,KlineType)
      
        #获取所有排名数据 
        (xd_idf,sortlist) = self.getIndexXdQr(bmidf,hidf,bkdict)
        
        #处理excel中的指数数据
        ebmidf  = self.getExcelIndexData(bmidf,bkdict)
             
        # 获取前几，后几排名数据     
        (xdhead_idf,xdtail_idf) =self.getSortedIndexdf(xd_idf,sortlist,rankingnum)
        #区分排名
        
        #画出指数排名图形
        self.bulidIndexExcelFrame(ebmidf,xdhead_idf,xdtail_idf)
        

if '__main__'==__name__:  
    
    pte = PlotToExcel()
    
    benchmarkIndex = '=399317'
    
    headIndexs     = 'IN (880493,880422,880489)'
    benchmarkName  =u'国证A指'
    
    headIndexs     = '880493,880422,880489'
    
    tailIndexs     = 'IN (880474,880423,880464)'
    
    start_date = datetime.strptime("2017-06-9", "%Y-%m-%d")

    end_date = datetime.strptime("2017-05-17", "%Y-%m-%d")


    
    KlineType ='D'
    
    #获取所有板块数据
    mktindex  = pte.mktindex
    
    #获取板块与下属关联股票
    (bkLinedf,bkxfdf) = mktindex.MktIndexToStocksClassify('80201')
    
    bkcodes = bkLinedf['bz_indexcode'].astype('str')
    
    bknames = bkLinedf['bz_name'].astype('str')
    
    #生成dict
    
    bkdicts = bkcodes+','+bknames
    
    bkdictlist = bkdicts.tolist()
    
    bkdict ={}
    
    bkdict[benchmarkIndex]=benchmarkName
    
    for bkdl in bkdictlist:
        bkinfo = bkdl.split(',')
        
        if len(bkinfo)==2:
           bkcode = bkinfo[0]
           
           bkname = bkinfo[1]
        
           bkdict[bkcode] = bkname
        
    bkcodelist = bkcodes.tolist()
    
    bkcodestr = ','.join(bkcodes)
    
    tailIndexs=''
    
    rankingnum = 5
    
    #指数分类对比图
    pte.PlotIndexPicToExcel(benchmarkIndex,bkcodestr,start_date,end_date,KlineType,rankingnum,bkdict)
    
    #指数量级分布图    
    
    
    m = 1
    
    #所有指数强弱，量比图
    
    
    
    
    
    
    
    