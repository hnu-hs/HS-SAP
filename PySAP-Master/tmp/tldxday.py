# encoding: utf-8 
#yellow = wbk.add_format({'border':1,'align':'center','bg_color':'C0504D','font_size':12,'font_color':'white'})
#sheet.merge_range(0,0,0,10,data,yellow) 
#sheet.write(1,0,'ab')
#cell=xl_rowcol_to_cell(1, 0,row_abs=True, col_abs=True)
#print cell
"""
Created on Wed Apr 12 10:26:17 2017

@author: Administrator
"""
import xlsxwriter 
#from xlsxwriter.utility import xl_rowcol_to_cell
import pandas as pd
import win32com.client 
import datetime

#识别中文的模块
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class picExcel():
    
    def __init__(self):
        #时间样本文件目录
        self.stime=datetime.datetime(2017,5,2)
        #目标生成
        self.filename='E:/test/tldx.xlsx'
        self.timelist=[]
    
    def picModel(self):                  #,codelist,stocklist,indexlist):
        #新建EXCEL
        wbk =xlsxwriter.Workbook(self.filename)  
        #获得工作sheet
        sheet = wbk.add_worksheet('tldx')
        #获得时间样本
        for i in xrange(7):
            day=self.stime.strftime( '%Y-%m-%d' )
            self.timelist.append(day)
            self.stime=self.stime+datetime.timedelta(days=1)

        #设置书写格式
        blue = wbk.add_format({'font_name':'微软雅黑','border':1,'align':'center','bg_color':'515F85','font_size':10,'font_color':'white'})
        red = wbk.add_format({'border':1,'align':'center','bg_color':'C0504D','font_size':12,'font_color':'white'})
        gray = wbk.add_format({'border':1,'align':'center','bg_color':'F8F8F8','font_size':12,'font_color':'red'})  
        small = wbk.add_format({'bg_color':'DADADA','font_size':12})  
        white=wbk.add_format({'font_name':'微软雅黑','align':'center','valign':'vcenter','font_size':12})
        
        #定义写数据的左端和顶端                    
        left=2      
        top=1
        #书写左上角题目
        sheet.write(0,0,'监控',blue)
        #书写框架
        num=0
        #按时间写
        for t in self.timelist:
            #取得time数据  
            #时间序列颜色交替变换，易于区分
            num+=1
            if num%2 == 0:
                sheet.merge_range(0,left,0,left+3,t,gray)
            else:        
                sheet.merge_range(0,left,0,left+3,t,red)
            
            sheet.merge_range(top,left,top,left+1,'特立独行',white)
            sheet.write_row(top+1,left,['股票','指标排名'],white)
                
#            #测试写入一个版块和一个指数的数据
#            if num>1 and num<240:
#                sheet.write(top,left,stocklist[num-1])
#                sheet.write(top+6,left,indexlist[num-1])
#                
            sheet.set_column(left+4,left+4,0.1,small)
            left+=5        
        for i in xrange(5):
            if i == 0:
                data='国证A指'
            else:
                data='板块'            
            sheet.merge_range(top,0,top+9,0,data,white)
            top+=10
        #分隔板块
        sheet.set_row(11,1,small)
        sheet.set_row(22,1,small)
        sheet.set_row(33,1,small)
        sheet.set_row(44,1,small)
        sheet.set_row(55,1,small)
        sheet.set_column(1,1,0.1,small)
        wbk.close()
        

    def update(self):
        #获得EXCEL进程，以便实时更新    
        xlApp = win32com.client.Dispatch('Excel.Application') 
        xlBook = xlApp.Workbooks.Open(self.filename) 
        xlApp.Visible=1
        sht=xlBook.Worksheets(1)
        
        teststock=u'E:\\股票数据\\测试分钟线\\SH600000.txt'
        testindex=u'E:\\股票数据\\测试分钟线\\000808.txt'
        for n in xrange(2):
            if n ==0:
                s=pd.read_table(teststock,header=1,usecols=[1,5],names=['time','close'],dtype=str)
            else:
                s=pd.read_table(testindex,header=1,usecols=[1,5],names=['time','close'],dtype=str)
                
            lens=len(s)
            for i in xrange(lens):
                ntime=str(s.iat[i,0])
                if '9' in ntime:
                    ntime=ntime.strip('0')
                    ntime=ntime[0:1]+':'+ntime[1:3]
                else:    
                    ntime=ntime[0:2]+':'+ntime[2:4]
    
                if ntime in self.timelist:
                    index=self.timelist.index(ntime)
                    left=5*index+3
                    if n ==0:
                        row=2
                    else:
                        row=8
                    sht.Cells(row,left).Value=s.iat[i,1]
                

        
        
#        while 1:
#            #获取当前时间，刷新数据
#            ntime=(time.ctime())[11:-8]  
#            if ntime in self.timelist:
#                index=self.timelist.index(ntime)
#                left=(index)*4+2
#                sht.Cells(2,left).Value=u'测试'
#                print '写入'
#                time.sleep(30)
#            else:
#               print '未更新'
#               time.sleep(30)

def main():
    pic=picExcel()
    pic.picModel()
    pic.update()

if __name__ =='__main__':
    main()
    



