# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 14:45:01 2017

@author: rf_hs
"""

from DataHanle.MySql.MySqlDB import MySQL

from DataHanle.BoardHandle import Board

from DataHanle.boardindexhandle import BoardIndex

from DataHanle.stockhandle import StockHandle

from sqlalchemy import create_engine  

#
#import sys
#
#reload(sys)
#          
#sys.setdefaultencoding('utf-8')
#      

#数据库连接参数  
dbconfig = {'host':'localhost', 
            'port': 3306, 
            'user':'root', 
            'passwd':'lzg000', 
            'db':'stocksystem', 
            'charset':'utf8'}   
            
            
engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')

def main():
    ''' 主程序入口 '''
    
    # 初始化数据库,手动处理初始化
    
    initflag = 0
    
    dataInputflag = 0
    
    verifyflag    = 0
    
    makeKlineflag = 0
    
    
    mysqldb     = MySQL(dbconfig)   
    
    
    # 更新板块个股信息    
    
    if initflag:
        
        board  = Board()
       
        # 读取配置文件，手动进行初始化
        # 插入初始化节点
        board.boardLayerInit(mysqldb)
        
        #处理关联文件，沪深A股，处理股票基本信息，板块基本信息，股票股本信息，股票股东信息等
        board.boardStockDeal(mysqldb)
        
        # 插入所有板块节点，板块信息入库
        board.boardLayerInsert(mysqldb)
        
        # 板块指数与个股进行关联 ,个股权重入库
        board.boardStockLink(mysqldb)    
          
    # 历史数据入库，新建一个类进行处理
    if dataInputflag==1:
        
        # 1.读取所有板块，股票tick文件
        # 2.校验tick数据，生成缺失时间
        # 3.针对缺失数据（重新下载，无数据补齐并标识）
        # 4.生成各级别数据并入库 
        # 5.生成各级别数据并入库

        # 读取所有板块起始时间
    
        boardindex = BoardIndex()
        
        bktimedict = {}
        
        bktradingtimedict ={}
        
        bkmisstimedict={}      
        
        intersectionset = set()
                
        # 获取板块时间文件
        
        lf_bktime = boardindex.getBoardIndexFile(boardindex.bktime_path)
        
        #板块交易时间
        
        bktimedict = boardindex.getBoardIndexTime(lf_bktime)
        
        # 板块数据时间与交易时间对比
        
        lf_bktradingtime = boardindex.getBoardIndexFile(boardindex.bkdata_path)
        
        #板块交易时间
        
        bktradingtimedict = boardindex.getBoardIndexTradingTime(lf_bktradingtime)
        
        # 集合差值求出缺少时间
        
            
        filestr = boardindex.missdata_path
            
        filestr = repr(filestr)+"missingfile"+".txt"
            
            
        for dkey in bktimedict:
            
            
            bktset              =  bktimedict[dkey]
                    
            bktradingset        =  bktradingtimedict[dkey]            
            
            # 校验数据，手动补齐
            if verifyflag:
                
                bktset              =  bktimedict[dkey]
                    
                bktradingset        =  bktradingtimedict[dkey]
                    
                intersectionset     =  bktradingset & bktset
                    
                    
                #求交集
                if intersectionset:
                       
                    misset   =  set()
                      
                    misset  = bktset - intersectionset
                       
                    print dkey,'len of misset',len(misset)
                       
                       
                    print dkey,'len of intersectionset',len(intersectionset)
                       
                    # 求差集
                    if misset:
                          
                        bkmisstimedict[dkey] = misset
                        
                        # 打印所有板块缺少数据，手动进行补齐
                        boardindex.getBoardMissdata(dkey,misset)
                          
                intersectionset.clear()
                
        # tick数据处理并生成1，5，15，30，60，日，周 k线
        if makeKlineflag:
        # 读取所有tick文件生成原始数据
        
           #导入分钟数据前初始化数据库
          boardindex.initdatabase(mysqldb)  
              
          boardindex.boardIndexTickHandle(boardindex,bktimedict)

    
    stockhandle = StockHandle() 
#    
    dirlist = []
#    
#    #获取所有股票目录
    capitalfile = stockhandle.getStockFile(stockhandle.sdata_capitalpath)
#    
    stockhandle.getStockCapital(capitalfile,engine)
#    
#    dirlist = stockhandle.getAllStockPath(stockhandle.sdata_hqpath)
##    
#    stockhandle.StockTickHandle(dirlist)
#    
           
    
    m =1     
 
    mysqldb.close()
    
    
if __name__ == '__main__':
    main()
    
    
    
    