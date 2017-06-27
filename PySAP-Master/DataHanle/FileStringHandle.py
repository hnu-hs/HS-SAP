# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 10:17:52 2017

@author: rf_hs
"""

import os
import re
import sys

# 封装文件夹操作

class FileStringHandle:
    
    
    def __init__(self):
        self.flag  = True
        
        reload(sys)
          
        sys.setdefaultencoding('utf-8')
    
        
    def IsSubString(self,SubStrList,Str):
        '''
        #判断字符串Str是否包含序列SubStrList中的每一个子字符串
        #>>>SubStrList=['F','EMS','txt']
        #>>>Str='F06925EMS91.txt'
        #>>>IsSubString(SubStrList,Str)#return True (or False)
        '''
        self.flag=True
        
        for substr in SubStrList:
            if not(substr in Str):
                self.flag=False

        return self.flag
    
    def GetFileList(self,FindPath,FlagStr=[]):
        '''
        #获取目录中指定的文件名
        #>>>FlagStr=['F','EMS','txt'] #要求文件名称中包含这些字符
        #>>>FileList=GetFileList(FindPath,FlagStr) #
        '''
        FileList=[]
    
        FileNames=os.listdir(FindPath)
    
    
        if(len(FileNames)>0):
       
           for fn in FileNames:
               if (len(FlagStr)>0):
                   #返回指定类型的文件名
                   if (self.IsSubString(FlagStr,fn)):
                       fullfilename=os.path.join(FindPath,fn)
                       FileList.append(fullfilename)
                   else:
                       #默认直接返回所有文件名
                       fullfilename=os.path.join(FindPath,fn)
                       FileList.append(fullfilename)
           
        #对文件名排序
        if (len(FileList)>0):
            FileList.sort()

        return FileList

class loop_file:  
    
    def __init__(self, root_dir, short_exclude=[], long_exclude=[], file_extend=[]):          
        self.bkggpth ="\\Data\\History\\BoardIndex\\板块个股组成"        
        self.root_dir = root_dir  
        self.short_exclude = short_exclude  
        self.long_exclude = long_exclude  
        self.file_extend = file_extend  
      
    def __del__(self):  
        pass  
      
    def start(self, func):  
        self.func = func  
        return self.loop_file(self.root_dir)  
      
    def loop_file(self, root_dir):  
        t_sum = []  
        sub_gen = os.listdir(root_dir)  
        for sub in sub_gen:  
            is_exclude = False  
            for extends in self.short_exclude:  ##在不检查文件、目录范围中  
                if extends in sub:              ##包含特定内容  
                    is_exclude = True  
                    break  
                if re.search(extends, sub):     ##匹配指定正则  
                    is_exclude = True  
                    break                      
            if is_exclude:  
                continue              
            abs_path = os.path.join(root_dir, sub)  
            is_exclude = False  
            for exclude in self.long_exclude:  
                if exclude == abs_path[-len(exclude):]:  
                    is_exclude = True  
                    break  
            if is_exclude:  
                continue  
            if os.path.isdir(abs_path):  
                t_sum.extend(self.loop_file(abs_path))  
            elif os.path.isfile(abs_path):              
                if not "." + abs_path.rsplit(".", 1)[1] in self.file_extend:  ##不在后缀名 检查范围中  
                    continue  
                t_sum.append(self.func(abs_path))  
        return t_sum  
        
'''
if '__main__'==__name__:  
    root_dir = "C:\\Users\\rf_hs\\Desktop\\PySAP-Master\\Pycode\\SAP" + "\\Data\\History\\BoardIndex\\板块个股组成"  
    short_exclude = []     ###不包含检查的短目录、文件  
    long_exclude = []                         ###不包含检查的长目录、文件  
    file_extend = ['.txt']                     ###包含检查的文件类型  
    lf = loop_file(root_dir.decode('utf-8'), short_exclude, long_exclude, file_extend)  
    for f in lf.start(lambda f: f):  
        print f  
    m =1
'''