﻿--
-- Script was generated by Devart dbForge Studio for MySQL, Version 7.1.31.0
-- Product home page: http://www.devart.com/dbforge/mysql/studio
-- Script date 2017/4/18 16:46:27
-- Server version: 5.7.12-log
-- Client version: 4.1
--


-- 
-- Disable foreign keys
-- 
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;

-- 
-- Set SQL mode
-- 
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;

-- 
-- Set character set the client will use to send SQL statements to the server
--
SET NAMES 'utf8';

-- 
-- Set default database
--
USE stocksystem;

--
-- Definition for table boardindexbaseinfo
--
DROP TABLE IF EXISTS boardindexbaseinfo;
CREATE TABLE boardindexbaseinfo (
  board_id FLOAT NOT NULL AUTO_INCREMENT COMMENT '板块ID',
  board_code INT(20) DEFAULT NULL COMMENT '板块代码',
  board_name VARCHAR(30) DEFAULT NULL COMMENT '板块名称',
  board_InMarketDate DATETIME DEFAULT NULL COMMENT '上市时间',
  board_BelongMarket CHAR(10) DEFAULT NULL COMMENT '所属市场 01-上海 02 -深圳 03-通达信',
  PRIMARY KEY (board_id),
  UNIQUE INDEX Bcodeindex (board_code, board_BelongMarket)
)
ENGINE = INNODB
AUTO_INCREMENT = 451
AVG_ROW_LENGTH = 145
CHARACTER SET utf8
COLLATE utf8_general_ci
ROW_FORMAT = DYNAMIC;

--
-- Definition for table boardstock_related
--
DROP TABLE IF EXISTS boardstock_related;
CREATE TABLE boardstock_related (
  rbs_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  board_id INT(11) DEFAULT NULL,
  board_name VARCHAR(30) DEFAULT NULL,
  stock_id INT(11) DEFAULT NULL,
  stock_name VARCHAR(30) DEFAULT NULL,
  PRIMARY KEY (rbs_id),
  UNIQUE INDEX boardquery_Index (rbs_id)
)
ENGINE = INNODB
AUTO_INCREMENT = 18051
AVG_ROW_LENGTH = 89
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数股票关联表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table bzcategory
--
DROP TABLE IF EXISTS bzcategory;
CREATE TABLE bzcategory (
  Bz_Id INT(11) NOT NULL AUTO_INCREMENT COMMENT '板块指数ID',
  Bz_Name CHAR(30) DEFAULT NULL COMMENT '板块指数名称
            ',
  Bz_Code INT(11) DEFAULT NULL COMMENT '板块指数编码（分类编码）',
  Bz_IndexCode INT(11) DEFAULT NULL COMMENT '板块指数代码',
  Bz_lft INT(11) DEFAULT NULL COMMENT '左节点编码',
  Bz_rgt INT(11) DEFAULT NULL COMMENT '右节点编码',
  Bz_pid INT(11) DEFAULT NULL COMMENT '父节点编码',
  Bz_isindex TINYINT(4) DEFAULT NULL COMMENT '是否为指数代码',
  PRIMARY KEY (Bz_Id)
)
ENGINE = INNODB
AUTO_INCREMENT = 462
AVG_ROW_LENGTH = 142
CHARACTER SET utf8
COLLATE utf8_general_ci
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindexquotationday
--
DROP TABLE IF EXISTS hindexquotationday;
CREATE TABLE hindexquotationday (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  `index` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  hq_code INT(11) NOT NULL,
  hq_date DATE DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id, `index`),
  UNIQUE INDEX Iquery_Index (hq_code, `index`)
)
ENGINE = INNODB
AUTO_INCREMENT = 2599
AVG_ROW_LENGTH = 73
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数日线历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindexquotationfifteen
--
DROP TABLE IF EXISTS hindexquotationfifteen;
CREATE TABLE hindexquotationfifteen (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  `index` TIMESTAMP NULL DEFAULT '0000-00-00 00:00:00',
  hq_code INT(11) NOT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, `index`)
)
ENGINE = INNODB
AUTO_INCREMENT = 44167
AVG_ROW_LENGTH = 85
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数15分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindexquotationfive
--
DROP TABLE IF EXISTS hindexquotationfive;
CREATE TABLE hindexquotationfive (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  `index` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  hq_code INT(11) NOT NULL,
  hq_date DATE NOT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, `index`)
)
ENGINE = INNODB
AUTO_INCREMENT = 127303
AVG_ROW_LENGTH = 75
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数5分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindexquotationone
--
DROP TABLE IF EXISTS hindexquotationone;
CREATE TABLE hindexquotationone (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  `index` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  hq_code INT(11) NOT NULL,
  hq_date DATE NOT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, `index`)
)
ENGINE = INNODB
AUTO_INCREMENT = 623521
AVG_ROW_LENGTH = 67
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数1分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindexquotationsixty
--
DROP TABLE IF EXISTS hindexquotationsixty;
CREATE TABLE hindexquotationsixty (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  `index` TIMESTAMP NULL DEFAULT NULL,
  hq_code INT(11) NOT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, `index`)
)
ENGINE = INNODB
AUTO_INCREMENT = 12991
AVG_ROW_LENGTH = 127
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数60分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindexquotationthirty
--
DROP TABLE IF EXISTS hindexquotationthirty;
CREATE TABLE hindexquotationthirty (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  `index` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  hq_code INT(11) NOT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, `index`)
)
ENGINE = INNODB
AUTO_INCREMENT = 23383
AVG_ROW_LENGTH = 70
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数30分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindexquotationweek
--
DROP TABLE IF EXISTS hindexquotationweek;
CREATE TABLE hindexquotationweek (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  `index` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数周线历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hindextick
--
DROP TABLE IF EXISTS hindextick;
CREATE TABLE hindextick (
  htq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  htq_code SMALLINT(6) DEFAULT NULL,
  htq_name INT(11) DEFAULT NULL,
  htq_date DATE DEFAULT NULL,
  htq_time FLOAT DEFAULT NULL,
  htq_price FLOAT DEFAULT NULL,
  htq_amout FLOAT DEFAULT NULL,
  PRIMARY KEY (htq_id),
  UNIQUE INDEX hitquery_Index (htq_code, htq_date, htq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数tick数据'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstcokquotationfifteen
--
DROP TABLE IF EXISTS hstcokquotationfifteen;
CREATE TABLE hstcokquotationfifteen (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  hq_type SMALLINT(6) DEFAULT NULL,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  hq_amount FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date, hq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股15分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstockquotationday
--
DROP TABLE IF EXISTS hstockquotationday;
CREATE TABLE hstockquotationday (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  hq_type SMALLINT(6) DEFAULT NULL,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  hq_amount FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股日线历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstockquotationfive
--
DROP TABLE IF EXISTS hstockquotationfive;
CREATE TABLE hstockquotationfive (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  hq_type SMALLINT(6) DEFAULT NULL,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  hq_amount FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date, hq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股5分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstockquotationone
--
DROP TABLE IF EXISTS hstockquotationone;
CREATE TABLE hstockquotationone (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  hq_type SMALLINT(6) DEFAULT NULL,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  hq_amount FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date, hq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股1分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstockquotationsixty
--
DROP TABLE IF EXISTS hstockquotationsixty;
CREATE TABLE hstockquotationsixty (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  hq_type SMALLINT(6) DEFAULT NULL,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  hq_amount FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date, hq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股60分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstockquotationthirty
--
DROP TABLE IF EXISTS hstockquotationthirty;
CREATE TABLE hstockquotationthirty (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  hq_type SMALLINT(6) DEFAULT NULL,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_time TIME DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  hq_amount FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date, hq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股30分钟历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstockquotationweek
--
DROP TABLE IF EXISTS hstockquotationweek;
CREATE TABLE hstockquotationweek (
  hq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  hq_type SMALLINT(6) DEFAULT NULL,
  hq_code INT(11) DEFAULT NULL,
  hq_date DATE DEFAULT NULL,
  hq_open FLOAT DEFAULT NULL,
  hq_high FLOAT DEFAULT NULL,
  hq_low FLOAT DEFAULT NULL,
  hq_close FLOAT DEFAULT NULL,
  hq_vol FLOAT DEFAULT NULL,
  hq_amount FLOAT DEFAULT NULL,
  PRIMARY KEY (hq_id),
  UNIQUE INDEX Iquery_Index (hq_code, hq_date)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股周线历史数据表'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table hstocktick
--
DROP TABLE IF EXISTS hstocktick;
CREATE TABLE hstocktick (
  htq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  htq_code SMALLINT(6) DEFAULT NULL,
  htq_name INT(11) DEFAULT NULL,
  htq_date DATE DEFAULT NULL,
  htq_time FLOAT DEFAULT NULL,
  htq_price FLOAT DEFAULT NULL,
  htq_amout FLOAT DEFAULT NULL,
  PRIMARY KEY (htq_id),
  UNIQUE INDEX hitquery_Index (htq_code, htq_date, htq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股tick数据'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table rindextick
--
DROP TABLE IF EXISTS rindextick;
CREATE TABLE rindextick (
  htq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  htq_code SMALLINT(6) DEFAULT NULL,
  htq_name INT(11) DEFAULT NULL,
  htq_date DATE DEFAULT NULL,
  htq_time FLOAT DEFAULT NULL,
  htq_price FLOAT DEFAULT NULL,
  htq_amout FLOAT DEFAULT NULL,
  PRIMARY KEY (htq_id),
  UNIQUE INDEX hitquery_Index (htq_code, htq_date, htq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '指数实时tick数据'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table rstocktick
--
DROP TABLE IF EXISTS rstocktick;
CREATE TABLE rstocktick (
  htq_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  htq_code SMALLINT(6) DEFAULT NULL,
  htq_name INT(11) DEFAULT NULL,
  htq_date DATE DEFAULT NULL,
  htq_time FLOAT DEFAULT NULL,
  htq_price FLOAT DEFAULT NULL,
  htq_amout FLOAT DEFAULT NULL,
  PRIMARY KEY (htq_id),
  UNIQUE INDEX hitquery_Index (htq_code, htq_date, htq_time)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '个股实时tick数据'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table stock_weight
--
DROP TABLE IF EXISTS stock_weight;
CREATE TABLE stock_weight (
  sw_id BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '股票权重id',
  stock_id INT(11) DEFAULT NULL COMMENT '股票id',
  stock_name VARCHAR(10) DEFAULT NULL COMMENT '股票名称',
  stock_weight CHAR(10) DEFAULT NULL COMMENT '股票权重',
  PRIMARY KEY (sw_id),
  UNIQUE INDEX swquery_Index (stock_id)
)
ENGINE = INNODB
AUTO_INCREMENT = 2806
AVG_ROW_LENGTH = 70
CHARACTER SET utf8
COLLATE utf8_general_ci
COMMENT = '股票权重表（最新中证全指为标准）'
ROW_FORMAT = DYNAMIC;

--
-- Definition for table stockbaseinfo
--
DROP TABLE IF EXISTS stockbaseinfo;
CREATE TABLE stockbaseinfo (
  stock_id FLOAT NOT NULL AUTO_INCREMENT COMMENT '股票ID',
  stock_code INT(11) DEFAULT NULL COMMENT '股票代码',
  stock_name VARCHAR(30) DEFAULT NULL COMMENT '股票名称',
  stock_InMarketDate DATETIME DEFAULT NULL COMMENT '上市时间',
  stock_BelongMarket CHAR(10) DEFAULT NULL COMMENT '所属市场 01-上海 02 -深圳',
  stock_MarketSymbol CHAR(10) DEFAULT NULL COMMENT '市场符合 上海-SH 深圳- SZ ',
  stock_MarketBlocks CHAR(10) DEFAULT NULL COMMENT '上市板块 上海主板-011 深圳主板-021 中小板-022 创业板-023',
  PRIMARY KEY (stock_id),
  UNIQUE INDEX Scodeindex (stock_code, stock_BelongMarket)
)
ENGINE = INNODB
AUTO_INCREMENT = 3098
AVG_ROW_LENGTH = 95
CHARACTER SET utf8
COLLATE utf8_general_ci
ROW_FORMAT = DYNAMIC;

--
-- Definition for table test
--
DROP TABLE IF EXISTS test;
CREATE TABLE test (
  `index` DATETIME DEFAULT NULL,
  volume DOUBLE DEFAULT NULL,
  ceiling_price DOUBLE DEFAULT NULL,
  opening_price DOUBLE DEFAULT NULL,
  closing_price DOUBLE DEFAULT NULL,
  floor_price DOUBLE DEFAULT NULL,
  INDEX ix_test_index (`index`)
)
ENGINE = INNODB
AVG_ROW_LENGTH = 163
CHARACTER SET utf8
COLLATE utf8_general_ci
ROW_FORMAT = DYNAMIC;

DELIMITER $$

--
-- Definition for procedure sp_boardIndex_bulid
--
DROP PROCEDURE IF EXISTS sp_boardIndex_bulid$$
CREATE DEFINER = 'root'@'localhost'
PROCEDURE sp_boardIndex_bulid(IN spid INT, IN mpid INT, IN flag TINYINT, IN tcontext TEXT)
BEGIN
	#Routine body goes here...

  DECLARE s_pid          int;                  # 父节点

  DECLARE m_pid          int;                  # 细分父节点

  DECLARE p_id           int;                  # 插入节点

  DECLARE fj_flag        int;                  # 分几个层次

  DECLARE t_context      MEDIUMTEXT;           # 分几个层次

  DECLARE len_text       int;                  # 计算文本长度
  
  DECLARE str_pos        int;                  # 计算字符|在文本中的位置

  DECLARE tmp_pos        int;                  # 计算字符|在文本中的位置

  DECLARE str_pos1       int;                  # 计算字符,在文本中的位置

  DECLARE str_pos2       int;                  # 计算字符,在文本中的位置

  DECLARE m_count        int;                  # 分号计数

  DECLARE index_code     varchar(20);          # 通达信指数代码

  DECLARE index_name     varchar(30);          # 通达信指数名称
  
  DECLARE index_flag     varchar(20);          # 通达信指数状态

  DECLARE index_xfjb     varchar(30);          # 通达信指数细分

  DECLARE varchar_txt    varchar(200);         # 单个录入数据文本

  DECLARE varchar_txt1   varchar(200);         # 单个录入数据文本

  DECLARE bkstr          varchar(20);          # 板块市场

  DECLARE bkdate         date;                 # 板块时间

  SET s_pid    = spid;

  SET m_pid    = mpid;

  SET fj_flag  = flag;

  SET t_context = tcontext;
  
  #SET str_pos  = LOCATE ('|',t_contex,1);

  SET str_pos   =  2;

  SET bkdate    =  DATE('2000-01-01');
  
  SET bkstr     = 'SH';


  IF fj_flag=0 THEN   # 如果为0，则处理单级分类
     
       WHILE str_pos!=0 DO
      
          SET tmp_pos =  str_pos;
     
          #初始化读取

          IF str_pos= 2 THEN

              SET str_pos      = LOCATE ('|',t_context,2);
              
              SET varchar_txt  = SUBSTRING(t_context,2,str_pos-2);
      
          ELSE
         
              SET tmp_pos = str_pos + 1 ;
         
              SET str_pos  = LOCATE ('|',t_context,tmp_pos);
  
              IF str_pos!=0 THEN
            
                  SET varchar_txt = SUBSTRING(t_context,tmp_pos,str_pos-tmp_pos);                  
         
              END IF;
           
          END IF;
          
          IF str_pos!=0 THEN
              
              # 单级目录只有代码，名称，2大项
              
              SET str_pos1    = LOCATE (',',varchar_txt,2);
             
              IF str_pos1!=0 THEN

                  SET len_text    = LENGTH(varchar_txt);

                  SET str_pos2    = LOCATE (',',varchar_txt,str_pos1+1);
                         
                  SET index_code  = SUBSTRING(varchar_txt,1,str_pos1-1);
      
                  SET index_name  = SUBSTRING(varchar_txt,str_pos1+1,str_pos2-str_pos1-1);

                  SET p_id = s_pid;

                  #单级目录的入库

             
                  
                  SELECT @myRight:= bz_rgt FROM bzcategory WHERE bz_id = p_id;

                  UPDATE bzcategory SET bz_rgt = bz_rgt +2 WHERE bz_rgt>=@myRight;
      
                  UPDATE bzcategory SET bz_lft = bz_lft +2 WHERE bz_lft>=@myRight;
                     
                  INSERT INTO bzcategory(bz_name,bz_code,bz_indexcode,bz_lft,bz_rgt,bz_pid,bz_isindex)
                         values(index_name,0,index_code,@myRight,@myRight+1,p_id,1);       
                  
                  INSERT INTO boardindexbaseinfo(board_code,board_name,board_InMarketDate,board_BelongMarket)
                         values(index_code,index_name,bkdate,bkstr);
                 

             END IF;


          END IF;


      END WHILE;
     
  ELSE               # 如果不为0，则处理多级分类
     
     WHILE str_pos!=0 DO
      
         SET tmp_pos =  str_pos;

         
          #初始化读取

          IF str_pos= 2 THEN

              SET str_pos      = LOCATE ('|',t_context,2);
              
              SET varchar_txt  = SUBSTRING(t_context,2,str_pos-2);


      
          ELSE
         
              SET tmp_pos = str_pos + 1 ;
         
              SET str_pos  = LOCATE ('|',t_context,tmp_pos);
  
              IF str_pos!=0 THEN
            
                  SET varchar_txt = SUBSTRING(t_context,tmp_pos,str_pos-tmp_pos);                  
         
              END IF;
           
          END IF;
       
         IF str_pos!=0 THEN
              
              # 多级目录至少4项，考虑扩展事情，以后怎么扩展 
              
              SET str_pos1    = 1;

              SET m_count     = 0; 
              
              while str_pos1!=0 Do
                 #初始化读取

                  SET m_count = m_count +1;

                  IF str_pos1= 1 THEN
        
                      SET str_pos1      = LOCATE (',',varchar_txt,2);
                      
                      SET varchar_txt1  = SUBSTRING(varchar_txt,1,str_pos1-1);
                                              
                      SET index_code    = varchar_txt1;

              
                  ELSE
                 
                      SET tmp_pos = str_pos1 + 1 ;
                 
                      SET str_pos1  = LOCATE (',',varchar_txt,tmp_pos);
          
                      IF str_pos1!=0 THEN
                    
                          SET varchar_txt1 = SUBSTRING(varchar_txt,tmp_pos,str_pos1-tmp_pos);
                          
                          IF  m_count=2  THEN
                              
                              SET index_name = varchar_txt1;

                          ELSE 
                            
                              IF m_count=3 THEN
                                 
                                 # 如果不为0，则为1级分类，否则为2级分类
                                 IF varchar_txt1!="0" THEN
                                     
                                    SET p_id = s_pid;

                                 ELSE
                                    
                                    SET p_id = m_pid;
                                    
                                 END IF;
                                 
                   
                              END IF;
                             
                          END IF;
                        
                      END IF;
                   
                  END IF;
                  
                  # 如果为0，则循环完毕，录入

                  IF str_pos1= 0 THEN 
                    
                      #单级目录的入库
                      
                      SELECT @myRight:= bz_rgt FROM bzcategory WHERE bz_id = p_id;
            
                      UPDATE bzcategory SET bz_rgt = bz_rgt +2 WHERE bz_rgt>=@myRight;
          
                      UPDATE bzcategory SET bz_lft = bz_lft +2 WHERE bz_lft>=@myRight;
        
                      INSERT INTO bzcategory(bz_name,bz_code,bz_indexcode,bz_lft,bz_rgt,bz_pid,bz_isindex)
                             values(index_name,0,index_code,@myRight,@myRight+1,p_id,1);

                      
                      INSERT INTO boardindexbaseinfo(board_code,board_name,board_InMarketDate,board_BelongMarket)
                             values(index_code,index_name,bkdate,bkstr);

                  END IF;



              END WHILE;

        END IF;     
  

     END WHILE;

  END IF;

  COMMIT;

  #INSERT INTO bzcategory(
  

END
$$

DELIMITER ;

-- 
-- Restore previous SQL mode
-- 
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;

-- 
-- Enable foreign keys
-- 
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;