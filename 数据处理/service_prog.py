
# coding: utf-8

# In[ ]:



# coding: utf-8

# In[1]:


##
##                       _oo0oo_
##                      o8888888o
##                      88" . "88
##                      (| -_- |)
##                      0\  =  /0
##                    ___/`---'\___
##                  .' \\|     |// '.
##                 / \\|||  :  |||// \
##                / _||||| -:- |||||- \
##               |   | \\\  -  /// |   |
##               | \_|  ''\---/''  |_/ |
##               \  .-\__  '-'  ___/-. /
##             ___'. .'  /--.--\  `. .'___
##          ."" '<  `.___\_<|>_/___.' >' "".
##         | | :  `- \`.;`\ _ /`;.`/ - ` : | |
##         \  \ `_.   \_ __\ /__ _/   .-` /  /
##     =====`-.____`.___ \_____/___.-`___.-'=====
##                       `=---='
##     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##              佛祖保佑 永无BUG 永不修改 
##         本项目已经过开光处理，绝无可能再出现bug! 
# -*- coding: utf-8 -*-
# C:\Users\Administrator\OneDrive python
"this is a demo for data integration、processing and analysis"


# In[2]:


##  导入模块
import os
import re
import pymysql
import datetime
import time
import shutil
import pandas as pd
import numpy as np
import math
import calendar
import threading
import matplotlib
from random import randint
from scipy.signal import savgol_filter
from scipy.fftpack import fft
from scipy import interpolate
import matplotlib.pyplot as plt
import cai

# In[3]:


##  蔡用库
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_
from statsmodels.tsa.arima_model import ARMA
import statsmodels.api as sm


# In[4]:


##  全局变量
host = "localhost"
user = "root"
password = "jndxroot"
#save_dir = "C:/Users/Administrator/Desktop/"
g_log_dir = "D:/Brigge/SDDataWeb/Manager/LogBook/"
g_loss_dir = "D:/Brigge/SDDataWeb/Manager/LossBook/"
#FREQ_REFERENCE = {"fx":500000,"dyb":20000,"jsd":10000,"dqw":500000,"sd":1000000,\
#                 "fs":500000,"wy":1000000}    # 频率对照表
#file_path_name = "C:/Users/Administrator/Desktop/SMJ_DYB-03-01_010000_013402.DY.7z"    # 传入的压缩包文件的绝对路径
# 用于桥面名匹配文件名中桥梁代号的字典
#dictionary01 = {"白露大桥":"BL","文惠桥.新桥":"WHN","文惠桥.老桥":"WHO","壶东大桥":"DH","三门江大桥":"SMJ","鹧鸪江大桥":"ZGJ"}
#dictionary02 = {"bl":"01","whn":"02","who":"03","hd":"04","smj":"05","zgj":"06"}
#dictionary03 = {"BL":"白露大桥","WHN":"文惠桥.新桥","WHO":"文惠桥.老桥","DH":"壶东大桥","SMJ":"三门江大桥","SGJ":"鹧鸪江大桥"}
#dictionary04 = {"dyb":1000,"dqw":999,"jsd":998}
# 用于将数字1,2...转为01,02...的list
#number = [1,2,3,4,5,6,7,8,9]


# In[5]:


##  交互信息类
class Communicate_Flag_Info(object):
    def __init__(self,communicate_flag):
        try:
            self.__FlagID = communicate_flag[0]
            self.__Flag = communicate_flag[1]
            self.__Operate_Content = communicate_flag[2]
            self.__Operator = communicate_flag[3]
        except:
            print("交互信息初始化失败！")
            raise Exception
        else:
            pass
    
    def get_FlagID(self):
        return self.__FlagID
    def get_Flag(self):
        return self.__Flag
    def get_Operate_Content(self):
        return self.__Operate_Content
    def get_Operator(self):
        return self.__Operator
            


# In[6]:


##  桥梁类
class Bridge_Info(object):
    def __init__(self,bus_bridgetype):
        try:
            self.__BridgeNoID = bus_bridgetype[0]
            self.__BridgeTypeID = bus_bridgetype[1]
            self.__BridgeGradeID = bus_bridgetype[2]
            self.__BridgeName = bus_bridgetype[3]
            self.__BridgeCode = bus_bridgetype[4]
            self.__BridgeAddress = bus_bridgetype[5]
            self.__BridgeAge = bus_bridgetype[6]
            self.__NodeNum = bus_bridgetype[7]
            self.__BuildDate = bus_bridgetype[8]
            self.__Company = bus_bridgetype[9]
            self.__RecName = bus_bridgetype[10]
            self.__RecTime = bus_bridgetype[11]
            self.__RecStatus = bus_bridgetype[12]
            self.__Remark = bus_bridgetype[13]
        except:
            print("桥梁配置初始化失败！")
            #return "wrong"
            raise Exception
        else:
            pass
        
    def get_BridgeNoID(self):
        return self.__BridgeNoID
    def get_BridgeName(self):
        return self.__BridgeName
    def get_BridgeCode(self):
        return self.__BridgeCode
    


# In[7]:


##  节点类
class BridgeNode_Info(object):
    def __init__(self,bas_nodeconfig):
        try:
            self.__NodeID = bas_nodeconfig[0]
            self.__BridgeNoID = bas_nodeconfig[1]
            self.__CheckTypeID = bas_nodeconfig[2]
            self.__CheckItemsID = bas_nodeconfig[3]
            self.__SensorTypeID = bas_nodeconfig[4]
            self.__PartsTypeID = bas_nodeconfig[5]
            self.__NodeCodeNo = bas_nodeconfig[6]
            self.__MonitorNo = bas_nodeconfig[7]
            self.__SectionID = bas_nodeconfig[8]
            self.__SectionNo = bas_nodeconfig[9]
            self.__SensorNum = bas_nodeconfig[10]
            self.__SensorNo = bas_nodeconfig[11]
            self.__SensorName = bas_nodeconfig[12]
            self.__ThresholdUpper = bas_nodeconfig[13]
            self.__ThresholdLower = bas_nodeconfig[14]
            self.__AccuracyUpper = bas_nodeconfig[15]
            self.__AccuracyLower = bas_nodeconfig[16]
            self.__Frequency = bas_nodeconfig[17]
            self.__FrequencyUnit = bas_nodeconfig[18]
            self.__FilePath = bas_nodeconfig[19]
            self.__DataFile = bas_nodeconfig[20]
            self.__DataStructure = bas_nodeconfig[21]
            self.__DataUnit = bas_nodeconfig[22]
            self.__PartsUpper = bas_nodeconfig[23]
            self.__PartsLower = bas_nodeconfig[24]
            self.__Explains = bas_nodeconfig[25]
            self.__DefaultField1 = bas_nodeconfig[26]
            self.__DefaultField2 = bas_nodeconfig[27]
            self.__DefaultField3 = bas_nodeconfig[28]
            self.__RecName = bas_nodeconfig[29]
            self.__RecTime = bas_nodeconfig[30]
            self.__RecStatus = bas_nodeconfig[31]
            self.__Remark = bas_nodeconfig[32]
            self.__DesignMax = bas_nodeconfig[33]
            self.__DesignMin = bas_nodeconfig[34]
        except:
            print("节点配置初始化失败！")
            #return "wrong"
            raise Exception
        else:
            pass
    
    def get_NodeId(self):
        return self.__NodeID
    def get_BridgeNoID(self):
        return self.__BridgeNoID
    def get_CheckTypeID(self):
        return self.__CheckTypeID
    def get_CheckItemsID(self):
        return self.__CheckItemsID
    def get_SensorTypeID(self):
        return self.__SensorTypeID
    def get_PartsTypeID(self):
        return self.PartsTypeID
    def get_NodeCodeNo(self):
        return self.__NodeCodeNo.replace("-","_")
    def get_MonitorNo(self):
        return self.__MonitorNo
    def get_SectionNo(self):
        return self.__SectionNo
    def get_SensorNum(self):
        return self.__SensorNum
    def get_SensorNo(self):
        return self.__SensorNo
    def get_ThresholdUpper(self):
        return self.__ThresholdUpper
    def get_ThresholdLower(self):
        return self.__ThresholdLower
    def get_AccuracyUpper(self):
        return self.__AccuracyUpper
    def get_AccuracyLower(self):
        return self.__AccuracyLower
    def get_Frequency(self):
        return 100 * self.__Frequency
    def get_FilePath(self):
        return self.__FilePath
    def get_DataFile(self):
        return self.__DataFile
    def get_DataStructure(self):
        return self.__DataStructure
    def get_DataUnit(self):
        return self.__DataUnit
    def get_PartsUpper(self):
        return self.__PartsUpper
    def get_PartsLower(self):
        return self.__PartsLower
    def get_Explains(self):
        return self.__Explains
    def get_RecName(self):
        return self.__RecName
    def get_RecTime(self):
        return self.__RecTime
    def get_RecStatus(self):
        return self.__RecStatus
    def get_Remark(self):
        return self.__Remark
    


# In[8]:


##  静态文件存放表类
class FileGet_Info(object):
    def __init__(self,bus_fileposition):
        try:
            self.__FileID = bus_fileposition[0]
            self.__BridgeNoID = bus_fileposition[1] 
            self.__NeedTypeID = bus_fileposition[2]
            self.__FileTypeID = bus_fileposition[3]
            self.__FilePath = bus_fileposition[4]
            self.__FileName = bus_fileposition[5]
            self.__RecName = bus_fileposition[6]
            self.__RecTime = bus_fileposition[7]
            self.__RecStatus = bus_fileposition[8]
            self.__Remark = bus_fileposition[9]
        except:
            print("文件存址配置初始化失败！")
            #return "wrong"
            raise Exception
        else:
            pass
    
    def get_FileName(self):
        return self.__FileName
    def get_FilePath(self):
        return self.__FilePath
    def get_BridgeNoID(self):
        return self.__BridgeNoID
    def get_FileTypeID(self):
        return self.__FileTypeID
    


# In[9]:


##  打开数据库
##  输入：mysql基础信息，其中host、user、psd必填，库名、端口、编码可以使用默认值
##  输出：mysql操作对象conn
def connect_mysql(my_host,my_user,my_password,my_database = "sddata",my_port = 3306,my_charset = "utf8"):
    try:
        conn =  pymysql.connect(
            host = my_host,
            user = my_user,
            passwd = my_password,
            db = my_database,
            port = my_port,
            charset= my_charset
            )
        return conn
    except:
        print("数据库连接失败！请核对信息重新连接！")
        #return "wrong"
        raise Exception
    


# In[10]:


##  关闭数据库  
##  输入：mysql操作对象 conn
def close_mysql(conn):
    try:
        conn.commit()
        conn.close()
    except:
        insert_log(conn,"关闭数据库操作对象失败！")
        #return "wrong"
        raise Exception
        


# In[11]:


##  创建操作日志
##  输入：mysql操作对象 conn
def create_log(conn):
    try:
        cur = conn.cursor()
        create_sql = "create table IF NOT EXISTS log_table(ID INT NOT NULL AUTO_INCREMENT COMMENT '序号',record_time DATETIME NOT NULL COMMENT '记录时间',record_comment VARCHAR(1000) COMMENT '记录内容',PRIMARY KEY(ID))ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        raise Exception
    


# In[12]:


##  添加操作日志
##  输入：mysql操作对象conn，日志内容 log_comment，日志记录时间 log_time，表名 log_table
def insert_log(conn, log_comment, log_time = None, log_table = "log_table"):
    try:
        log_time = datetime.datetime.now()
        cur = conn.cursor()
        sql = "insert into " + log_table + " (record_time,record_comment) VALUES (\"%s\",\"%s\")"%(log_time,log_comment)
        cur.execute(sql)
        cur.close()
        conn.commit()
    except:
        print("添加操作日志失败！")
        #return "wrong"
        raise Exception
        


# In[13]:


##  判断数据表是否存在
##  输入：数据库对象conn，表名table_name
##  输出：存在返回True，否则返回False
def table_exist(conn,table_name):
    try:
        cur = conn.cursor()
        sql = "show tables;"
        cur.execute(sql)
        # 将命令运行获取的库中所有表名存在table_list中
        tables = [cur.fetchall()]
        table_list = re.findall('(\'.*?\')',str(tables))
        table_list = [re.sub("'",'',each) for each in table_list]    # 将表名的点引号删掉
        cur.close()    # 关闭命令接口
        conn.commit()    # 提交
    except:
        insert_log(conn,str(table_name) + ":查找数据表出错！")
        #return "wrong"
        raise Exception
    else:
        if table_name in table_list:
            return True
        else:
            return False
        


# In[14]:


##  创建丢包记录表
##  输入：数据库操作对象conn，欲建表名table_name
def create_lost_package_table(conn,table_name):
    try:
        cur = conn.cursor()
        create_sql = "CREATE TABLE IF NOT EXISTS `" + table_name + "` (ID INT unsigned not null AUTO_INCREMENT COMMENT '记序',                     BridgeName_NodeID VARCHAR(100) not null COMMENT '唯一标识', Last_lost_time DATETIME not null COMMENT '上次丢失时间'                    ,Lost_num INT unsigned not null COMMENT '连续丢包数',Alarm_flag INT unsigned not null COMMENT '报警标志',primary key(ID))ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":创建丢包记录表错误！")
        #return "wrong"
        raise Exception
    


# In[15]:


##  从丢包记录表查找记录
##  输入：数据库操作对象 conn，桥梁+节点名 lost_info，表名 table_name=lost_package_table
##  输出：查询结果
def inquire_lost_info(conn,lost_info,table_name = "lost_package_table"):
    try:
        cur = conn.cursor()
        inquire_sql = "SELECT * FROM " + table_name + " WHERE BridgeName_NodeID = \'" + lost_info + "\'" 
        cur.execute(inquire_sql)
        cur.close()
        conn.commit()
        return cur.fetchall()
    except:
        insert_log(conn,str(lost_info) + ":查找丢包记录失败！")
        #return "wrong"
        raise Exception


# In[16]:


##  向丢包记录表插入记录
##  输入：数据库操作对象 conn，桥梁+节点名 lost_info，丢包数 lost_num，标志 lost_flag，表名 table_name
def insert_into_lost_table(conn,lost_info,lost_num,lost_flag,table_name = "lost_package_table"):
    try:
        cur = conn.cursor()
        insert_sql = "INSERT INTO " + table_name + " (BridgeName_NodeID,Last_lost_time,Lost_num,Alarm_flag) VALUES(\"%s\",\"%s\",\"%s\",\"%s\")"                    %(lost_info,datetime.datetime.now(),lost_num,lost_flag)
        cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(lost_info) + ":插入丢包记录失败！")
        #return "wrong"
        raise Exception
    


# In[17]:


##  修改丢包记录表
##  输入：数据库操作对象conn，桥梁+节点名 lost_info，丢包数 lost_num，标志 lost_flag，表名 table_name
def update_lost_table(conn,lost_info,lost_num,lost_flag,table_name = "lost_package_table"):
    try:
        cur = conn.cursor()
        update_sql = "UPDATE " + table_name + " SET Last_lost_time = \'" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") +                     "\',Lost_num = " + str(lost_num) + ",Alarm_flag = "                 + str(lost_flag) + " WHERE BridgeName_NodeID = \'" + lost_info + "\'"
        cur.execute(update_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(lost_info) + ":修改丢包记录失败！")
        #return "wrong"
        raise Exception
    


# In[18]:


##  创建各节点丢包记录表
##  输入：数据库操作对象conn，欲建表名table_name
def create_node_lost_table(conn,table_name):
    try:
        cur = conn.cursor()
        create_sql = "CREATE TABLE IF NOT EXISTS " + table_name + " (ID INT not null AUTO_INCREMENT comment '记序',package_name VARCHAR(100) not null comment '丢包名',            lost_time DATETIME not null comment '丢包时间',primary key(ID))ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        raise Exception
        


# In[19]:


##  向各节点丢包记录表插入记录
##  输入：数据库操作对象 conn，表名 table_name，丢包名称 package_name，丢包时间 lost_time
def insert_into_node_lost_table(conn,table_name,lost_info,package_name):
    try:
        cur = conn.cursor()
        insert_sql = "INSERT INTO " + table_name + " (package_name,lost_time) VALUES(\"%s\",\"%s\")"                    %(package_name,datetime.datetime.now())
        cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(lost_info) + ":该节点丢包信息记录失败！")
        #return "wrong"
        raise Exception
    


# In[20]:


##  创建原始数据数据表
##  输入：数据库操作对象conn，欲建表名table_name datetime没有3
def create_data_table(conn,table_name):
    try:
        cur = conn.cursor()
        # 创建包含ID、Date_Time、Detect_Value、Package_Number的数据表
        create_sql = "CREATE TABLE IF NOT EXISTS `" + table_name + "` (ID INT unsigned not null AUTO_INCREMENT comment '编号',                     Date_Time DATETIME not null comment '检测时间', Detect_Value FLOAT(10,4) comment '值',                    Package_Number INT unsigned not null comment '包号',primary key(ID))ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        raise Exception


# In[21]:


##  向数据表中插入数据
##  输入：数据库操作对象conn，欲插入的表名table_name，插入的数据data，数据起始时间begin_time，该类型传感器采样频率freq，包号package_number
def insert_into_original_table(conn,table_name,data,begin_time,freq,package_number):
    try:
        cur = conn.cursor()
        package_number = str(package_number)
        record_time = begin_time
        timedelta = datetime.timedelta(microseconds = int(freq))
        insert_sql = "INSERT INTO " + table_name + " (Date_Time,Detect_Value,Package_Number) VALUES "
        for d in data[0:-1]:
            insert_data = "(\"" + str(record_time) + "\"," + str(d) + "," + package_number +"),"
            insert_sql += insert_data
            record_time = record_time + timedelta
        insert_sql += "(\"" + str(record_time) + "\"," + str(data[len(data)-1]) + "," + package_number + ");"
        n = cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":插入原始数据失败！")
        #return "wrong"
        raise Exception


# In[22]:


##  创建预处理后数据表
##  输入：数据库操作对象conn，欲建表名table_name datetime没有3
def create_alterdata_table(conn,table_name):
    try:
        cur = conn.cursor()
        create_sql = "CREATE TABLE IF NOT EXISTS `" + table_name + "` (ID INT unsigned not null AUTO_INCREMENT COMMENT '数据ID 自增长主键',                     Date_Time DATETIME not null COMMENT '检测时间',Treat_Value FLOAT(10,4) COMMENT '预处理后的值',                    Package_Number INT unsigned not null COMMENT '所属包编号',primary key(ID))ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":创建预处理后数据表失败！")
        #return "wrong"
        raise Exception
    


# In[23]:


##  向预处理后数据表中插入数据
##  输入：数据库操作对象conn，欲插入的表名table_name，插入的数据data，数据起始时间begin_time，该类型传感器采样频率freq，包号package_number
##        data: pandas.Series类型，只存储数据值
def insert_into_alter_table(conn,table_name,data,begin_time,freq,package_number):
    try:
        cur = conn.cursor()
        package_number = str(package_number)
        record_time = begin_time
        # 将采样频率转换为数据间隔时间
        timedelta = datetime.timedelta(microseconds = int(freq))
        # 一次insert指令插入多个记录
        insert_sql = "INSERT INTO `" + table_name + "` (Date_Time,Treat_Value,Package_Number) VALUES "
        for d in data[0:-1]:
            insert_data = "(\"" + str(record_time) + "\"," + str(d) + "," + package_number +"),"
            insert_sql += insert_data
            record_time = record_time + timedelta
        insert_sql += "(\"" + str(record_time) + "\"," + str(data[len(data)-1]) + "," + package_number + ");"
        num = cur.execute(insert_sql)    # 插入的记录数 如果和len（data）一致则无问题
        cur.close()
        # 提交处理 不然不会实时与数据库同步
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":插入预处理数据失败！")
        #return "wrong"
        raise Exception
    


# In[24]:


##  创建质量分析报告表 一个节点对应一张表
##  输入：数据库操作对象conn，欲建表名table_name
def create_report_table(conn,table_name):
    try:
        cur = conn.cursor()
        create_sql = "CREATE TABLE IF NOT EXISTS `" + table_name + "`(ID INT(10) unsigned not null AUTO_INCREMENT comment '序号',Package_ID INT unsigned not null comment '包编号--主键',Bridge_Name VARCHAR(20) not null comment '桥梁名称',                    Bridge_ID INT(10) not null comment '桥梁编号',Node_Name VARCHAR(20) not null comment '节点名称',Report_Date DATETIME not null comment '报告查询时间',                    count_sum INT unsigned not null comment '数据总量',count_not_null INT unsigned not null comment '非空数据量',                    mean_quality FLOAT(10,4) not null comment '数据均值',var_quality FLOAT(10,4) not null comment '数据方差',                    max_quality FLOAT(10,4) not null comment '数据最大值',min_quality FLOAT(10,4) not null comment '数据最小值',                    at_0_25_percent FLOAT not null comment '分布在前25%的数据量',at_25_50_percent FLOAT not null comment '分布在25~50%数据量',                    at_50_75_percent FLOAT not null comment '分布在50~75%数据量',at_75_100_percent FLOAT not null comment '分布在后25%数据量',positive_num INT unsigned not null comment '正值数量',                    negative_num INT unsigned not null comment '负值数量',positive_percent FLOAT not null comment '正值占比',                    negative_percent FLOAT not null comment '负值占比',abnormal_num INT unsigned not null comment '异常值数量',                    abnormal_percent FLOAT not null comment '异常值占比',null_percent FLOAT not null comment '空值占比',                    fengfengzhi FLOAT(10,4) not null comment '峰峰值',zhongshu FLOAT(10,4) not null comment '众数',                    skewness FLOAT(10,4) not null comment '偏度',overall_quality FLOAT(10,4) not null comment '该包数据总体质量',youxiaozhi FLOAT(10,4) not null comment '有效值',                    piancha FLOAT(10,4) not null comment '标准偏差',center_frequency FLOAT(10,4) comment '中心频率',abnormal_judge INT DEFAULT 0 comment '包异常情况判断',primary key(ID))                    ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":创建质量分析报告失败！")
        #return "wrong"
        raise Exception
    


# In[25]:


##  向报告表插入包报告
##  输入：数据库操作对象 conn，桥名 bridge_name，节点名 node_name，日期 date，
##        统计分析结果 sta_analyse（Series），值分析结果 value_analyse（list），包号 package_number，报告表名 table_name
def insert_into_reporttable(conn,bridge_name,node_name,date,sta_analyse,value_analyse,package_number,table_name):
    try:
        bridge_name = bridge_name.upper()
        cur = conn.cursor()
        insert_sql = "INSERT INTO `" + table_name + "` (Package_ID,Bridge_Name,Bridge_ID,Node_Name,Report_Date,count_sum,count_not_null,            mean_quality,var_quality,max_quality,min_quality,at_0_25_percent,at_25_50_percent,at_50_75_percent,at_75_100_percent,positive_num,negative_num,            positive_percent,negative_percent,abnormal_num,abnormal_percent,null_percent,fengfengzhi,zhongshu,skewness,overall_quality,youxiaozhi,piancha)            VALUES (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',            \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',            \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');"%(package_number,            bridge_name,dictionary02.get(bridge_name),node_name,date,value_analyse[-1],value_analyse[0],sta_analyse[1],            str(float(sta_analyse[2])**2),sta_analyse[7],sta_analyse[3],value_analyse[12],value_analyse[13],            value_analyse[14],value_analyse[15],value_analyse[1],value_analyse[2],value_analyse[3],            value_analyse[4],value_analyse[5],value_analyse[6],value_analyse[-3]/value_analyse[-1],value_analyse[8],            value_analyse[9],value_analyse[10],value_analyse[11],value_analyse[-2],str(sta_analyse[2]))
        n = cur.execute(insert_sql)    # 一次插入一条记录
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(bridge_name) + " " + str(node_name) + ":插入报告表失败！")
        #return "wrong"
        raise Exception
    


# In[26]:


##  向报告表更新中心频率
##  输入：数据库操作对象 conn，表名 table_name，包号 package_number，中心频率 center_freq
def update_centerFreq_into_report_table(conn,table_name,package_number,center_freq):
    try:
        cur = conn.cursor()
        update_sql = "UPDATE `" + table_name + "` SET center_frequency = \'" + str(center_freq) + "\' WHERE Package_ID = \'" + str(package_number) + "\'"
        cur.execute(update_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":插入中心频率失败！")
        raise Exception
        


# In[27]:


##  创建分布直方图绘图数据表
##  输入：数据库操作对象conn，欲建表名table_name
def create_dist_hist_table(conn,table_name = "dist_hist_report"):
    try:
        cur = conn.cursor()
        create_sql = "CREATE TABLE IF NOT EXISTS " + table_name + "(ID INT unsigned not null AUTO_INCREMENT comment '记录自增主键',Bridge_Name VARCHAR(20) not null comment '桥梁名称',                    Bridge_ID INT(10) not null COMMENT '桥梁编号',Node_Name VARCHAR(20) not null comment '节点名称',date_inquery VARCHAR(50) not null comment '该条记录数据对应的日期',                    hist_x VARCHAR(200) not null comment '横坐标的列表',hist_y VARCHAR(200) not null comment '纵坐标的列表',PRIMARY KEY(ID))                    ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,"创建数据分布直方图绘图数据表失败！")
        #return "wrong"
        raise Exception
    


# In[28]:


##  分布直方图绘图数据表中插入数据
##  输入：数据库操作对象conn，桥名bridge_name，节点名node_name，日期date，
##         直方图横坐标列表hist_x，直方图纵坐标列表hist_y，表名table_name
def insert_into_dist_hist(conn,bridge_name,node_name,date,hist_x,hist_y,table_name = "dist_hist_report"):
    try:
        bridge_name = dictionary01.get(bridge_name)
        tmp_x = ""    # 将list转换为str
        tmp_y = ""
        for x in hist_x:
            if tmp_x == "":
                tmp_x = str(x)
            else:
                tmp_x += "," + str(x)
        for y in hist_y:
            if tmp_y == "":
                tmp_y = str(y)
            else:
                tmp_y += "," + str(y)
        cur = conn.cursor()
        insert_sql = "INSERT INTO " + table_name + " (Bridge_Name,Bridge_ID,Node_Name,date_inquery,hist_x,hist_y)VALUES(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');"                %(bridge_name,dictionary02.get(bridge_name),node_name,date,tmp_x,tmp_y)
        cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(bridge_name) + " " + str(node_name) + ":插入数据分布直方图绘图数据失败！")
        #return "wrong"
        raise Exception
    


# In[29]:


##  创建统计直方图绘图数据表
##  输入：数据库操作对象conn，欲建表名table_name
def create_stat_hist_table(conn,table_name = "stat_hist_report"):
    try:
        cur = conn.cursor()
        create_sql = "CREATE TABLE IF NOT EXISTS " + table_name + "(ID INT unsigned not null AUTO_INCREMENT comment '记录自增主键',Bridge_Name VARCHAR(20) not null comment '桥梁名称',Bridge_ID INT(10) not null comment '桥梁编号',                    Node_Name VARCHAR(20) not null comment '节点名称',date_inquery VARCHAR(50) not null comment '该条记录对应的数据日期',                    mean_value FLOAT(10,4) not null comment '均值',std_value FLOAT(10,4) not null comment '标准差',jicha FLOAT(10,4) not null comment '极差',skew FLOAT(10,4)not null comment '偏度'                    ,PRIMARY KEY(ID))ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,"创建统计直方图绘图数据表失败！")
        #return "wrong"
        raise Exception
    


# In[30]:


##  统计直方图绘图数据表中插入数据
##  输入：数据库操作对象conn，桥名bridge_name，节点名node_name，日期date，数据df，统计结果data_info，表名table_name
def insert_into_stat_hist(conn,bridge_name,node_name,date,df,data_info,table_name = "stat_hist_report"):
    try:
        bridge_name = dictionary01.get(bridge_name)
        cur = conn.cursor()
        insert_sql = "INSERT INTO " + table_name + " (Bridge_Name,Bridge_ID,Node_Name,date_inquery,mean_value,std_value,jicha,skew)                VALUES(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');"%(bridge_name,dictionary02.get(bridge_name),node_name,date,data_info[1],data_info[2],data_info[7]-data_info[3],df["value"].skew())
        cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(bridge_name) + " " + str(node_name) + ":插入统计直方图绘图数据失败！")
        #return "wrong"
        raise Exception
    


# In[31]:


##  废弃
##  创建分布饼图绘图数据表
##  输入：数据库操作对象conn，表名table_name
def create_dist_pie_table(conn,table_name = "dist_pie_report"):
    try:
        cur = conn.cursor()
        create_sql = "create table IF NOT EXISTS " + table_name + "(ID INT(100) not null AUTO_INCREMENT comment '记录自增主键',bridge_name varchar(50) not null comment '桥梁名称',bridge_ID INT(10) not null comment '桥梁编号',                node_name varchar(50) not null comment '节点名称',date_inquery varchar(50) not null comment '数据对应日期',a_perc float(10,4) not null comment 'a部分占比',b_perc float(10,4) not null comment 'b部分占比',                c_perc float(10,4) not null comment 'c部分占比',d_perc float(10,4) not null comment 'd部分占比',a_label varchar(50) not null comment 'a部分范围',b_label varchar(50) not null comment 'b部分范围',                c_label varchar(50) not null comment 'c部分范围',d_label varchar(50) not null comment 'd部分范围',primary key(id))ENGINE = InnoDB DEFAULT CHARSET=UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,"创建分布饼图绘图数据表失败！")
        #return "wrong"
        raise Exception
    


# In[32]:


##  废弃
##  分布饼图数据表中插入数据
##  输入：数据库操作对象conn，表名bridge_name,节点名node_name,查询日期date,饼图数据perc,对应标签labels,表名table_name
def insert_into_dist_pie(conn,bridge_name,node_name,date,perc,labels,table_name = "dist_pie_report"):
    try:
        bridge_name = dictionary01.get(bridge_name)
        cur = conn.cursor()
        insert_sql = "insert into " + table_name + "(bridge_name,bridge_id,node_name,date_inquery,a_perc,b_perc,c_perc,d_perc,a_label,b_label,                c_label,d_label)VALUES(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')"                %(bridge_name,dictionary02.get(bridge_name),node_name,date,perc[0],perc[1],perc[2],perc[3],labels[0],labels[1],labels[2],labels[3])
        cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(bridge_name) + " " + str(node_name) + ":插入分布饼图绘图数据失败！")
        #return "wrong"
        raise Exception
    


# In[33]:


##  创建质量分析饼图绘图数据表
##  输入：数据库操作对象conn，表名table_name
def create_qual_pie_table(conn,table_name = "qual_pie_report"):
    try:
        cur = conn.cursor()
        create_sql = "create table IF NOT EXISTS " + table_name + "(ID INT(100) not null AUTO_INCREMENT comment '记录自增主键',bridge_name varchar(50) not null comment '桥梁名称',                bridge_id int(10) not null comment '桥梁编号',node_name varchar(50) not null comment '节点名称',date_inquery varchar(50) not null comment '数据对应日期',                data_num INT(200) not null comment '数据总量',execent_num INT(100) comment '优秀品质数据量',good_num INT(100) comment '良好品质数据量',                soso_num INT(100) comment '一般品质数据量',bad_num INT(100) comment '差品质数据量',worse_num INT(100) comment '糟糕品质数据量',primary key(id))ENGINE = InnoDB DEFAULT CHARSET=UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,"创建质量分析饼图绘图数据表失败！")
        #return "wrong"
        raise Exception
    


# In[34]:


##  质量饼图数据表中插入数据
##  输入：数据库操作对象conn，表名bridge_name,节点名node_name,查询日期date,质量分析结果result,表名table_name
def insert_into_qual_pie(conn,bridge_name,node_name,date,result,table_name = "qual_pie_report"):
    try:
        bridge_name = dictionary01.get(bridge_name)
        cur = conn.cursor()
        insert_sql = "insert into " + table_name + "(bridge_name,bridge_id,node_name,date_inquery,data_num,execent_num,good_num,soso_num,                bad_num,worse_num)VALUES(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')"                %(bridge_name,dictionary02.get(bridge_name),node_name,date,sum(result),result[0],result[1],result[2],result[3],result[4])
        cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(bridge_name) + " " + str(node_name) + ":插入质量分析饼图绘图数据失败！")
        #return "wrong"
        raise Exception
    


# In[35]:


##  创建傅里叶变换数据表
##  输入：数据库操作对象 conn，表名 table_name
def create_fft_table(conn,table_name):
    try:
        cur = conn.cursor()
        create_sql = "create table IF NOT EXISTS " + table_name + "(ID INT(100) not null AUTO_INCREMENT comment '记录自增主键',fft_freq FLOAT(10,6) comment '频率',                fft_value FLOAT(10,4) comment '傅里叶变换值',Package_Number INT unsigned not null comment '所属包编号',primary key(ID))ENGINE = InnoDB DEFAULT CHARSET=UTF8MB4;"
        cur.execute(create_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":创建傅里叶变换数据表失败！")
        #return "wrong"
        raise Exception
    


# In[36]:


##  向傅里叶变换数据表插入数据
##  输入：数据库操作对象 conn，表名 table_name，数据 fft_time，数据开始记录时间 begin_date，采样频率 freq，包名 package_name
def insert_into_fft_table(conn,table_name,fft_data,freq,package_name):
    try:
        i = 0
        # 横坐标变换为频率
        freq = (1000000 / int(freq)) / 2
        x = np.arange(0,len(fft_data)) / len(fft_data) * freq
        cur = conn.cursor()
        insert_sql = "INSERT INTO " + table_name + " (fft_freq,fft_value,Package_Number) VALUES "
        for d in fft_data[0:-1]:
            insert_data = "(\'" + str(x[i]) + "\'," + str(d) + "," + str(package_name) + "),"
            insert_sql += insert_data
            i += 1
        insert_sql += "(\'" + str(x[i]) + "\'," + str(fft_data[len(fft_data)-1]) +"," + str(package_name) +");"
        n = cur.execute(insert_sql)
        cur.close()
        conn.commit()
    except:
        insert_log(conn,str(table_name) + ":插入傅里叶变换数据失败！")
        #return "wrong"
        raise Exception
    


# In[37]:


##  插入文件转存信息表
##
def insert_datafile(host,user,password,table_name,bridge_name,file_name,file_type,file_path):
    try:
        if file_name.split(".")[-1] != "txt":
            file_path_list = file_path.split("/")
            file_path = os.path.join(file_path_list[4],file_path_list[5])
        conn = connect_mysql(host,user,password,"sddata",my_port = 3306,my_charset = "utf8")
        cur = conn.cursor()
        insert_sql = "insert into " + table_name + "(BridgeNoID,FileName,FileType,FilePath,FileDate)VALUES(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')"                %(dictionary02.get(bridge_name),file_name,file_type,file_path,datetime.datetime.now())
        cur.execute(insert_sql)
        cur.close()
        conn.commit()
        close_mysql(conn)
    except:
        insert_log(conn,str(bridge_name) + " " + str(node_name) + ":插入文件转存信息失败！")
        close_mysql(conn)
        #return "wrong"
        raise Exception
        


# In[38]:


##  创建地址文件 
##  输入：目录地址
##  创建成功返回true 已存在返回false
def mkdir_ifnotExist(path):
    try:
        path = path.strip() #删除地址首尾空格
        path = path.rstrip("\\") #保留\\之后的内容 以空格结尾
        isExist = os.path.exists(path)
        if not isExist :
            os.makedirs(path)
            #print("%s did not exist.\nNow is created."%path)
            return True
        else :
            #print("%s has exist!"%path)
            return False
    except:
        print("创建地址路径失败:" + str(path))
        #return "wrong"
        raise Exception
    


# In[39]:


##  移动文件 
##  假设拷贝的文件路径为 盘/文件1/文件2/.../桥名/年/月/日/检测类型/数据文件名（这个就是source_path）
##  复制到的路径为 你指定的文件夹（这个是direct_path）/桥名/年/月/日/检测类型/数据文件名 
##  输入：源文件绝对路径source_path，源文件的本地存放目录direct_path
##  输出：成功 True；源文件不存在 False
def move_file(source_path , direct_path):
    try:
        #print("source_path:",source_path)
        if os.path.exists(source_path):
            [cwd_path,filename] = os.path.split(direct_path)
            mkdir_ifnotExist(direct_path)
            shutil.copy(source_path , direct_path)
            #print("move %s -> %s"%(source_path , direct_path))
            return True
        else :
            print("files not exists")
            return False
    except:
        print("移动文件失败:" + source_path + "-->" + direct_path)
        #return "wrong"
        raise Exception
    


# In[40]:


##  从消息队列获取文件消息，读取、存放
##  输入：7z文件转换txt所需的参数parameter（C:/Users/Administrator/Desktop/SMJ_DYB-03-01_010000_013402.DY.7z）即传入文件的绝对路径
##        解压缩程序绝对路径command
##  输出：txt文件名及其绝对路径
def file_get_from_message(parameter,command = "D:/数据转换小程序/DataTransform.exe"):
    try:
        # command是转换程序exe的绝对路径
        # 首先要进入到转换程序所在目录
        command_dir = command.split("/")
        enter_dir = ""
        for c_dir in command_dir[0:-1]:
            enter_dir += c_dir + "/"
        os.chdir(enter_dir)
        #os.chdir("C:/Users/Administrator/Desktop/数据转换小程序")
        file_name = parameter.split(".")[0]    # 相对路径 + SMJ_DYB-03-01_010000_013402 （.../SMJ_DYB-03-01_010000_013402）
        suffix = parameter.split(".")[1]       # 压缩词缀、传感器类型 DY
        parameter1 = file_name + "." + suffix  # 第二条指令的参数1：第一条命令执行后产生文件的绝对路径 .../SMJ_DYB-03-01_010000_013402.DY
        parameter2 = file_name + ".txt"        # 第二条指令的参数2：数据存放的文件的绝对路径 .../SMJ_DYB-03-01_010000_013402.txt
        # 执行转换程序（参数加上目录组成绝对路径）
        state1 = os.popen(command + " " + parameter + " " + parameter).read()
        print("解压文件第一步完成:",state1)
        state2 = os.popen(command + " " + parameter1 + " " + parameter2).read()
        print("解压文件第二步完成:",state2)
        state3 = os.remove(parameter1)
        print("删除中间文件完成:",state1)
        # 返回文件名SMJ_DYB-03-01_010000_013402.txt和txt绝对路径
        return parameter2.split("/")[-1],parameter2
    except:
        print("解压文件失败！")
        #return "wrong"
        raise Exception
    


# In[41]:


##  将txt放置对应目录下
##  输入：文件名file_name（SMJ_DYB-03-01_010000_013402.txt），日期 date，文件txt的绝对路径source_path，保存根目录save_path（可作默认值）
##  输出：该txt具体保存在本地的目录
def put_file_in(file_name,date,source_path,save_path = "D:/SensorFile"):
    try:
        # 文件信息拆分
        file_type = file_name.split(".")[-1]
        if file_type == "txt":
            txt_flag = 1
            file_info = file_name.split("_")
            bridge_name = dictionary03.get(file_info[0])
            node_name = file_info[1]
            begin_time = file_info[2]
            end_time = file_info[3].split(".")[0]
            # 获取当日日期
            year = date.split("-")[0]
            month = date.split("-")[1]
            day = date.split("-")[2]
            # 对文件类型进行判断
            # txt为传感器数据、doc为其他等
            #if file_info[3].split(".")[1] == "txt":  
            print("bridge_name:",bridge_name,node_name)
            save_path = os.path.join(save_path,bridge_name,year,month,day,node_name)
        else:
            txt_flag = 0
            file_name = file_name.split(".")[0]
            file_info = file_name.split("_")
            bridge_name = file_info[0]
            file_class = file_info[1]
            if file_class == "档案文件":
                save_path = os.path.join("D:/Brigge/SDDataWeb/Manager/DocumentFile/ArchivesFile/")
            elif file_class == "病害文件":
                save_path = os.path.join("D:/Brigge/SDDataWeb/Manager/DocumentFile/DiseaseFile/")
            elif file_class == "案例文件":
                save_path = os.path.join("D:/Brigge/SDDataWeb/Manager/DocumentFile/CaseFile/")
            elif file_class == "巡检文件":
                save_path = os.path.join("D:/Brigge/SDDataWeb/Manager/DocumentFile/CheckFile/")
            elif file_class == "法规文件":
                save_path = os.path.join("D:/Brigge/SDDataWeb/Manager/DocumentFile/StatuteFile/")
            elif file_class == "预案文件":
                save_path = os.path.join("D:/Brigge/SDDataWeb/Manager/DocumentFile/ReservePlanFile/")
            else:
                pass
        # 检查该文件保存目录是否存在，否则建之
        mkdir_ifnotExist(save_path)
        # 移动该文件至指定目录
        move_file(source_path,save_path)
        # 返回该文件当前所在目录
        return save_path,txt_flag
    except:
        insert_log(conn,"剪切文件置对应目录失败:" + source_path + "-->" + save_path)
        #return "wrong"
        raise Exception
    


# In[42]:


##  获取指定时间段内数据
##  输入：桥梁名bridgename，节点detectpoint，起始日期date_begin，终止日期date_end，数据存放根目录local_path
##  输出：DataFrame结构数据raws
def get_specified_data(bridgename,detectpoint,date_begin,date_end,local_path = "D:/SensorFile"):
    try:
        # 将要分析的日期格式化
        date1 = datetime.datetime.strptime(date_begin,"%Y-%m-%d")
        date2 = datetime.datetime.strptime(date_end,"%Y-%m-%d")
        # 根据01字典获取桥梁在文件中的名字
        #bridge_code = dictionary01.get(bridgename)
        bridge_code = bridgename
        # 录入数据库中的时间数据
        input_date = str(date1)[0:10] + "--" + str(date2)[0:10]
        # TMP————文件名与ftp路径
        tmp_filename = str(bridge_code) + "_" + str(detectpoint) + "_"
        tmp_path = os.path.join(local_path,bridge_code)
        # 利用date进行查询日期范围内的迭代 
        date = date1
        first_file_flag = 1
        raws = None
        while date <= date2:
            # 扩展ftp路径
            # 将日期转换为str格式
            year = str(date.year)
            # 将“月”与“日”拓展为“XX”形式
            if date.month in [1,2,3,4,5,6,7,8,9]:
                month = "0" + str(date.month)
            else:
                month = str(date.month)
            if date.day in [1,2,3,4,5,6,7,8,9]:
                day = "0" + str(date.day)
            else:
                day = str(date.day)
            #print(year,month,day)
            source_path = os.path.join(tmp_path,year,month,day,detectpoint)
            #print("source_path:",source_path)
            '''
            # 将日期转换为str格式
            year = str(date.year)
            # 将“月”与“日”拓展为“XX”形式
            if date.month in number:
                month = "0" + str(date.month)
            if date.day in number:
                day = "0" + str(date.day)
            '''
            # 遍历指定目录下所有文件
            for root, dirs, files in os.walk(source_path):
                for file in files: 
                    # 合成最终读取文件的完整地址 读并存入raws中
                    file = os.path.join(source_path,file).replace("\\","/")
                    # 空文件不读
                    if os.path.getsize(file) != 0 :
                        if( first_file_flag == 1 and date == date1):
                            raws = pd.read_csv(file,header = 1,names = ["value","remarks"],engine='python')
                            #print("first",raws['value'].count())
                            first_file_flag = 0
                        else :
                            tmp = pd.read_csv(file,header = 1,names = ["value","remarks"],engine='python')
                            raws = pd.concat([raws,tmp],axis=0,ignore_index=True)
                            #print("\nraws:",raws['value'].count())
                        # 显示预读取的文件名
                        #print(file)
            # 日期 + 1
            date = date + datetime.timedelta(days=1)    
        # raws存储日期范围内的数据 DataFrame结构
        return raws
    except:
        print("获取指定时段内数据失败！")
        #return "wrong"
        raise Exception
    


# In[43]:


##  样条插值函数
##  输入：原始数据 primitive_values
##  输出：样条插值处理后的数据
def insert_value(primitive_values):
    try:
        a = []#空值的序号
        b = []#空值的值
        c = []#非空值的序号
        d = []#非空值的值
        # 找空值
        for i in range(len(primitive_values)):
            if math.isnan(primitive_values[i]):
                a.append(i)
                #b.append(primitive_values[i])
            else:
                c.append(i)
                d.append(primitive_values[i])
        s = pd.Series(d , index = c)
        s = s.sort_values(ascending = True)
        # 建模
        func = interpolate.interp1d(c,d,kind='cubic')
        # 应插入的值
        b = func(a)
        # 补充空值（插值）
        for i in range(len(a)):
            primitive_values[a[i]] = b[i]
        # 返回插值后的结果
        return primitive_values
    except:
        print("样条插值失败，以均值插入！")
        try:
            return primitive_values.fillna(primitive_values.mean())
        except:
            print("均值插值失败！")
            raise Exception
    


# In[44]:


##  平滑数据
##  输入：需平滑的数据data（Series）
##  输出：平滑后结果result（Series）
def filtering_data(data):
    try:
        result = pd.Series(savgol_filter(data,5,2))
        return result
    except:
        print("平滑数据失败！")
        #return "wrong"
        raise Exception
    


# In[45]:


##  统计分析
##  输入：需分析的数据data（Series）
##  输出：统计结果（Series）
def statistics_analyze(data):
    try:
        return pd.to_numeric(data,errors='ignore').describe()
    except:
        print("统计分析失败！")
        #return "wrong"
        raise Exception
    


# In[46]:


##  值分析
##  输入：需要分析的数据df（Series）
##  输出：分析结果（list）
def value_analyze(df):
    try:
        # 数量统计
        null_num = df["value"].isnull().sum()
        not_null_df = df['value'].count()
        positive_df = df[df['value'] > 0].count()['value']
        negative_df = df[df['value'] < 0].count()['value']
        normal_data = df[(df['value'] >= df['value'].describe()[1] - 3 * df['value'].describe()[2]) & (df['value'] <= df['value'].describe()[1] + 3 * df['value'].describe()[2])]
        unnormal_count = not_null_df - normal_data['value'].count()
        # 比重统计
        null_percentage = null_num / (not_null_df + null_num)
        positive_percentage = positive_df / (not_null_df + null_num)
        negative_percentage = negative_df / (not_null_df + null_num)
        unnormal_percentage = unnormal_count / (not_null_df + null_num)
        # 极差、众数、偏度
        max_value = df['value'].max()
        min_value = df['value'].min()
        jicha = max_value - min_value
        zhongshu = df['value'].mode()[0]
        skew = df['value'].skew()
        # 有效值
        youxiaozhi = ((df['value']**2).mean())**0.5
        # 整体数据质量
        overall_ineffect = 1 - (null_percentage + unnormal_percentage + (positive_percentage if positive_percentage < negative_percentage else negative_percentage))
        #overall_ineffect = (max_value - df['value'].mean())/ df['value'].std()
        # 数据分布占比
        interval = (max_value - min_value)/4
        value_quarter = min_value + interval
        value_half = min_value + 2*interval
        value_three_quarter = min_value + 3*interval
        # 各分布块所占比例
        a_df = df[(df['value'] >= min_value)&(df['value'] < value_quarter)]
        a_percentage = a_df['value'].count() / df['value'].count()
        b_df = df[ (df['value'] >= value_quarter)&(df['value'] < value_half)]
        b_percentage = b_df['value'].count() / df['value'].count()
        c_df = df[(df['value'] >= value_half)&(df['value'] < value_three_quarter)]
        c_percentage = c_df['value'].count() / df['value'].count()
        d_df = df[(df['value'] >= value_three_quarter)&(df['value'] <= max_value)]
        d_percentage = d_df['value'].count() / df['value'].count()
        # 返回分析结果列表
        return [not_null_df,positive_df,negative_df,positive_percentage,negative_percentage,unnormal_count,unnormal_percentage,               null_percentage,jicha,zhongshu,skew,overall_ineffect,a_percentage,b_percentage,c_percentage,d_percentage,null_num,               youxiaozhi,(not_null_df + null_num)]
    except:
        print("值分析失败！")
        #return "wrong"
        raise Exception
    


# In[47]:


##  直方图分析 取整
##  输入：数据集df（Series），分组数group_num
##  输出：直方图横坐标的数据列表hist_x，直方图横坐标对应纵坐标的数据列表hist_y
def demo_hist_dist_analyse(df , group_num):
    try:
        # 取极值
        min_floor = math.floor(df['value'].min())
        max_ceil = math.ceil(df['value'].max())
        # 极差
        delta = ( max_ceil - min_floor ) / group_num
        # 储值列表
        hist_count = list()    # 该块数量（y）
        hist_value = list()    # 该块坐标（x）
        hist_value.append(min_floor)
        i = 1    # 记序
        # 储值列表赋初值
        while i <= group_num:
            hist_count.append(0)
            hist_value.append(min_floor+i*delta)
            i+=1
        # 统计
        for tmp_value in df['value']:
            hist_count[ int((math.floor(tmp_value) - min_floor) / delta) ] += 1
        # 返回结果
        return hist_value,hist_count
    except:
        print("直方图分析失败！")
        #return "wrong"
        raise Exception
    


# In[48]:


##  数据分布直方图分析
##  输入：数据集df（Series），分组数group_num
##  输出：直方图横坐标的数据列表hist_x，直方图横坐标对应纵坐标的数据列表hist_y
def hist_dist_analyse(df , group_num):
    try:
        # 取极值
        min_floor = (df['value'].min())
        max_ceil =(df['value'].max())
        # 极差
        delta = ( max_ceil - min_floor ) * 1.001 / group_num    # 考虑到最大值问题 乘上1.001系数
        # 储值列表
        hist_count = list()    # 该块数量（y）
        hist_value = list()    # 该块坐标（x）
        hist_value.append(round(min_floor,4))
        i = 1    # 记序
        # 储值列表赋初值
        while i <= group_num:
            hist_count.append(0)
            hist_value.append(round((min_floor+i*delta),4))
            i+=1
        # 统计
        for tmp_value in df['value']:
            hist_count[ math.floor((tmp_value - min_floor) / delta) ] += 1
        # 返回结果
        return hist_value,hist_count
    except:
        print("数据分布直方图分析失败！")
        #return "wrong"
        raise Exception
    


# In[49]:


##  数据分布饼图
##  输入：数据集df（Series）
##  输出：饼图四块的占比fraces，对应范围标签labels
def distribute_pie_analyse(df):
    try:
        # 数据分布饼图    
        max_value = df['value'].max()
        min_value = df['value'].min()
        interval = (max_value - min_value)/4
        value_quarter = min_value + interval
        value_half = min_value + 2*interval
        value_three_quarter = min_value + 3*interval

        # 传入数据库中的各块标签
        labels = ['A:{:.1f}-{:.1f}'.format(min_value,value_quarter),'B:{:.1f}-{:.1f}'.format(value_quarter,value_half),'C:{:.1f}-{:.1f}'.format(value_half,value_three_quarter),'D:{:.1f}-{:.1f}'.format(value_three_quarter,max_value)]

        # 各数据块所占比例
        a_df = df[(df['value'] >= min_value)&(df['value'] < value_quarter)]
        a_percentage = a_df['value'].count() / df['value'].count()
        b_df = df[ (df['value'] >= value_quarter)&(df['value'] < value_half)]
        b_percentage = b_df['value'].count() / df['value'].count()
        c_df = df[(df['value'] >= value_half)&(df['value'] < value_three_quarter)]
        c_percentage = c_df['value'].count() / df['value'].count()
        d_df = df[(df['value'] >= value_three_quarter)&(df['value'] <= max_value)]
        d_percentage = d_df['value'].count() / df['value'].count()
        # 传入数据库中的各块占比
        fraces = [a_percentage,b_percentage,c_percentage,d_percentage]
        return fraces,labels
    except:
        print("数据分布饼图分析失败！")
        #return "wrong"
        raise Exception
    


# In[50]:


##  数据质量分析饼图
##  输入：数据df、数据均值total_mean、数据标准差total_std，等级划分level_num默认为5
##  输出：各等级数据数
def quality_pie_analyse(df,total_mean,total_std,level_num = 5):
    try:
        # 根据等级划分指标和评价  未做  默认4级
        execent_count = 0    # 优秀数据数
        good_count = 0       # 良好数据数
        soso_count = 0       # 中等数据数
        bad_count = 0        # 差数据数
        worse_count = 0      # 极差数据数
        # 结果列表
        result_list = list()
        # 绘制饼图的数据，呈现数据块占比
        for tmp_value in df['value']:
            # 根据公式进行数据值质量判定
            tmp_level = (tmp_value - total_mean ) / total_std
            if tmp_level < 0.2 :  
                execent_count += 1
            elif tmp_level < 0.4 :
                good_count += 1
            elif tmp_level < 0.5 :
                soso_count += 1
            elif tmp_level < 0.7 :
                bad_count += 1
            else:
                worse_count += 1 
        # 将结果保存进输出列表
        result_list.append(execent_count)
        result_list.append(good_count)
        result_list.append(soso_count)
        result_list.append(bad_count)
        result_list.append(worse_count)
        # 输出结果
        return result_list
    except:
        print("数据质量饼图分析失败！")
        #return "wrong"
        raise Exception
    


# In[51]:


##  将数据报中的空数据置空
##  输入：原始数据（Series）Original_value，空数据标志 need_replace_value
def set_null_value(Original_value, need_replace_value):
    try:
        new_value = Original_value.replace(float(need_replace_value), float("nan"))
        return new_value
    except:
        print("空数据处理失败！")
        #return "wrong"
        raise Exception
    


# In[52]:


##  删除质量极差数据
##  输入：数据df，均值total_mean，标准差total_std，阈值threshold
##  输出：删除后的数据（删除的用'nan'代替），修改的数量
def delte_bad_data(df,total_mean,total_std,threshold = 0.8):
    try:
        nan_record = df['value'].isna()
        for i in range(0,len(df['value'])):
            if (nan_record[i]) == False and (((df['value'].loc[i] - total_mean) / total_std ) >= threshold) :
                df['value'].at[i] = np.nan
        return df
    except:
        print("删除质量极差数据失败！")
        #return "wrong"
        raise Exception
        
    '''
    index = 0
    count = 0
    for i_value in df['value']:
        # 根据公式进行数据值质量判定
        if (( i_value - total_mean ) / total_std ) >= threshold :
            df["value"][index] = np.nan
            count += 1
        index += 1
        print("删除质量极差数据后：",df['value'])
    return df , count
    '''
    


# In[53]:


##  傅里叶变换
##  输入：原始数据 data，采样频率 fre = 100
##  输出：变换后各采样点频率 
def fft_transform(data,fre = 100):
    try:
        fft_data = fft(data)
        fft_data = abs(fft_data)
        fft_data[0] = 0
        return fft_data
    except:
        print("数据傅里叶变换失败！")
        #return "wrong"
        raise Exception


# In[54]:


##  预处理+质量分析操作
##  输入：txt文件绝对路径source_path，文件名file_name
def read_analyze_save_file(source_path , file_name , package_number , old_date , now_date , loss_path , log_path):
    try:
        start_time_1 = time.strftime("%Y-%m-%d %H:%M:%S")
        # 日志文件名
        log_file_name = now_date + "_LogBook.txt"
        log_file_path = os.path.join(log_path,log_file_name).replace("\\","/")
        # 将字母小写
        file_info = file_name.lower().split("_")
        # 表名类：桥_节点
        table_name = file_info[0] + "_" + file_info[1].replace("-","_")
        node_name = table_name.upper()
        # 两个拓展表名
        table_name_original = table_name + "_raw_data"
        table_name_quality_report = table_name + "_quality_report"
        table_name_alter = table_name + "_processed_data"
        # 检测类型
        detect_name = file_info[1].split("-")[0]
        # 起始时间 str
        begin_time = old_date + " "                     + file_info[2][0:2] + ":" + file_info[2][2:4] + ":" + file_info[2][4:6] + ".0"
        # 起始时间 datetime：%Y-%m-%d %H:%M:%S.%f
        begin_time = datetime.datetime.strptime(begin_time,"%Y-%m-%d %H:%M:%S.%f")
        # 读取文件路径
        file_source = os.path.join(source_path,file_name).replace("\\","/")
        # 文件中的数据 remarks为none
        try :
            file_data = pd.read_csv(file_source,header = 1,names = ["value","remarks"],engine='python')
        except :
            file_data = pd.read_csv(file_source,names = ["value","remarks"],engine='python')
        #print("原始数据：",file_data["value"])
        # 判断是否丢包
        # 丢包
        if (file_data.shape[0] == 0):
            #try:
                # 丢包记录txt
                loss_file_name = now_date + "_LossPacket.txt"
                loss_file_path = os.path.join(loss_path,loss_file_name).replace("\\","/")
                loss_time_begin = old_date + " " + file_info[2][0:2] + ":" + file_info[2][2:4] + ":" + file_info[2][4:6]
                loss_time_end = old_date + " " + file_info[3][0:2] + ":" + file_info[3][2:4] + ":" + file_info[3][4:6]
                Write_LossPackage(table_name,loss_file_path,package_number,file_info,node_name,loss_time_begin,loss_time_end)
                conn = connect_mysql(host,user,password)
                # 创建丢包记录表
                if table_exist(conn,"lost_package_table") is not True:
                    create_lost_package_table(conn,"lost_package_table")
                # 查询该节点先前的丢包信息
                package_last_lost_info = inquire_lost_info(conn,table_name)
                # 如果该节点第一次丢失，则初始化该节点记录
                if package_last_lost_info == () :
                    insert_into_lost_table(conn,table_name,1,0)
                # 已存在丢包记录
                else :
                    # 获取该节点之前的丢包记录
                    last_lost_num = package_last_lost_info[0][3]
                    last_lost_flag = package_last_lost_info[0][4]
                    last_lost_time = package_last_lost_info[0][2]
                    # 计算前后丢包事件差
                    delta = datetime.datetime.now() - last_lost_time
                    # 判断续包次数
                    delta_h = delta.total_seconds() // 10.0
                    # 如果续包小于1 即连续丢包
                    if delta_h < 1.0 :
                        # 丢包数+1
                        lost_num = last_lost_num + 1
                        # 如果丢包数大于等于3且报警位不是2（已知，取消报警信息，继续计数）
                        if lost_num >= 3 and last_lost_flag != 2:
                            # 令报警位置1（发出报警信息）
                            lost_flag = 1
                        else:
                            # 保持报警位0（无需报警）
                            lost_flag = last_lost_flag
                    # 如果有续包
                    else :
                        # 如果续包数大于丢包数+1，则丢包记录清空
                        if last_lost_num - delta_h <= -1.0:
                            lost_num = 1
                            lost_flag = 0
                        # 否则 累计丢包数 上一次丢包数 - 中间续包数 + 1（本次丢包）
                        else:
                            lost_num = last_lost_num - delta_h + 1
                            # 如果累计后丢包数小于3 报警位值0
                            if lost_num < 3:
                                lost_flag = 0
                            # 否则 报警位保留
                            else:
                                lost_flag = last_lost_flag
                    # 更新该节点丢包记录
                    update_lost_table(conn,table_name,lost_num,lost_flag)
                # 更新至相应节点丢包表
                package_lost_table = table_name + "_lost"
                #print(package_lost_table)
                if table_exist(conn,package_lost_table) is not True:
                    create_node_lost_table(conn,package_lost_table)
                insert_into_node_lost_table(conn,package_lost_table,table_name,file_name)
                # 关闭与数据库的连接
                close_mysql(conn)         
                end_time_1 = time.strftime("%Y-%m-%d %H:%M:%S")
                Write_LogBook(log_file_path,start_time_1,end_time_1,"丢包处理",file_name,"成功")
                print("丢包处理完成！")
                return "lost package","lost package","lost package","lost package","lost package"
            #except:
            #    end_time_1 = time.strftime("%Y-%m-%d %H:%M:%S")
            #    Write_LogBook(log_file_path,start_time_1,end_time_1,"丢包处理",file_name,"失败")
            #    print("丢包处理失败！")
            #    return "lost package","lost package","lost package","lost package","lost package"
        # 未丢包
        else:
            # 将原始数据取出
            data_value = file_data["value"]
            # 连接数据库
            conn = connect_mysql(host,user,password)
            # 判断该节点源数据表是否存在
            if table_exist(conn,table_name_original) is not True:
                create_data_table(conn,table_name_original)
            # 在对应表中顺序插入最新数据
            insert_into_original_table(conn,table_name_original,data_value,begin_time,FREQ_REFERENCE.get(node_name),package_number)
            # 将包中丢失的数据置 NaN
            data_value = set_null_value(data_value,dictionary04.get(node_name))
            #print("置空后的data_value:",data_value)
            file_data = pd.DataFrame(data_value,columns=["value"])
            #print("置空后的file_data：",file_data)
            # 创建质量分析报告表
            if table_exist(conn,table_name_quality_report) is not True:
                create_report_table(conn,table_name_quality_report)
            # 统计分析
            sta_analyse = statistics_analyze(data_value)
            # 值分析
            value_analyse = value_analyze(file_data)
            # 向报告表中插入最近包的质量分析报告
            insert_into_reporttable(conn,file_info[0],file_info[1],begin_time,sta_analyse,value_analyse,package_number,table_name_quality_report)
            # 删除质量差的数据
            data_after_delte = delte_bad_data(file_data,sta_analyse[1],sta_analyse[2])
            #print("删除质量差的数据后：",data_after_delte)
            #data_after_delte ,delte_num = delte_bad_data(file_data,sta_analyse[1],sta_analyse[2])
            # 3阶B样条曲线插值
            data_after_insert = insert_value(data_after_delte["value"])
            #print("插值后的数据：",data_after_insert)
            # 平滑处理
            data_after_filtering = filtering_data(data_after_insert)
            #print("平滑后的数据：",data_after_filtering)
            # 查询是否创建该节点预处理后的数据表
            if table_exist(conn,table_name_alter) is not True:
                create_alterdata_table(conn,table_name_alter)
            # 将预处理后的数据存入数据库
            insert_into_alter_table(conn,table_name_alter,data_after_filtering,begin_time,FREQ_REFERENCE.get(node_name),package_number)
            # 对于振动传感器数据进行傅里叶变换
            if detect_name == "jsd":
                table_name_fft = table_name + "_fft"
                fft_draw = draw_fft_thread(table_name_fft,node_name,begin_time,data_after_filtering,r"C:/Users/Administrator/Desktop/","C:/Users/Administrator/Desktop/",package_number,table_name_quality_report)
                fft_draw.start()    
            insert_log(conn,str(table_name) + "数据质量分析 & 预处理成功")
            # 写入日志
            end_time_1 = time.strftime("%Y-%m-%d %H:%M:%S")
            Write_LogBook(log_file_path,start_time_1,end_time_1,"质量分析&预处理",file_name,"成功")
            # 关闭数据库连接
            close_mysql(conn)
            return table_name_quality_report,file_info[0],file_info[1].replace("-","_"),data_after_filtering,package_number  
    except:
        insert_log(conn,str(table_name) + ":数据质量分析 & 预处理失败！")
        end_time_1 = time.strftime("%Y-%m-%d %H:%M:%S")
        Write_LogBook(log_file_path,start_time_1,end_time_1,"质量分析&预处理",file_name,"失败")
        #return "wrong"
        raise Exception
    


# In[55]:


##  绘图程序
##  输入：所选桥梁名bridgename，节点名detectpoint，起始日期date_begin，终止日期date_end，数据文件存放根目录local_path，柱状图参数rects
def draw_quality_map(bridgename,detectpoint,date_begin,date_end,local_path = "D:/SensorFile",rects=8):
    try:
        df = get_specified_data(bridgename,detectpoint,date_begin,date_end)
        # 将传感器值数值化
        df['value'] = pd.to_numeric(df['value'],errors='ignore')
        # 得到统计数据
        file_info = df['value'].describe()
        # 给存储日期做判断
        if date_begin != date_end :
            date_begin = date_begin.split("-")[0] + "-" + date_begin.split("-")[1]
        # 连接数据库
        conn = connect_mysql(host,user,password)
        # 统计直方图数据表（只1次）
        if table_exist(conn,"stat_hist_report") is not True:
            create_stat_hist_table(conn,"stat_hist_report")
        # 绘图数据插入数据表
        insert_into_stat_hist(conn,bridgename,detectpoint,date_begin,df,file_info)
        # 统计直方图绘制数据表（只1次）
        if table_exist(conn,"dist_hist_report") is not True:
            create_dist_hist_table(conn,"dist_hist_report")
        # 分布直方图分析
        hist_x , hist_y = hist_dist_analyse(df,rects)
        # 分布直方图绘图数据插入数据表
        insert_into_dist_hist(conn,bridgename,detectpoint,date_begin,hist_x,hist_y)
        # 质量分析饼图数据表（只1次）
        if table_exist(conn,"qual_pie_report") is not True:
            create_qual_pie_table(conn,"qual_pie_report")
        # 质量分析饼图分析
        quality_result = quality_pie_analyse(df,file_info[1],file_info[2])
        # 绘图数据插入数据表
        insert_into_qual_pie(conn,bridgename,detectpoint,date_begin,quality_result)
        '''
        # 数据分布饼图数据表（只1次）
        if table_exist(conn,"dist_pie_report") is not True:
            create_dist_pie_table(conn,"dist_pie_report")
        # 数据分布饼图分析
        pie_perc,pie_lab = distribute_pie_analyse(df)
        # 绘图数据插入数据表
        insert_into_dist_pie(conn,bridgename,detectpoint,date_begin,pie_perc,pie_lab)
        '''
        insert_log(conn,str(bridgename) + " " + str(detectpoint) + ":绘图成功！")
        close_mysql(conn)
    except:
        insert_log(conn,str(bridgename) + " " + str(detectpoint) + ":绘图失败！")
        #return "wrong"
        raise Exception
    


# In[56]:


##  主函数 程序逻辑 后台一直运作
def data_processing(file_path_name,old_date,now_date,loss_path,log_path):
    #try:
        # 包标识号+1
        global package_number
        package_number += randint(1,99999)
        log_file_name = now_date + "_LogBook.txt"
        log_file_path = os.path.join(log_path,log_file_name).replace("\\","/")
        start_time_02 = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            # 从消息机制获取7z转换为txt
            if file_path_name.split(".")[-1] == "7z":
                # 解压
                detect_file_name ,read_path = file_get_from_message(file_path_name)
                # 将txt保存到本地树型目录
                tree_path,txt_flag = put_file_in(detect_file_name,old_date,read_path)
            else:
                detect_file_name= file_path_name.split("/")[-1]
                # 将txt保存到本地树型目录
                tree_path,txt_flag = put_file_in(detect_file_name,old_date,file_path_name)
            tree_path = tree_path.replace("\\","/")
            end_time_02 = time.strftime("%Y-%m-%d %H:%M:%S")
            if detect_file_name.split(".")[-1] == "txt":
                bridge_name = detect_file_name.split("_")[0]
            else:
                bridge_name = dictionary01.get(detect_file_name.split("_")[0])
            #print("tree_path:",tree_path)
            #print("detect_file_name:",detect_file_name)
            #print("bridge_name:",bridge_name)
            insert_datafile(host,user,password,"bus_datafile",bridge_name,detect_file_name,0,tree_path)
            Write_LogBook(log_file_path,start_time_02,end_time_02,"文件整合",detect_file_name,"成功")
        except:
            Write_LogBook(log_file_path,start_time_02,end_time_02,"文件整合",detect_file_name,"失败")
            raise Exception
        # 分析目录下指定txt数据，并存入mysql两表
        if txt_flag == 1:
            table_name,bridge_name,node_name,data_after_filtering,package_name = read_analyze_save_file(tree_path,detect_file_name,package_number,old_date,now_date,loss_path,log_path)
            # 空包判断
            if table_name == "lost package":
                # 写日志 停止后续操作
                conn = connect_mysql(host,user,password,my_database = "sddata",my_port = 3306,my_charset = "utf8")
                insert_log(conn,str(detect_file_name) + " " + str(package_number) + "lost!")
                close_mysql(conn)
            else :
                # 异常检测
                try:
                    cai.jiance(data_after_filtering,bridge_name,node_name,package_name,table_name) 
                except:
                    print('异常检测模块出错！')
                    raise Exception
    #except:
    #    print("数据处理主程序运行出错！")
        #return "wrong"
    #    raise Exception
    


# In[57]:


##  读取模块状态标志信息
##  输入：数据库基本信息 my_host,my_user,my_password,my_database，交互标志表明 table_name
##  输出：交互标志列表 flag_info_list
def load_flag_info(my_host,my_user,my_password,my_database,table_name):
    try:
        conn = connect_mysql(my_host,my_user,my_password,my_database,my_port = 3306,my_charset = "utf8")
        cur = conn.cursor()
        sql = "select * from " + table_name + ";"
        cur.execute(sql)
        results = cur.fetchall()
        flag_info_list = []
        row_num = 0
        for row_data in results:
            flag_info_list.append("")
            flag_info_list[row_num] = Communicate_Flag_Info(row_data)
            row_num += 1
        cur.close()
        conn.commit()
        close_mysql(conn)
        return flag_info_list
    except:
        print("读取下发模块节点信息失败！")
        #return "wrong"
        raise Exception


# In[58]:


##  根据下发模块加载桥梁信息
##  输入：数据库基本信息 my_host,my_user,my_password,my_database，下发表名 table_name
##  输出：桥梁类列表 bridge_info_list
def load_bridge_info(my_host,my_user,my_password,my_database,table_name):
    try:
        conn = connect_mysql(my_host,my_user,my_password,my_database,my_port = 3306,my_charset = "utf8")
        cur = conn.cursor()
        sql = "select * from " + table_name + ";"
        cur.execute(sql)
        results = cur.fetchall()
        bridge_info_list = []
        row_num = 0
        for row_data in results:
            bridge_info_list.append("")
            bridge_info_list[row_num] = Bridge_Info(row_data)
            row_num += 1
        cur.close()
        conn.commit()
        close_mysql(conn)
        return bridge_info_list
    except:
        print("读取下发模块桥梁信息失败！")
        #return "wrong"
        raise Exception
        


# In[59]:


##  根据下发模块加载节点信息
##  输入：数据库基本信息 my_host,my_user,my_password,my_database，下发表名 table_name
##  输出：节点类列表 node_info_list
def load_node_info(my_host,my_user,my_password,my_database,table_name):
    try:
        conn = connect_mysql(my_host,my_user,my_password,my_database,my_port = 3306,my_charset = "utf8")
        cur = conn.cursor()
        sql = "select * from " + table_name + ";"
        cur.execute(sql)
        results = cur.fetchall()
        node_info_list = []
        row_num = 0
        for row_data in results:
            node_info_list.append("")
            node_info_list[row_num] = BridgeNode_Info(row_data)
            row_num += 1
        cur.close()
        conn.commit()
        close_mysql(conn)
        return node_info_list
    except:
        print("读取下发模块节点信息失败！")
        #return "wrong"
        raise Exception


# In[60]:


def load_file_info(my_host,my_user,my_password,my_database,table_name):
    try:
        conn = connect_mysql(my_host,my_user,my_password,my_database,my_port = 3306,my_charset = "utf8")
        cur = conn.cursor()
        sql = "select * from " + table_name + ";"
        cur.execute(sql)
        results = cur.fetchall()
        node_info_list = []
        row_num = 0
        for row_data in results:
            node_info_list.append("")
            node_info_list[row_num] = FileGet_Info(row_data)
            row_num += 1
        cur.close()
        conn.commit()
        close_mysql(conn)
        return node_info_list
    except:
        print("读取下发模块节点信息失败！")
        #return "wrong"
        raise Exception


# In[61]:


##  每日质量分析绘图线程
class draw_daily_report_thread(threading.Thread):   #继承父类threading.Thread
    def __init__(self,bridgename,nodedname,date1,date2,rects,path):
        try:
            threading.Thread.__init__(self)
            self.bridgename = bridgename
            self.nodename = nodedname
            self.date1 = date1
            self.date2 = date2
            self.rects = rects
            self.logpath = path
        except:
            print("每日质量分析绘图线程初始化失败！")
            #return "wrong"
            raise Exception
        
    def run(self):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数 
        try:
            log_file_name = str(self.date2) + "_LogBook.txt"
            log_file_path = os.path.join(self.logpath,log_file_name).replace("\\","/")
            start_time_03 = time.strftime("%Y-%m-%d %H:%M:%S")
            print("Start drawing yesterday's report :",self.bridgename,"-",self.nodename,"-",self.date1,)
            draw_quality_map(self.bridgename,self.nodename,self.date1,self.date2,self.rects)
            print("Drawing down day!")
            end_time_03 = time.strftime("%Y-%m-%d %H:%M:%S")
            Write_LogBook(log_file_path,start_time_03,end_time_03,self.bridgename+self.nodename+"日质量分析绘图",str(self.date2),"成功")
        except:
            print("每日质量分析绘图失败:",self.bridgename+"-"+self.nodename)
            end_time_03 = time.strftime("%Y-%m-%d %H:%M:%S")
            Write_LogBook(log_file_path,start_time_03,end_time_03,self.bridgename+self.nodename+"日质量分析绘图",str(self.date2),"失败")
            raise Exception
            


# In[62]:


##  每月质量分析绘图线程
class draw_monthly_report_thread(threading.Thread):   #继承父类threading.Thread
    def __init__(self,bridgename,nodedname,date1,date2,rects,path):
        try:
            threading.Thread.__init__(self)
            self.bridgename = bridgename
            self.nodename = nodedname
            self.date1 = date1
            self.date2 = date2
            self.rects = rects
            self.logpath = path
        except:
            print("每月质量分析绘图线程初始化失败！")
            #return "wrong"
            raise Exception
        
    def run(self):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数 
        try:
            log_file_name = str(self.date2) + "_LogBook.txt"
            log_file_path = os.path.join(self.logpath,log_file_name).replace("\\","/")
            month_num = self.date2.split("-")[0] + "-" + self.date2.split("-")[1]
            start_time_04 = time.strftime("%Y-%m-%d %H:%M:%S")
            print("Start drawing last month's report :",self.bridgename,"-",self.nodename,"-",self.date1,"-",self.date2)
            draw_quality_map(self.bridgename,self.nodename,self.date1,self.date2,self.rects)
            print("Drawing down month!")
            end_time_04 = time.strftime("%Y-%m-%d %H:%M:%S")
            Write_LogBook(log_file_path,start_time_04,end_time_04,self.bridgename+self.nodename+"每月质量分析绘图",month_num,"成功")
        except:
            print("每月质量分析绘图失败！")
            end_time_04 = time.strftime("%Y-%m-%d %H:%M:%S")
            Write_LogBook(log_file_path,start_time_04,end_time_04,self.bridgename+self.nodename+"每月质量分析绘图",month_num,"失败")
            raise Exception
            


# In[63]:


##  振动传感器傅里叶变换绘图线程
class draw_fft_thread(threading.Thread):
    def __init__(self,table_name,detect_name,begin_time,data_after_filtering,save_path_base,log_path,package_number,report_table):
        try:
            threading.Thread.__init__(self)
            self.table_name = table_name
            self.detect_name = detect_name
            self.begin_time = begin_time
            self.data_after_filtering = data_after_filtering
            self.save_path_base = save_path_base
            self.logpath = log_path
            self.package_number = package_number
            self.report_table = report_table
        except:
            print("傅里叶变换绘图线程初始化失败！")
            #return "wrong"
            raise Exception
        
    def run(self):
        try:
            print("Start analysing data and do FFT :")
            start_time_06 = time.strftime("%Y-%m-%d %H:%M:%S")
            now_date = time.strftime("%Y-%m-%d")
            log_file_name = str(now_date) + "_LogBook.txt"
            log_file_path = os.path.join(self.logpath,log_file_name).replace("\\","/")
            conn = connect_mysql(host,user,password)
            # fft处理
            data_after_fft = fft_transform(self.data_after_filtering)
            #print("傅里叶变换后的数据：",data_after_fft)
            # 将傅里叶变换后的数据存入数据库
            table_name_fft = self.table_name + "_data"
            if table_exist(conn,table_name_fft) is not True:
                create_fft_table(conn,table_name_fft)
            insert_into_fft_table(conn,table_name_fft,data_after_fft[0:len(data_after_fft)//2],FREQ_REFERENCE.get(self.detect_name),self.package_number)
            '''
            # 绘图
            # 横坐标变换为频率
            data_after_fft = data_after_fft[0:len(data_after_fft)//2]
            freq = (1000000 / int(FREQ_REFERENCE.get(self.detect_name))) / 2
            x = np.arange(0,len(data_after_fft)) / len(data_after_fft) * freq
            max_index = round(np.argmax(data_after_fft)* freq / len(data_after_fft) , 4 )
            max_value = round(max(data_after_fft) , 2 )
            font = matplotlib.font_manager.FontProperties(fname=r"C:\Windows\Fonts\simsun.ttc",size=12)
            plt.rcParams['savefig.dpi'] = 120 #图片像素
            #plt.rcParams['figure.dpi'] = 300 #分辨率
            #plt.plot(max_index,max_value,'*b')
            show_max = '[' + str(max_index) + ',' + str(max_value) + ']'
            plt.annotate(show_max, xytext=(max_index,max_value), xy=(max_index, max_value))
            plt.plot(x[:],data_after_fft[:])
            plt.xlabel("频率/Hz",fontproperties=font)
            #plt.ylabel("")
            plt.title('FFT of Mixed wave at ' + str(self.begin_time) + " in 1 hour",fontsize=7,color='#7A378B')  #注意这里的颜色可以查询颜色代码表
            save_path = self.save_path_base + self.table_name + ".jpg"
            plt.savefig(save_path)
            #plt.show()
            update_centerFreq_into_report_table(conn,self.report_table,self.package_number,max_index)
            insert_log(conn,str(self.table_name) + ":傅里叶变换绘图成功！")
            close_mysql(conn)
            end_time_06 = time.strftime("%Y-%m-%d %H:%M:%S")
            Write_LogBook(log_file_path,start_time_06,end_time_06,"傅里叶变换&绘图",self.detect_name,"成功")
            '''
            print("Drawing down fft!")
        except:
            insert_log(conn,str(self.table_name) + ":傅里叶变换绘图失败！")
            end_time_06 = time.strftime("%Y-%m-%d %H:%M:%S")
            Write_LogBook(log_file_path,start_time_06,end_time_06,"傅里叶变换&绘图",self.detect_name,"失败")
            #return "wrong"
            raise Exception
        


# In[64]:


##  记录日志txt 逗号
##  输入：文件绝对路径 file_path,开始时间 start_time,结束时间 end_time,操作 comment,文件名, filename,状态 status
def Write_LogBook(file_path,start_time,end_time,comment,filename,status):
    try:
        with open(file_path,"a+",encoding='utf-8') as f:
            if os.path.getsize(file_path) == 0:
                f.write("起始时间" + " \t" + "结束时间" + " \t" + "记录事件" + " \t" + "操作信息" + "\t\t" + "状态"  + "\t\t" + "操作人IP\t\t备用" + "\n")        
                f.write(start_time + "|" + end_time + "|" + comment + "|" + filename + "|" + status + "|\n")
            else:
                f.write(start_time + "|" + end_time + "|" + comment + "|" + filename + "|" + status + "|\n")
    except:
        print("写入日志失败！")
        raise Exception
        


# In[65]:


##  记录丢包信息txt
##  输入：丢包包名 table_name，文件写地址 loss_file_path，包号 package_number，文件信息file_info，节点名 node_nmae，丢包记录始末时间 loss_time_begin/end
def Write_LossPackage(table_name,loss_file_path,package_number,file_info,node_name,loss_time_begin,loss_time_end):
    #try:
        with open(loss_file_path,"a+",encoding='utf-8') as f:
            if os.path.getsize(loss_file_path) == 0:
                f.write("包号\t桥梁ID/名称\t节点ID\t\t测点+面+传感器ID 开始时间 结束时间 丢包\t备用\n")
                f.write(str(package_number) + "|\t" + dictionary03.get(file_info[0].upper()) + "|\t" + node_name + "|\t" + file_info[1] + "|\t" + loss_time_begin + "|\t" + loss_time_end + "|\t" + "确认丢包|\n")
            else:
                f.write(str(package_number) + "|\t" + dictionary03.get(file_info[0].upper()) + "|\t" + node_name + "|\t" + file_info[1] + "|\t" + loss_time_begin + "|\t" + loss_time_end + "|\t" + "确认丢包|\n")
            print("该包信息丢失！丢包信息请参看lost_package_table:",table_name)
    #except:
    #    print("写入每日丢包记录失败！")
    #    raise Exception
        


# In[66]:


##  监听程序
if __name__ == "__main__":
    # 初始化工作
    package_number = 27                        # 包初始标识号
    # 建立基础库
    conn = connect_mysql(host,user,password)
    if table_exist(conn,"log_table") is not True:
        create_log(conn)
    if table_exist(conn,"stat_hist_report") is not True:
        create_stat_hist_table(conn,"stat_hist_report")
    if table_exist(conn,"qual_pie_report") is not True:
        create_qual_pie_table(conn,"qual_pie_report")
    if table_exist(conn,"dist_hist_report") is not True:
        create_dist_hist_table(conn,"dist_hist_report")
    close_mysql(conn)
    # 读取下发模块桥梁信息
    # dictionary01 = {"白露大桥":"BL","文惠桥.新桥":"WHN","文惠桥.老桥":"WHO","壶东大桥":"DH","三门江大桥":"SMJ","鹧鸪江大桥":"ZGJ"}
    # dictionary02 = {"BL":"01","WHN":"02","WHO":"03","HD":"04","SMJ":"05","ZGJ":"06"}
    # dictionary03 = {"BL":"白露大桥","WHN":"文惠桥.新桥","WHO":"文惠桥.老桥","DH":"壶东大桥","SMJ":"三门江大桥","SGJ":"鹧鸪江大桥"}
    bridge_info_list = load_bridge_info(host,user,password,'sddata',"bus_bridgeuser")
    dictionary01 = {}
    dictionary02 = {}
    dictionary03 = {}
    for bridge_info in bridge_info_list:
        dictionary01[bridge_info.get_BridgeName()] = bridge_info.get_BridgeCode()
        dictionary02[bridge_info.get_BridgeCode()] = bridge_info.get_BridgeNoID()
        dictionary03[bridge_info.get_BridgeCode()] = bridge_info.get_BridgeName() 
    # 读取下发模块节点信息
    # FREQ_REFERENCE = {"SMJ_DYB_03_01":20000,"SMJ_JSD_03_01":10000}        # 频率对照表
    # dictionary04 = {"SMJ_DYB_03_01":1000,"SMJ_JSD_03_01":2000}            # 空值对照表
    node_info_list = load_node_info(host,user,password,'sddata',"bus_nodeconfig")
    #print(dictionary02.get("SMJ"))
    FREQ_REFERENCE = {}
    dictionary04 = {}
    for node_info in node_info_list:
        FREQ_REFERENCE[node_info.get_NodeCodeNo()] = node_info.get_Frequency()
        dictionary04[node_info.get_NodeCodeNo()] = node_info.get_ThresholdUpper()
    '''
    for key,value in dictionary04.items():
        print(key,value)
    for key,value in FREQ_REFERENCE.items():
        print("dsa",key,value)
    '''
    # 读取文件读取地址等信息
    file_info_list = load_file_info(host,user,password,'sddata',"bus_fileposition")
    FILE_GET_PATH = {}
    FILE_TYPE_DICT = {}
    for file_info in file_info_list:
        FILE_GET_PATH[file_info.get_FileTypeID()] = file_info.get_FilePath()
    '''
    for key,value in FILE_GET_PATH.items():
        print("dsa",key,value)
    '''
    # 当有新消息传入
    test_flag = 1
    # 初始化old_date
    old_date = time.strftime("%Y-%m-%d")
    while( test_flag == 1 ):
        try:
            print("The program is interrupted !")
            # 随机获取测试文件
            raw_file_path = "D:/YunDataFile/TestFile/"
            pathDir = os.listdir(raw_file_path)
            random_file_name = pathDir[randint(0,len(pathDir)-1)]
            print(random_file_name)
            # 每一次获取消息，判断当前日期
            now_date = time.strftime("%Y-%m-%d")
            '''
            # 测试用
            now_date = "2019-04-25"
            old_date = "2019-04-24"
            '''
            log_name = now_date + "_LogBook.txt"
            # 读取各模块标志位
            try:
                start_time_08 = time.strftime("%Y-%m-%d %H:%M:%S")
                flag_info_list = load_flag_info(host,user,password,'sddata','communicate_flag')
                end_time_08 = time.strftime("%Y-%m-%d %H:%M:%S")
                Write_LogBook(g_log_dir + log_name,start_time_08,end_time_08,"读取交互标志","communicate_flag","成功")
            except:
                end_time_08 = time.strftime("%Y-%m-%d %H:%M:%S")
                Write_LogBook(g_log_dir + log_name,start_time_08,end_time_08,"读取交互标志","communicate_flag","失败")                         
            
            # 更新读取下发模块桥梁信息  标志未定
            if (flag_info_list[0].get_Flag() != 0):
                try:
                    start_time_09 = time.strftime("%Y-%m-%d %H:%M:%S")
                    bridge_info_list = load_bridge_info(host,user,password,'sddata',"bus_bridgeuser")
                    dictionary01 = {}
                    dictionary02 = {}
                    dictionary03 = {}
                    for bridge_info in bridge_info_list:
                        dictionary01[bridge_info.get_BridgeName()] = bridge_info.get_BridgeCode()
                        dictionary02[bridge_info.get_BridgeCode()] = bridge_info.get_BridgeNoID()
                        dictionary03[bridge_info.get_BridgeCode()] = bridge_info.get_BridgeName() 
                    end_time_09 = time.strftime("%Y-%m-%d %H:%M:%S")
                    Write_LogBook(g_log_dir + log_name,start_time_09,end_time_09,"重新读取桥梁信息","bas_bridge","成功")
                except:
                    end_time_09 = time.strftime("%Y-%m-%d %H:%M:%S")
                    Write_LogBook(g_log_dir + log_name,start_time_09,end_time_09,"重新读取桥梁信息","bas_bridge","失败")
                    
            # 更新读取下发模块节点信息  标志未定
            if (flag_info_list[1].get_Flag() != 0):
                try:
                    start_time_10 = time.strftime("%Y-%m-%d %H:%M:%S")
                    node_info_list = load_node_info(host,user,password,'sddata',"bus_nodeconfig")
                    FREQ_REFERENCE = {}
                    dictionary04 = {}
                    for node_info in node_info_list:
                        FREQ_REFERENCE[node_info.get_NodeCodeNo()] = node_info.get_Frequency()
                        dictionary04[node_info.get_NodeCodeNo()] = node_info.get_ThresholdUpper()
                    end_time_10 = time.strftime("%Y-%m-%d %H:%M:%S")
                    Write_LogBook(g_log_dir + log_name,start_time_10,end_time_10,"重新读取节点信息","bas_bridge","成功")
                except:
                    end_time_10 = time.strftime("%Y-%m-%d %H:%M:%S")
                    Write_LogBook(g_log_dir + log_name,start_time_10,end_time_10,"重新读取节点信息","bas_bridge","失败")
            
            # 根据交互信息判断是否需要运行质量分析与预处理模块
            if (flag_info_list[2].get_Flag() == 0):
                # 后台数据处理运行程序
                data_processing(raw_file_path+random_file_name,old_date,now_date,g_loss_dir,g_log_dir)  
                # 解压测试
                #data_processing("D:/YunDataFile/TestFile/SMJ_DYB-03-01_000000_003411.DY.7z",old_date,now_date,g_loss_dir,g_log_dir)  
                '''
                for root, dirs, files in os.walk("C:/Users/Administrator/Desktop/新建/"):
                    print('files:', files)  # 当前路径下所有非目录子文件
                for file in files:
                    data_processing("C:/Users/Administrator/Desktop/新建/"+file,old_date,now_date,save_dir,save_dir)  
                '''
                # 正常数据测试
                #data_processing("D:/YunDataFile/TestFile/HDQ_ND-06-02_010000_013411.txt",old_date,now_date,g_loss_dir,g_log_dir)   
                # 有空值数据测试
                #data_processing("D:/YunDataFile/TestFile/SMJ_DYB-03-01_000000_003411.txt",old_date,now_date,g_loss_dir,g_log_dir)     
                # 傅里叶绘图测试
                #data_processing("D:/YunDataFile/TestFile/SMJ_JSD-03-01_000000_005959.txt",old_date,now_date,g_loss_dir,g_log_dir)   
                #data_processing("D:/YunDataFile/TestFile/BL_JSD-03-01_000000_005959.txt",old_date,now_date,g_loss_dir,g_log_dir)   
                # 丢失数据测试
                #data_processing("D:/YunDataFile/TestFile/SMJ_JSD-03-01_010000_015959.txt",old_date,now_date,g_loss_dir,g_log_dir)
                # 其他文件测试	
                '''   
                data_processing("D:/YunDataFile/TestFile/三门江大桥_病害文件.docx",old_date,now_date,g_loss_dir,g_log_dir)    
                data_processing("D:/YunDataFile/TestFile/三门江大桥_巡检文件.docx",old_date,now_date,g_loss_dir,g_log_dir)       
                '''
            #'''
            # 根据交互信息判断是否需要日绘图
            if (flag_info_list[3].get_Flag() == 0):
                # 绘图判断
                # 日绘
                if now_date != old_date:
                    for node_info in node_info_list:
                        NodeCodeNo = node_info.get_NodeCodeNo()
                        bridge_name = dictionary03.get(NodeCodeNo.split("_")[0])
                        node_name = node_info.get_MonitorNo()
                        print("bridge name :%s ; node name :%s ;"%(bridge_name,node_name))
                        day_thread = draw_daily_report_thread(bridge_name,node_name,old_date,old_date,5,g_log_dir)
                        day_thread.start()
                        #time.sleep(30)
            #'''
            # 根据交互信息判断是否需要月绘图
            if (flag_info_list[4].get_Flag() == 0):
                # 月绘
                if now_date.split("-")[2] == "01" and now_date != old_date:
                    # 查询上月所有日内的对应文件
                    tmp_year = old_date.split("-")[0]
                    tmp_month = old_date.split("-")[1]
                    date_base = tmp_year + "-" + tmp_month + "-"
                    begin_date = date_base + "01"
                    month_num = calendar.monthrange(int(tmp_year),int(tmp_month))
                    end_date = date_base + str(month_num[1])
                    for node_info in node_info_list:
                        NodeCodeNo = node_info.get_NodeCodeNo()
                        bridge_name = dictionary03.get(NodeCodeNo.split("_")[0])
                        node_name = node_info.get_MonitorNo()
                        print(bridge_name,node_name,begin_date,end_date,g_log_dir)
                        month_thread = draw_monthly_report_thread(bridge_name,node_name,begin_date,end_date,5,g_log_dir)
                        month_thread.start()
                        #time.sleep(20)
            #'''
            # 日期更新
            old_date = now_date
            print("The program is listenging again !")
            #test_flag = 0
        except:
            print("本包处理出现异常！")
            print("The program is listenging again !")
            #test_flag = 0
            pass
        finally:
            wait_time = randint(120,300)
            print("wait %d seconds..."%wait_time)
            time.sleep(wait_time)

            

