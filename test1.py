# -*- coding:utf-8 -*-
#@Time : 2022/3/31 14:11
#@Author: Geoff Nie
#@File : test1.py


import numpy as np
import pandas as pd
# 显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 200)
# 显示宽度
pd.set_option('display.width', 2000)

df = pd.read_excel(r"D:\项目\测试文件\东锅集宁二期1#上部技术清单.xls", sheet_name='Sheet1', header=None).replace(np.nan, '', regex=True)

# print(df.iloc[:500, :])



from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
from multiprocessing import Pool
# import logging
# LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
# DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
# logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

host = 'mysql.dev.jiuxiniot.com'
# host = '127.0.0.1'
user = 'root'
passwd = 'Jiuxin2019nn'
port = '30299'
db = 'test'

scheduler = BlockingScheduler()
engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
                       encoding='utf-8',
                       echo=False,
                       pool_pre_ping=True,
                       pool_recycle=3600)

sql = '''select * from test.`excel_config`'''
# 获取查询数据
df = pd.read_sql_query(sql, engine)
print(df)







