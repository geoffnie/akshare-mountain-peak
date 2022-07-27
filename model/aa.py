from pydruid.db import connect
import time
import datetime
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import logging
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
import requests
import numpy as np
import pymysql
from urllib import parse
#信义环境
# druid_host = '124.70.188.197'   #本地地址
# druid_port = 8888          #本地端口号
druid_host = '192.168.1.30'   #信义服务器地址
druid_port = 8888         #信义服务器端口号
username = 'root'
password = 'Jiuxin2120@nm'
host = '124.70.142.226'
db = 'dataquality'
def connect_druid_single(sql):
    heard = {

    }
    conn = connect(host=druid_host, port=druid_port, path='/druid/v2/sql/', scheme='http', user="admin", password="JiuxinAdmin")
    curs = conn.cursor()

    druid_points_num = []


    curs.execute(sql)


    result = curs.fetchall()
    col_result = curs.description

    # 获取字段信息
    columns = []
    if col_result is None:
        print("col_result is none")
        druid_points_num.append(0)

    else:
        for m in range(len(col_result)):
            columns.append(col_result[m][0])
            # df_druid = pd.DataFrame(columns=columns)
        df_druid = pd.DataFrame(result)

    curs.close()
    return df_druid

# sql = '''SELECT * FROM "ods.motr.pnt.data.ip.tab-test3" limit 10'''
#
# aa = connect_druid_single(sql)
#
#
# print(aa)
def create_onehot_label(x):
    label = np.zeros((1, 7), dtype=np.float32)
    label[:, int(x)] = 1
    print(label)
    return label

# data_frame = pd.read_csv("C:\\Users\\jiuxin\\Desktop\\train.csv")
# print(data_frame['Emotion'].values)
# bb = [list(map(create_onehot_label, data_frame['Emotion'].values))]
# print(bb)
# print(map(create_onehot_label, data_frame['Emotion'].values))
# print([map(create_onehot_label, data_frame['Emotion'].values)])
# # train_labels = np.array([map(create_onehot_label, data_frame['Emotion'].values)]).reshape(-1, 7)
# # print(train_labels)
#
# print(create_onehot_label(data_frame['Emotion'].values[0]))

import akshare as ak

aa = ak.stock_em_yjkb(date='20220331')
print(aa)






