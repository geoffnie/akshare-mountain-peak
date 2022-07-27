# -*- coding:utf-8 -*-
#@Time : 2022/3/2 10:42
#@Author: Geoff Nie
#@File : model20.py





# import re
#
#
# pattern = r"Production parameters".lower()
#
# aa = re.search(pattern, (''.join(['Production param23s','Production values','Production parameters'])).lower())
# if aa == None:
#     print("aaa")
# else:
#     print("bb")
#     print(type(aa))
# print(aa)
# print((''.join(['Production parameters','Production values','Production parameters'])).lower())
# print(pattern)
#
#
# excel_path = '/mnt/xy/realTime_reports/Prod_Rep_20220302_141555.xlsx'
#
# print(excel_path)
# print(excel_path.find("."))
#
# print(excel_path[-20: excel_path.find(".")].replace("_", " "))
#
#
# # end_time = excel_path[-20: aa.find(".")].replace("_", " ")
# import time
# end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(excel_path[-20: excel_path.find(".")].replace("_", " "), "%Y%m%d %H%M%S"))
# print(end_time)

import datetime
print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

print(type(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

from obs import ObsClient

# '''初始化照片存放目录'''

def connect_obs():
    obsClient = ObsClient(
        access_key_id='YN1B0GU8UECHNXN8IVB3',
        secret_access_key='p7RYS9lSEO4TpiFrBHTG0PAmauXd0A1cX7zAcrng',
        server='obs.cn-east-2.myhuaweicloud.com')
    return obsClient

def create_directory_to_obs(bucketName, directory):
    try:
        obsClient = connect_obs()
        print(obsClient)
        resp = obsClient.putContent(bucketName, directory, content=None)
        print(resp)
        if resp.status < 300:
            print('requestId:', resp.requestId)
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)

        # # 在文件夹下创建对象
        # resp = obsClient.putContent('bucketname', 'parent_directory/objectname', content='Hello OBS')
        #
        # if resp.status < 300:
        #     print('requestId:', resp.requestId)
        # else:
        #     print('errorCode:', resp.errorCode)
        #     print('errorMessage:', resp.errorMessage)
    except:
        import traceback
        print(traceback.format_exc())


def get_obs_all_file_name(bucketname, prefixpath):
    obsClient = connect_obs()

    # 获取文件夹下所有的文件
    try:
        resp = obsClient.listObjects(bucketname, prefix=prefixpath, max_keys=100)

        if resp.status < 300:
            index = 1
            list_key = []
            for content in resp.body.contents:
                if index != 1:
                    list_key.append(content.key)
                index += 1
            print(list_key)
            return list_key

        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
    except:
        import traceback
        print(traceback.format_exc())


def get_obs_meta_info(bucketname, prefixpath):
    try:
        obsClient = connect_obs()
        resp = obsClient.getObjectMetadata(bucketname, prefixpath)

        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('etag:', resp.body.etag)
            print('lastModified:', resp.body.lastModified)
            print('contentType:', resp.body.contentType)
            print('contentLength:', resp.body.contentLength)
            print('status      ', resp.status)
            print('reason      ', resp.reason)
            print('errorCode   ', resp.errorCode)
            print('errorMessage', resp.errorMessage)
            print('body        ', resp.body)
            print('requestId   ', resp.requestId)
            print('hostId      ', resp.hostId)
            print('resource    ', resp.resource)
            print('header      ', resp.header)
            print('indicator   ', resp.indicator)
        else:
            print('status:', resp.status)
    except:
        import traceback
        print(traceback.format_exc())

def del_obs_object(bucketName, directory):
    try:
        obsClient = connect_obs()
        resp = obsClient.deleteObject(bucketName, directory)

        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('deleteMarker:', resp.body.deleteMarker)
            print('versionId:', resp.body.versionId)
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
    except:
        import traceback
        print(traceback.format_exc())

# create_directory_to_obs('testplatform',  'aaa/')
print("-------")
# get_obs_all_file_name('testplatform',  '')

# get_obs_meta_info('testplatform',  'admin/test1/')
# del_obs_object('testplatform',  'admin/test10/')


import gc
import pandas as pd
import requests
from bs4 import BeautifulSoup
#显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', 200)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',200)
#显示宽度
pd.set_option('display.width', 200)
from pykafka import KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
from pykafka import KafkaClient


import akshare as ak
import time
import datetime
import json
def get_real_data():
    starttime = time.time()
    print(datetime.datetime.now())
    # print(starttime)
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    # print(stock_zh_a_spot_em_df.columns)
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df.drop_duplicates(subset=['代码'], keep='first')
    # print(        stock_zh_a_spot_em_df    )
    stock_zh_a_spot_em_df.columns = ['id', 'code', 'name', 'newPrice', 'ratio', 'gapPrice', 'vol', 'amount', 'gapRatio', 'max', 'min', 'open' ,'closeLastDay', 'ratioVol', 'ratioChange', 'movePE', 'PE']
    dtime = time.time() - starttime
    # print(dtime)
    # 获取总消耗时间
    print("time_elapsed_all : %s ms", str(int(dtime * 100)))
    print(len(stock_zh_a_spot_em_df))
    print("----------------------------")
    # return stock_zh_a_spot_em_df.to_json(orient='records' ,force_ascii=False)
    return stock_zh_a_spot_em_df

# for i in range(1000):
#     time.sleep(0.1)
#     json_df = get_real_data()
#     topic =  "ranktest"
#     msg = json.dumps(json_df)
#     producer = KafkaProducer("localhost:9092,localhost:9093,localhost:9094")
#     producer.send(topic=topic, value=msg)


json_df = get_real_data()
print(json_df)
print("-----------------------------------")
array = json_df.values
# array = json_df.to_numpy()
print(array)
print("-----------------------------------")
dic={}
dic['index']=array.tolist()
dicJson = json.dumps(dic, ensure_ascii=False)
print(dicJson)
print(len(dicJson))

from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler


# host = 'yunfuwu01'
# # host = '127.0.0.1'
# user = 'root'
# passwd = 'root'
# port = '3306'
# db = 'akshare'
#
# scheduler = BlockingScheduler()
# engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
#                        encoding='utf-8',
#                        echo=False,
#                        pool_pre_ping=True,
#                        pool_recycle=3600)
#
# # # aa = ak.stock_em_yjkb(date='20211231')
# # aa = ak.stock_em_yjyg(date='20211231')
# # print(aa)
# # print(aa[aa['股票简称'] == '天保基建'])
# #
# # # aa = ak.stock_em_yjyg(date='20211231')
# # table_name = 'yjyg_season'
# # # 插入数据
# # aa.to_sql(table_name, engine, if_exists='append', index=False)
#
# stock_em_yjbb_df = ak.stock_em_yjbb(date='20220331')
#
# # print(stock_em_yjbb_df)
# #
# # print(stock_em_yjbb_df[stock_em_yjbb_df['股票简称'] == '国民技术'])
#
#
# # stock_em_yjkb_df  = ak.stock_em_yjkb(date='20220331')
# # stock_em_yjkb_df ['dat'] = '{0}'.format('20220331')
#
# print(stock_em_yjbb_df)














