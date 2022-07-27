# -*- coding:utf-8 -*-
#@Time : 2022/5/20 9:55
#@Author: Geoff Nie
#@File : get_real_data.py



import gc
import gzip
from io import StringIO

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
from kafka import KafkaProducer

from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler()

import akshare as ak
import time
import datetime
import json
from kafka.errors import kafka_errors
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

def is_between_time(begin_time, end_time):

    now = time.strftime('%H:%M:%S')
    print(now)
    if begin_time <= now <= end_time:
        # print('当前时间在两个时间之间')
        return True
    else:
        return False

def gzip_compress(msg_str):
    try:
        buf = StringIO.StringIO()
        with gzip.GzipFile(mode='wb', fileobj=buf) as f:
            f.write(msg_str)
        return buf.getvalue()
    except BaseException as e:
        print ("Gzip压缩错误" + e)


def gzip_uncompress(c_data):
    try:
        buf = StringIO.StringIO(c_data)
        with gzip.GzipFile(mode='rb', fileobj=buf) as f:
            return f.read()
    except BaseException as e:
        print ("Gzip解压错误" + e)


@scheduler.scheduled_job('cron', hour='9', minute='10' , coalesce=False, misfire_grace_time=60, max_instances=10)
def get_data_job():
    print("start job: ")
    for i in range(10000):
        time.sleep(1)
        flag_morning = is_between_time('09:20:00', '11:30:00')
        flag_afternoon = is_between_time('13:00:00', '15:00:00')
        while flag_morning or flag_afternoon:
            timestamp = int(time.time() * 1000)
            json_df = get_real_data()
            array = json_df.values
            # array = json_df.to_numpy()
            # print(array)
            dic = {}
            dic['index'] = array.tolist()
            dic['timestamp'] = timestamp
            dicJson = json.dumps(dic, ensure_ascii=False)
            topic = 'real-data-test'
            # bootstrap_servers_list = ['localhost:9092', 'localhost:9093', 'localhost:9094']
            # bootstrap_servers_list = ['node01:9092', 'node02:9092', 'node03:9092']
            bootstrap_servers_list = ['yunfuwu01:9092']
            msg = bytes((dicJson).encode('utf-8'))
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers_list, acks=1, retries=3, batch_size=10485760,
                                     reconnect_backoff_max_ms=3000, buffer_memory=10485760, max_request_size=10485760, value_serializer=gzip_compress)
            future = producer.send(topic, msg)
            producer.flush()
            producer.close()

            time.sleep(1)
            flag_morning = is_between_time('09:20:00', '11:30:00')
            flag_afternoon = is_between_time('13:00:00', '15:00:00')
            # flag = (is_between_time('09:25:00', '11:30:00') or is_between_time('13:00:00', '15:00:00')  )
            # if not is_between_time('09:25:00', '11:30:00') :
            #     break
            # try:
            #     record_metadata = future.get(timeout=10)
            # # print(record_metadata.topic)
            # # print(record_metadata.partition)
            # # print(record_metadata.offset)
            # except kafka_errors as e:
            #     print(str(e))

if __name__ == '__main__':
    print("开始运行：")
    get_data_job()
    # scheduler.start()




