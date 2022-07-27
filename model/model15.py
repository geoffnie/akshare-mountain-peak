# -*- coding:utf-8 -*-
#@Time : 2022/2/12 20:17
#@Author: Geoff Nie
#@File : model15.py


import time
import pandas as pd
import tushare as ts
import pymysql
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
from multiprocessing import Pool
import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

scheduler = BlockingScheduler()
engine = create_engine('mysql+pymysql://root:root@yunfuwu01/dm?charset=utf8',
                       encoding='utf-8',
                       echo=False,
                       pool_pre_ping=True,
                       pool_recycle=3600)
ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
pro = ts.pro_api()

# 显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 200)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 200)
# 显示宽度
pd.set_option('display.width', 200)


def get_data(sql):
#     sql = '''
# select * from akshare.trade_date_hist_sina where trade_date >= '2020-01-01'
#     '''
    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
    return df


# def getdata_mysql(sql):
#     conn = pymysql.connect(host='yunfuwu01', port=3306, user='root', passwd='root', db='akshare')
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     conn.commit()
#     result = cursor.fetchall()
#     df = pd.DataFrame(result)
#     cursor.close()
#     return df
#
# df = getdata_mysql(sql)


if __name__ == '__main__':
    # sql = '''
    # select * from akshare.trade_date_hist_sina where trade_date >= '2022-02-01' and trade_date <= CURRENT_DATE() order by trade_date desc
    #     '''
    # df = get_data(sql)
    # print(df)
    # print(len(df))
    # print(df['trade_date'].to_list())
    # dat_list = df['trade_date'].to_list()

    day_list = [0 ,4 ,21 ,43 ,119 ]

    for dat in day_list:
        classify = 'pre_' + str(dat + 1) + '_day'
        sql_model = '''
     '''.format(classify, dat)
        df_model = get_data(sql_model)
        table_name = 'model_limitup_back_test_all'
        #插入数据
        try:
            df_model.to_sql(table_name, engine, if_exists='append',index= False)
            print("已插入")
            print("-----", dat)
        except Exception:
            print("未插入")
            print("-----", dat)
            continue

