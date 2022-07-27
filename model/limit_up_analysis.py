# -*- coding:utf-8 -*-
#@Time : 2021/12/20 8:36
#@Author: Geoff Nie
#@File : limit_up_analysis.py


import pandas as pd
import akshare as ak
import numpy as np
import  pymysql
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
from multiprocessing import Pool
import logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

# 显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 200)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 200)
# 显示宽度
pd.set_option('display.width', 200)


# 思路：
# 分层：流通市值分层、质押占比分层、连续涨停占比、封板时间分层(一字板占比、一次封板【不包含一字板】占比、多次封板占比、首次封板时间分布、最后封板时间分布)、最新价分层、行业分层、市盈率分层统计
#
#
# 19亿以下
# 20-60
# 60-200
# 200-500
# 500-1000
# 1000以上


# host = 'yunfuwu01'
host = '127.0.0.1'
user = 'root'
passwd = 'root'
port = '3306'
db = 'akshare'


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
                           encoding='utf-8',
                           echo=False,
                           pool_pre_ping=True,
                           pool_recycle=3600)
    dat = '20211223'
    def function_map(x):
        if x <= 20:
            return 20
        elif x <= 60:
            return 60
        elif x <= 200:
            return 200
        elif x <=500:
            return 500
        elif x <= 1000:
            return 1000
        elif x > 1000:
            return 5000
        else :
            print("出错了")
    stock_em_zt_pool_df = ak.stock_em_zt_pool(date=dat)
    stock_em_zt_pool_df['amount_area'] = stock_em_zt_pool_df.apply(lambda x:function_map(x['流通市值']/100000000), axis=1)
    stock_em_zt_pool_df_amount_area = stock_em_zt_pool_df.groupby(['amount_area'], as_index=False)['代码'].count()
    stock_em_zt_pool_df_amount_area.columns = ['amount_area', 'cnt']
    stock_em_zt_pool_df_amount_area['ratio'] = (stock_em_zt_pool_df_amount_area['cnt']/stock_em_zt_pool_df_amount_area['cnt'].sum()).round(2)
    # stock_em_zt_pool_df_amount_area['ratio'] = stock_em_zt_pool_df.groupby(['amount_area'])['代码'].count()/stock_em_zt_pool_df.groupby(['amount_area'])['代码'].count().sum().round(2)
    # stock_em_zt_pool_df['amount_area'] = [x  for x in stock_em_zt_pool_df['流通市值'].tolist()/10000000 if x <= 20 ]
    print(stock_em_zt_pool_df)
    print(stock_em_zt_pool_df.groupby(['amount_area'])['代码'].count()/stock_em_zt_pool_df.groupby(['amount_area'])['代码'].count().sum().round(2))
    print("流通市值在涨停板池中占比(单位：亿)：")
    print(stock_em_zt_pool_df_amount_area)

    stock_em_zt_pool_df['price_area'] = stock_em_zt_pool_df.apply(lambda x:function_map(x['最新价']/1000), axis=1)
    stock_em_zt_pool_df_price_area = stock_em_zt_pool_df.groupby(['price_area'], as_index=False)['代码'].count()
    stock_em_zt_pool_df_price_area.columns = ['price_area', 'cnt']
    stock_em_zt_pool_df_price_area['ratio'] = (stock_em_zt_pool_df_price_area['cnt']/stock_em_zt_pool_df_price_area['cnt'].sum()).round(2)
    print("股价区间在涨停板池中占比：")
    print(stock_em_zt_pool_df_price_area)

    sql = "select * from akshare.history_trade_day_qfq htdq where 日期 = date({0})".format(dat)
    df = pd.read_sql_query(sql, engine)
    df['price_area'] = df.apply(lambda x: function_map(x['收盘']), axis=1)
    df_stat = df.groupby(['price_area'], as_index=False)['code'].count()
    df_stat.columns = ['price_area', 'all_cnt']
    # print(df)
    df_concat = pd.merge(df_stat, stock_em_zt_pool_df_price_area, how='left', on=['price_area']).replace(np.nan, 0)
    df_concat['limit_up_ratio'] = (df_concat['cnt']*100/df_concat['all_cnt']).round(2)
    print("=============")
    print("涨停板股价区间在所有股票中股价区间占比：")
    print(df_concat)

    print("===========")
    stock_em_zt_pool_df['is_one_limitup'] = ["一字板" if int(x[:4]) < 930 else ("一次封板" if x == y else "多次封板") for x,y in zip(stock_em_zt_pool_df["最后封板时间"],stock_em_zt_pool_df["首次封板时间"]) ]
    # stock_em_zt_pool_df['is_one_limitup'] = stock_em_zt_pool_df.apply(lambda x: x.最后封板时间 )
    print("涨停池中一字板占比：")
    stock_em_zt_pool_df_one_limitup = stock_em_zt_pool_df.groupby(['is_one_limitup'], as_index=False)['代码'].count()
    stock_em_zt_pool_df_one_limitup.columns = ['is_one_limitup', 'cnt']
    stock_em_zt_pool_df_one_limitup['ratio'] = (stock_em_zt_pool_df_one_limitup['cnt']/stock_em_zt_pool_df_one_limitup['cnt'].sum()).round(2)
    print(stock_em_zt_pool_df_one_limitup)

    print("=============")
    stock_em_zt_pool_df_one_limit_up = stock_em_zt_pool_df[stock_em_zt_pool_df['is_one_limitup']== '一次封板']
    # print(stock_em_zt_pool_df_one_limit_up)
    def function_time_map(x):
        if x <= 926:
            return 925
        elif x <= 930:
            return 929
        elif x <= 1000:
            return 930
        elif x <= 1030:
            return 1000
        elif x <= 1100:
            return 1030
        elif x <= 1130:
            return 1100
        elif x <= 1330:
            return 1300
        elif x <= 1400:
            return 1330
        elif x <= 1430:
            return 1400
        elif x <= 1500 :
            return 1430
        else:
            print("出错了")
    stock_em_zt_pool_df_one_limit_up['time_area'] = stock_em_zt_pool_df_one_limit_up.apply(lambda x:function_time_map(int(x['首次封板时间'])/100), axis=1)
    # print(stock_em_zt_pool_df_one_limit_up)
    stock_em_zt_pool_df_one_limit_up_time_area = stock_em_zt_pool_df_one_limit_up.groupby(['time_area'], as_index=False)['代码'].count()
    stock_em_zt_pool_df_one_limit_up_time_area.columns = ['time_area', 'cnt']
    print("一次封板中时间区间分布：")
    print(stock_em_zt_pool_df_one_limit_up_time_area)

    stock_em_zt_pool_df['time_area'] = stock_em_zt_pool_df.apply(lambda x:function_time_map(int(x['首次封板时间'])/100), axis=1)
    # print(stock_em_zt_pool_df_one_limit_up)
    stock_em_zt_pool_df_time_area = stock_em_zt_pool_df.groupby(['time_area'], as_index=False)['代码'].count()
    stock_em_zt_pool_df_time_area.columns = ['time_area', 'cnt']
    print("所有涨停板中首次封板时间区间分布：")
    print(stock_em_zt_pool_df_time_area)

    stock_em_zt_pool_df['time_area_last'] = stock_em_zt_pool_df.apply(lambda x:function_time_map(int(x['最后封板时间'])/100), axis=1)
    # print(stock_em_zt_pool_df_one_limit_up)
    stock_em_zt_pool_df_time_area_last = stock_em_zt_pool_df.groupby(['time_area_last'], as_index=False)['代码'].count()
    stock_em_zt_pool_df_time_area_last.columns = ['time_area_last', 'cnt']
    print("所有涨停板中最后封板时间区间分布：")
    print(stock_em_zt_pool_df_time_area_last)

    stock_em_zt_pool_df_industry = stock_em_zt_pool_df.groupby(['所属行业'], as_index=False)['代码'].count()
    stock_em_zt_pool_df_industry.columns = ['所属行业', 'cnt']
    print("涨停板中行业分布：")
    print(stock_em_zt_pool_df_industry.sort_values(by=['cnt'], ascending=False).reset_index())































