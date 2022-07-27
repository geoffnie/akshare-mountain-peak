# -*- coding:utf-8 -*-
#@Time : 2022/2/12 20:17
#@Author: Geoff Nie
#@File : model13.py



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
    sql = '''
    select * from akshare.trade_date_hist_sina where trade_date >= '2022-02-08' and trade_date <= CURRENT_DATE() order by trade_date desc
        '''
    df = get_data(sql)
    print(df)
    print(len(df))
    print(df['trade_date'].to_list())
    dat_list = df['trade_date'].to_list()
    for dat in dat_list:
        sql_model = '''
select * from (
select * 
, round( (max_price - min_price) / min_price * 100 ) as back_max_ratio
from (
select *
, vol - max10back_vol as backvol_gap
, (select round((max(收盘) - min(收盘) ) / min(收盘) *100 ) from akshare.history_trade_day_qfq where code = t.code and 日期 >= t.dat and 日期 <= date_sub(t.dat , INTERVAL -90 day) ) as backdat_ratio
, (select min(收盘) from akshare.history_trade_day_qfq where code = t.code and 日期 >= t.dat and 日期 <= t.max_price_dat ) as min_price
from (
select *
, (select max(成交量) from akshare.history_trade_day_qfq where code = t.code and 日期 <= t.back10dat and 日期 > t.dat) as max10back_vol
, (select round((max(收盘) - min(收盘) ) / min(收盘) *100 ) from akshare.history_trade_day_qfq where code = t.code and 日期 <= t.dat and 日期 >= t.pre3dat) as cur3dat_ratio
, (select round((max(收盘) - min(收盘) ) / min(收盘) *100 ) from akshare.history_trade_day_qfq where code = t.code and 日期 >= t.pre60dat and 日期 <= t.dat) as pre60dat_ratio
, (select round((max(收盘) - min(收盘) ) / min(收盘) *100 ) from akshare.history_trade_day_qfq where code = t.code and 日期 >= t.dat and 日期 <= t.back20dat) as back20dat_ratio
, (select max(日期) from akshare.history_trade_day_qfq where code = t.code and 日期 >= t.dat and 收盘 = max_price ) as max_price_dat
, pre3dat_price * 0.95 - (select min(收盘) from akshare.history_trade_day_qfq where code = t.code and 日期 >= t.dat and 日期 <= date_sub(t.dat, INTERVAL -30 day)  ) as lower_pre3dat_price
from (
select *
, (select * from akshare.trade_date_hist_sina t where trade_date < t.dat order by trade_date desc limit 4,1) as pre10dat
, (select * from akshare.trade_date_hist_sina t where trade_date > t.dat order by trade_date asc limit 4,1) as back10dat
, (select * from akshare.trade_date_hist_sina t where trade_date > t.dat order by trade_date asc limit 19,1) as back20dat
, (select * from akshare.trade_date_hist_sina t where trade_date < t.dat order by trade_date desc limit 59,1) as pre60dat
, vol/min3_vol as 3vol
, (select max(收盘) from akshare.history_trade_day_qfq where code = t.code and 日期 >= t.dat and 日期 <= date_sub(CURRENT_DATE() , INTERVAL 0 day) ) as max_price
, (select 收盘 from akshare.history_trade_day_qfq where code = t.code and 日期 = t.pre3dat ) as pre3dat_price
from (
select *
, (select min(成交量) from akshare.history_trade_day_qfq where code = t.code and 日期 < t.dat and 日期 >= t.pre3dat) as min3_vol
from (
select 日期 as dat, 成交量 as vol, 收盘 as price, code , name
, (select * from akshare.trade_date_hist_sina t where trade_date < t.日期 order by trade_date desc limit 2,1) as pre3dat
from akshare.history_trade_day_qfq t where 日期 = '{0}' 
-- and code = '300204'
and substring(code , 1, 3) !='688'
and name not like "ST%%"
) t
) t
having 3vol >=3                     -- 代表前4个交易日内当前成交量比最小成交量大于3倍，形成巨量
) t
having cur3dat_ratio >= 10          -- 代表前4个交易日内涨幅大于10
and lower_pre3dat_price < 0         -- 代表后续一个月内股价未脱离当前股价的95
) t
having backvol_gap >= 0             -- 代表当前日期在后续10天内形成成交量最大值
and pre60dat_ratio < 40             -- 前60个交易日最大振幅不超过40
-- and back20dat_ratio < 15
) tt
) ttt
order by back_max_ratio desc            '''.format(str(dat))
        df_model = get_data(sql_model)
        table_name = 'model_make_base_and_climb'
        #插入数据
        try:
            df_model.to_sql(table_name, engine, if_exists='append',index= False)
            print("已插入")
            print("-----", dat)
        except Exception:
            print("未插入")
            print("-----", dat)
            continue





