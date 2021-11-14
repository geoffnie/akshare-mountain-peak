import datetime
from dateutil.relativedelta import relativedelta
import akshare as ak

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
engine = create_engine('mysql+pymysql://root:root@yunfuwu01/akshare?charset=utf8',
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


def get_data():
    sql = '''

    '''
    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
    return df


# def getdata_mysql(sql):
#     conn = pymysql.connect(host='192.168.52.110', port=3306, user='root', passwd='root', db='akshare')
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
    # df = get_data()
    # print(df)
    # print(len(df))
    import sys

    # print(sys.argv[0])  # sys.argv[0] 类似于shell中的$0,但不是脚本名称，而是脚本的路径
    # print(sys.argv[1])
    # print(sys.argv[2])











