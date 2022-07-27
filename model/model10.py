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

def mymovefile(srcfile,dstfile):
    import os
    import shutil
    if not os.path.isfile(srcfile):
        print("%s not exist!"%(srcfile))
    else:
        fpath,fname=os.path.split(dstfile)    #分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)                #创建路径
        shutil.move(srcfile,dstfile)          #移动文件
        print("move %s -> %s"%( srcfile,dstfile))

def mycopyfile(srcfile,dstfile):
    import os
    import shutil
    if not os.path.isfile(srcfile):
        print("%s not exist!"%(srcfile))
    else:
        fpath,fname=os.path.split(dstfile)    #分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)                #创建路径
        shutil.copyfile(srcfile,dstfile)      #复制文件
        print("copy %s -> %s"%( srcfile,dstfile))

def mydeletefile(srcfile):
    import os
    if not os.path.isfile(srcfile):
        print("%s not exist!"%(srcfile))
    else:
        os.remove(srcfile)
        print("delete----%s "%( srcfile))

def handle_file(root, file):
    import platform
    if platform.system().lower() == 'windows':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '\\' + file
            mydeletefile(srcfile)

    elif platform.system().lower() == 'linux':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '/' + file
            mydeletefile(srcfile)

def handle_file_move(root, file, dstfile_path):
    import os
    import platform
    if platform.system().lower() == 'windows':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '\\' + file
            dstfile = dstfile_path + '\\' + file
            if not os.path.isfile(dstfile):
                dstfile = dstfile_path + '\\' + file
            else:
                cur_time = time.time()
                dstfile = dstfile_path + '\\' + file +'_{0}'.format(str(int(cur_time)))
            mymovefile(srcfile,dstfile)

    elif platform.system().lower() == 'linux':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '/' + file
            dstfile = dstfile_path + file
            if not os.path.isfile(dstfile):
                dstfile = dstfile_path  + file
            else:
                cur_time = time.time()
                dstfile = dstfile_path + file +'_{0}'.format(str(int(cur_time)))
            mymovefile(srcfile,dstfile)


# 链接sqlite数据库，用于查询数据或者插入等操作
def connect_sqlite(sql,db):
    import sqlite3
    # 连接数据库(如果不存在则创建)
    conn = sqlite3.connect(db)
    # 创建游标
    cursor = conn.cursor()
    # 执行任务
    cursor.execute(sql)
    result = cursor.fetchall()
    col_result = cursor.description
    if col_result is None:
        print("col_result is none")
        values = pd.DataFrame([[]])
    else:
        col = pd.DataFrame(col_result)
        values = pd.DataFrame(result, columns=col.iloc[:, 0])
    # 提交事物
    conn.commit()
    # 关闭游标
    cursor.close()
    # 关闭连接
    conn.close()

    return values

if __name__ == '__main__':
    # df = get_data()
    import pandas as pd
    import numpy as np
    df = pd.read_excel("C:\\Users\\jiuxin\\Desktop\\Report_ReportRandom.xlsx", sheet_name='Report_ReportRandom', header=None)#.replace(np.nan, '', regex=True)
    print(df)
    print("-------------")
    print(len(df))
    aa = df[df.isnull().T.all()]
    print(aa)
    print("============")
    print(df.isnull().T.all())
    print(type(df.isnull().T.all()))
    #获取空行索引
    print(df.isnull().T.all().index[df.isnull().T.all() == True])
    print(df.iloc[4,1])
    print(df.iloc[6, 1])
    print(type(df.iloc[6, 1]))
    #空行索引
    index_nullrow = df.isnull().T.all().index[df.isnull().T.all() == True]
    print([df.iloc[i + 1, 1]  for i in index_nullrow])
    print([df.iloc[i + 1, 1] is np.nan for i in index_nullrow])
    new_index = [i + 1 for i in index_nullrow if df.iloc[i + 1, 1] is not np.nan]
    print(new_index)

    for i in range(len(new_index)):
        print(i, len(new_index), new_index[i])
        # aa = "df_" + "{0}".format(str(i))
        if i != len(new_index)-1:
            print(i)
            df_i = df.iloc[new_index[i]:new_index[i+1]]
        else:
            print(i, new_index[i])
            df_i = df.iloc[new_index[i]:]
        df_i = df_i.dropna(axis=0, how='all').dropna(axis=1, how='all').reset_index(drop=True)
        # df_i = df_i.dropna(axis=1, how='all')
        print(df_i)
        print("==================================")

    #查询sqlite中的points
    sql = '''select * from main.points;'''
    # sql = '''CREATE TABLE if not exists Student(id integer PRIMARY KEY autoincrement, Name  varchar(30), Age integer)'''
    db_points = "D:\\tmp\\sqlite_test\\points.db"
    df = connect_sqlite(sql, db_points)
    print(df)

    #插入数据到sqlite
    sql_insert = '''insert into main.msgBuffered (msg) VALUES ('aa');'''
    # sql = '''CREATE TABLE if not exists Student(id integer PRIMARY KEY autoincrement, Name  varchar(30), Age integer)'''
    db_buffered_msg = "D:\\tmp\\sqlite_test\\buffered_msg.db"
    df = connect_sqlite(sql_insert, db_buffered_msg)

    #移动文件到/root/CIFS/tobedelete路径下
    handle_file_move("C:\\Users\\jiuxin\\Desktop\\需要比较的文件20201217\\", "Prod_Rep_20211125_115626.xlsx", "C:\\Users\\jiuxin\\Desktop\\需要比较的文件20201217\\最新文件\\")



    # print(df.describe())
    # print(df.info())











