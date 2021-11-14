# -*- coding: utf-8 -*-
"""
Created on Sun Jul 25 18:31:50 2021

@author: le
"""


import tushare as ts
import pandas as pd
import time
from multiprocessing import Pool 
import logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

import  pymysql
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler() 
engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8',
                       encoding='utf-8',
                       echo=False,
                       pool_pre_ping=True,
                       pool_recycle=3600) 
 
def delete_data_from_mysql(delete_sql):
    print(111)
    conn=pymysql.connect('192.168.52.110','root','root','akshare',3306)
    cursor =conn.cursor()
    cursor.execute(delete_sql)
    conn.close()

def get_stock(code, jun):
    ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
    pro = ts.pro_api()    
    # 以当前日期为文件名保存结果文件
    now_day = time.strftime('%Y%m%d')
    # now_day = '20210927'
#    filename = r'G:\投资\日均线指标\result\\' + now_day + 'result.txt'
#    fp1 = open(filename, 'a')
    table_name_all = 'average_line_day'
    table_name_60 = 'average_line_outof60day'
    # 用try为了避免有些股票没有数据而报错
    try:
        # 获取股票数据从2019-01-01开始到当天的数据
        print(code)
        df = pro.query('daily', ts_code=code, start_date='20200101', end_date=now_day)
        df = df.sort_values(by='trade_date', ascending=True)
        df.index = df['trade_date']
        # print(df)
        df_close = df['close']
        x = pd.DataFrame(df_close)
        # print(x)
        # print(x.rows)
        # y = x.sort_values(by='trade_date', ascending=True)
        # print(y)
        # 获取均线数据
        stockDate = x.rolling(jun).mean()
        lists = []
        # print(stockDate)
        # print(df)
        print(len(df_close), len(stockDate), len(stockDate))
        for i in range(0, len(df_close)):
            lists.append([df_close.index[i], code.split(".")[0], stockDate.close[i], df_close[i]])

        stock_jun = pd.DataFrame(lists, columns=('date', 'code', 'jun', 'close'))
        jun_value = float('%.2f' % stock_jun.iloc[-1].jun)
        close = float('%.2f' % stock_jun.iloc[-1].close)
 
        jun_1_value = float('%.2f' % stock_jun.iloc[-2].jun)
        close_1 = float('%.2f' % stock_jun.iloc[-2].close)

        if jun_1_value > close_1 and jun_value < close:
            logging.info("{0} %s  突破%s日均线啦".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())), (code, jun))
            print('%s  突破%s日均线啦' % (code, jun))
#            fp1.write('%s  突破%s日均线啦\n' % (code, jun))
            stock_jun.iloc[-1 :].to_sql(table_name_60, engine, if_exists='append',index= False)
        stock_jun.to_sql(table_name_all, engine, if_exists='append',index= False)
    except Exception as e:
        pass
        logging.error("{0} 出错啦！！！！！%s".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())), e)
        print("出错啦！！！！！")
#    fp1.close()
def get_all_stock(jun):
#    fp = open(r'G:\投资\日均线指标\stock_code.txt')
#    codes = fp.readlines()#[:5]
    starttime = time.time()
    try:
        
        delete_sql = 'truncate table akshare.average_line_day'
        # delete_data_from_mysql(delete_sql)
        print("truncate table akshare.average_line_day")
        logging.info("{0} truncate table akshare.average_line_day".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
        pro = ts.pro_api()
        data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        data_code = data['ts_code'].to_list()   
#        pool = Pool(processes = 40)
        
        # 遍历所有的股票
        for i,code in enumerate(data_code):
#            print(code , "---------------"+str(i))
            code_all = code.strip('\r\n').strip('\r')
            get_stock(code_all, jun)
#            pool.apply_async(get_stock, (code_all, jun, )) 
    except Exception:
        logging.error("{0} 执行批量平均线任务失败".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        print("执行批量平均线任务失败")    
#    finally:
    # pool.close()
    # pool.join()   #调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
    print("Sub-process(es) done.")  
    dtime = time.time() - starttime
    #获取总消耗时间
    logging.info("time_elapsed_all : %s",str(int(dtime)))  
        
#    fp.close()

# ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
# pro = ts.pro_api()
# now_day = time.strftime('%Y%m%d')
# df = pro.query('daily', ts_code='000001.SZ', start_date='20210903', end_date=now_day)
# df = df.sort_values(by='trade_date', ascending=True)
# print(df)


if __name__ == '__main__':
    
    get_all_stock(60)















































































