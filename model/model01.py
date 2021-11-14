import datetime
from dateutil.relativedelta import relativedelta
import akshare as ak

import time
import pandas as pd
import tushare as ts
import  pymysql
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
from multiprocessing import Pool
import logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

scheduler = BlockingScheduler()
engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8',
                       encoding='utf-8',
                       echo=False,
                       pool_pre_ping=True,
                       pool_recycle=3600)
ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
pro = ts.pro_api()


#显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', 200)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',200)
#显示宽度
pd.set_option('display.width', 200)

# #取000001的前复权行情
# df = ts.pro_bar(ts_code='600995.SH', adj='qfq', start_date='20210920', end_date='20211019')
# df = df.sort_values(['trade_date'], ascending=False)
# print(df)

# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="603045", period="daily", start_date='20210920', end_date='20211019',
#                                         )
# # stock_zh_a_hist_df['code'] = code
# # print(code)
# # print(date)
# print(stock_zh_a_hist_df)

def get_data(gap_currentdate=0 ):
    sql = '''
    # select * 
    # , round((pre0vol - pre01vol)/pre01vol, 1) as beishu
    # from (
    select *
    , t2.name, t2.list_date
    , round((收盘 - pre0prise)/pre0prise *100) as ratio
    , round(((select max(收盘) from akshare.history_trade_day_qfq where 日期 >= t.pre0dat COLLATE utf8_unicode_ci and code=t.code) - pre0prise)/pre0prise*100) as next_ratio
    , (select 成交量 from akshare.history_trade_day_qfq where 日期 < t.pre0dat COLLATE utf8_unicode_ci and code=t.code order by 日期 desc limit 1)  as pre01vol
    , DATE_SUB(CURDATE(), INTERVAL {0} day) as dat
    -- , (select * from akshare.trade_date_hist_sina where trade_date <= t.日期 order by trade_date asc limit 20,1) as pre20date 
    from (
    select case when @precode =t1.code COLLATE utf8_unicode_ci and @prevol > t1.成交量 COLLATE utf8_unicode_ci then @i:=@i+1 else @i:=1 end rank
    , case when @precode =t1.code COLLATE utf8_unicode_ci  then @j:= @j + t1.成交量 else @j:=0 end vol_accu
    , case when @precode !=t1.code COLLATE utf8_unicode_ci then @pre0dat:=t1.日期
      when @precode =t1.code COLLATE utf8_unicode_ci and @i =1  then @pre0dat:=t1.日期 else @pre0dat:=@pre0dat end pre0dat
    , case when @precode !=t1.code COLLATE utf8_unicode_ci then @pre0prise:=t1.收盘
      when @precode =t1.code COLLATE utf8_unicode_ci and @i =1  then @pre0prise:=t1.收盘 else @pre0prise:=@pre0prise end pre0prise
    , case when @precode !=t1.code COLLATE utf8_unicode_ci then @pre0vol:=t1.成交量
      when @precode =t1.code COLLATE utf8_unicode_ci and @i =1  then @pre0vol:=t1.成交量 else @pre0vol:=@pre0vol end pre0vol
    , @precode:=t1.code as percode
    , @prevol:=t1.成交量 as prevol
    -- , @pre0dat:=t1.日期 
    , t1.*
    from (
    select * from akshare.history_trade_day_qfq where 日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= DATE_SUB(CURDATE(), INTERVAL {0} day)  order by trade_date desc limit 20,1) t )
    and SUBSTRING(code, 1, 3) !='688'
    order by code , 日期 asc
    ) t1 , (select @i:=1, @j:=0, @precode:='', @prevol:='', @pre0dat:='', @pre0prise:='', @pre0vol:='' ) t2 
    ) t
    left join akshare.stock_baseinfo t2 on t.code = t2.symbol where 
    rank  >=4   -- 成交量几日连续缩量
    and 日期 = DATE_SUB(CURDATE(), INTERVAL {0} day)  -- 限定查看不同日期数据
    -- and round((收盘 - pre0prise)/pre0prise *100) <=8  -- 连续缩量，股价涨跌幅不能超过阈值
    -- and round((收盘 - pre0prise)/pre0prise *100) >=-5
    # ) t
    # having beishu >=2.8   -- 开始缩量日期是前一日成交量的倍数
    '''.format(gap_currentdate)
    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
    table_name = 'vol_decrese'
    # 插入数据
    df.to_sql(table_name, engine, if_exists='append', index=False)
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
if __name__ == '__main__':
    for i in range(10):
        df = get_data(i)
        print(df.head(5))
        print(len(df))
        print(i, "-------------------")












