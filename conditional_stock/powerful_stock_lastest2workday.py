

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
engine = create_engine('mysql+pymysql://root:root@yunfuwu01/akshare?charset=utf8',
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



def get_powerful_stock_lastest2workday_day():
    sql = '''select *
    , case when decre_vol_rank >=4 then 1 else 0 end as decre_vol_mark
    from (
    select 
    case when @precode = t1.code COLLATE utf8_unicode_ci and @prevol >= t1.成交量 COLLATE utf8_unicode_ci then @i:=@i+1 else @i:=1 end as decre_vol_rank
    , @precode:=t1.code
    , @prevol:=t1.成交量
    , t1.*
    from (
    select t1.*, t2.name from (
    select * from akshare.history_trade_day_qfq
    where code in (
    select distinct code from (
    select *
    , round((收盘 - preprice)/preprice * 100,1) as ratio
    from (
    select 
    case when @precode = '' or @precode!= t1.code COLLATE utf8_unicode_ci then @preprice:= t1.收盘 else @preprice:= @preprice end as preprice
    -- , @preprice:= @preprice
    , @precode:= t1.code
    , t1.*
    from (
    select * from akshare.history_trade_day_qfq where 日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= date_sub(CURRENT_DATE(), interval 1 day ) order by trade_date desc limit 2,1 ) t) order by code , 日期 asc
    ) t1, (select @precode:='', @preprice:=0) t2
    ) t
    where 日期 = date_sub(CURRENT_DATE(), interval 1 day )
    having ratio >=16
    ) t
    )
    and 日期 > (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURDATE() order by trade_date desc limit 100,1 ) t)
    order by code, 日期 asc
    ) t1 
    left join 
    akshare.stock_baseinfo t2 on t1.code = t2.symbol 
    ) t1, (select @i:=1, @precode:='', @prevol:=0) t2
    ) t
      '''

    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
    return df





if __name__ == "__main__":
    df = get_powerful_stock_lastest2workday_day()
    print(df)








































