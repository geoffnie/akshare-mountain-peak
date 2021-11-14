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

stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="603045", period="daily", start_date='20210920', end_date='20211019',
                                        )
# stock_zh_a_hist_df['code'] = code
# print(code)
# print(date)
print(stock_zh_a_hist_df)

def get_powerful_stock_lastest100workday_day():
    sql = '''select * from (
    select * from akshare.trade_date_hist_sina t where trade_date <= CURRENT_DATE()  order by trade_date desc limit 100  -- 查看最近100个交易日强势股
    ) t1
    left join (
    select 日期 as date, group_concat(code) as code_list from (
    select 
    case when @precode = t1.code COLLATE utf8_unicode_ci and round(replace(t1.涨跌幅, '%', ''), 1) >=9.8 then @i:=@i+1 else @i:=1 end as chg_rank  -- 定义涨跌幅超过9.8才记录为强势股
    , @precode:=t1.code
    , t1.* from (
    select  *  from akshare.history_trade_day_qfq t where 日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURDATE() order by trade_date  desc limit 100,1) t) -- 定义100个工作日
    order by code ,日期 asc
    ) t1, (select @i:=1, @precode:="") t2
    ) t where chg_rank>5    -- 定义连续超过5个交易日才算强势股
    group by 日期 order by 日期 desc
    ) t2 on t1.trade_date = t2.date
    order by t1.trade_date desc  '''

    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
    return df


def get_powerful_stock_lastest100workday_month():
    sql = '''select MONTH(日期 ) as month, group_concat(distinct code) as code_list from (
    select 
    case when @precode = t1.code COLLATE utf8_unicode_ci and round(t1.涨跌幅, 1) >=9.8 then @i:=@i+1 else @i:=1 end as chg_rank  -- 定义涨跌幅超过9.8才记录为强势股
    , @precode:=t1.code
    , t1.* from (
    select  *  from akshare.history_trade_day_qfq t where 日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURDATE() order by trade_date  desc limit 100,1) t) 
    order by code ,日期 asc
    ) t1, (select @i:=1, @precode:="") t2
    ) t where chg_rank>5    -- 定义连续超过5个交易日才算强势股
    group by MONTH(日期 ) order by MONTH(日期 ) desc'''

    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
    return df

def get_powerful_stock_lastest100workday_detail():
    sql = '''-- 获取强势股明细
    select *
    , case when decre_vol_rank >=4 then 1 else 0 end as decre_vol_mark
    from (
    select 
    case when @precode = t1.code and @prevol >= t1.成交量 then @i:=@i+1 else @i:=1 end as decre_vol_rank
    , @precode:=t1.code
    , @prevol:=t1.成交量
    , t1.*
    from (
    select  t1.*, t2.name ,t2.list_date from  (
    select * from (
    select  * from akshare.history_trade_day_qfq t where
     日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURDATE() order by trade_date  desc limit 140,1) t)  -- 定义之前的40个交易日
     ) t where  code in ( select distinct code  from (
    select 
    case when @precode = t1.code COLLATE utf8_unicode_ci and round(replace(t1.涨跌幅, '%', ''), 1) >=9.8 then @i:=@i+1 else @i:=1 end as chg_rank  -- 定义涨跌幅超过9.8才记录为强势股
    , @precode:=t1.code
    , t1.* from (
    select  *  from akshare.history_trade_day_qfq t where 日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURDATE() order by trade_date  desc limit 100,1) t) 
    order by code ,日期 asc
    ) t1, (select @i:=1, @precode:="") t2
    ) t where chg_rank>5    -- 定义连续超过5个交易日才算强势股
    ) 
     ) t1
    left join 
    akshare.stock_baseinfo t2
    on 
    t1.code = t2.symbol 
    where datediff(CURRENT_DATE(), t2.list_date) >=30  -- 新股上市需要满足超过1个月
    ) t1, (select @i:=1, @precode:='', @prevol:=0) t2
    ) t'''

    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
    return df


if __name__ == "__main__":
    df = get_powerful_stock_lastest100workday_detail()
    print(df)








































