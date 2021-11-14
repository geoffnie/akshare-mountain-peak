# 获取最近2个工作日成交量比之前5个成交日累计成交量大的股票池，可计算最近的情况

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


def get_data(gap_currentdate=0 ):

    sql = '''
-- 连续两日成交量比之前5日累计成交量更大且股价低于20的股票，乃至从吸筹开始到今天股价波动情况
select t1.*, t2.name, t2.list_date
, round((t3.收盘 - t1.收盘)/t1.收盘*100,1) as ratio
# , round((select max(涨跌幅) from akshare.history_trade_day_qfq where 日期 >= DATE_SUB(CURDATE(), INTERVAL {0} day) and code=t1.code ),1) as max_ratio
# , round((select min(涨跌幅) from akshare.history_trade_day_qfq where 日期 >= DATE_SUB(CURDATE(), INTERVAL {0} day) and code=t1.code ),1) as min_ratio
# , round((select avg(涨跌幅) from akshare.history_trade_day_qfq where 日期 >= DATE_SUB(CURDATE(), INTERVAL {0} day) and code=t1.code ),1) as avg_ratio
,  DATE_SUB(CURDATE(), INTERVAL {0} day) as dat
from (
select *
, round((收盘 - pre0prise)/pre0prise *100) as ratio
, round(((select max(收盘) from akshare.history_trade_day_qfq where 日期 >= t.pre0dat COLLATE utf8_unicode_ci and code=t.code) - pre0prise)/pre0prise*100) as next_ratio
, (select 成交量 from akshare.history_trade_day_qfq where 日期 < t.pre0dat COLLATE utf8_unicode_ci and code=t.code order by 日期 desc limit 1)  as pre01vol
, case when vol_accu2 >= vol_accu10 then 10
       when vol_accu2 >= vol_accu9 then 9
       when vol_accu2 >= vol_accu8 then 8
       when vol_accu2 >= vol_accu7 then 7
       when vol_accu2 >= vol_accu6 then 6
       when vol_accu2 >= vol_accu5 then 5
       else 0 end as vol_accu_incre
from (
select case when @precode =t1.code COLLATE utf8_unicode_ci  then @i:=@i+1 else @i:=0 end prerank
, case when @precode =t1.code COLLATE utf8_unicode_ci  then @j:= @j + t1.成交量 else @j:=0 end vol_accu
, case when @precode =t1.code COLLATE utf8_unicode_ci and @i>=10 then @pre2vol_accu:= @pre2vol_accu + t1.成交量 else @pre2vol_accu:=0 end vol_accu2
, case when @precode =t1.code COLLATE utf8_unicode_ci and @i>=5 and @i<=9 then @pre5vol_accu:= @pre5vol_accu + t1.成交量
       when @precode =t1.code COLLATE utf8_unicode_ci and @i>9 then @pre5vol_accu:= @pre5vol_accu else @pre5vol_accu:=0 end vol_accu5
, case when @precode =t1.code COLLATE utf8_unicode_ci and @i>=4 and @i<=9 then @pre6vol_accu:= @pre6vol_accu + t1.成交量
       when @precode =t1.code COLLATE utf8_unicode_ci and @i>9 then @pre6vol_accu:= @pre6vol_accu else @pre6vol_accu:=0 end vol_accu6
, case when @precode =t1.code COLLATE utf8_unicode_ci and @i>=3 and @i<=9 then @pre7vol_accu:= @pre7vol_accu + t1.成交量
       when @precode =t1.code COLLATE utf8_unicode_ci and @i>9 then @pre7vol_accu:= @pre7vol_accu else @pre7vol_accu:=0 end vol_accu7
, case when @precode =t1.code COLLATE utf8_unicode_ci and @i>=2 and @i<=9 then @pre8vol_accu:= @pre8vol_accu + t1.成交量
       when @precode =t1.code COLLATE utf8_unicode_ci and @i>9 then @pre8vol_accu:= @pre8vol_accu else @pre8vol_accu:=0 end vol_accu8
, case when @precode =t1.code COLLATE utf8_unicode_ci and @i>=1 and @i<=9 then @pre9vol_accu:= @pre9vol_accu + t1.成交量
       when @precode =t1.code COLLATE utf8_unicode_ci and @i>9 then @pre9vol_accu:= @pre9vol_accu else @pre9vol_accu:=0 end vol_accu9
, case when @precode =t1.code COLLATE utf8_unicode_ci and @i>=0 and @i<=9 then @pre10vol_accu:= @pre10vol_accu + t1.成交量 
       when @precode =t1.code COLLATE utf8_unicode_ci and @i>9 then @pre10vol_accu:= @pre10vol_accu else @pre10vol_accu:=0 end vol_accu10
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
select * from akshare.history_trade_day_qfq where 日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= DATE_SUB(CURDATE(), INTERVAL {0} day)  order by trade_date desc limit 11,1) t )
and 日期 <= DATE_SUB(CURDATE(), INTERVAL {0} day)
and SUBSTRING(code, 1, 3) !='688'
order by code , 日期 asc
) t1 , (select @i:=0, @j:=0, @precode:='', @prevol:='', @pre0dat:='', @pre0prise:='', @pre0vol:='', @pre2vol_accu:='', @pre5vol_accu:='', @pre6vol_accu:='', @pre7vol_accu:='', @pre8vol_accu:='', @pre9vol_accu:='', @pre10vol_accu:='' ) t2 
) t 
where 
日期 = DATE_SUB(CURDATE(), INTERVAL {0} day)  -- 限定查看不同日期数据
and 收盘 <=20      -- 限定股价，低价股才有成长空间
having vol_accu_incre >=5 
) t1 
left join akshare.stock_baseinfo t2 on t1.code = t2.symbol
left join (select * from akshare.history_trade_day_qfq where 日期 = (select * from (select * from akshare.trade_date_hist_sina where trade_date <=CURDATE() order by trade_date desc limit 1) t) ) t3 on t1.code = t3.code -- 当前股价    
    '''.format(gap_currentdate)
    # 获取查询数据
    df = pd.read_sql_query(sql, engine)


    table_name = 'vol_accu_incre'
    # 插入数据
    df.to_sql(table_name, engine, if_exists='append', index=False)
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
    for i in range(10):
        df = get_data(i)
        print(df.head(5))
        print(len(df))
        print(i, "-------------------")












