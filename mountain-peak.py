# -*- coding: utf-8 -*-
"""
Created on Mon May 17 09:33:05 2021

@author: jiuxin
"""

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

#qfq_factor_df = ak.stock_zh_a_daily(symbol="sz000002", adjust="qfq-factor")
#print(qfq_factor_df)
#print(time.time())
#tim = time.strftime('%Y%m%d%H', time.localtime(time.time()))
#print(tim)




#today = datetime.date.today()
#quarter_end_day = datetime.date(today.year,today.month - (today.month - 1) % 3 +2, 1) + relativedelta(months=1,days=-1)
#quarter_end_day.isoformat()  
#print(quarter_end_day.isoformat() )
#
#
#dat = datetime.datetime.strptime('2021-02-18', "%Y-%m-%d")
#quarter_end_day = datetime.date(dat.year,dat.month - (dat.month - 1) % 3 +2, 1) + relativedelta(months=1,days=-1)
#quarter_end_day.isoformat()  
#print(quarter_end_day.isoformat() )
#print(dat)
#print(datetime.date.today().year-3)
#print(today.strftime("%Y%m%d"))
#print(str(today.strftime("%Y%m%d")))
#
#
#dat_list = []






# =============================================================================
# data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
# print(data)
# #print(data.iloc[447:450,:])
# #df = ts.pro_bar(ts_code='{0}'.format('001201.SZ'), adj='qfq', start_date='20180101', end_date='20181011')
# #print(df)
# 
# for i,tscode in enumerate(data['ts_code']):
# 
#     df = ts.pro_bar(ts_code='{0}'.format(tscode), adj='qfq', start_date='20180101', end_date='20181011')
#     if df is not None :
#         print(df.head(2))
#     print("========================" + str(i))
# #    if i % 400 == 399:
# #        time.sleep(60)
# =============================================================================




#每日早晨8点开始跑任务，获取今年股市日历
def get_trade_cal(is_init=False):
    today = str(datetime.date.today()).replace('-','')
    table_name = 'trade_cal'
    this_year_end = datetime.datetime(datetime.date.today().year + 1, 1, 1) - datetime.timedelta(days=1)
    this_year_end = str(this_year_end).replace("-","")[:10]
    if is_init:
#        sql = 'delete from akshare.trade_cal'
#        dml_mysql(sql)
        df = pro.trade_cal(exchange='', start_date='20180101', end_date=this_year_end)
        df.to_sql(table_name, engine, if_exists='replace',index= False)
    else:
        df = pro.trade_cal(exchange='', start_date=today, end_date=today)
        df.to_sql(table_name, engine, if_exists='append',index= False)
    print('get trade_cal success')



#工具类，删除数据
def dml_mysql(sql):
    conn = pymysql.connect(host='yunfuwu01', port=3306, user='root', passwd='root', db='akshare')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()



@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_stock_baseinfo():
    sql = 'truncate table akshare.stock_baseinfo'
    dml_mysql(sql)
    ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    table_name = 'stock_baseinfo'
    data.to_sql(table_name, engine, if_exists='append',index= False)

# get_stock_baseinfo()

def get_realtime_price():
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    
#    stock_zh_a_spot_df = ak.stock_zh_a_spot()
    table_name = 'realtime_price'
    data.to_sql(table_name, engine, if_exists='replace',index= False)

#    sql = 'select distinct 代码 as code, 名称 as name from akshare.realtime_price'
#    df = pd.read_sql_query(sql, engine)

#get_realtime_price()

def get_day_price(dat_start = str(datetime.date.today()).replace("-", ""), dat_end = str(datetime.date.today()).replace("-", "")):
      
    sql = 'select code from akshare.stock_code'
    #获取查询数据
    df = pd.read_sql_query(sql, engine)
    table_name = 'day_price'
    for stock in df.iloc[:, 0]:
        
        print(stock.lower())
        try:
            print(stock.lower())
            stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol=stock.lower(), start_date=dat_start, end_date=dat_end, adjust="qfq")
            stock_zh_a_daily_qfq_df['code'] = stock.lower()
#            stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol='{0}'.format(stock), start_date='20210101', end_date='20210519', adjust="qfq")
            stock_zh_a_daily_qfq_df.to_sql(table_name, engine, if_exists='append',index= True)
            print("-------------")
        except Exception:
            print("===========")
            continue
    

#get_day_price('20210101', '20210519')
#get_day_price()            
#sql = 'select code from akshare.stock_code'
#    #获取查询数据
#df = pd.read_sql_query(sql, engine)
#for stock in df.iloc[:, 0]:
#    stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol=stock, start_date='20210101', end_date='20210519', adjust="qfq")
#    print(stock_zh_a_daily_qfq_df)


#获取个股每日资金流向
# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_half_day_fund_flow():
    
    stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
#    print(stock_fund_flow_individual_df)
    if datetime.datetime.now().hour == 11 or datetime.datetime.now().hour == 12: 
        stock_fund_flow_individual_df['dat'] = '{0}12'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    else:
        stock_fund_flow_individual_df['dat'] = '{0}'.format(time.strftime('%Y%m%d%H', time.localtime(time.time())))
#    stock_fund_flow_individual_df['dat'] = '2021060312'
    
    stock_fund_flow_individual_df = stock_fund_flow_individual_df.drop_duplicates(subset=['股票代码', 'dat'], keep='first')
    stock_fund_flow_individual_df['股票代码'] = [str(x).zfill(6) for x in stock_fund_flow_individual_df['股票代码'].to_list()]
    
    table_name = 'half_day_fund_flow'
    #插入数据
    stock_fund_flow_individual_df.to_sql(table_name, engine, if_exists='append',index= False)


#    filter1 = stock_fund_flow_individual_df['股票简称'].isin({'佳力图' ,'旗天科技','金种子酒','宋城演艺', '诚迈科技', '复星医药', '岳阳林纸', '长盈精密', '我爱我家', '出版传媒'})
#    print(stock_fund_flow_individual_df[filter1].sort_values('涨跌幅',ascending = False))
# get_half_day_fund_flow()

#获取概念每日资金流向
# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_half_day_fund_flow_concept():
    
    stock_fund_flow_concept_df = ak.stock_fund_flow_concept(symbol="即时")
    stock_fund_flow_concept_df.columns = ['序号', '行业', '行业指数', '概念涨跌幅', '流入资金', '流出资金', '净额', '公司家数', '领涨股', '个股涨跌幅','当前价']
    if datetime.datetime.now().hour == 11 or datetime.datetime.now().hour == 12:
        stock_fund_flow_concept_df['dat'] = '{0}12'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    else:
        stock_fund_flow_concept_df['dat'] = '{0}'.format(time.strftime('%Y%m%d%H', time.localtime(time.time())))
#    stock_fund_flow_concept_df['dat'] = '2021060312'
    print(stock_fund_flow_concept_df)

    stock_fund_flow_concept_df = stock_fund_flow_concept_df.drop_duplicates(subset=['行业', 'dat'], keep='first')
    stock_fund_flow_concept_df['行业'] = [str(x).zfill(6) for x in stock_fund_flow_concept_df['行业'].to_list()]
    
        
    table_name = 'half_day_fund_flow_concept'
    #插入数据
    stock_fund_flow_concept_df.to_sql(table_name, engine, if_exists='append',index= False)

# get_half_day_fund_flow_concept()

#获取行业每日资金流向
# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_half_day_fund_flow_industry():
    
    stock_fund_flow_industry_df = ak.stock_fund_flow_industry(symbol="即时")
    stock_fund_flow_industry_df.columns = ['序号', '行业', '行业指数', '概念涨跌幅', '流入资金', '流出资金', '净额', '公司家数', '领涨股', '个股涨跌幅','当前价']
    if datetime.datetime.now().hour == 11 or datetime.datetime.now().hour == 12:
        stock_fund_flow_industry_df['dat'] = '{0}12'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    else:
        stock_fund_flow_industry_df['dat'] = '{0}'.format(time.strftime('%Y%m%d%H', time.localtime(time.time())))
#    stock_fund_flow_industry_df['dat'] = '2021060312'
    print(stock_fund_flow_industry_df)

    stock_fund_flow_industry_df = stock_fund_flow_industry_df.drop_duplicates(subset=['行业', 'dat'], keep='first')
    stock_fund_flow_industry_df['行业'] = [str(x).zfill(6) for x in stock_fund_flow_industry_df['行业'].to_list()]
    
    
    table_name = 'half_day_fund_flow_industry'
    #插入数据
    stock_fund_flow_industry_df.to_sql(table_name, engine, if_exists='append',index= False)
    
# get_half_day_fund_flow_industry()

# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_yjbb_season(is_init=False):
    dat_list = []
    if is_init:
        for year in range(datetime.date.today().year-3, datetime.date.today().year+1):
            if year == datetime.date.today().year:
                month_0 = datetime.date.today().month - 3
                if month_0 < 0:
                    month_0 = 1
            else:
                month_0 = 13
    
            for month in range(1,month_0,3):
                if month_0 == 1:
                    continue
                else:
                    dat = datetime.date(year, month, 1)
                quarter_end_day = datetime.date(dat.year,dat.month - (dat.month - 1) % 3 +2, 1) + relativedelta(months=1,days=-1)
                quarter_end_day.isoformat()  
                dat_list.append(str(quarter_end_day.strftime("%Y%m%d")))  
    else:
        if datetime.date.today().month <4:
            dat = datetime.date(datetime.date.today().year-1, 10, 1)
        else:
            dat = datetime.date(datetime.date.today().year, datetime.date.today().month-3, 1)
        quarter_end_day = datetime.date(dat.year,dat.month - (dat.month - 1) % 3 +2, 1) + relativedelta(months=1,days=-1)
        quarter_end_day.isoformat()  
        dat_list.append(str(quarter_end_day.strftime("%Y%m%d")))  
    for season_date in dat_list:
        print(season_date)
        stock_em_yjbb_df = ak.stock_em_yjbb(date=season_date)
        print(stock_em_yjbb_df)
        stock_em_yjbb_df['dat'] = '{0}'.format(season_date)
                
        
        table_name = 'yjbb_season'
        #先删除季度数据
        dml_sql = "delete from akshare.yjbb_season where dat='{0}'".format(season_date)
        dml_mysql(dml_sql)
        #插入数据
        stock_em_yjbb_df.to_sql(table_name, engine, if_exists='append',index= False)    



# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_yjkb_season(is_init=False):
    dat_list = []
    if is_init:
        for year in range(datetime.date.today().year-3, datetime.date.today().year+1):
            if year == datetime.date.today().year:
                month_0 = datetime.date.today().month - 3
                if month_0 < 0:
                    month_0 = 1
            else:
                month_0 = 13
    
            for month in range(1,month_0,3):
                if month_0 == 1:
                    continue
                else:
                    dat = datetime.date(year, month, 1)
                quarter_end_day = datetime.date(dat.year,dat.month - (dat.month - 1) % 3 +2, 1) + relativedelta(months=1,days=-1)
                quarter_end_day.isoformat()  
                dat_list.append(str(quarter_end_day.strftime("%Y%m%d")))  
    else:
        if datetime.date.today().month <4:
            dat = datetime.date(datetime.date.today().year-1, 10, 1)
        else:
            dat = datetime.date(datetime.date.today().year, datetime.date.today().month-3, 1)
        quarter_end_day = datetime.date(dat.year,dat.month - (dat.month - 1) % 3 +2, 1) + relativedelta(months=1,days=-1)
        quarter_end_day.isoformat()  
        dat_list.append(str(quarter_end_day.strftime("%Y%m%d")))  
    for season_date in dat_list:
        stock_em_yjkb_df  = ak.stock_em_yjkb(date=season_date)
        print(stock_em_yjkb_df )
        stock_em_yjkb_df ['dat'] = '{0}'.format(season_date)
                
        
        table_name = 'yjkb_season'
        #先删除季度数据
        dml_sql = "delete from akshare.yjkb_season where dat='{0}'".format(season_date)
        dml_mysql(dml_sql)
        #插入数据
        stock_em_yjkb_df .to_sql(table_name, engine, if_exists='append',index= False)  


#stock_em_tfp_df = ak.stock_em_tfp(trade_date="2021-05-27")
#print(stock_em_tfp_df)
#
#tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
#print(tool_trade_date_hist_sina_df)
#
#stock_info_sz_delist_df = ak.stock_info_sz_delist(indicator="终止上市公司")
#print(stock_info_sz_delist_df.sort_values('终止上市日期').reset_index())
#
#
#stock_info_sh_delist_df = ak.stock_info_sh_delist(indicator="终止上市公司")
#print(stock_info_sh_delist_df.sort_values('QIANYI_DATE').reset_index())


#获取同花顺概念板块
# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_ths_board_concept_name():
    
    board_concept_name = ak.stock_board_concept_name_ths()
    board_concept_name['code'] = [url.split("/")[-2] for url in board_concept_name['url'].to_list()]
    print(board_concept_name)

    table_name = 'ths_board_concept_name'
    #插入数据
    board_concept_name.to_sql(table_name, engine, if_exists='replace',index= False)


#获取同花顺概念相关股票
# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_ths_board_concept_stock():
    
    dml_mysql("insert into akshare.board_concept_cons_stock_bak select * from akshare.ths_board_concept_stock")
    dml_mysql("truncate table akshare.ths_board_concept_stock")
    
    board_concept_name = ak.stock_board_concept_name_ths()
    for i,name in enumerate(board_concept_name['name'].to_list()):
        print(i)
        time.sleep(5)
        print()
        df = ak.stock_board_concept_cons_ths(symbol="{0}".format(name))
        df['dat'] = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
        df['concept_name'] = name
    
        table_name = 'ths_board_concept_stock'
        #插入数据
        df.to_sql(table_name, engine, if_exists='append',index= False)

# get_ths_board_concept_name()
# get_ths_board_concept_stock()



# import akshare as ak
# stock_history_dividend_detail_df = ak.stock_history_dividend_detail(indicator="分红", stock="600026", date="")
# print(stock_history_dividend_detail_df)


#获取股票分红数据
#@scheduler.scheduled_job('cron', hour='15')
def get_xl_stock_history_dividend_detail():
    # dml_mysql("insert into akshare.board_concept_cons_stock_bak select * from akshare.ths_board_concept_stock")
    # dml_mysql("truncate table akshare.ths_board_concept_stock")
    

    sql = "select distinct LPAD(股票代码,6,'0') as code from akshare.half_day_fund_flow where SUBSTRING(LPAD(股票代码,6,'0'), 1, 3) != '688'"
    df = pd.read_sql_query(sql, engine)
    
    for i,code in enumerate(df['code'].to_list()):
        time.sleep(5)
        print(code,i)
        try:
            stock_history_dividend_detail_df = ak.stock_history_dividend_detail(indicator="分红", stock="{0}".format(code), date="")
            stock_history_dividend_detail_df['code'] = '{0}'.format(code)
            print("OK-----------------")
        except Exception as e:
            print(e)
        table_name = 'xl_stock_history_dividend_detail'
        #插入数据
        stock_history_dividend_detail_df.to_sql(table_name, engine, if_exists='append',index= False)
        
        
# get_xl_stock_history_dividend_detail()

@scheduler.scheduled_job('cron', hour='17', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_daily_basic():
    engine = create_engine('mysql+pymysql://root:root@yunfuwu01/akshare?charset=utf8')
    
    ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
    pro = ts.pro_api()
    
    df = pro.daily_basic(ts_code='', trade_date='20211025', fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,total_share,float_share,free_share,total_mv,circ_mv')
#    print(df.head(5).iloc[0,1:].tolist())
    table_name = 'daily_basic'
    print(df)
    #插入数据
    df.to_sql(table_name, engine, if_exists='append',index= False)


# get_daily_basic()

# @scheduler.scheduled_job('cron', hour='8', minute='0')
def schedule_0800():
    get_trade_cal(True)





#stock_fund_flow_industry_df = ak.stock_fund_flow_industry(symbol="即时")
#print(stock_fund_flow_industry_df)
#print(stock_fund_flow_industry_df.columns)
#
#stock_fund_flow_industry_df.columns = ['序号', '行业', '行业指数', '概念涨跌幅', '流入资金', '流出资金', '净额', '公司家数', '领涨股', '个股涨跌幅','当前价']
#print(stock_fund_flow_industry_df.columns)


#get_daily_basic()


#get_yjbb_season(is_init=True)
#get_yjkb_season(is_init=True)    









#print(datetime.datetime.strptime('13.05.2021 00:00:00', "%d.%m.%Y %H:%M:%S"))

#if __name__ == '__main__':
#    scheduler.start()
#    get_half_day_fund_flow()
#    get_half_day_fund_flow_concept()
#    get_half_day_fund_flow_industry()
    
    
##    scheduler.start()
#file_timestamp = 1621440600.0
#file_timestamp = time.localtime(file_timestamp)
#print(time.strftime('%Y-%m-%d %H:%M:%S', file_timestamp))
##print(time.strftime('%Y-%m-%d %H:%M:%S', 1621440600))
#print(file_timestamp)
#
#
#
#print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
#print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
#print(datetime.datetime.strptime('24.05.2021 08:00:00', "%d.%m.%Y %H:%M:%S"))
#print(time.mktime(time.strptime('2021-05-24 08:00:00',"%Y-%m-%d %H:%M:%S")))
#print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1621814400)))
#print(time.mktime(time.strptime('2021-05-24 03:00:00',"%Y-%m-%d %H:%M:%S")))
#
#print(time.time())
##1621827444
#
##1621814400










# =============================================================================
# sql = 'select * from akshare.history_trade_day_tmp order by code,日期'
# #获取查询数据
# df = pd.read_sql_query(sql, engine)
# #df = df.head(100)
# #print(df)
# list_sort_amount = []
# for i in range(len(df)):
# #    print(df.iloc[i,1], df.iloc[i-1,1])
# #    print(df.iloc[i,11] , df.iloc[i-1,11])
# #    print("==========")
#     if i ==0 :
#         value = 0
#     else:
#         if df.iloc[i,1] != df.iloc[i-1,1]:
#             value = 0
#         else:
#             if  df.iloc[i,11] < df.iloc[i-1,11]:
#                 value = list_sort_amount[-1] + 1
#             else:
#                 value = 0
#     list_sort_amount.append(value)
# 
# df['sort_amount'] = list_sort_amount
# 
# table_name = 'history_trade_day_tmp1'
# df.to_sql(table_name, engine, if_exists='replace',index= False) 
# 
# print("=============")
# =============================================================================



# =============================================================================
# tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
# print(tool_trade_date_hist_sina_df)
# 
# table_name = 'trade_date_hist_sina'
# 
# tool_trade_date_hist_sina_df.to_sql(table_name, engine, if_exists='replace',index= False)
# =============================================================================





#def get_flow_anasys():
    
    

# =============================================================================
# sql = "select trade_date from akshare.trade_date_hist_sina where trade_date >= '2021-05-17' and trade_date <CURRENT_DATE() "
# 
# df = pd.read_sql_query(sql, engine)
# 
# print(df)
# for i in df.iloc[:,0]:
#     print(i.replace("-","") + '15')
#     dat = i.replace("-","") + '15'
# 
# 
# 
# 
#     sql_data = '''select 
#     -- fw.*, yj.net_income, yj.net_income_ratio, yj.net_income_ratio2, yj.up_ratio 
#     *
#     from 
#     (
#     select t1.*
#     ,t2.total_mv
#     , round(t1.big_amount_flow/t2.total_mv *100, 2) as total_mv_ratio
#     -- , round(t1.big_amount_flow/t2.circ_mv *100,3) as circ_mv_ratio 
#     , round(t1.all_amount/t2.total_mv *100,2) as all_amount_ratio 
#     , round(t1.all_amount/t1.big_amount_flow) as big_amount_ratio
#     from (
#     select *
#     ,round(case when instr(大单流入, '万') > 0 then  replace(大单流入, '万', '')   when instr(大单流入, '亿') > 0 then replace(大单流入, '亿', '') * 10000 else 大单流入 / 10000 end , 0) as big_amount_flow 
#     ,round(case when instr(成交额, '万') > 0 then  replace(成交额, '万', '')   when instr(成交额, '亿') > 0 then replace(成交额, '亿', '') * 10000 else 成交额 / 10000 end, 0) as all_amount
#     ,round(replace(涨跌幅, '%', ''), 1) as up_down_ratio
#     ,LPAD(股票代码,6,'0') as code
#     from half_day_fund_flow where 
#     dat ='{0}' 
#     ) t1
#     left join 
#     (
#     select left(ts_code ,6) as ts_code, trade_date, round(total_mv) as total_mv, round(circ_mv) as circ_mv  from akshare.daily_basic 
#     ) t2
#     on t1.code  = t2.ts_code
#     order by round(t1.big_amount_flow/t2.total_mv *1000, 3) desc
#     ) fw   -- 获取每只股的资金流向信息
#     left join
#     tmp.yj_info yj
#     on fw.code = yj.code
#     -- where yj.up_ratio >1000
#     -- and big_amount_flow >4000
#     -- where big_amount_ratio <=10
#     where SUBSTRING(fw.code,1,3) !='688'
#     order by 
#     up_down_ratio desc,
#     -- yj.up_ratio desc,
#     total_mv_ratio desc,
#     big_amount_ratio
#     -- dat desc'''.format(dat)
#     df_flow_ana = pd.read_sql_query(sql_data, engine)
# #    print(df_flow_ana)
#     table_name_flow = 'flow_anasys'
#     df_flow_ana.to_sql(table_name_flow, engine, if_exists='append',index= False)
# =============================================================================





def get_board_concept_cons_stock():

    #获取各个概念下的股票，或者获取各个股票对应的概念
    df = ak.stock_board_concept_name_ths()
    print(df)
    print(len(df))
    print("---------------")
    
    
    for i,name in enumerate(df['name'].values):
        if i < 251:
            continue
        print(i)
        print(name)
        stock_board_concept_cons_ths_df = ak.stock_board_concept_cons_ths(symbol=name)
        stock_board_concept_cons_ths_df['concept_name'] = name
        dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
        stock_board_concept_cons_ths_df['dat'] = dat
        table_name = 'board_concept_cons_stock'
        #插入数据
        stock_board_concept_cons_ths_df.to_sql(table_name, engine, if_exists='append',index= False)    
        print(len(stock_board_concept_cons_ths_df))
        print("=========================")
        if i % 50 ==0:
            time.sleep(60)



#get_board_concept_cons_stock()






print(datetime.datetime.now())
print(type(datetime.datetime.now()))
print(datetime.datetime.now().hour)


# get_half_day_fund_flow()
# get_half_day_fund_flow_concept()
# get_half_day_fund_flow_industry()


# get_yjbb_season()
# get_yjkb_season()



# stock_zh_a_spot_df = ak.stock_zh_a_spot()
# #print(stock_zh_a_spot_df)
# filter1 = stock_zh_a_spot_df['名称'].isin({
# '新疆交建'
# ,'捷成股份'
# ,'海南橡胶'
# ,'洛阳钼业'
# ,'中金岭南'
# ,'钢研高纳'
# ,'星源材质'
# ,'东软载波'
# ,'丽尚国潮'
# ,'济南高新'
# ,'上港集团'
# ,'华胜天成'
# ,'海欣股份'
# ,'广汇汽车'
# ,'韶钢松山'
# ,'华西股份'
# ,'亚盛集团'
# ,'粤电力A'
# ,'康尼机电'
# ,'绿茵生态'
# ,'兰花科创'
# ,'新大正'
# ,'震安科技'
# ,'吉大正元','宜宾纸业',
# '天汽模',
# '章源钨业',  '英科医疗', '湘电股份',
# '金陵体育','吉大正元',
# '宝新能源',
# '宜宾纸业', '人民同泰', '神马股份', '天汽模', '中信特钢', '海得控制', '隆盛科技', '恒润股份', '章源钨业',  '英科医疗', '湘电股份', '宝新能源', '三元股份', '研奥股份', '长白山', '飞利信', '冀中能源', '兰花科技', '海特高新', '粤电力A','新世界' , '达实智能', '福田汽车', 'TCL科技', '西部矿业', '应流股份','北京银行', '飞利信', '佳禾智能', '金运激光', '润和软件', '吉艾科技', '西藏矿业',  '豆神教育', '山煤国际', '广大证券',  '宝钢股份', '蒙泰高新', '闽东电力', '佳力图' ,'旗天科技','金种子酒','宋城演艺', '诚迈科技', '复星医药', '岳阳林纸', '长盈精密', '我爱我家', '出版传媒', '国光电器', '宸展光电', '秋田微', '天源股份', '泰福泵业', '易联众'
# ,'吉艾科技'
# ,'机器人'
# ,'易事特'
# ,'吉艾科技'
# ,'创业黑马'
# ,'恒星科技'
# ,'金辰股份' 
# ,'上海能源'
# ,'中国重工'
# ,'瑞丰新材'
# ,'佳创视讯'
# ,'万马科技'
# ,'冰轮环境'
# ,'康龙化成'
# ,'彤程新材'
# ,'鹏辉能源' 
# ,'万华化学'
# ,'三七互娱'
# ,'万盛股份'
# ,'中国黄金'
# ,'清水源' 
# ,''
# ,''
# ,''
# ,''
# ,''       })
# print(stock_zh_a_spot_df[filter1].sort_values('涨跌幅',ascending = False))





#获取股票代码
def get_code():
    sql = "select distinct  LPAD(股票代码,6,'0') as code from akshare.half_day_fund_flow "
    conn = pymysql.connect(host='yunfuwu01', port=3306, user='root', passwd='root', db='akshare')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    data_list = cursor.fetchall()
    cursor.close()
    data = tuple(row[0] for row in data_list)
    conn.close()
    return data



def get_stock_zh_a_hist(code, date, cnt, i):
    try:
        engine = create_engine('mysql+pymysql://root:root@yunfuwu01/akshare?charset=utf8',
                               encoding='utf-8',
                               echo=False,
                               pool_pre_ping=True,
                               pool_recycle=3600)
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="{0}".format(code), period="daily", start_date=date, end_date=date, adjust="qfq")
        stock_zh_a_hist_df['code']=code
        print(stock_zh_a_hist_df)
        name_df = pd.read_sql_query("select symbol as code, name from akshare.stock_baseinfo", engine)
        stock_zh_a_hist_df['name'] = name_df[name_df['code'] == code]['name'].to_list()[0]
        # print(code)
        # print(date)
        print(stock_zh_a_hist_df)
    
        engine = create_engine('mysql+pymysql://root:root@yunfuwu01/akshare?charset=utf8')
           
        table_name = 'history_trade_day_qfq'
        #插入数据
        stock_zh_a_hist_df.to_sql(table_name, engine, if_exists='append',index= False) 
        print("已插入数据！总数：" + str(cnt) + " | i:" + str(i))
    except Exception as e:   
        print("未插入")
        print(e)
        # continue
# get_stock_zh_a_hist('000001', '20211026', 3, 2)

# @scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_stock_zh_a_hist_batch(is_today=True):
    starttime = time.time()
    try:
        pool = Pool(processes = 3)
        if is_today:
            sql = '''select REPLACE(t1.trade_date , "-", "") as date
, t1.code 
from (
select * from (
select trade_date from  akshare.trade_date_hist_sina 
where trade_date <= CURRENT_DATE() 
and trade_date >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURRENT_DATE()  order by trade_date desc limit 60,1) t )
-- and trade_date not in (select 日期 as dat from akshare.history_trade_day_qfq where code= '300437')
order by trade_date desc
) m,
(
select distinct  LPAD(股票代码,6,'0') as code from akshare.half_day_fund_flow
) n
) t1
left join 
(select code, 日期 as dat from akshare.history_trade_day_qfq where 日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURRENT_DATE()  order by trade_date desc limit 60,1) t )
) t2 on t1.trade_date = t2.dat and t1.code = t2.code
where t2.code is null
 and t1.trade_date = CURRENT_DATE() 
-- and t1.trade_date = "2021-10-13"
'''
            df = pd.read_sql_query(sql, engine)
            date_list = df['date'].to_list()
            code_list = df['code'].to_list()
            # today = str(datetime.date.today()).replace('-','')
            # date_list = [today]
            # date_list = [ date  for date in df['date'].to_list() if date == '20211013' ]
            # date_list = ['20211013']
        else:
            sql = '''select REPLACE(t1.trade_date , "-", "") as date
, t1.code 
from (
select * from (
select trade_date from  akshare.trade_date_hist_sina 
where trade_date <= CURRENT_DATE() 
and trade_date >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURRENT_DATE()  order by trade_date desc limit 220,1) t )
-- and trade_date not in (select 日期 as dat from akshare.history_trade_day_qfq where code= '300437')
order by trade_date desc
) m,
(
select distinct  LPAD(股票代码,6,'0') as code from akshare.half_day_fund_flow
) n
) t1
left join 
(select code, 日期 as dat from akshare.history_trade_day_qfq where 
日期 >= (select * from (select * from akshare.trade_date_hist_sina where trade_date <= CURRENT_DATE()  order by trade_date desc limit 220,1) t )
) t2 on t1.trade_date = t2.dat and t1.code = t2.code
where t2.code is null order by date desc
-- and t1.trade_date = CURRENT_DATE() 
-- and t1.trade_date = "2021-10-13"
'''
            df = pd.read_sql_query(sql, engine)
            date_list = df['date'].to_list()
            code_list = df['code'].to_list()
            

        for i in range(len(df)):
            code = code_list[i]
            date = date_list[i]
            print("i: " + str(i) + " | 总数： " + str(len(df)) + ",  Code: " + str(code) + "================")
            print("       date: " + str(date) + "--------------")
            cnt = len(df)
            pool.apply_async(get_stock_zh_a_hist, (code, date, cnt, i, ))
            # get_stock_zh_a_hist(code, date, cnt, i)
    except Exception as e:
        print("执行批量ETL任务失败")
        print(e)
    pool.close()
    pool.join()   #调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
    dtime = time.time() - starttime
    #获取总消耗时间
    print("time_elapsed_all : %s",str(int(dtime)))
    logging.info("time_elapsed_all : %s",str(int(dtime)))


@scheduler.scheduled_job('cron', hour='11', minute='31', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_1131_day_fund_flow():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_half_day_fund_flow()
            print("个股资金流任务成功")
        except Exception as e:
            print("个股资金流任务失败")


@scheduler.scheduled_job('cron', hour='11', minute='31', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_1131_day_fund_flow_concept():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_half_day_fund_flow_concept()
            print("概念资金流任务成功")
        except Exception as e:
            print("概念资金流任务失败")



@scheduler.scheduled_job('cron', hour='11', minute='31', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_1131_day_fund_flow_industry():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_half_day_fund_flow_industry()
            print("行业资金流任务成功")
        except Exception as e:
            print("行业资金流任务失败")


@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_1501_day_fund_flow():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_half_day_fund_flow()
            print("个股资金流任务成功")
        except Exception as e:
            print("个股资金流任务失败")


@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_1501_day_fund_flow_concept():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_half_day_fund_flow_concept()
            print("概念资金流任务成功")
        except Exception as e:
            print("概念资金流任务失败")



@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_1501_day_fund_flow_industry():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_half_day_fund_flow_industry()
            print("行业资金流任务成功")
        except Exception as e:
            print("行业资金流任务失败")


@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_yj_season():
    get_yjbb_season()
    get_yjkb_season()


# 批量获取东方财务--个股资金情况
@scheduler.scheduled_job('cron', hour='15', minute='1,20,40', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_get_stock_zh_a_hist_batch():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_stock_zh_a_hist_batch(True)
            print("个股当日详情任务成功")
        except Exception as e:
            print("个股当日详情任务失败")


@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_get_ths_board_concept():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_ths_board_concept_name(True)
            print("获取概念板块任务成功")
        except Exception as e:
            print("个股当日详情任务失败")
        try:
            get_ths_board_concept_stock(True)
            print("获取概念对应股票任务成功")
        except Exception as e:
            print("获取概念对应股票任务失败")


def get_important_stock():
    stock_zh_a_spot_df = ak.stock_zh_a_spot()
    #print(stock_zh_a_spot_df)
    filter1 = stock_zh_a_spot_df['名称'].isin({
    '英科医疗'
    ,'国脉科技'
    ,'吉艾科技'
    ,'佳创视讯'
    ,'海南橡胶'
    ,'清水源' 
    ,'卡倍亿'
    ,'悦心健康'
    ,'好利科技'
    ,'科德教育'
    ,'吉大通信' 
    ,'徐家汇'
    ,'惠泉啤酒'
    ,'三一重工'
    ,'闽东电力' 
    ,'兰花科创'
    ,'华西证券' 
    ,'中视传媒'
    ,'华西股份' 
    ,'卓翼科技' 
    ,'新日股份'
    ,'美丽生态'
    ,'万年青'
    ,'川能动力'
    ,'潍柴重机'
    ,'南网能源'
    ,'鞍重股份'
    ,'中青宝'
    , '华软科技'
    , '章源钨业'
    , '山东海化'
    , '祥源文化'
    , '华景股份'
    ,'昇兴股份'
    ,'宁德时代'
    ,'华电重工'
    ,'富祥药业'
    ,'百亚股份'
    , '宸展光电'
    , '科翔股份'
    , '欢乐家'
    , '恒瑞医药'
    , ''
    , ''
    , ''
    , ''
    , ''
    , ''
    , ''
    , ''
    })
    print(stock_zh_a_spot_df[filter1].sort_values('涨跌幅',ascending = False))





@scheduler.scheduled_job('cron', hour='8,12,15,18,22', minute='0', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_yj_season():
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print("季度业绩报")
    try:
        get_yjbb_season()
        get_yjkb_season()
    except Exception as e:
        print(e)
    logging.info("time : %s",str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))


# df = ts.pro_bar(ts_code='600723.SZ', adj='qfq', start_date='20211018', end_date='20211018')
# print(df)

if __name__ == '__main__':
    print("开始运行：")
    # scheduler.start()
    # get_important_stock()
    get_stock_zh_a_hist_batch(True)
    get_yjbb_season()
    get_yjkb_season()
    get_half_day_fund_flow()
    get_half_day_fund_flow_concept()
    get_half_day_fund_flow_industry()
    schedule_get_ths_board_concept()
    # get_ths_board_concept_name()
    # today = str(datetime.date.today()).replace('-','')
    # print(today)
    # print()
    # print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    # schedule_yj_season()
    # schedule_yj_season()
    # schedule_day_fund_flow()
    # import akshare as ak
    # stock_em_ggcg_df = ak.stock_em_ggcg()
    # print(stock_em_ggcg_df)
    # print(stock_em_ggcg_df[(stock_em_ggcg_df['持股变动信息-增减']=='增持') & (stock_em_ggcg_df['公告日'] >= '2021-09-10')])
    # print(len(stock_em_ggcg_df[(stock_em_ggcg_df['持股变动信息-增减'] == '增持') & (stock_em_ggcg_df['公告日'] >= '2021-09-10')]))
    # print()
    # print(stock_em_ggcg_df[(stock_em_ggcg_df['代码'] == '300792') ])
    # get_stock_baseinfo()

# import requests
# from bs4 import BeautifulSoup

# def stock_board_concept_name_ths() -> pd.DataFrame:
#     """
#     http://emweb.eastmoney.com/BusinessAnalysis/Index?type=web&code=SZ301072#zyfw-0
#     :return: 
#     :rtype: pandas.DataFrame
#     """
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
#     }
#     url = 'http://basic.10jqka.com.cn/301077/operate.html#stockpage'
#     r = requests.get(url, headers=headers)
#     print(r.text)

#     soup = BeautifulSoup(r.text, "lxml")
#     aa = soup.select('div > div > ul > li > span')
#     print(aa)    
#     print(soup.find('div', attrs={'class': 'bd'}))
#     # html_list = soup.find('th', attrs={'class': 'tips'})#.find_all('td', attrs={'class': 'tips-dataL'})
#     # print(html_list)
#     # name_list = [item.text for item in html_list]
#     # url_list = [item['href'] for item in html_list]
#     # temp_df = pd.DataFrame([name_list, url_list], index=['name', 'url']).T
#     # return temp_df


# stock_board_concept_name_ths()































































