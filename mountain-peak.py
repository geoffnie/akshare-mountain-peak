# -*- coding: utf-8 -*-
"""
Created on Mon May 17 09:33:05 2021

@author: jiuxin
"""

import datetime
from dateutil.relativedelta import relativedelta 
import akshare as ak
import random
import gc
import time
import pandas as pd
import tushare as ts
import  pymysql
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
from multiprocessing import Pool 
import logging
import requests
from bs4 import BeautifulSoup
from stock_bussiness_analysis import get_jgcc_gdrs_xsjj
from stock_bussiness_analysis import get_jygc_jyps_ywfw
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

host = 'yunfuwu01'
# host = '127.0.0.1'
user = 'root'
passwd = 'akpq92nieqingoo*rootNQ'
port = '3306'
db = 'akshare'

scheduler = BlockingScheduler() 
engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
                       encoding='utf-8',
                       echo=False,
                       pool_pre_ping=True,
                       pool_recycle=3600) 
ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
# pro = ts.pro_api()


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








def ua_random():
    '''
    随机获取一个user-agent
    :return: user-agent
    '''
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv,2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Mozilla/5.0 (Windows NT 6.1; rv,2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
        "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"]

    return random.choice(user_agent_list)


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
    conn = pymysql.connect(host=host, port=3306, user=user, passwd=passwd, db=db)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()



@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_stock_baseinfo():
    sql = 'truncate table akshare.stock_baseinfo'
    try:
        dml_mysql(sql)
    except Exception as e:
        print(e)
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
#     print(stock_fund_flow_concept_df)

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
#     print(stock_fund_flow_industry_df)

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
        # print(season_date)
        stock_em_yjbb_df = ak.stock_em_yjbb(date=season_date)
        # print(stock_em_yjbb_df)
        stock_em_yjbb_df['dat'] = '{0}'.format(season_date)
                
        
        table_name = 'yjbb_season'
        #先删除季度数据
        try:
            dml_sql = "delete from akshare.yjbb_season where dat='{0}'".format(season_date)
            dml_mysql(dml_sql)
        except Exception as e:
            print(e)
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
        # print(stock_em_yjkb_df )
        stock_em_yjkb_df ['dat'] = '{0}'.format(season_date)
                
        
        table_name = 'yjkb_season'
        #先删除季度数据
        try:
            dml_sql = "delete from akshare.yjkb_season where dat='{0}'".format(season_date)
            dml_mysql(dml_sql)
        except Exception as e:
            print(e)
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
@scheduler.scheduled_job('cron', hour='7,15,20', minute='5', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_ths_board_concept_name():
    try:
        print("INFO: 开始同花顺概念板块数据获取任务：")
        # dml_mysql("truncate table akshare.ths_board_concept_name_bak")
        # dml_mysql("insert into akshare.ths_board_concept_name_bak select * from akshare.ths_board_concept_name")
        # dml_mysql("truncate table akshare.ths_board_concept_name")
        board_concept_name = ak.stock_board_concept_name_ths()
        print(board_concept_name)
        for i in board_concept_name.name.tolist():
            print(i)
        # board_concept_name.columns = ['dat', 'name', 'stock_num' , 'url']
        board_concept_name.columns = ['name',  'url']
        board_concept_name['code'] = [url.split("/")[-2] for url in board_concept_name['url'].to_list()]
        # board_concept_name['dat'] = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
        # print(board_concept_name)

        table_name = 'ths_board_concept_name'
        #插入数据
        board_concept_name.to_sql(table_name, engine, if_exists='replace',index= False)
        print("INFO: 插入同花顺概念板块成功")
    except Exception as e:
        print(e)
        print("ERROR: 插入同花顺概念板块失败！")




#获取同花顺概念相关股票
@scheduler.scheduled_job('cron', hour='7,12,14,17,20', minute='15', coalesce=False, misfire_grace_time=600, max_instances=20)
def get_ths_board_concept_stock():
    try:
        print("INFO: 开始同花顺概念板块相关股票数据获取任务：")
        dml_mysql("truncate table if exists akshare.ths_board_concept_stock_bak")
        dml_mysql("truncate table if exists akshare.ths_board_concept_stock_newest")

        board_concept_name = ak.stock_board_concept_name_ths()
        # board_concept_name.columns = ['dat', 'name', 'stock_num', 'url']
        board_concept_name.columns = [ 'name', 'url']
        print(board_concept_name)

        # print(board_concept_name)
        for i,name in enumerate(board_concept_name['name'].to_list()):
            try:
                # print(i)
                time.sleep(3)
                # print()
                df = ak.stock_board_concept_cons_ths(symbol="{0}".format(name))
                df['dat'] = '{0}'.format(time.strftime('%Y-%m-%d', time.localtime(time.time())))
                df['concept_name'] = name
                df = df.drop_duplicates(subset=['代码', 'concept_name'], keep='first')
                df = df.replace('--', '')
                # print(df)

                table_name = 'ths_board_concept_stock_bak'
                #插入数据，每日如果概念板块中增加数据，则增加相应股票
                df.to_sql(table_name, engine, if_exists='append',index= False)
                #插入最新信息，获取每日最新市值
                table_name1 = "ths_board_concept_stock_newest"
                df.to_sql(table_name1, engine, if_exists='append', index=False)

                print("INFO: 第{0}个概念{1}板块插入成功".format(str(i), name))

            except Exception as e:
                print(e)
                print("ERROR: 第{0}个概念{1}板块插入失败！".format(str(i), name))
                continue
        dml_mysql('''insert into akshare.ths_board_concept_stock 
        select t1.* from akshare.ths_board_concept_stock_bak t1 left join 
        akshare.ths_board_concept_stock t2         
        on t1.代码 = t2.代码
        and t1.concept_name  = t2.concept_name  
        where t2.concept_name is null''')
        print("同花顺概念板块相关股票数据插入成功，任务成功")
    except Exception as e:
        dml_mysql('''create table if not exists akshare.ths_board_concept_stock 
        select * from akshare.ths_board_concept_stock_bak''')
        print(e)
        print("ERROR: 同花顺概念板块相关股票数据获取任务失败！")




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

# @scheduler.scheduled_job('cron', hour='17', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_daily_basic():
    engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db))

    ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
    pro = ts.pro_api()
    
    df = pro.daily_basic(ts_code='', trade_date='20211025', fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,total_share,float_share,free_share,total_mv,circ_mv')
#    print(df.head(5).iloc[0,1:].tolist())
    table_name = 'daily_basic'
    print(df)
    #插入数据
    df.to_sql(table_name, engine, if_exists='append',index= False)




# @scheduler.scheduled_job('cron', hour='8', minute='0')
def schedule_0800():
    get_trade_cal(True)







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




#获取股票代码
def get_code():
    sql = "select distinct  LPAD(股票代码,6,'0') as code from akshare.half_day_fund_flow "
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
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
        engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
                               encoding='utf-8',
                               echo=False,
                               pool_pre_ping=True,
                               pool_recycle=3600)
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="{0}".format(code), period="daily", start_date=date, end_date=date, adjust="qfq")
        stock_zh_a_hist_df['code']=code
        # print(stock_zh_a_hist_df)
        name_df = pd.read_sql_query("select symbol as code, name from akshare.stock_baseinfo", engine)
        stock_zh_a_hist_df['name'] = name_df[name_df['code'] == code]['name'].to_list()[0]
        # print(code)
        # print(date)
        # print(stock_zh_a_hist_df)
    
        engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db))
           
        table_name = 'history_trade_day_qfq'
        #插入数据
        stock_zh_a_hist_df.to_sql(table_name, engine, if_exists='append',index= False) 
        print("已插入数据！总数：" + str(cnt) + " | i:" + str(i))
    except Exception as e:   
        print("未插入")
        print(e)
        # continue

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
where trade_date = CURRENT_DATE() 
) m,
(
select LPAD(股票代码,6,'0') as code from( select distinct 股票代码  from akshare.half_day_fund_flow ) t
) n
) t1
left join 
(select  日期 as dat, code from akshare.history_trade_day_qfq where 
日期 = CURRENT_DATE()
) t2 on t1.trade_date = t2.dat and t1.code = t2.code
where t2.code is null 
'''
            df = pd.read_sql_query(sql, engine)
            date_list = df['date'].to_list()
            code_list = df['code'].to_list()
            # today = str(datetime.date.today()).replace('-','')
            # date_list = [today]
            # date_list = [ date  for date in df['date'].to_list() if date == '20211013' ]
            # date_list = ['20211013']
        else:
            # 初始化时，初始最近3个月
            sql = '''select REPLACE(t1.trade_date , "-", "") as date
, t1.code 
from (
select * from (
select trade_date from  akshare.trade_date_hist_sina 
where trade_date <= CURRENT_DATE() 
order by trade_date desc limit 66
) m,
(
select LPAD(股票代码,6,'0') as code from( select distinct 股票代码  from akshare.half_day_fund_flow ) t
) n
) t1
left join 
(select  日期 as dat, code from akshare.history_trade_day_qfq where 
日期 >= date_sub(CURRENT_DATE(), INTERVAL 100 day)
) t2 on t1.trade_date = t2.dat and t1.code = t2.code
where t2.code is null order by date desc
'''
            df = pd.read_sql_query(sql, engine)
            date_list = df['date'].to_list()
            code_list = df['code'].to_list()
            

        for i in range(len(df)):
            code = code_list[i]
            date = date_list[i]
            # print("i: " + str(i) + " | 总数： " + str(len(df)) + ",  Code: " + str(code) + "================")
            # print("       date: " + str(date) + "--------------")
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


# @scheduler.scheduled_job('cron', hour='11', minute='31', coalesce=False, misfire_grace_time=60, max_instances=20)
# def schedule_1131_day_fund_flow():
#     dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
#     sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
#     df = pd.read_sql_query(sql, engine)
#     if len(df) != 0:
#         try:
#             get_half_day_fund_flow()
#             print("个股资金流任务成功")
#         except Exception as e:
#             print("个股资金流任务失败")
#
#
# @scheduler.scheduled_job('cron', hour='11', minute='31', coalesce=False, misfire_grace_time=60, max_instances=20)
# def schedule_1131_day_fund_flow_concept():
#     dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
#     sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
#     df = pd.read_sql_query(sql, engine)
#     if len(df) != 0:
#         try:
#             get_half_day_fund_flow_concept()
#             print("概念资金流任务成功")
#         except Exception as e:
#             print("概念资金流任务失败")
#
#
#
# @scheduler.scheduled_job('cron', hour='11', minute='31', coalesce=False, misfire_grace_time=60, max_instances=20)
# def schedule_1131_day_fund_flow_industry():
#     dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
#     sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
#     df = pd.read_sql_query(sql, engine)
#     if len(df) != 0:
#         try:
#             get_half_day_fund_flow_industry()
#             print("行业资金流任务成功")
#         except Exception as e:
#             print("行业资金流任务失败")


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


# @scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
# def schedule_yj_season():
#     get_yjbb_season()
#     get_yjkb_season()


# 批量获取东方财务--个股资金情况
@scheduler.scheduled_job('cron', hour='15', minute='1,20,40,55', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_get_stock_zh_a_hist_batch():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_stock_zh_a_hist_batch(True)
            print("INFO: 个股当日详情任务成功")
        except Exception as e:
            print(e)
            print("ERROR: 个股当日详情任务失败!")


@scheduler.scheduled_job('cron', hour='15', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_get_ths_board_concept():
    dat = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
    sql = " select * from akshare.trade_date_hist_sina where trade_date =date('{0}')".format(dat)
    df = pd.read_sql_query(sql, engine)
    if len(df) != 0:
        try:
            get_ths_board_concept_name()
            print("INFO: 获取概念板块任务成功")
        except Exception as e:
            print(e)
            print("ERROR: 个股当日详情任务失败!")
        try:
            get_ths_board_concept_stock()
            print("INFO: 获取概念对应股票任务成功")
        except Exception as e:
            print(e)
            print("ERROR: 获取概念对应股票任务失败!")


# 批量获取新浪交易日
@scheduler.scheduled_job('cron', hour='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_get_trade_date_hist_sina():
    try:
        tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
        # print(tool_trade_date_hist_sina_df)
        table_name = 'trade_date_hist_sina'
        tool_trade_date_hist_sina_df.to_sql(table_name, engine, if_exists='replace',index= False)
        print("INFO: 插入新浪交易日成功【trade_date_hist_sina】")
    except Exception as e:
        print(e)
        print("ERROR: 插入新浪交易日失败【trade_date_hist_sina】？？？")

def get_important_stock():
    stock_zh_a_spot_df = ak.stock_zh_a_spot()
    #print(stock_zh_a_spot_df)
    filter1 = stock_zh_a_spot_df['名称'].isin({
    '英科医疗'
    ,'国脉科技'
        , '红宝丽'
        , '罗牛山'
        , '吉峰科技'
    })
    print(stock_zh_a_spot_df[filter1].sort_values('涨跌幅',ascending = False))


def get_stock_notice_info_subtask(is_init, stock_code,  stock_name):
    df_notice_info = pd.DataFrame(columns=['code', 'name', 'dat', 'title', 'content'])
    try:

        print(stock_code, stock_name)

        headers = {
            'User-Agent': ua_random(),
            'Referer': 'http://basic.10jqka.com.cn/'
        }
        url = "http://basic.10jqka.com.cn/{0}/event.html#stockpage".format(stock_code)
        r = requests.get(url, headers=headers)
        r.encoding = 'GBK'
        soup = BeautifulSoup(r.text, "lxml", fromEncoding="gb18030")
        # print(soup)
        # print(soup.select("#tableList > tbody > tr > td"))
        list_notice_info = []

        #获取当日公告信息
        today_text = soup.select("#tableToday > tbody > tr ")
        # print(today_text)

        for i in range(len(today_text)):
            # print(i)
            try:
                # dat = today_text[0].find_all('td')[0].string
                dat = str(datetime.datetime.now())[:10]
                print(dat)
            except Exception as e:
                print(e)
            try:
                title = ''.join([tag_title.string for tag_title in today_text[i].find_all('strong')]).replace(" ", "").replace(
                    "：", "").replace("\r\n", "").replace("\r", "").replace("\n", "").strip()
            except Exception as e:
                print(e)
            try:
                content = ''.join([tag_content.text for tag_content in today_text[i].select("td > span") if
                                   tag_content.text != None]).replace(" ", "").replace("\r\n", "").replace("\r",
                                                                                                           "").replace(
                    "\n", "").strip()
            except Exception as e:
                print(e)

            try:
                list_notice_info.append([stock_code, stock_name, dat, title, content])
            except Exception as e:
                print(e)

        # 获取之前公告信息
        aa = soup.select("#tableList > tbody > tr")

        for i, tag in enumerate(aa):
            try:
                # print(len(aa[i].find_all('td')))
                # print(aa[i].find_all('td')[0].string)
                dat = aa[i].find_all('td')[0].string
            except Exception as e:
                print(e)
            try:
                title = ''.join([tag_title.string for tag_title in aa[i].find_all('strong')]).replace(" ", "").replace(
                    "：", "").replace("\r\n", "").replace("\r", "").replace("\n", "").strip()
            except Exception as e:
                print(e)
            try:
                # print(len(aa[i].find_all('span')))
                # print(''.join([tag_content.text for tag_content in aa[i].select("td > span") if tag_content.text != None]).replace(" ", "").replace("\r\n", "").replace("\r", "").replace("\n", "").strip())
                content = ''.join([tag_content.text for tag_content in aa[i].select("td > span") if
                                   tag_content.text != None]).replace(" ", "").replace("\r\n", "").replace("\r",
                                                                                                           "").replace(
                    "\n", "").strip()
            except Exception as e:
                print(e)

            try:
                list_notice_info.append([stock_code, stock_name, dat, title, content])
            except Exception as e:
                print(e)

        df_notice_info_sub = pd.DataFrame(list_notice_info, columns=['code', 'name', 'dat', 'title', 'content'])
        # print(df_notice_info_sub)
        df_notice_info = df_notice_info.append(df_notice_info_sub)
        # print(df_notice_info)
        # except Exception as e:
        #         print(e)
        # try:
        table_name = 'stock_notice_info'
        print("==============")
        # print(df_notice_info)
        if is_init == True:
            df_notice_info.to_sql(table_name, engine, if_exists='append', index=False)
        else:
            dml_sql = "delete from akshare.{0} where dat>='{1}' and code = '{2}'".format(table_name,
                                                                                         datetime.datetime.now().strftime(
                                                                                             '%Y-%m-%d'), stock_code)
            dml_mysql(dml_sql)
            df_notice_info = df_notice_info[df_notice_info['dat'] >= datetime.datetime.now().strftime('%Y-%m-%d')]
            df_notice_info.to_sql(table_name, engine, if_exists='append', index=False)
        del r, soup, aa, dat, title, content, list_notice_info, df_notice_info_sub
        gc.collect()
    except Exception as e:
        print(e)

def get_stock_notice_info(is_init):

    sql = " select distinct code ,name from akshare.history_trade_day_qfq htdq  where 日期 >= DATE_SUB(CURDATE(), INTERVAL 10 day)"
    df_stock = pd.read_sql_query(sql, engine)
    # print(df_stock)
    if is_init == True:
        try:
            dml_sql = "truncate table akshare.{0} ".format("stock_notice_info")
            dml_mysql(dml_sql)
        except Exception as e:
            print(e)
    pool = Pool(processes=10)
    starttime = time.time()
    for stock_num in range(len(df_stock)):
    #     time.sleep(1)
    #     print(stock_num)
        stock_code = df_stock["code"][stock_num]
        stock_name = df_stock["name"][stock_num]
        # get_stock_notice_info_subtask(is_init, stock_code, stock_name)
        pool.apply_async(get_stock_notice_info_subtask, (is_init, stock_code, stock_name,))
    pool.close()
    # pool.join()
    dtime = time.time() - starttime
    print("Time_elapsed_all : {0}min".format(str(int(dtime)/60)))
    print("开始时间：", starttime)
    print("当前时间：", time.time())



@scheduler.scheduled_job('cron', hour='8,12,15,18,22', minute='0', coalesce=False, misfire_grace_time=60, max_instances=20)
def schedule_yj_season():
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print("季度业绩报")
    try:
        get_yjbb_season()
        get_yjkb_season()
        print("INFO: 获取季度业绩报成功。")
    except Exception as e:
        print(e)
        print("ERROR: 获取季度业绩报失败！？？？")
    logging.info("time : %s",str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))


# df = ts.pro_bar(ts_code='600723.SZ', adj='qfq', start_date='20211018', end_date='20211018')
# print(df)

def get_limit_up_stock(dat):
    stock_em_zt_pool_df = ak.stock_em_zt_pool(date=dat)
    print(stock_em_zt_pool_df)
    table_name = 'limit_up_stock_pool'
    stock_em_zt_pool_df['dat'] = dat
    stock_em_zt_pool_df = stock_em_zt_pool_df.drop_duplicates()
    stock_em_zt_pool_df.to_sql(table_name, engine, if_exists='append',index= False)



@scheduler.scheduled_job('cron', hour='15', minute='5', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_limitup():
    try:
        sql = " select * from akshare.trade_date_hist_sina tdhs where trade_date <= DATE_SUB(CURDATE(), INTERVAL 0 day) order by trade_date desc limit 1"
        # sql = " select * from akshare.trade_date_hist_sina tdhs where trade_date <= DATE_SUB(CURDATE(), INTERVAL 164 day) order by trade_date desc limit 133"
        df_trade_date = pd.read_sql_query(sql, engine)
        list_trade_date = df_trade_date['trade_date'].tolist()
        for dat in list_trade_date:
            time.sleep(5)
            get_limit_up_stock(str(dat).replace("-", ""))
        print("INFO: 获取每日涨停板成功。")
    except Exception as e:
        print(e)
        print("ERROR: 获取每日涨停板失败！？？？")

@scheduler.scheduled_job('cron', hour='4,8,11,13,15,17,20', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_notice_info():
    try:
        get_stock_notice_info(is_init=False)
        print("INFO: 获取每日公告信息成功。")
    except Exception as e:
        print(e)
        print("ERROR: 获取每日公告信息失败！？？？")

def get_dragon_tiger_info(dat):
    time.sleep(3)
    # url1 = f'http://data.10jqka.com.cn/market/longhu/'
    url1 = 'http://data.10jqka.com.cn/ifmarket/lhbggxq/report/{0}/'.format(dat)
    # url1 = f'http://data.10jqka.com.cn/ifmarket/lhbggxq/report/2022-02-15/'
    headers = {
        'User-Agent': ua_random()
    }
    t = requests.get(url1, headers=headers)
    soup = BeautifulSoup(t.text, "lxml")
    # 历史交易日
    code_lhb_details = soup.select('body > div.ggmx.clearfix > div.rightcol.fr > div')
    # 最近一个交易日
    # code_lhb_details = soup.select('#ggmx > div.ggmxcont > div.ggmx.clearfix > div.rightcol.fr > div')
    # 最终路径
    # body > div.ggmx.clearfix > div.rightcol.fr > div:nth-child(1) > div.cell-cont.cjmx > table:nth-child(2) > tbody > tr:nth-child(3) > td.tl.rel > a
    # 最近一个交易日
    #ggmx > div.ggmxcont > div.ggmx.clearfix > div.rightcol.fr > div:nth-child(5) > div.cell-cont.cjmx > table:nth-child(2) > tbody > tr:nth-child(5) > td.tl.rel > a
    array_lhb = []
    array_lhb_detail = []
    for num, item_code in enumerate(code_lhb_details):
        try:
            item_list = (item_code.text).split("\n")
            print(item_list, "-------------",len(item_list))
            if num == len(code_lhb_details) - 1:
                break

            print("===================================", num)
            # print(item_list)
            # for i,item in enumerate(item_list):
            #     print(i,": ", item)
            name = item_list[1][:item_list[1].find('(') ]
            code = item_list[1][item_list[1].find('(') + 1:item_list[1].find(')')]
            reason = item_list[1][item_list[1].find('：') + 1:]
            reason_type = '3' if "连续三个" in reason else '1'
            # 以下金额单位转换为亿元
            amount = item_list[8][item_list[8].find('：') + 1:]
            amount = round(float(amount.replace("亿元", "").replace(" ", "")), 3) if "亿" in amount else round(float(amount.replace("万元", "").replace(" ", ""))/10000, 3)
            amount_buy = item_list[9][item_list[9].find('：') + 1:]
            amount_buy = round(float(amount_buy.replace("亿元", "").replace(" ", "")), 3) if "亿" in amount_buy else round(float(amount_buy.replace("万元", "").replace(" ", ""))/10000, 3)
            amount_sell = item_list[10][item_list[10].find('：') + 1:]
            amount_sell = round(float(amount_sell.replace("亿元", "").replace(" ", "")), 3) if "亿" in amount_sell else round(float(amount_sell.replace("万元", "").replace(" ", ""))/10000, 3)
            amount_net = item_list[11][item_list[11].find('：') + 1:]
            amount_net = round(float(amount_net.replace("亿元", "").replace(" ", "")), 3) if "亿" in amount_net else round(float(amount_net.replace("万元", "").replace(" ", ""))/10000, 3)

            buy_top5name1 = item_list[25].replace(" ", "")
            buy_top5name1_nick = item_list[26].replace(" ", "")
            buy_top5name1_in = item_list[27].replace(" ", "")
            buy_top5name1_out = item_list[28].replace(" ", "")
            buy_top5name1_net = item_list[29].replace(" ", "")

            buy_top5name2 = item_list[33].replace(" ", "")
            buy_top5name2_nick = item_list[34].replace(" ", "")
            buy_top5name2_in = item_list[35].replace(" ", "")
            buy_top5name2_out = item_list[36].replace(" ", "")
            buy_top5name2_net = item_list[37].replace(" ", "")

            buy_top5name3 = item_list[41].replace(" ", "")
            buy_top5name3_nick = item_list[42].replace(" ", "")
            buy_top5name3_in = item_list[43].replace(" ", "")
            buy_top5name3_out = item_list[44].replace(" ", "")
            buy_top5name3_net = item_list[45].replace(" ", "")

            buy_top5name4 = item_list[49].replace(" ", "")
            buy_top5name4_nick = item_list[50].replace(" ", "")
            buy_top5name4_in = item_list[51].replace(" ", "")
            buy_top5name4_out = item_list[52].replace(" ", "")
            buy_top5name4_net = item_list[53].replace(" ", "")

            buy_top5name5 = item_list[57].replace(" ", "")
            buy_top5name5_nick = item_list[58].replace(" ", "")
            buy_top5name5_in = item_list[59].replace(" ", "")
            buy_top5name5_out = item_list[60].replace(" ", "")
            buy_top5name5_net = item_list[61].replace(" ", "")

            sell_top5name1 = item_list[77].replace(" ", "")
            sell_top5name1_nick = item_list[78].replace(" ", "")
            sell_top5name1_in = item_list[79].replace(" ", "")
            sell_top5name1_out = item_list[80].replace(" ", "")
            sell_top5name1_net = item_list[81].replace(" ", "")

            sell_top5name2 = item_list[85].replace(" ", "")
            sell_top5name2_nick = item_list[86].replace(" ", "")
            sell_top5name2_in = item_list[87].replace(" ", "")
            sell_top5name2_out = item_list[88].replace(" ", "")
            sell_top5name2_net = item_list[89].replace(" ", "")

            sell_top5name3 = item_list[93].replace(" ", "")
            sell_top5name3_nick = item_list[94].replace(" ", "")
            sell_top5name3_in = item_list[95].replace(" ", "")
            sell_top5name3_out = item_list[96].replace(" ", "")
            sell_top5name3_net = item_list[97].replace(" ", "")

            sell_top5name4 = item_list[101].replace(" ", "")
            sell_top5name4_nick = item_list[102].replace(" ", "")
            sell_top5name4_in = item_list[103].replace(" ", "")
            sell_top5name4_out = item_list[104].replace(" ", "")
            sell_top5name4_net = item_list[105].replace(" ", "")

            sell_top5name5 = item_list[109].replace(" ", "")
            sell_top5name5_nick = item_list[110].replace(" ", "")
            sell_top5name5_in = item_list[111].replace(" ", "")
            sell_top5name5_out = item_list[112].replace(" ", "")
            sell_top5name5_net = item_list[113].replace(" ", "")

            print(dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net)

            array_lhb.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net])


            print(buy_top5name1, buy_top5name1_nick, buy_top5name1_in, buy_top5name1_out, buy_top5name1_net, buy_top5name2, buy_top5name2_nick, buy_top5name2_in, buy_top5name2_out, buy_top5name2_net, buy_top5name3, buy_top5name3_nick, buy_top5name3_in, buy_top5name3_out, buy_top5name3_net, buy_top5name4, buy_top5name4_nick, buy_top5name4_in, buy_top5name4_out, buy_top5name4_net, buy_top5name5, buy_top5name5_nick, buy_top5name5_in, buy_top5name5_out, buy_top5name5_net, sell_top5name1, sell_top5name1_nick, sell_top5name1_in, sell_top5name1_out, sell_top5name1_net, sell_top5name2, sell_top5name2_nick, sell_top5name2_in, sell_top5name2_out, sell_top5name2_net, sell_top5name3, sell_top5name3_nick, sell_top5name3_in, sell_top5name3_out, sell_top5name3_net, sell_top5name4, sell_top5name4_nick, sell_top5name4_in, sell_top5name4_out, sell_top5name4_net, sell_top5name5, sell_top5name5_nick, sell_top5name5_in, sell_top5name5_out, sell_top5name5_net)

            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, buy_top5name1, buy_top5name1_nick, buy_top5name1_in, buy_top5name1_out, buy_top5name1_net, "buy", 1])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, buy_top5name2, buy_top5name2_nick, buy_top5name2_in, buy_top5name2_out, buy_top5name2_net, "buy", 2])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, buy_top5name3, buy_top5name3_nick, buy_top5name3_in, buy_top5name3_out, buy_top5name3_net, "buy", 3])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, buy_top5name4, buy_top5name4_nick, buy_top5name4_in, buy_top5name4_out, buy_top5name4_net, "buy", 4])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, buy_top5name5, buy_top5name5_nick, buy_top5name5_in, buy_top5name5_out, buy_top5name5_net, "buy", 5])

            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, sell_top5name1, sell_top5name1_nick, sell_top5name1_in, sell_top5name1_out, sell_top5name1_net, "sell", 1])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, sell_top5name2, sell_top5name2_nick, sell_top5name2_in, sell_top5name2_out, sell_top5name2_net, "sell", 2])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, sell_top5name3, sell_top5name3_nick, sell_top5name3_in, sell_top5name3_out, sell_top5name3_net, "sell", 3])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, sell_top5name4, sell_top5name4_nick, sell_top5name4_in, sell_top5name4_out, sell_top5name4_net, "sell", 4])
            array_lhb_detail.append([dat, name, code, reason, reason_type, amount, amount_buy, amount_sell, amount_net, sell_top5name5, sell_top5name5_nick, sell_top5name5_in, sell_top5name5_out, sell_top5name5_net, "sell", 5])
        except Exception as e:
            print(e)
            continue

    # print(array_lhb)
    df_lhb = pd.DataFrame(array_lhb, columns=['dat', 'name', 'code', 'reason', 'reason_type', 'amount', 'amount_buy', 'amount_sell', 'amount_net'])
    df_lhb_detail = pd.DataFrame(array_lhb_detail, columns=['dat', 'name', 'code', 'reason', 'reason_type', 'amount', 'amount_buy', 'amount_sell', 'amount_net', 'top5name', 'top5name_nick', 'top5name_in', 'top5name_out', 'top5name_net', 'type', 'rank1'])

    table_name1 = "dragon_tiger_info"
    # print(table_name1)
    # print(dat)
    df_lhb.to_sql(table_name1, engine, if_exists='append', index=False)
    table_name2 = "dragon_tiger_info_detail"
    df_lhb_detail.to_sql(table_name2, engine, if_exists='append', index=False)


@scheduler.scheduled_job('cron', hour='16,17', minute='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_dragon_tiger_info_task(is_init=True):
    if is_init == True:
        dml_sql1 = "truncate table akshare.{0} ".format("dragon_tiger_info")
        dml_mysql(dml_sql1)
        dml_sql2 = "truncate table akshare.{0} ".format("dragon_tiger_info_detail")
        dml_mysql(dml_sql2)
        sql_dat = "select * from akshare.trade_date_hist_sina tdhs where trade_date <= DATE_SUB(CURDATE(), INTERVAL 0 day) order by trade_date desc limit 120"
        df_dat = pd.read_sql_query(sql_dat, engine)
        list_dat = df_dat["trade_date"].tolist()
        pool = Pool(processes=10)
        for dat in list_dat:
            pool.apply_async(get_dragon_tiger_info, (dat,))
        pool.close()
        pool.join()
    else:
        sql_dat = "select * from akshare.trade_date_hist_sina tdhs where trade_date <= DATE_SUB(CURDATE(), INTERVAL 0 day) order by trade_date desc limit 1"
        df_dat = pd.read_sql_query(sql_dat, engine)
        list_dat = df_dat["trade_date"].tolist()
        for dat in list_dat:
            get_dragon_tiger_info(dat)


# 文本类型消息
def send_txt(text, send_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ee09cff2-5e7e-4421-b91e-4af3645670f5"):
    headers = {"Content-Type": "text/plain"}
    #测试企业微信
    send_data = {
        "msgtype": "text",  # 消息类型
        "text": {
            "content": "{0}".format(text), # 文本内容，最长不超过2048个字节，必须是utf8编码
            "mentioned_list": [""],        # userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人，如果开发者获取不到userid，可以使用mentioned_mobile_list
            "mentioned_mobile_list": [""]  # 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
        }
    }

    res = requests.post(url=send_url, headers=headers, json=send_data)
    print(res.text)

@scheduler.scheduled_job('cron', hour='7', coalesce=False, misfire_grace_time=60, max_instances=20)
def send_topic():
    text = '''业绩快报净利润同比增长超过500%的昨日新增股票：\r\n'''
    # sql = '''select 股票简称 as name from akshare.yjkb_season ys where date(dat) = LAST_DAY(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) + interval QUARTER(CURDATE())*3-4 month) and `净利润-同比增长` >= 500 order by 公告日期 desc '''
    # sql = '''select 股票简称 as name from akshare.yjkb_season ys where date(dat) = LAST_DAY(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) + interval QUARTER(CURDATE())*3-4 month) and `净利润-同比增长` >= 500 and date(公告日期 ) =  date_sub(CURRENT_DATE(), INTERVAL 1 day)  '''
    sql = '''select 股票简称 as name from akshare.yjkb_season ys where `净利润-同比增长` >= 500 and date(公告日期 ) =  date_sub(CURRENT_DATE(), INTERVAL 1 day) 
union ALL 
select 股票简称 from akshare.yjbb_season ys where  `净利润-同比增长` >= 500 and date(最新公告日期 ) =  date_sub(CURRENT_DATE(), INTERVAL 1 day) '''
    df_stock_name = pd.read_sql_query(sql, engine)
    text =  text + ("\r\n".join(df_stock_name['name'].tolist()))
    print(text)

    send_txt(text, 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ee09cff2-5e7e-4421-b91e-4af3645670f5')

@scheduler.scheduled_job('cron', hour='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_jgcc_gdrs_xsjj_info():
    try:
        get_jgcc_gdrs_xsjj()
        print("INFO: 获取每日机构持仓、股东人数、限售解禁成功。")
    except Exception as e:
        print(e)
        print("ERROR: 获取每日机构持仓、股东人数、限售解禁失败！？？？")

@scheduler.scheduled_job('cron', hour='1', coalesce=False, misfire_grace_time=60, max_instances=20)
def get_jygc_jyps_ywfw_info():
    try:
        get_jygc_jyps_ywfw()
        print("INFO: 获取每日经营构成、经营评述、业务范围信息成功。")
    except Exception as e:
        print(e)
        print("ERROR: 获取每日经营构成、经营评述、业务范围信息失败！？？？")


if __name__ == '__main__':
    print("开始运行：")
    # scheduler.start()
    # schedule_get_trade_date_hist_sina()
    # get_ths_board_concept_stock()
    # get_notice_info()
    # send_topic()
    # get_ths_board_concept_stock()
    # get_stock_notice_info(is_init=True)
    # get_ths_board_concept_name()
    # get_ths_board_concept_stock()
    # get_important_stock()
    # get_stock_baseinfo()
    get_limitup()
    # get_stock_zh_a_hist_batch(False)
    # get_limitup()
    # get_yjbb_season(True)
    # get_yjkb_season(True)
    # get_half_day_fund_flow()
    # get_half_day_fund_flow_concept()
    # get_half_day_fund_flow_industry()
    # schedule_get_ths_board_concept()
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
    # tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
    # print(tool_trade_date_hist_sina_df)
    #
    # table_name = 'trade_date_hist_sina'

    # tool_trade_date_hist_sina_df.to_sql(table_name, engine, if_exists='replace',index= False)


    # stock_em_zt_pool_df = ak.stock_em_zt_pool(date='20211216')
    # print(stock_em_zt_pool_df)
    # print(stock_em_zt_pool_df.sort_values(by=['连板数', '总市值']))
    # stock_em_zt_pool_df['new'] = stock_em_zt_pool_df['代码'] + '  ' + stock_em_zt_pool_df['名称'] + '  ' + stock_em_zt_pool_df['所属行业']
    # print(stock_em_zt_pool_df)
    # print('\r\n'.join(stock_em_zt_pool_df['new'].tolist()))


    # sql = " select * from akshare.trade_date_hist_sina tdhs where trade_date <= DATE_SUB(CURDATE(), INTERVAL 0 day) order by trade_date desc limit 1"
    # sql = " select * from akshare.trade_date_hist_sina tdhs where trade_date < DATE_SUB(CURDATE(), INTERVAL 1 day) order by trade_date desc limit 33"
    # df_trade_date = pd.read_sql_query(sql, engine)
    # list_trade_date = df_trade_date['trade_date'].tolist()
    # for dat in list_trade_date:
    #     time.sleep(5)
    #     try:
    #         get_limit_up_stock(str(dat).replace("-", ""))
    #     except Exception:
    #         print("aaa")
    #         continue

    # stock_em_zt_pool_zbgc_df = ak.stock_em_zt_pool_zbgc(date='20220106')
    # print(stock_em_zt_pool_zbgc_df)



    # get_stock_notice_info(is_init = True)
    # get_notice_info()




























































