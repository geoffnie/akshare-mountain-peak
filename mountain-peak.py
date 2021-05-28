# -*- coding: utf-8 -*-
"""
Created on Mon May 17 09:33:05 2021

@author: jiuxin
"""

import datetime
from dateutil.relativedelta import relativedelta 
import akshare as ak
from sqlalchemy import create_engine
import time
import pandas as pd
import tushare as ts
import  pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler() 
engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8') 
ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
pro = ts.pro_api()


#显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', 100)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',None)

#qfq_factor_df = ak.stock_zh_a_daily(symbol="sz000002", adjust="qfq-factor")
#print(qfq_factor_df)
#print(time.time())
#tim = time.strftime('%Y%m%d%H', time.localtime(time.time()))
#print(tim)

#stock_em_tfp_df = ak.stock_em_tfp(trade_date="2021-05-18")
#print(stock_em_tfp_df)

#stock_em_yjyg_df = ak.stock_em_yjyg(date="20210514")
#print(stock_em_yjyg_df)

#stock_em_yjbb_df = ak.stock_em_yjbb(date="20210331")
#print(stock_em_yjbb_df)
#print(stock_em_yjbb_df[stock_em_yjbb_df['股票简称'] == '士兰微'])


#stock_em_yysj_df = ak.stock_em_yysj(date="20210331")
#print(stock_em_yysj_df)
##print(stock_em_yysj_df[stock_em_yysj_df['股票简称'] == '士兰微'])

#实时行情
#stock_zh_a_spot_df = ak.stock_zh_a_spot()
#print(stock_zh_a_spot_df)



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
#def get_season_last_date():
#    for year in range(datetime.date.today().year-3, datetime.date.today().year+1):
#        if year == datetime.date.today().year:
#            month_0 = datetime.date.today().month - 3
#            if month_0 < 0:
#                month_0 = 1
#        else:
#            month_0 = 13
#
#        for month in range(1,month_0,3):
#            if month_0 == 1:
#                continue
#            else:
#                dat = datetime.date(year, month, 1)
#            quarter_end_day = datetime.date(dat.year,dat.month - (dat.month - 1) % 3 +2, 1) + relativedelta(months=1,days=-1)
#            quarter_end_day.isoformat()  
#            print(quarter_end_day.isoformat() ) 
#            print(quarter_end_day)
#            dat_list.append(str(quarter_end_day.strftime("%Y%m%d")))
#            print(dat_list[0])
#            print("---------------")
#
#get_season_last_date()



#stock_zh_a_daily_hfq_df = ak.stock_zh_a_daily(symbol="sz000430", start_date='20210101', end_date='20210519', adjust="qfq")
#print(stock_zh_a_daily_hfq_df)
#
#print(datetime.date.today())
#print(str(datetime.date.today()).split(" ")[0])
#
#
#stock_zh_a_spot_df = ak.stock_zh_a_spot()
#print(stock_zh_a_spot_df)

stock_zh_a_spot_df = ak.stock_zh_a_spot()
#print(stock_zh_a_spot_df)
filter1 = stock_zh_a_spot_df['名称'].isin({'佳力图' ,'旗天科技','金种子酒','宋城演艺', '诚迈科技', '复星医药', '岳阳林纸', '长盈精密'})
print(stock_zh_a_spot_df[filter1].sort_values('涨跌幅',ascending = False))



#stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
#print(stock_fund_flow_individual_df)

#df = pro.trade_cal(exchange='', start_date='20210530', end_date='20210530')
#print(df)
#print(datetime.date.today())
#print(str(datetime.date.today()).replace('-',''))


#每日早晨8点开始跑任务
def get_trade_cal(is_init=False):
    today = str(datetime.date.today()).replace('-','')
    table_name = 'trade_cal'
    if is_init:
#        sql = 'delete from akshare.trade_cal'
#        dml_mysql(sql)
        df = pro.trade_cal(exchange='', start_date='20180101', end_date='20211230')
        df.to_sql(table_name, engine, if_exists='replace',index= False)
    else:
        df = pro.trade_cal(exchange='', start_date=today, end_date=today)
        df.to_sql(table_name, engine, if_exists='append',index= False)
    print('get trade_cal success')

get_trade_cal(True)

def dml_mysql(sql):
    conn = pymysql.connect(host='192.168.52.110', port=3306, user='root', passwd='root', db='akshare')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()



def get_stock_baseinfo():
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    

    table_name = 'stock_baseinfo'
    data.to_sql(table_name, engine, if_exists='replace',index= False)

#get_stock_baseinfo()

def get_realtime_price():
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    
#    stock_zh_a_spot_df = ak.stock_zh_a_spot()
    table_name = 'realtime_price'
    data.to_sql(table_name, engine, if_exists='replace',index= False)

#    sql = 'select distinct 代码 as code, 名称 as name from akshare.realtime_price'
#    df = pd.read_sql_query(sql, engine)

#get_realtime_price()

def get_day_price(dat_start = str(datetime.date.today()).replace("-", ""), dat_end = str(datetime.date.today()).replace("-", "")):
    engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8')
      
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
#@scheduler.scheduled_job('cron', hour='12,15')
def get_half_day_fund_flow():
    
    stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
    print(stock_fund_flow_individual_df)
    stock_fund_flow_individual_df['dat'] = '{0}'.format(time.strftime('%Y%m%d%H', time.localtime(time.time())))
#    stock_fund_flow_individual_df['dat'] = '2021052812'
    
    
    engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8')
    
    table_name = 'half_day_fund_flow'
    #插入数据
    stock_fund_flow_individual_df.to_sql(table_name, engine, if_exists='append',index= False)


#获取概念每日资金流向
#@scheduler.scheduled_job('cron', hour='12,15')
def get_half_day_fund_flow_concept():
    
    stock_fund_flow_concept_df = ak.stock_fund_flow_concept(symbol="即时")
    print(stock_fund_flow_concept_df)
    stock_fund_flow_concept_df['dat'] = '{0}'.format(time.strftime('%Y%m%d%H', time.localtime(time.time())))
#    stock_fund_flow_concept_df['dat'] = '2021052812'
    
    
    engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8')
    
    table_name = 'half_day_fund_flow_concept'
    #插入数据
    stock_fund_flow_concept_df.to_sql(table_name, engine, if_exists='append',index= False)


#获取行业每日资金流向
#@scheduler.scheduled_job('cron', hour='12,15')
def get_half_day_fund_flow_industry():
    
    stock_fund_flow_industry_df = ak.stock_fund_flow_industry(symbol="即时")
    print(stock_fund_flow_industry_df)
    stock_fund_flow_industry_df['dat'] = '{0}'.format(time.strftime('%Y%m%d%H', time.localtime(time.time())))
#    stock_fund_flow_industry_df['dat'] = '2021052812'
    
    
    engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8')
    
    table_name = 'half_day_fund_flow_industry'
    #插入数据
    stock_fund_flow_industry_df.to_sql(table_name, engine, if_exists='append',index= False)
    
    

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
        
        stock_em_yjbb_df = ak.stock_em_yjbb(date=season_date)
        print(stock_em_yjbb_df)
        stock_em_yjbb_df['dat'] = '{0}'.format(season_date)
                
        engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8')
        
        table_name = 'yjbb_season'
        #先删除季度数据
        dml_sql = "delete from akshare.yjbb_season where dat='{0}'".format(season_date)
        dml_mysql(dml_sql)
        #插入数据
        stock_em_yjbb_df.to_sql(table_name, engine, if_exists='append',index= False)    



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
                
        engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8')
        
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

def get_daily_basic():
    engine = create_engine('mysql+pymysql://root:root@192.168.52.110/akshare?charset=utf8')
    
    ts.set_token('60ee696150438df37e3b503ebd7e7c74df03784344985a80b4143384')
    pro = ts.pro_api()
    
    df = pro.daily_basic(ts_code='', trade_date='20210525', fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,total_share,float_share,free_share,total_mv,circ_mv')
#    print(df.head(5).iloc[0,1:].tolist())
    table_name = 'daily_basic'
    #插入数据
    df.to_sql(table_name, engine, if_exists='append',index= False)

#get_daily_basic()


#get_yjbb_season(is_init=True)
#get_yjkb_season(is_init=True)    


#get_yjbb_season()
#get_yjkb_season()  


get_half_day_fund_flow()
get_half_day_fund_flow_concept()
get_half_day_fund_flow_industry()



#print(datetime.datetime.strptime('13.05.2021 00:00:00', "%d.%m.%Y %H:%M:%S"))

#if __name__ == '__main__':
#    get_half_day_fund_flow()
#    get_half_day_fund_flow_concept()
#    get_half_day_fund_flow_industry()
    
    
#    scheduler.start()
file_timestamp = 1621440600.0
file_timestamp = time.localtime(file_timestamp)
print(time.strftime('%Y-%m-%d %H:%M:%S', file_timestamp))
#print(time.strftime('%Y-%m-%d %H:%M:%S', 1621440600))
print(file_timestamp)



print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
print(datetime.datetime.strptime('24.05.2021 08:00:00', "%d.%m.%Y %H:%M:%S"))
print(time.mktime(time.strptime('2021-05-24 08:00:00',"%Y-%m-%d %H:%M:%S")))
print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1621814400)))
print(time.mktime(time.strptime('2021-05-24 03:00:00',"%Y-%m-%d %H:%M:%S")))

print(time.time())
#1621827444

#1621814400




print(time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))+ 'gwid')



lis = ['','',5,'']
print(lis)

import random
print(random.randint(100,10000))


































