# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 22:22:14 2021

@author: jiuxin
"""

import random
import pandas as pd
import time
import requests
import json


# host = 'yunfuwu01'
host = '127.0.0.1'
user = 'root'
passwd = 'akpq92nieqingoo*rootNQ'
port = '3306'
db = 'akshare'
def ua_random():
    import random
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


def stock_bussiness_analysis(
    stock: str = "SZ300750"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-个股
    http://data.eastmoney.com/zjlx/detail.html
    :param stock: 股票代码
    :type stock: str
    :param market: 股票市场; 上海证券交易所: sh, 深证证券交易所: sz
    :type market: str
    :return: 近期个股的资金流数据
    :rtype: pandas.DataFrame
    """
    market_map = {"sh": 1, "sz": 0}
    url = "http://emweb.eastmoney.com/BusinessAnalysis/PageAjax"

    headers = {
        'user-agent': ua_random()
    }
    print(headers)
    # headers = {
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
    # }
    params = {
        "code": "{0}".format(stock)
    }
    r = requests.get(url, params=params, headers=headers)
    text_data = r.text
    print(text_data)
    # json_data = json.loads(text_data[text_data.find("{") : -2])
    json_data = json.loads(text_data)
    content_zyfw_list = json_data["zyfw"]   #主营范围
    print(content_zyfw_list)
    temp_zyfw_df = pd.DataFrame(content_zyfw_list)
    temp_zyfw_df.columns = [
        "代码全称",
        "代码",
        "主营范围"
    ]
    # print(temp_zyfw_df)

    content_jyps_list = json_data["jyps"]  #经营评述
    print(content_jyps_list)
    temp_jyps_df = pd.DataFrame(content_jyps_list)
    temp_jyps_df.columns = [
        "代码全称",
        "代码",
        "报告日期",
        "业务评述"
    ]
    # print(temp_jyps_df)

    content_zygcfx_list = json_data["zygcfx"]  #主营构成分析
    print(content_zygcfx_list)
    temp_zygcfx_df = pd.DataFrame(content_zygcfx_list)

    temp_zygcfx_df.columns = [
        "代码全称",
        "代码",
        "报告日期",
        "type",
        "主营构成",
        "主营收入",
        "收入比例",
        "主营成本",
        "成本比例",
        "主营利润",
        "利润比例",
        "毛利率",
        "主营收入排名"
    ]
    #type：1：按行业分类；2：按产品分类；3：按地区分类
    # print(temp_zygcfx_df)
    return temp_zyfw_df, temp_jyps_df, temp_zygcfx_df

# a, b, c = stock_bussiness_analysis( "SZ300750")
# print(a)
# print(b)
# print(c)






def jgcc_analysis(
    stock: str = "SZ300750"
        ,date: str = '2021-06-30'
) -> pd.DataFrame:
    """
    东东东东方财富网-数据中心-资机构机构持仓
    https://emweb.eastmoney.com/PC_HSF10/ShareholderResearch/PageJGCC?code=SZ300677&date=2021-06-30
    :param stock: 股票代码
    :type stock: str
    :param market: 股票市场; 上海证券交易所: sh, 深证证券交易所: sz
    :type market: str
    :return: 近期个股的资金流数据
    :rtype: pandas.DataFrame
    """
    market_map = {"sh": 1, "sz": 0}
    url = "http://emweb.eastmoney.com/PC_HSF10/ShareholderResearch/PageJGCC"

    headers = {
        'user-agent': ua_random()
    }
    print(headers)

    params = {
        "code": "{0}".format(stock),
        "date": "{0}".format(date)
    }
    # print(params)
    r = requests.get(url, params=params, headers=headers)
    text_data = r.text
    print(text_data)
    # # json_data = json.loads(text_data[text_data.find("{") : -2])
    json_data = json.loads(text_data)
    content_jgcc_list = json_data["jgcc"]   #机构持仓
    print(content_jgcc_list)
    temp_jgcc_df = pd.DataFrame(content_jgcc_list)
    temp_jgcc_df.columns = [
        "SECUCODE",
        "SECURITY_CODE",
        "REPORT_DATE",
        "ORG_TYPE",
        "TOTAL_ORG_NUM",
        "TOTAL_FREE_SHARES",
        "TOTAL_SHARES_RATIO",
        "ALL_SHARES_RATIO"
    ]
    print(temp_jgcc_df)

    return temp_jgcc_df

def gdrs_analysis(
    stock: str = "SZ300750"
) -> pd.DataFrame:
    """
    东东东东方财富网-数据中心-资机构机构持仓
    https://emweb.eastmoney.com/PC_HSF10/ShareholderResearch/PageJGCC?code=SZ300677&date=2021-06-30
    :param stock: 股票代码
    :type stock: str
    :param market: 股票市场; 上海证券交易所: sh, 深证证券交易所: sz
    :type market: str
    :return: 近期个股的资金流数据
    :rtype: pandas.DataFrame
    """
    market_map = {"sh": 1, "sz": 0}
    url = "http://emweb.eastmoney.com/ShareholderResearch/PageAjax"

    headers = {
        'user-agent': ua_random()
    }
    print(headers)

    params = {
        "code": "{0}".format(stock)
    }
    r = requests.get(url, params=params, headers=headers)
    text_data = r.text
    print(text_data)
    # # json_data = json.loads(text_data[text_data.find("{") : -2])
    json_data = json.loads(text_data)
    content_gdrs_list = json_data["gdrs"]   #股东人数
    print(content_gdrs_list)
    temp_gdrs_df = pd.DataFrame(content_gdrs_list)
    temp_gdrs_df.columns = [
        "SECUCODE",
        "SECURITY_CODE",
        "END_DATE",
        "HOLDER_TOTAL_NUM",
        "TOTAL_NUM_RATIO",
        "AVG_FREE_SHARES",
        "AVG_FREESHARES_RATIO",
        "HOLD_FOCUS",
        "PRICE",
        "AVG_HOLD_AMT",
        "HOLD_RATIO_TOTAL",
        "FREEHOLD_RATIO_TOTAL"
    ]
    print(temp_gdrs_df)

    return temp_gdrs_df


def xsjj_analysis(
    stock: str = "SZ300750"
) -> pd.DataFrame:
    """
    东东东东方财富网-数据中心-资机构机构持仓
    https://emweb.eastmoney.com/PC_HSF10/ShareholderResearch/PageJGCC?code=SZ300677&date=2021-06-30
    :param stock: 股票代码
    :type stock: str
    :param market: 股票市场; 上海证券交易所: sh, 深证证券交易所: sz
    :type market: str
    :return: 近期个股的资金流数据
    :rtype: pandas.DataFrame
    """
    market_map = {"sh": 1, "sz": 0}
    url = "http://emweb.eastmoney.com/ShareholderResearch/PageAjax"

    headers = {
        'user-agent': ua_random()
    }
    print(headers)

    params = {
        "code": "{0}".format(stock)
    }
    r = requests.get(url, params=params, headers=headers)
    text_data = r.text
    print(text_data)
    # # json_data = json.loads(text_data[text_data.find("{") : -2])
    json_data = json.loads(text_data)
    content_xsjj_list = json_data["xsjj"]   #机构持仓
    print(content_xsjj_list)
    temp_xsjj_df = pd.DataFrame(content_xsjj_list)
    temp_xsjj_df.columns = [
        "SECUCODE",
        "SECURITY_CODE",
        "LIFT_DATE",
        "LIFT_NUM",
        "LIFT_TYPE",
        "TOTAL_SHARES_RATIO",
        "UNLIMITED_A_SHARES_RATIO"
    ]
    print(temp_xsjj_df)

    return temp_xsjj_df





#工具类，删除数据
def dml_mysql(sql):
    conn = pymysql.connect(host=host, port=3306, user=user, passwd=passwd, db=db)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()

import pymysql
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler() 
engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
                       encoding='utf-8',
                       echo=False,
                       pool_pre_ping=True,
                       pool_recycle=3600) 

# for i in range(100):
#     print(i, "---------------------------------------------------------------")
#     a, b, c = stock_bussiness_analysis("SZ300750")
#     print(a)
#     print(b)
#     print(c)

#获取机构持仓、股东人数、限售解禁
def get_jgcc_gdrs_xsjj():
    sql = 'select distinct name, concat(substring(ts_code, 8, 2), substring(ts_code, 1, 6)) as code from akshare.stock_baseinfo'
    #获取查询数据
    df = pd.read_sql_query(sql, engine)
    try:
        dml_mysql("truncate table akshare.stockholder_jgcc")
        dml_mysql("truncate table akshare.stockholder_gdrs")
        dml_mysql("truncate table akshare.stockholder_xsjj")
    except Exception as e:
        print(e)

    for i,index in enumerate(df.index):
        print(i, df['code'][index], df['name'][index])
        print("------------------------------------------")
        try:
            jgcc_df = jgcc_analysis("{0}".format(df['code'][index]), '2021-09-30')
            table_name = 'stockholder_jgcc'
            #插入数据
            jgcc_df.to_sql(table_name, engine, if_exists='append',index= False)

            print()
            print()
            gdrs_df = gdrs_analysis("{0}".format(df['code'][index]))
            table_name = 'stockholder_gdrs'
            #插入数据
            gdrs_df.to_sql(table_name, engine, if_exists='append',index= False)

            print()
            print()
            xsjj_df = xsjj_analysis("{0}".format(df['code'][index]))
            table_name = 'stockholder_xsjj'
            #插入数据
            xsjj_df.to_sql(table_name, engine, if_exists='append',index= False)
            print("====================================")
        except Exception as e:
            print(e)
            continue


#获取经营构成、经营评述、业务范围信息
def get_jygc_jyps_ywfw():
    sql = 'select distinct name, concat(substring(ts_code, 8, 2), substring(ts_code, 1, 6)) as code from akshare.stock_baseinfo'
    #获取查询数据
    df = pd.read_sql_query(sql, engine)
    try:
        dml_mysql("truncate table akshare.stock_bussiness_analysis_zyfw")
        dml_mysql("truncate table akshare.stock_bussiness_analysis_jyps")
        dml_mysql("truncate table akshare.stock_bussiness_analysis_zygcfx")
    except Exception as e:
        print(e)

    for i,index in enumerate(df.index):
        print(i)
        print(i, df['code'][index], df['name'][index])
        # time.sleep(5)
        print("--------------------------")
        try:
            temp_zyfw_df, temp_jyps_df, temp_zygcfx_df = stock_bussiness_analysis("{0}".format(df['code'][index]))
            temp_zyfw_df['dat'] = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
            # temp_zyfw_df['code'] = df['code'][index]
            temp_zyfw_df['name'] = df['name'][index]
            table_name = 'stock_bussiness_analysis_zyfw'
            #插入数据
            temp_zyfw_df.to_sql(table_name, engine, if_exists='append',index= False)

            temp_jyps_df['dat'] = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
            # temp_jyps_df['code'] = df['code'][index]
            temp_jyps_df['name'] = df['name'][index]
            table_name = 'stock_bussiness_analysis_jyps'
            #插入数据
            temp_jyps_df.to_sql(table_name, engine, if_exists='append',index= False)

            temp_zygcfx_df['dat'] = '{0}'.format(time.strftime('%Y%m%d', time.localtime(time.time())))
            # temp_zygcfx_df['code'] = df['code'][index]
            temp_zygcfx_df['name'] = df['name'][index]
            table_name = 'stock_bussiness_analysis_zygcfx'
            #插入数据
            temp_zygcfx_df.to_sql(table_name, engine, if_exists='append',index= False)
        except Exception as e:
            print(e)
            continue



get_jgcc_gdrs_xsjj()
get_jygc_jyps_ywfw()



# ==========================================================================
# 同花顺个股近期大事
# http://basic.10jqka.com.cn/300506/event.html#stockpage
# Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3
# Accept-Encoding: gzip, deflate
# Accept-Language: zh-CN,zh;q=0.9
# Cache-Control: max-age=0
# Connection: keep-alive
# Cookie: searchGuide=sg; reviewJump=nojump; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1640652749; spversion=20130314; historystock=300506%7C*%7C603787; cid=311dcd4148b1d66ef393be03f4895e6c1640652794; usersurvey=1; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1640653127; v=A2un8X2fjni1G9KsqQHzxeYx-oRWgH8Z-ZRDtt3oR6oBfIV4ZVAPUglk0xzu
# Host: basic.10jqka.com.cn
# Upgrade-Insecure-Requests: 1
# User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36

# ===========================================================================



