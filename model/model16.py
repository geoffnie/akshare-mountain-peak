# -*- coding:utf-8 -*-
#@Time : 2022/2/15 16:53
#@Author: Geoff Nie
#@File : model16.py

import pandas as pd
# import akshare as ak
# indicator_name_list = ak.stock_sina_lhb_detail_daily(trade_date="20220218", symbol="返回当前交易日所有可查询的指标")
# print(indicator_name_list)  # 输出当前交易日可以查询的指标
# stock_sina_lhb_detail_daily_df = ak.stock_sina_lhb_detail_daily(trade_date="20220218", symbol="涨幅偏离值达7%的证券")
# print(stock_sina_lhb_detail_daily_df)
#
# df = pd.DataFrame([])
# print(df)
# print("------------------------")
# for indicator_name in indicator_name_list:
#     stock_sina_lhb_detail_daily_df = ak.stock_sina_lhb_detail_daily(trade_date="20220218", symbol=indicator_name)
#     print(stock_sina_lhb_detail_daily_df)
#     df = pd.concat([df, stock_sina_lhb_detail_daily_df], axis=0)
# print("===================")
# df["序号"] = 0
# df = df.reset_index(drop=True)
# print(df.columns)
# print(list(df.columns))
# print(df)
# df.drop_duplicates(inplace=True)
# # df = df.drop_duplicates(subset=list(df.columns), keep='first', inplace=True)
# print(df)
# print(len(df))

# print(df.drop_duplicates(inplace=True))


# df = df.dropDuplicates()
# df.show()


#
# import akshare as ak
# stock_lh_yyb_capital_df = ak.stock_lh_yyb_capital()
# print(stock_lh_yyb_capital_df)
import requests
import pandas as pd
from tqdm import tqdm

# def stock_lh_yyb_most() -> pd.DataFrame:
#     """
#     同花顺-数据中心-营业部排名-上榜次数最多
#     http://data.10jqka.com.cn/market/longhu/
#     :return: 上榜次数最多
#     :rtype: pandas.DataFrame
#     """
#     big_df = pd.DataFrame()
#     for page in tqdm(range(1, 11)):
#         headers = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
#         }
#         url = f'http://data.10jqka.com.cn/ifmarket/lhbyyb/type/1/tab/sbcs/field/sbcs/sort/desc/page/{page}/'
#         r = requests.get(url, headers=headers)
#         temp_df = pd.read_html(r.text)[0]
#         big_df = big_df.append(temp_df)
#     big_df.reset_index(inplace=True, drop=True)
#     return big_df



big_df = pd.DataFrame()
headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'
        }
# headers = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 Edg/98.0.1108.55'
#         }

# url = f'http://data.10jqka.com.cn/ifmarket/lhbyyb/type/1/tab/sbcs/field/sbcs/sort/desc/page/1/'
url = f'http://data.10jqka.com.cn/ifmarket/lhbtable/report/2022-02-18/tab/all/field/STOCKCODE/sort/asc/'
r = requests.get(url, headers=headers)
temp_df = pd.read_html(r.text)[0]
big_df = big_df.append(temp_df)
print(type(r.headers),r.headers) #获取请求头
print(type(r.headers),r.headers) #获取响应头
print(type(r.cookies),r.cookies)#获取响应cookie
print(type(r.url),r.url) #获取响应url



from bs4 import BeautifulSoup
soup = BeautifulSoup(r.text, "lxml")
# day_lhb_contents = soup.select('body > div > table > tbody > tr')
#
# print(day_lhb_contents)
# print(len(day_lhb_contents))
# print(day_lhb_contents[0])
# print(day_lhb_contents[0].text)
# print(type(day_lhb_contents[0].text))
# print(list((day_lhb_contents[0].text).split("\n")))
# print(day_lhb_contents[0].text)
# print(day_lhb_contents[0]['href'])
# print(day_lhb_contents[0]['stockcode'])
# print(day_lhb_contents[0]['rid'])

# 获取方式
# for item in day_lhb_contents:
#     item.text
#     item['href']
#     item['stockcode']
#     item['rid']

array = []
day_lhb_contents = soup.select('body > div > table > tbody > tr')
for i, item in enumerate(day_lhb_contents):
    item_list = (item.text).split("\n")
    # item_list.pop(1) if item_list[1] == ' 3日' else item_list
    item_list.insert(1, "1日") if item_list[1] != ' 3日' else item_list
    item_list[1] = item_list[1].replace(" ", "")
    item_list[3] = item_list[3].replace(" ", "")
    item_list[5] = item_list[5].replace(" ", "")
    item_list[6] = item_list[6].replace("%", "").replace(" ", "")
    item_list[7] = round(float(item_list[7].replace("亿", "").replace(" ", "")), 3) if "亿" in item_list[7] else round(float(item_list[7].replace("万", "").replace(" ", ""))/10000, 3)
    item_list[8] = round(float(item_list[8].replace("亿", "").replace(" ", "")), 3) if "亿" in item_list[8] else round(float(item_list[8].replace("万", "").replace(" ", ""))/10000, 3)
    print(item_list, "------------" ,  len(item_list))
    array.append(item_list)

df = pd.DataFrame(array, columns = ['1', 'type','2', 'code', 'name', 'price', 'ratio', 'amount', 'net_buy', '3',])

print(df)
print(len(df))
print(len(df))

df = df.drop_duplicates(subset=['code'], keep="first")
print(len(df))


print(round(1.0357,2))

import time
from multiprocessing import Pool
from sqlalchemy import create_engine
import  pymysql
import random


host = 'yunfuwu01'
# host = '127.0.0.1'
user = 'root'
passwd = 'root'
port = '3306'
db = 'akshare'

engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, passwd, host, port, db),
                       encoding='utf-8',
                       echo=False,
                       pool_pre_ping=True,
                       pool_recycle=3600)


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


#工具类，删除数据
def dml_mysql(sql):
    conn = pymysql.connect(host=host, port=3306, user=user, passwd=passwd, db=db)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()

dat = '2022-02-18'
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


def get_dragon_tiger_info_task(is_init=False):
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


if __name__ == '__main__':
    start_time = time.time()
    get_dragon_tiger_info_task(is_init=True)
    print("共消耗：", time.time() - start_time)































