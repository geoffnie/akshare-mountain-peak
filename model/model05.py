import datetime
from dateutil.relativedelta import relativedelta
import akshare as ak

import time
import pandas as pd
import tushare as ts
import pymysql
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

# 显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 200)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 200)
# 显示宽度
pd.set_option('display.width', 200)


def get_data():
    sql = '''

    '''
    # 获取查询数据
    df = pd.read_sql_query(sql, engine)
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

def sent(txt, url='http://124.70.188.197:8888/unified-console.html#query'):
    # url = 'http://124.70.188.197:8888/unified-console.html#query'
    print(txt, url)

def stock_board_concept_name_ths() -> pd.DataFrame:
    """
    同花顺-板块-概念板块-概念
    http://q.10jqka.com.cn/gn/detail/code/301558/
    :return: 所有概念板块的名称和链接
    :rtype: pandas.DataFrame
    """
    import requests
    from bs4 import BeautifulSoup
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
        , 'Accept': 'text/html, */*; q=0.01'
        , 'hexin-v': 'AyXp3_c9qKdLsMz7T82Vk8xzNOpcYtn2Ixa9SCcK4dxrPksW77LpxLNmzRq2'
        , 'Referer': 'http://data.10jqka.com.cn/funds/gnzjl/'
        , 'X-Requested-With': 'XMLHttpRequest'
    }
    df = pd.DataFrame()
    for i in range(1, 10):
        try:
            print(i,"-------------------------")
            url = 'http://data.10jqka.com.cn/funds/gnzjl/'
            print(url)
            r = requests.get(url, headers=headers)
            soup = BeautifulSoup(r.text, "lxml")
            print(r.text)
            html_list = soup.find('table', attrs={'class': 'm-table'}).find_all('a', attrs={'target': '_blank'})
            name_list = [item.text for item in html_list]
            url_list = [item['href'] for item in html_list]
            temp_df = pd.DataFrame([name_list, url_list], index=['name', 'url']).T
            # print(temp_df)
            filter1 = temp_df['url'].str.contains("http://q\.10jqka\.com\.cn/gn/detail/code")
            # print(filter1)
            temp_df = temp_df[filter1]
            temp_df = temp_df.reset_index(drop=True)
            print(temp_df)
            if len(temp_df)!=0:
                df = pd.concat([df, temp_df], axis=0, ignore_index=True)
            time.sleep(10)
        except Exception as e:
            print(e)
            break

    df = df.reset_index(drop=True)
    print(df)
    return df


if __name__ == '__main__':
    # df = get_data()
#     # print(df)
#     # print(len(df))

    # txt = 'information'
    # sent(txt)
    # sent(txt, 'https://www.runoob.com/python3/ref-set-difference.html')

    stock_board_concept_name_ths()








