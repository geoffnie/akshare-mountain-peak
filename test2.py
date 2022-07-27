# -*- coding:utf-8 -*-
#@Time : 2022/4/2 13:48
#@Author: Geoff Nie
#@File : test2.py

from pydruid.db import connect
import pandas as pd


druid_host = '124.70.188.197'   #信义服务器地址
druid_port = 8888          #信义服务器端口号
username = 'root'
password = 'Jiuxin2120@nm'
host = '172.18.4.34'
db = 'dataquality'


import time

def connect_druid_single(sql):
    conn = connect(host=druid_host, port=druid_port, path='/druid/v2/sql/', scheme='http')
    curs = conn.cursor()

    druid_points_num = []

    time_start = time.time()
    print("time_start", time_start)
    print("000")
    curs.execute(sql)
    print(curs)
    time1 = time.time()
    print("time1: ", time1)

    print("aaa")
    result = curs.fetchall()
    time2 = time.time()
    print("time2: ", time2)
    col_result = curs.description
    print("bbb")

    # 获取字段信息
    columns = []
    if col_result is None:
        print("col_result is none")
        druid_points_num.append(0)

    else:
        for m in range(len(col_result)):
            columns.append(col_result[m][0])
            # df_druid = pd.DataFrame(columns=columns)
        df_druid = pd.DataFrame(result)

        # #获取查询数据
        # for n in range(len(result)):
        #     df_druid.loc[n] = list(result[n])
    time3 = time.time()
    print("time3: ", time3)
    print("=====================")

    curs.close()
    print("time1: ", time_start - time1)
    print("time2: ", time_start - time2)
    print("time3: ", time_start - time3)
    return df_druid



sql = '''SELECT "__time", "cur_name", "cur_val", "devicCode", "ep_id", "gw_id", "mche_id", "pd_motr_pnt_id", "plc_tp_id", "pt_name", "signal_name", "status", "tagCode", "type"
FROM "ods.motr.pnt.data.ip.tab"
WHERE "ep_id" = '2000000003' and "__time" >='2022-03-02 00:00:00' and "__time" <='2022-04-02 00:00:00' -- and "cur_val" = -2075.955
'''


# sql = '''SELECT "__time", "cur_name", "cur_val", "devicCode", "ep_id", "gw_id", "mche_id", "pd_motr_pnt_id", "plc_tp_id", "pt_name", "signal_name", "status", "tagCode", "type"
# FROM "ods.motr.pnt.data.ip.tab"
# -- limit 10
# WHERE  "__time" >='2022-04-01 23:55:00' and "__time" <='2022-04-02 00:00:00' '''

df = connect_druid_single(sql)

print(df.head(5))
print(len(df))


if "__main__" == "":
    print()

