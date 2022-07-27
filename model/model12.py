# -*- coding:utf-8 -*-
#@Time : 2022/1/12 15:32
#@Author: Geoff Nie
#@File : model12.py





import pandas as pd
import numpy
import  pymysql
from urllib import parse
from sqlalchemy import create_engine
import json
username = 'root'
# password = parse.quote_plus('Jiuxin2120@nm')
password = 'Jiuxin2120@nm'
# host = '172.18.4.34'
host = '124.70.142.226'
db = 'dataquality'

def connect_mysql(username, password, host, db, sql):
    password = parse.quote_plus(password)
    engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}?charset=utf8'.format(username, password, host, db))

    # 获取查询数据
    df = pd.read_sql_query(sql, engine)

    # 返回数据
    return df

# sql = '''select * from dataquality.long_time_no_data_gwid where create_time = '2021-12-29 01:00:00.0' limit 10'''
sql = '''select UNIX_TIMESTAMP(create_time) as timestamp ,intervaltime_min as value,mac_addr ,gw_name ,gw_location ,org_name ,'gateway_dataupdate_minute_gap' as metric from dataquality.long_time_no_data_gwid where create_time = '2022-01-10 01:00:00.0' limit 10
'''


df = connect_mysql(username, password, host, db, sql)
print(df)


# j = (df.groupby(['create_time' ,'intervaltime_min' ], as_index=False)
#              .apply(lambda x: x[['mac_addr' ,'gw_name' ,'gw_location' ,'org_name']].to_dict('r'))
#              .reset_index()
#              .rename(columns={0:'Tide-Data'       })
#              .to_json(orient='records', force_ascii=False))
#
#
#
# print(j)


# df['tag'] = ''
# for i in range(len(df)):
#     # print(df.loc[i, ['tag']])
#     # print(df.iloc[i, :])
#     # print("============")
#     df.loc[i, ['tag']] = json.dumps(json.loads(df.apply(lambda x: x[["mac_addr", "gw_name","gw_location","org_name"]], axis=1)
#                  # .reset_index()
#                  # .rename(columns={0:'Tide-Data'})
#                  .to_json(orient='records', force_ascii=False))[i],ensure_ascii=False)
#     # print(json.loads(df.apply(lambda x: x[["mac_addr", "gw_name","gw_location","org_name"]], axis=1)
#     #              # .reset_index()
#     #              # .rename(columns={0:'Tide-Data'})
#     #              .to_json(orient='records', force_ascii=False))[i])
# print(df)
#
# df_json = (df[['timestamp','value','metric']].to_json(orient='records', force_ascii=False))
#
# print(df_json)
# print(type(df_json))
# print(json.loads(df_json))
# print(type(json.loads(df_json)[1]))
#
# aaa = json.loads(df_json)
# # print(aaa)
# print(df_json)
#
# for i in range(len(df)):
#     aaa[i]['tags'] = json.loads(df.apply(lambda x: x[["mac_addr", "gw_name","gw_location","org_name"]], axis=1)
#                  .to_json(orient='records', force_ascii=False))[i]
#
# # print(json.dumps(json.loads(df_json)))
# # print(json.dumps(aaa, ensure_ascii=False))


def transform_to_nightingale_msg(df, tags_list):
    df['tag'] = ''
    df_json = (df[['timestamp','value','metric']].to_json(orient='records', force_ascii=False))
    print(df_json)

    json_aaa = json.loads(df_json)
    print("aa")
    json_bbb = json.loads(df.apply(lambda x: x[tags_list], axis=1)
                     .to_json(orient='records', force_ascii=False))

    for i in range(len(df)):
        print(i)
        json_aaa[i]['tags'] = json_bbb[i]

    return json.dumps(json_aaa, ensure_ascii=False)

tags_list = ["mac_addr", "gw_name", "gw_location", "org_name"]

aa = transform_to_nightingale_msg(df, tags_list)

print(aa)




#设备编码空值
sql = '''select t1.*, UNIX_TIMESTAMP(now()) as timestamp, case when t2.value is null then 0 else t2.value end as value, 'gateway_devicecode_null_cnt' as metric from (
select distinct t1.mac_addr, t2.gw_id, replace(replace(replace(t1.gw_nm,"E-4G",""),"-4G",""),"-E","") as gw_name, t1.dtl_addr as gw_location
        ,so.ORGNAME as org_name from amdb.gateway t1 right join
            amdb.gateway_x_enterprise t2
            on t1.gw_id =t2.gw_id 
            left join ytfinst.sys_org so on t2.org_id = so.ORGCODE 
            where t1.mac_addr is not null
            and ORGNAME != "信义玻璃"
) t1 
left join 
 (
select macaddr as mac_addr,count(*) as value from amdb.monitor_point mp where devicCode ='' or devicCode is null group by macaddr
) t2 on t1.mac_addr = t2.mac_addr
order by t2.value desc'''


df = connect_mysql(username, password, host, db, sql)
# df = df.iloc[:5, :]
print(df)

tags_list = ["mac_addr", "gw_name", "gw_location", "org_name"]

aa = transform_to_nightingale_msg(df, tags_list)

print(aa)




#设测点编码空值
sql = '''select t1.*, UNIX_TIMESTAMP(now()) as timestamp, case when t2.value is null then 0 else t2.value end as value, 'gateway_tagcode_null_cnt' as metric from (
select distinct t1.mac_addr, t2.gw_id, replace(replace(replace(t1.gw_nm,"E-4G",""),"-4G",""),"-E","") as gw_name, t1.dtl_addr as gw_location
        ,so.ORGNAME as org_name from amdb.gateway t1 right join
            amdb.gateway_x_enterprise t2
            on t1.gw_id =t2.gw_id 
            left join ytfinst.sys_org so on t2.org_id = so.ORGCODE 
            where t1.mac_addr is not null
            and ORGNAME != "信义玻璃"
) t1 
left join 
 (
select macaddr as mac_addr,count(*) as value from amdb.monitor_point mp where tagCode ='' or tagCode is null group by macaddr
) t2 on t1.mac_addr = t2.mac_addr
order by t2.value desc'''


df = connect_mysql(username, password, host, db, sql)
# df = df.iloc[:5, :]
print(df)

tags_list = ["mac_addr", "gw_name", "gw_location", "org_name"]

aa = transform_to_nightingale_msg(df, tags_list)

print(aa)


#测点编码重复
sql = '''select t1.*, UNIX_TIMESTAMP(now()) as timestamp, case when t2.value is null then 0 else t2.value end as value, 'gateway_tagcode_repeat_cnt' as metric from (
select distinct t1.mac_addr, t2.gw_id, replace(replace(replace(t1.gw_nm,"E-4G",""),"-4G",""),"-E","") as gw_name, t1.dtl_addr as gw_location
        ,so.ORGNAME as org_name from amdb.gateway t1 right join
            amdb.gateway_x_enterprise t2
            on t1.gw_id =t2.gw_id 
            left join ytfinst.sys_org so on t2.org_id = so.ORGCODE 
            where t1.mac_addr is not null
            and ORGNAME != "信义玻璃"
) t1 
left join 
 (
select mac_addr, sum(value) as value from (
select macaddr as mac_addr, tagCode ,count(*) as value from amdb.monitor_point mp group by macaddr, tagCode having value > 1
) t group by mac_addr ) t2 on t1.mac_addr = t2.mac_addr
order by t2.value desc'''


df = connect_mysql(username, password, host, db, sql)
# df = df.iloc[:5, :]
print(df)

tags_list = ["mac_addr", "gw_name", "gw_location", "org_name"]

aa = transform_to_nightingale_msg(df, tags_list)

print(aa)




#数据地址重复
sql = '''select t1.*, UNIX_TIMESTAMP(now()) as timestamp, case when t2.value is null then 0 else t2.value end as value, 'gateway_dataaddr_repeat_cnt' as metric from (
select distinct t1.mac_addr, t2.gw_id, replace(replace(replace(t1.gw_nm,"E-4G",""),"-4G",""),"-E","") as gw_name, t1.dtl_addr as gw_location
        ,so.ORGNAME as org_name from amdb.gateway t1 right join
            amdb.gateway_x_enterprise t2
            on t1.gw_id =t2.gw_id 
            left join ytfinst.sys_org so on t2.org_id = so.ORGCODE 
            where t1.mac_addr is not null
            and ORGNAME != "信义玻璃"
) t1 
left join 
 (
select mac_addr, sum(value) as value from (
select macaddr as mac_addr, pdaddr, data_addr ,count(*) as value from amdb.monitor_point mp group by macaddr, pdaddr, data_addr  having value > 1
) t group by mac_addr 
) t2 on t1.mac_addr = t2.mac_addr
order by t2.value desc'''


df = connect_mysql(username, password, host, db, sql)
# df = df.iloc[:5, :]
print(df)

tags_list = ["mac_addr", "gw_name", "gw_location", "org_name"]

aa = transform_to_nightingale_msg(df, tags_list)

print(aa)

















