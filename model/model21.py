# -*- coding:utf-8 -*-
#@Time : 2022/4/19 11:15
#@Author: Geoff Nie
#@File : model21.py


#import sqlite3
import pandas as pd
import shutil
import os
import configparser
import sys
import sqlalchemy
from sqlalchemy import create_engine
import time
import paho.mqtt.client as mqtt
import platform
from obs import ObsClient
import logging
import numpy as np
from pydruid.db import connect
import pymysql
import signal
from urllib import parse

#显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',200)
pd.set_option('display.width', 100)


import getopt
import consul
import yaml

def get_args():
    argv = sys.argv[1:3]
    try:
        opts, args = getopt.getopt(argv, "h:p:",
                                   ["spring.cloud.consul.host=",
                                    "spring.cloud.consul.port="])  # 长选项模式
    except:
        print("Error")
    for opt, arg in opts:
        if opt in ['-h', '--spring.cloud.consul.host']:
            consul_host = arg
        elif opt in ['-p', '--spring.cloud.consul.port']:
            consul_port = arg
    print(consul_host, consul_port)
    return consul_host, consul_port


def get_config_info_from_consul():
    #    consul_url='csappserver.jiuxiniot.com'
    #    consul_port=8500
    #    consul_url=get_config('spring' , 'spring.cloud.consul.host')
    #    consul_port=str(get_config('spring' , 'spring.cloud.consul.port'))
    try:
        consul_url, consul_port = get_args()
        c = consul.Consul(host=consul_url, port=consul_port, scheme='http')
        service = 'config/gateway-info-xy/data'
        data = c.kv.get(service)
        x = yaml.safe_load(str.encode((bytes.decode(data[1]['Value'])).replace("\t", "")))
        return x
    except Exception as e:
        print(e)

# 获取配置类
class ConfigParser(configparser.RawConfigParser):
    def __init__(self, **kwargs):
        kwargs['allow_no_value'] = True
        configparser.RawConfigParser.__init__(self, **kwargs)

    def __remove_quotes(self, value):
        quotes = ["'", "\""]
        for quote in quotes:
            if len(value) >= 2 and value[0] == value[-1] == quote:
                return value[1:-1]
        return value

    def get(self, section, option):
        value = configparser.RawConfigParser.get(self, section, option)
        return self.__remove_quotes(value)


# 获取配置文件
def get_config(read_default_group, key, arg=None):
    """
    获取配置值
    :param key:
    :return:
    """
    if arg:
        return arg

    try:
        cfg = ConfigParser()
        if sys.platform.startswith("win"):
            work_path = os.path.dirname(os.path.realpath(__file__))
            read_default_file = os.path.join(work_path, 'config.ini')
        else:
            work_path = os.path.dirname(os.path.realpath(__file__))
            read_default_file = os.path.join(work_path, 'config.cnf')

        cfg.read(os.path.expanduser(read_default_file))
        return cfg.get(read_default_group, key)
    except Exception:
        return arg

def get_gateway_datainfo_from_mysql():
    # 获取mysql相关信息
    config_data = get_config_info_from_consul()
    print("===========")
    print(config_data)

    username = config_data['mysql']['username']
    password = config_data['mysql']['password']
    password = parse.quote_plus(password)
    host = config_data['mysql']['host']
    port = config_data['mysql']['port']
    db = config_data['mysql']['db']

    # username = get_config('mysql', 'username')
    # password = get_config('mysql', 'password')
    # password = parse.quote_plus(password)
    # host = get_config('mysql', 'host')
    # db = get_config('mysql', 'db')

    engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{4}/{3}?charset=utf8'.format(username, password, host, db, port))

    # 获取历史数据，用于比较及删除、更新及插入新数据
    sql = ''' select * from (
select  t.ep_id, t1.mac_addr as mac, convert(now(), char(20)) as endTime, convert(compare_time, char(20)) as startTime , case when t.updatetime >= compare_time then 1 else 0 end as status from (
    select ep_id, gw_id ,max(updatetime) as updatetime, date_add(now(), interval - 20 minute) as compare_time from (
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id 
		union all 
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status_str where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id		
		) t group by ep_id, gw_id
    ) t
    left join 
    (select * from amdb.gateway where mac_addr not in ('99:00:AA:AA:02:3E','99:00:AA:AA:02:1C','99:00:AA:AA:02:E0','99:00:AA:AA:03:15')) t1
    on t.gw_id = t1.gw_id 
    where t.gw_id in (
    select a.gw_id
    from
    amdb.gateway a
    left join amdb.gateway_x_enterprise b on a.gw_id = b.gw_id
    left join ytfinst.sys_enterprise d ON b.ep_id=d.ep_id
    where 1=1  and (
    father_id IN (select father_id FROM ytfinst.sys_enterprise WHERE FIND_IN_SET(29, authorityMenu) ) AND FIND_IN_SET(29, authorityMenu) )
    and FIND_IN_SET(29, d.authorityMenu)
    ) -- 获取am显示信义里面所有的网关
    and convert(t.updatetime, char(20)) != 'null' and t1.mac_addr is not null
union all
select  t.ep_id, t1.mac_addr as mac, convert(now(), char(20)) as endTime, convert(compare_time, char(20)) as startTime , case when t.updatetime >= compare_time then 1 else 0 end as status from (
    select ep_id, gw_id ,max(updatetime) as updatetime, date_add(now(), interval - 30 minute) as compare_time from (
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id 
		union all 
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status_str where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id		
		) t group by ep_id, gw_id
    ) t
    left join 
    (select * from amdb.gateway where mac_addr in ('99:00:AA:AA:02:3E','99:00:AA:AA:02:1C','99:00:AA:AA:02:E0','99:00:AA:AA:03:15')) t1
    on t.gw_id = t1.gw_id 
    where t.gw_id in (
    select a.gw_id
    from
    amdb.gateway a
    left join amdb.gateway_x_enterprise b on a.gw_id = b.gw_id
    left join ytfinst.sys_enterprise d ON b.ep_id=d.ep_id
    where 1=1  and (
    father_id IN (select father_id FROM ytfinst.sys_enterprise WHERE FIND_IN_SET(29, authorityMenu) ) AND FIND_IN_SET(29, authorityMenu) )
    and FIND_IN_SET(29, d.authorityMenu)
    ) -- 获取am显示信义里面所有的网关
    and convert(t.updatetime, char(20)) != 'null' and t1.mac_addr is not null
 ) t   order by status	   '''
    # 获取查询数据
    df = pd.read_sql_query(sql, engine)

    username_guangneng = config_data['mysql']['username_guangneng']
    password_guangneng = config_data['mysql']['password_guangneng']
    password_guangneng = parse.quote_plus(password_guangneng)
    host_guangneng = config_data['mysql']['host_guangneng']
    port_guangneng = config_data['mysql']['port_guangneng']
    db_guangneng = config_data['mysql']['db_guangneng']

    # username_guangneng = get_config('mysql', 'username_guangneng')
    # password_guangneng = get_config('mysql', 'password_guangneng')
    # password_guangneng = parse.quote_plus(password_guangneng)
    # host_guangneng = get_config('mysql', 'host_guangneng')
    # port_guangneng = get_config('mysql', 'port_guangneng')
    # db_guangneng = get_config('mysql', 'db_guangneng')

    engine_guangneng = create_engine('mysql+pymysql://{0}:{1}@{2}:{4}/{3}?charset=utf8'.format(username_guangneng, password_guangneng, host_guangneng, db_guangneng, port_guangneng))
    sql_guangneng = '''select * from (
select  
	ep_id, gw_id, convert(now(), char(20)) as endTime, convert(compare_time, char(20)) as startTime , case when t.updatetime >= compare_time then 1 else 0 end as status from (
    select ep_id, gw_id ,max(updatetime) as updatetime, date_add(now(), interval - 20 minute) as compare_time from (
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id 
		union all 
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status_str where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id		
		) t group by ep_id, gw_id
    ) t
 ) t   order by status	 '''

    df_guangneng_info = pd.read_sql_query(sql_guangneng, engine_guangneng)


    sql_gateway_info_guangneng = '''  select t2.ep_id ,t2.gw_id, t1.mac_addr as mac from  amdb.gateway t1
 left join 
amdb.gateway_x_enterprise  t2 
 on t1.gw_id = t2.gw_id 
 where t2.ep_id = '2000000007' '''

    # 从信义玻璃数据库中获取信义光能网关信息
    df_gateway_info_guangneng = pd.read_sql_query(sql_gateway_info_guangneng, engine)
    print(len(df_gateway_info_guangneng))

    #将光能网关信息与数据信息关联起来
    df_guangneng = pd.merge(df_guangneng_info, df_gateway_info_guangneng, how='left', on = ['ep_id', 'gw_id'])[['ep_id', 'mac', 'endTime', 'startTime', 'status']]


    #将信义玻璃与信义光能合并起来
    df_result = pd.concat([df, df_guangneng])
    df_result = df_result.sort_values(by=['status','ep_id', 'mac'])
    df_result = df_result.reset_index(drop=True)


    # 返回数据
    return df_result


def get_gateway_statusinfo_from_mysql():
    # 获取mysql相关信息

    config_data = get_config_info_from_consul()

    username = config_data['mysql']['username']
    password = config_data['mysql']['password']
    password = parse.quote_plus(password)
    host = config_data['mysql']['host']
    port = config_data['mysql']['port']
    db = config_data['mysql']['db']

    # username = get_config('mysql', 'username')
    # password = get_config('mysql', 'password')
    # password = parse.quote_plus(password)
    # host = get_config('mysql', 'host')
    # db = get_config('mysql', 'db')

    engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{4}/{3}?charset=utf8'.format(username, password, host, db, port))

    # 获取历史数据，用于比较及删除、更新及插入新数据
    sql = ''' select  t.ep_id, t1.mac_addr as mac, convert(now(), char(20)) as time , case when t.updatetime > compare_time then 1 else 0 end as status from (
    select ep_id, gw_id ,max(updatetime) as updatetime, date_add(now(), interval - 20 minute) as compare_time from (
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id 
		union all 
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status_str where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id
        union all
    select ep_id, gw_id ,status_time as updatetime  from datamart.network_link_status where status !=-1 and  ep_id = '2000000003' and mac_addr not in
    ('99:50:AA:AA:00:62',
    '99:50:AA:AA:00:A7',
    '99:50:AA:AA:00:A6',
    '99:50:AA:AA:00:AD',
    '99:50:AA:AA:00:66',
    '99:50:AA:AA:00:5D',
    '99:50:AA:AA:00:5C',
    '99:50:AA:AA:00:82',
    '99:50:AA:AA:00:84',
    '99:50:AA:AA:00:57',
    '99:50:AA:AA:00:A2',
    '99:50:AA:AA:00:A3',
    '99:50:AA:AA:02:07',
    '99:50:AA:AA:02:09',
    '99:50:AA:AA:01:2B',
    '99:50:AA:AA:01:3A',
    '99:50:AA:AA:02:2B',
    '99:50:AA:AA:02:1D',
    '99:50:AA:AA:01:4E',
    '99:50:AA:AA:01:4D',
    '99:50:AA:AA:01:60',
    '99:50:AA:AA:00:C6',
    '99:50:AA:AA:01:78',
    '99:50:AA:AA:01:DF',
    '99:50:AA:AA:01:EA',
    '99:50:AA:AA:01:77',
    '99:50:AA:AA:02:11',
    '99:50:AA:AA:02:13',
    '99:50:AA:AA:02:0D',
    '99:50:AA:AA:02:37',
    '99:50:AA:AA:02:3C',
    '99:50:AA:AA:02:35',
    '99:50:AA:AA:01:28',
    '99:50:AA:AA:00:D1',
    '99:50:AA:AA:00:D3') 		
		) t group by ep_id, gw_id
    ) t
    left join 
    amdb.gateway t1
    on t.gw_id = t1.gw_id 
    where t.gw_id in (
    select a.gw_id
    from
    amdb.gateway a
    left join amdb.gateway_x_enterprise b on a.gw_id = b.gw_id
    left join ytfinst.sys_enterprise d ON b.ep_id=d.ep_id
    where 1=1  and (
    father_id IN (select father_id FROM ytfinst.sys_enterprise WHERE FIND_IN_SET(29, authorityMenu) ) AND FIND_IN_SET(29, authorityMenu) )
    and FIND_IN_SET(29, d.authorityMenu)
    ) -- 获取am显示信义里面所有的网关
    and convert(t.updatetime, char(20)) != 'null' and t1.mac_addr is not null
    order by status'''
    # 获取查询数据
    df = pd.read_sql_query(sql, engine)


    username_guangneng = config_data['mysql']['username_guangneng']
    password_guangneng = config_data['mysql']['password_guangneng']
    password_guangneng = parse.quote_plus(password_guangneng)
    host_guangneng = config_data['mysql']['host_guangneng']
    port_guangneng = config_data['mysql']['port_guangneng']
    db_guangneng = config_data['mysql']['db_guangneng']

    # username_guangneng = get_config('mysql', 'username_guangneng')
    # password_guangneng = get_config('mysql', 'password_guangneng')
    # password_guangneng = parse.quote_plus(password_guangneng)
    # host_guangneng = get_config('mysql', 'host_guangneng')
    # port_guangneng = get_config('mysql', 'port_guangneng')
    # db_guangneng = get_config('mysql', 'db_guangneng')

    engine_guangneng = create_engine(
        'mysql+pymysql://{0}:{1}@{2}:{4}/{3}?charset=utf8'.format(username_guangneng, password_guangneng,
                                                                  host_guangneng, db_guangneng, port_guangneng))
    sql_guangneng = '''  select  ep_id, gw_id , convert(now(), char(20)) as time , case when t.updatetime > compare_time then 1 else 0 end as status from (
    select ep_id, gw_id ,max(updatetime) as updatetime, date_add(now(), interval - 20 minute) as compare_time from (
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id 
		union all 
    select ep_id, gw_id ,max(updatetime) as updatetime  from emq.mche_point_status_str where gw_id !='' and gw_id is not null and updatetime != '' and updatetime is not null group by ep_id, gw_id
        union all
    select ep_id, gw_id ,status_time as updatetime  from datamart.network_link_status where status !=-1  and ep_id = '2000000007'
		) t group by ep_id, gw_id
    ) t
  '''

    df_guangneng_info = pd.read_sql_query(sql_guangneng, engine_guangneng)

    # sql_network_link_status_guangneng = '''     select  ep_id, gw_id , convert(now(), char(20)) as time , case when t.updatetime > compare_time then 1 else 0 end as status from (
    # select ep_id, gw_id ,max(updatetime) as updatetime, date_add(now(), interval - 20 minute) as compare_time from (
    # select ep_id, gw_id ,status_time as updatetime  from datamart.network_link_status where status !=-1  and ep_id = '2000000007'
	# 	) t group by ep_id, gw_id
    # ) t'''
    #
    #
    #
    # df_network_link_status_guangneng = pd.read_sql_query(sql_network_link_status_guangneng, engine_guangneng)
    #
    # if len(df_network_link_status_guangneng) != 0:
    #     list_df_guangneng_info = df_guangneng_info['gw_id'].tolist()
    #     for gw_id in df_network_link_status_guangneng['gw_id'].tolist():
    #         if gw_id in list_df_guangneng_info:
    #             if df_network_link_status_guangneng[df_network_link_status_guangneng['gw_id'] == gw_id]['status'].tolist()[0] == 1\
    #                     and df_guangneng_info[df_guangneng_info['gw_id'] == gw_id]['status'].tolist()[0] == 0:
    #                 df_guangneng_info.loc[df_guangneng_info['gw_id'] == gw_id, 'status'] = 1
    #                 print("已更改", gw_id)
    #             else:
    #                 print()
    #         else :
    #             df_guangneng_info = df_guangneng_info.append(df_network_link_status_guangneng[df_network_link_status_guangneng['gw_id'] == gw_id])
    #             print(df_network_link_status_guangneng[df_network_link_status_guangneng['gw_id'] == gw_id])
    #             print("变化了！！！！！！！！新增网关", gw_id)

    sql_gateway_info_guangneng = ''' select t2.ep_id ,t2.gw_id, t1.mac_addr as mac from  amdb.gateway t1
 left join 
 amdb.gateway_x_enterprise  t2 
 on t1.gw_id = t2.gw_id 
 where t2.ep_id = '2000000007'   '''

    # 从信义玻璃数据库中获取信义光能网关信息
    df_gateway_info_guangneng = pd.read_sql_query(sql_gateway_info_guangneng, engine)
    print(len(df_gateway_info_guangneng))
    print(len(df_guangneng_info))
    print(df_guangneng_info)

    # 将光能网关信息与数据信息关联起来
    df_guangneng_result = pd.merge(df_guangneng_info, df_gateway_info_guangneng, how='left', on=['ep_id', 'gw_id'])[
        ['ep_id', 'mac', 'time', 'status']]

    # 将信义玻璃与信义光能合并起来
    df_result = pd.concat([df, df_guangneng_result])
    df_result = df_result.sort_values(by=['status','ep_id', 'mac'])
    df_result = df_result.reset_index(drop=True)

    # 返回数据
    return df_result

# df = get_gateway_statusinfo_from_mysql()
df = get_gateway_datainfo_from_mysql()
print(df)
# print(len(df))

print(len(df))
import math
print(math.ceil(845 / 200))

for i in range(round(len(df)/200)):
    if i != round(len(df)/200) - 1:
        print()
    else:
        print()
    print("======================")
    print((df.iloc[i*200:i*200 + 200,:]).to_json(orient='records')  )


print(df.iloc[900:90000])












