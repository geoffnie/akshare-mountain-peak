
from __future__ import print_function
import datetime
from dateutil.relativedelta import relativedelta

import time
from sqlalchemy import create_engine
import logging
import ast
import pandas as pd
import numpy as np

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='my.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)




# 显示所有列
# pd.set_option('display.max_columns', 10)
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 200)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 200)
# 显示宽度
pd.set_option('display.width', 200)







def mymovefile(srcfile,dstfile):
    import os
    import shutil
    if not os.path.isfile(srcfile):
        print("%s not exist!"%(srcfile))
    else:
        fpath,fname=os.path.split(dstfile)    #分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)                #创建路径
        shutil.move(srcfile,dstfile)          #移动文件
        print("move %s -> %s"%( srcfile,dstfile))

def mycopyfile(srcfile,dstfile):
    import os
    import shutil
    if not os.path.isfile(srcfile):
        print("%s not exist!"%(srcfile))
    else:
        fpath,fname=os.path.split(dstfile)    #分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)                #创建路径
        shutil.copyfile(srcfile,dstfile)      #复制文件
        print("copy %s -> %s"%( srcfile,dstfile))

def mydeletefile(srcfile):
    import os
    if not os.path.isfile(srcfile):
        print("%s not exist!"%(srcfile))
    else:
        os.remove(srcfile)
        print("delete----%s "%( srcfile))

def handle_file(root, file):
    import platform
    if platform.system().lower() == 'windows':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '\\' + file
            mydeletefile(srcfile)

    elif platform.system().lower() == 'linux':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '/' + file
            mydeletefile(srcfile)

def handle_file_move(root, file, dstfile_path):
    import os
    import platform
    if platform.system().lower() == 'windows':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '\\' + file
            dstfile = dstfile_path + '\\' + file
            if not os.path.isfile(dstfile):
                dstfile = dstfile_path + '\\' + file
            else:
                cur_time = time.time()
                dstfile = dstfile_path + '\\' + file +'_{0}'.format(str(int(cur_time)))
            mymovefile(srcfile,dstfile)

    elif platform.system().lower() == 'linux':
        #创建文件夹、解析完成，移动文件，如果文件同名存在，则附加当前时间戳
        if file !='':
            srcfile = root + '/' + file
            dstfile = dstfile_path + file
            if not os.path.isfile(dstfile):
                dstfile = dstfile_path  + file
            else:
                cur_time = time.time()
                dstfile = dstfile_path + file +'_{0}'.format(str(int(cur_time)))
            mymovefile(srcfile,dstfile)


# 链接sqlite数据库，用于查询数据或者插入等操作
def connect_sqlite(sql,db):
    import sqlite3
    # 连接数据库(如果不存在则创建)
    conn = sqlite3.connect(db)
    # 创建游标
    cursor = conn.cursor()
    # 执行任务
    cursor.execute(sql)
    result = cursor.fetchall()
    col_result = cursor.description
    if col_result is None:
        print("col_result is none")
        values = pd.DataFrame([[]])
    else:
        col = pd.DataFrame(col_result)
        values = pd.DataFrame(result, columns=col.iloc[:, 0])
    # 提交事物
    conn.commit()
    # 关闭游标
    cursor.close()
    # 关闭连接
    conn.close()
    return values

def process_excel(excel_path):
    df = pd.read_excel(excel_path, sheet_name='Report_ReportRandom', header=None)#.replace(np.nan, '', regex=True)

    #获取空行索引
    index_nullrow = df.isnull().T.all().index[df.isnull().T.all() == True]
    new_index = [i + 1 for i in index_nullrow if df.iloc[i + 1, 1] is not np.nan]
    # df = df.dropna()
    # df.dropna(axis=0)
    df = df.dropna(axis=0, how='all')
    df = df.dropna(axis=1, how='all')
    df = df.reset_index(drop=True)
    df.columns = range(len(df.columns))

    #查找特殊标识符所在行，
    special_mark = ['Begin date:', 'End date:', 'Production parameters', 'Thickness', 'Hot end losses', 'Stacker details', 'Production packing data', 'Packing data', 'Lossed details']
    special_mark_index = []
    print(df)

    for i in special_mark:
        for j in range(len(df)):
            set_row = list(set(df.iloc[j, :].tolist()))
            if i in set_row:
                special_mark_index.append(j)
                break

    print(special_mark_index)
    print()

    print(df.shape[1])
    import re
    # colnum_Production_parameters = 1
    # colnum_Production_values = 2
    # colnum_Defect_density = 1
    for i in range(df.shape[1]):
        set_col = list(set(df.iloc[:, i].tolist()))
        print("=====",set_col )
        #判断结束符，全部字符都转化为小写进行比较
        if re.search("Production parameters".lower(), (''.join([str(x) for x in set_col])).lower()) != None:
            colnum_Production_parameters = i
            print("--------------------------")
        elif re.search("Production values".lower(), (''.join([str(x) for x in set_col])).lower()) != None:
            colnum_Production_values = i
            print()
        elif re.search("Defect density".lower(), (''.join([str(x) for x in set_col])).lower()) != None:
            colnum_Defect_density = i
    print(colnum_Production_parameters, colnum_Production_values,colnum_Defect_density)

    array_datas = []

    # time
    print()
    print((df.iloc[special_mark_index[0], :]).dropna(axis=0, how='all').tolist()[1] )
    print((df.iloc[special_mark_index[1], :]).dropna(axis=0, how='all').tolist()[1] )
    array_datas.append(["StartDate", (df.iloc[special_mark_index[0], :]).dropna(axis=0, how='all').tolist()[1]] )
    # array_datas.append(["EndDate", (df.iloc[special_mark_index[1], :]).dropna(axis=0, how='all').tolist()[1] ])
    array_datas.append(["EndDate", time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(excel_path[-20: excel_path.find(".")].replace("_", " "), "%Y%m%d %H%M%S"))])

    # Production parameters
    print()
    df_Production_parameters = df.iloc[special_mark_index[2] + 1:special_mark_index[3], colnum_Production_parameters:colnum_Production_values - 1]
    df_Production_parameters = df_Production_parameters.dropna(axis=0, how='all').dropna(axis=1, how='all')
    df_Production_parameters = df_Production_parameters.reset_index(drop=True)
    df_Production_parameters.columns = range(len(df_Production_parameters.columns))
    print(df_Production_parameters)
    key_pp = [col.split('[')[0].strip().replace(" ", "") for col in df_Production_parameters.iloc[:, 0].tolist()]
    value_pp = df_Production_parameters.iloc[:, 1].tolist()
    for m in range(len(df_Production_parameters)):
        array_datas.append([key_pp[m], value_pp[m]])

    # Production values
    df_Production_values = df.iloc[special_mark_index[2] + 1:special_mark_index[3], colnum_Production_values - 1:colnum_Defect_density - 1]
    df_Production_values = df_Production_values.dropna(axis=0, how='all').dropna(axis=1, how='all')
    df_Production_values = df_Production_values.reset_index(drop=True)
    df_Production_values.columns = range(len(df_Production_values.columns))
    print(df_Production_values)
    col_pv = df_Production_values.iloc[0, 1:3]
    # 获取df1_1中key和value
    for row, row_num in zip(df_Production_values.iloc[1:-1, 0], range(len(df_Production_values) - 2)):
        for col, col_num in zip(col_pv, range(len(col_pv))):
            key_pv = row + "+" + col
            key_pv = key_pv.strip().replace(" ", "")
            value_pv = df_Production_values.iloc[row_num + 1, col_num + 1]
            if type(value_pv) is not int and type(value_pv) is not float and type(
                    value_pv) is not np.int64 and type(value_pv) is not np.float64:
                value_pv = value_pv.strip()
            #        print(value_pv + ": " + str(value_pv))
            array_datas.append([key_pv, value_pv])
    # 获取Production values中的Yield
    array_datas.append(["Yield", df_Production_values.iloc[-1, 1]])
    print(array_datas)


    # Thickness Value  ==>需要组合name：value, 且需要变为字典数组
    df_Thickness_Value = df.iloc[special_mark_index[3] + 1:special_mark_index[4], :]
    df_Thickness_Value = df_Thickness_Value.dropna(axis=0, how='all').dropna(axis=1, how='all')
    df_Thickness_Value = df_Thickness_Value.reset_index(drop=True)
    df_Thickness_Value.columns = range(len(df_Thickness_Value.columns))
    df_Thickness_Value = df_Thickness_Value.iloc[ : , :2]
    df_Thickness_Value = df_Thickness_Value.dropna(axis=0, how='all').dropna(axis=1, how='all')
    df_Thickness_Value = df_Thickness_Value.reset_index(drop=True)
    df_Thickness_Value.columns = range(len(df_Thickness_Value.columns))
    df_Thickness_Value.columns = df_Thickness_Value.iloc[0, :].tolist()
    df_Thickness_Value = df_Thickness_Value.iloc[1:, :] #排除Total行
    text_Thickness_Value = str(df_Thickness_Value.to_dict('records')).replace("'", "`")
    # text_Thickness_Value = df_Thickness_Value.to_dict('records')
    print(df_Thickness_Value)
    print(text_Thickness_Value)
    # 获取Thickness Value--
    array_datas.append(["Thickness", text_Thickness_Value])



    for i in range(df.shape[1]):
        set_col = list(set(df.iloc[:, i].tolist()))
        # print("=====",set_col )
        if "Hot end losses" in set_col:
            colnum_Hot_end_losses = i
            print("--------------------------")
        elif "Cold end losses" in set_col:
            colnum_Cold_end_losses = i
            print()

    # Hot end losses
    df_Hot_end_losses = df.iloc[special_mark_index[4] + 1:special_mark_index[5], colnum_Hot_end_losses:colnum_Cold_end_losses - 1]
    df_Hot_end_losses = df_Hot_end_losses.dropna(axis=0, how='all').dropna(axis=1, how='all')
    df_Hot_end_losses = df_Hot_end_losses.reset_index(drop=True)
    df_Hot_end_losses.columns = range(len(df_Hot_end_losses.columns))
    # df_Hot_end_losses.columns = df_Hot_end_losses.iloc[0, :].tolist()
    df_Hot_end_losses = df_Hot_end_losses.iloc[:, [0, 2, 3]]
    df_Hot_end_losses.columns = range(len(df_Hot_end_losses.columns))
    col_hel = df_Production_values.iloc[0, 1:3]
    # 获取df1_1中key和value
    for row, row_num in zip(df_Hot_end_losses.iloc[1:-1, 0], range(len(df_Hot_end_losses) - 2)):
        for col, col_num in zip(col_hel, range(len(col_hel))):
            key_hel = row + "+" + col
            key_hel = key_hel.strip().replace(" ", "")
            value_hel = df_Hot_end_losses.iloc[row_num + 1, col_num + 1]
            if type(value_hel) is not int and type(value_hel) is not float and type(
                    value_hel) is not np.int64 and type(value_hel) is not np.float64:
                value_hel = value_hel.strip()
            #        print(value_pv + ": " + str(value_pv))
            array_datas.append([key_hel, value_hel])
    print(df_Hot_end_losses)
    print(special_mark_index)


    # Production packing data   ==》需要进行数组处理,需要组合name：value
    df_Production_packing_data = df.iloc[special_mark_index[6] + 1:special_mark_index[7], :]
    df_Production_packing_data = df_Production_packing_data.dropna(axis=0, how='all').dropna(axis=1, how='all')
    df_Production_packing_data = df_Production_packing_data.reset_index(drop=True)
    df_Production_packing_data.columns = range(len(df_Production_packing_data.columns))
    df_Production_packing_data.columns = df_Production_packing_data.iloc[0, :].tolist()
    df_Production_packing_data = df_Production_packing_data.iloc[1:-1, :] #排除Total行
    print(df_Production_packing_data)
    text_Production_packing_data = str(df_Production_packing_data.to_dict('records')).replace("'", "`")
    # text_Production_packing_data = df_Production_packing_data.to_dict('records')
    print(text_Production_packing_data)
    # 添加Production packing data数据
    array_datas.append(["Productionpackingdata", text_Production_packing_data])
    print(pd.DataFrame(array_datas))
    print((pd.DataFrame(array_datas)).iloc[:, 0].tolist())
    df = pd.DataFrame(array_datas)
    print(len([col.lower() for col in (pd.DataFrame(array_datas)).iloc[:, 0].tolist()]))
    df.iloc[:, 0] = [col.lower() for col in (pd.DataFrame(array_datas)).iloc[:, 0].tolist()]
    print(df)
    print("=============================")
    # print(len(text_Production_packing_data.encode('utf-8')))
    # print(len(text_Production_packing_data))

    df_datas = df
    # df_datas = pd.DataFrame(array_datas, columns=['dataaddr', 'value']).replace(np.nan, '', regex=True)
    df_datas.columns = ['dataaddr', 'value']

    print("==================================")
    # print(df_datas)
    return df_datas


def tranform_send_msg(excel_path):
    # excel_path = "C:\\Users\\jiuxin\\Desktop\\Report_ReportRandom.xlsx"
    # excel_path = "C:\\Users\\jiuxin\\Desktop\\Prod_Rep_20220223_110057.xlsx"
    # db_points = r"D:\tmp\sqlite_test\points (10).db"
    db_points = r"C:\Users\jiuxin\Desktop\points (10).db"
    # db_buffered_msg = "D:\\tmp\\sqlite_test\\buffered_msg.db"
    # excel_path = "/mnt/xy/realTime_reports/Prod_Rep_20211207_160223.xlsx"
    db_points = "/root/IIG3000/gwagent/config/points.db"
    db_buffered_msg = "/root/IIG3000/datacollecter/buffered_msg.db"
    gwid_file = "/root/IIG3000/gwagent/config/gateway.properties"
    df_datas = process_excel(excel_path)

    #查询sqlite中的points
    sql_points = '''select ptid, lower(dataaddr) as dataaddr from main.points;'''
    # sql = '''CREATE TABLE if not exists Student(id integer PRIMARY KEY autoincrement, Name  varchar(30), Age integer)'''
    df_compare = connect_sqlite(sql_points, db_points)
    # print(df_compare)

    # 获取ptid，并且合并，找出key，对应的ptid
    datas = pd.merge(df_datas, df_compare, how='left', on=['dataaddr']).replace(np.nan, '', regex=True)
    print(len(datas[datas['ptid'] != '']), len(datas[datas['ptid'] == '']))
    # datas_abnormal = datas[datas['dataaddr'].isin(['thickness', 'productionpackingdata']) ][['ptid', 'value', 'dataaddr']]
    # datas = datas[datas['ptid'] != '' ][datas['dataaddr'] != 'thickness' ][datas['dataaddr'] != 'productionpackingdata'][['ptid', 'value']]
    # print(datas_abnormal)
    datas = datas[datas['ptid'] != ''][['ptid', 'value']]
    print("--------------===!!!!!!!!!!!!!")

    datas.reset_index(drop=True)
    datas.columns = ['ptid', 'result']
    datas['ptid'] = datas['ptid'].astype('int64').astype(str)
    datas['result'] = datas['result'].astype(str)
    print(datas)

    # 获取datas_text，并且增加第三部分数据
    datas_text = datas.to_dict(orient='records')
    print(datas_text)
    # print(datas_abnormal)
    # for i in range(len(datas_abnormal)):
    #     datas_text.append({'ptid': datas_abnormal.iloc[i, 0],'result':ast.literal_eval('{0}'.format(datas_abnormal.iloc[i, 1]))})
    print(datas_text)




    # file_timestamp = time.mktime(time.strptime(time.time(), "%Y-%m-%d %H:%M:%S"))
    # print("file_timestamp_type==2,file_timestamp:{0}".format(file_timestamp))
    print(round(time.time() * 1000))
    file_timestamp = round(time.time() * 1000)

    #获取gwid
    # gwid_file = "C:\\Users\\jiuxin\\Desktop\\gateway.properties"
    gwid_list = pd.read_table(gwid_file).iloc[:, 0].tolist()
    # print(gwid_list)
    print()
    gwid = [m.split("=")[1] for m in gwid_list if "=" in m and m.split("=")[0] == 'gateway.binding.gwid'][0]

    datas_text_dict = ast.literal_eval('{0}'.format(datas_text))
    json_text = {"datas": datas_text_dict, "gwid": "{0}".format(gwid),
                 "timestamp": "{0}".format(round(time.time() * 1000))}
    json_text = str(json_text).replace("'", '''"''')#.replace("`", "'")
    print(json_text)

    #插入数据到sqlite
    try:
        sql_insert = '''insert into main.msgBuffered (msg) VALUES ('{0}');'''.format(json_text)
        # sql = '''CREATE TABLE if not exists Student(id integer PRIMARY KEY autoincrement, Name  varchar(30), Age integer)'''
        df = connect_sqlite(sql_insert, db_buffered_msg)
        print("数据已插入到sqlite")
    except Exception as e:
        print(e)
        print("ERROR:数据已插入sqlite中失败！")

    # #查询数据--sqlite
    # try:
    #     sql_query= '''select * from  main.msgBuffered '''
    #     # sql = '''CREATE TABLE if not exists Student(id integer PRIMARY KEY autoincrement, Name  varchar(30), Age integer)'''
    #     df = connect_sqlite(sql_query, db_buffered_msg)
    #     print(df)
    #     print("sqlite中查询数据成功")
    # except Exception as e:
    #     print(e)
    #     print("ERROR:sqlite中查询数据失败！")

    #移动文件到/root/CIFS/tobedelete路径下
    try:
        move_path = excel_path.split(excel_path.split("/")[-1])[0]
        move_file = excel_path.split("/")[-1]
        move_to_path = '/root/CIFS/tobedelete/'
        handle_file_move(move_path, move_file, move_to_path)
        print("已将文件{0}移动文件到路径{1}下".format(excel_path, move_to_path))
    except Exception as e:
        print(e)
        print("ERROR:文件移动失败！")

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


WATCH_PATH = '/mnt/xy/realTime_reports'  # 监控目录

class FileMonitorHandler(FileSystemEventHandler):
    def __init__(self, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        # 监控目录 目录下面以device_id为目录存放各自的图片
        self._watch_path = WATCH_PATH

    # 重写文件创建函数，文件创建都会触发文件夹变化
    def on_created(self, event):
        if not event.is_directory:  # 文件改变都会触发文件夹变化
            file_path = event.src_path
            excel_path = file_path
            time.sleep(2)
            try:
                tranform_send_msg(excel_path)
            except Exception as e:
                print(e)
                print("该文件处理失败！存在异常情况。")
            print("该文件夹新增文件: %s " % file_path)


if __name__ == '__main__':
    # df = get_data()
    print("开始运行：")

    event_handler = FileMonitorHandler()
    observer = Observer()
    observer.schedule(event_handler, path=WATCH_PATH, recursive=True)  # recursive递归的
    observer.start()
    observer.join()

    # start_time = time.time()
    # excel_path = "C:\\Users\\jiuxin\\Desktop\\Prod_Rep_20220223_110057.xlsx"
    # # process_excel(excel_path)
    # tranform_send_msg(excel_path)
    # end_time = time.time()
    # print("时间消耗s：")
    # print(end_time - start_time)

























