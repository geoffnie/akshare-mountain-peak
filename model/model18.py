# -*- coding:utf-8 -*-
#@Time : 2022/2/15 16:53
#@Author: Geoff Nie
#@File : model18.py

# import pywinauto
from pywinauto import Application # 导包
# from pywinauto import application

def pywinauto_task(): # 定义一个自动化任务的函数

    # for i in range(7, 8): # 让这个任务执行指定次数，这里是5次
    #     print(i)
    #
    #     # app = application.Application(backend='uia').start(r"D:\Program Files\Notepad++\notepad++.exe")
    #     # app = Application().start(r"D:\Program Files\Notepad++\notepad++.exe") # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    #     # app = Application().start("notepad++.exe") # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    #     app = Application().start(r"C:\WINDOWS\system32\notepad.exe") # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    #     # app = Application().start(r"D:\Program Files\DBeaver\dbeaver.exe") # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    #
    #     # print(app)
    #
    #     app['无标题-记事本'].MenuSelect('帮助->关于记事本') # 在指定标题的窗口中，选择菜单
    #
    #     app['关于“记事本”']['确定'].click() # 在弹出的窗口中，定位确定按钮，并点击
    #
    #     app['无标题-记事本'].MenuSelect('文件->另存为...') # 打开记事本的另存为窗口
    #
    #     file_name = '第' + str(i) + '个.txt'  # 定义好文件的名字
    #
    #     app['另存为']['edit'].TypeKeys(file_name)  # 将文件名键入
    #     import time
    #     time.sleep(1)
    #
    #     app['另存为']['保存'].click()  # 更改文件名之后保存
    #
    #     app[file_name].edit.TypeKeys('hello\n', with_newlines=True)  # 在记事本窗口中写入内容，并换行
    #
    #     app[file_name].edit.TypeKeys('这是第' + str(i) + '个文件')  # 写入第二行内容
    #
    #     app.Notepad.MenuSelect('文件->退出')  # 选择菜单退出
    #
    #     app['记事本']['保存'].click()  # 保存写好的记事本

    # app = Application().start(r"D:\Program Files\DBeaver\dbeaver.exe") # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    # # app[r'C:\Users\jiuxin\AppData\Roaming\DBeaverData\workspace6 - DBeaver 7.0.2 - <Apache Hive - default> Scripts'].MenuSelect('SQL编辑器->SQL编辑器')  # 在指定标题的窗口中，选择菜单
    #
    # app[r'C:\Users\jiuxin\AppData\Roaming\DBeaverData\workspace6 - DBeaver 7.0.2 - <none> Scripts'].MenuSelect('SQL编辑器->SQL编辑器')  # 在指定标题的窗口中，选择菜单
    # app['Tip of the day']['关闭'].click()  # 更改文件名之后保存
    # app[r'C:\Users\jiuxin\AppData\Roaming\DBeaverData\workspace6 - DBeaver 7.0.2 - <none> Scripts'].MenuSelect('SQL编辑器->SQL编辑器')  # 在指定标题的窗口中，选择菜单
    # # app['Tip of the day']['New Script'].click()  # 更改文件名之后保存
    # # app["Choose SQL script for 'Apache Hive - default'"]['New Script'].click()  # 更改文件名之后保存
    # # app["Choose SQL script for 'Apache Hive - default'"]['New Script'].click()  # 更改文件名之后保存
    # app["选择数据源"].edit.TypeKeys('A-yunfuwu01').ClickInput()
    # app["选择数据源"].MenuSelect('A-yunfuwu01')  # 选择菜单退出
    # # app["选择数据源"]['A-yunfuwu01'].click()  # 更改文件名之后保存
    # app["选择数据源"]['Select'].click()

    # app = Application().start(r"D:\Program Files\DBeaver\dbeaver.exe")  # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    app = Application().connect(path=r"D:\Program Files\DBeaver\dbeaver.exe")  # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    # app = Application().connect(process=47180)  # 实例化一个对象，并启动指定的应用程序，start参数也可写入路径
    # app[r'C:\Users\jiuxin\AppData\Roaming\DBeaverData\workspace6 - DBeaver 7.0.2 - <none> Scripts'].MenuSelect(
    #     'SQL编辑器->SQL编辑器')
    # +
    # app["选择数据源"].TypeKeys('{ENTER}').ClickInput()
    # app["选择数据源"]['Select'].click()
    # app["Choose SQL script for 'A-yunfuwu01'"]['New Script'].click()
    # dlg = app.YourDialogTitle
    # print(dlg)
    # # dlg = app.child_window(title="your title", classname="your class",)
    # # print(dlg)
    # dlg = app['Your Dialog Title']
    # print(dlg)
    app[r'C:\Users\jiuxin\AppData\Roaming\DBeaverData\workspace6 - DBeaver 7.0.2 - <none> Scripts'].close()
    # app['退出 DBeaver']['确定'].click()
    # app['退出 DBeaver']['确定'].click()
    # app['退出 DBeaver'].TypeKeys('{ENTER}').ClickInput()
    app['退出 DBeaver']['取消'].click()





pywinauto_task()
