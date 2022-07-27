# -*- coding:utf-8 -*-
#@Time : 2022/2/15 16:53
#@Author: Geoff Nie
#@File : model19.py

# import win32api
# import win32gui,win32con
#
# win32api.MessageBox(0,"Hello PYwin32","MessageBox",win32con.MB_OK | win32con.MB_ICONWARNING)

import os
import time
import win32gui
import win32api
import win32con


# 设置appdict
pyexe = r"D:\Program Files\anaconda3\python.exe"
# qqexe = r"C:\Program Files (x86)\Tencent\QQ\Bin\QQScLauncher.exe"
qqexe = "C:\Program Files (x86)\Tencent\QQ\Bin\QQScLauncher.exe"
txtexe = r"C:\WINDOWS\system32\notepad.exe"
# appdict = {'qq': '"D:\...\QQScLauncher.exe"',
#            'pl/sql': '"E:\...\plsqldev.exe"',
#            'idea': '"E:...\idea64.exe"',
#            'chrome': '"C:\...\chrome.exe"'}
appdict = {'qq': '"C:\Program Files (x86)\Tencent\QQ\Bin\QQScLauncher.exe"',
           'chrome': '"C:\Program Files\Google\Chrome\Application\chrome.exe"'}
# qq登录按钮位置，pl/sql取消按钮位置，idea第一个工程的位置
coorddict = {'qq': [988, 662], 'pl/sql': [1060, 620], 'idea': [700, 245]}
namedict = {'qq': 'QQ', 'pl/sql': 'Oracle Logon', 'idea': 'Welcome to IntelliJ IDEA'}

# ldexe = r"D:\LeiDian\LDPlayer4.0\dnplayer.exe"
# # win32api.ShellExecute(1, 'open', ldexe, '', '', 1)
# # para_hld = win32gui.FindWindow(None, "New Text Document.txt - Notepad")
# para_hld = win32gui.FindWindow(None, 'TXGuiFoundation')# 1836416
# para_hld = win32gui.FindWindow("Notepad" , "新建文本文档.txt - 记事本")# 1836416
# print(para_hld)
# window_name = u'xxxx'
# hwnd = win32gui.FindWindow("Notepad" , "新建文本文档.txt - 记事本")
# menu = win32gui.GetMenu(hwnd)
# menu1 = win32gui.GetSubMenu(menu, 1)#第几个菜单
# cmd_ID = win32gui.GetMenuItemID(menu1, 1)#第几个子菜单
# win32gui.PostMessage(hwnd, win32con.WM_COMMAND, cmd_ID, 0)
#
# title = win32gui.GetWindowText(337008)
#
# classname = win32gui.GetClassName(337008)
#
# print("windows handler:{0}; title:{1}; classname:{2}".format(337008, title, classname))


# 打开应用并且鼠标点击按钮（获取按钮的像素坐标很麻烦）
def open_by_grab():

    pyhd = win32gui.FindWindow(None , pyexe)  # 360会拦截pyexe,可以添加信任或者关闭360
    time.sleep(1)
    # 设置pyexe窗口属性和位置，太大会挡住一些窗口
    win32gui.SetWindowPos(pyhd, win32con.HWND_TOPMOST, 0, 0, 500, 500, win32con.SWP_SHOWWINDOW)
    print("py exe 句柄: %s ..." % pyhd)
    for key in appdict.keys():
        print("启动 %s ..." % key)
        os.popen(r'%s' % appdict[key])  # os.system会阻塞
        time.sleep(3)
        if key == "chrome":
            pass
        else:
            winhd = win32gui.FindWindow(None, namedict[key])  # 根据窗口名获取句柄
            while winhd == 0:
                print("等待获取%s窗口 ..." % key)
                time.sleep(3)
                winhd = win32gui.FindWindow(None, namedict[key])
            print("获取%s窗口成功,开始登录 ..." % key)
            a, b = coorddict[key]
            mouse_click(a, b)
            time.sleep(3)
    print("完毕 ...")
    time.sleep(1)
    win32gui.SendMessage(pyhd, win32con.WM_CLOSE)


# 模拟鼠标点击
def mouse_click(a, b):
    time.sleep(1)
    win32api.SetCursorPos((a, b))
    time.sleep(1)
    win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0, 0, 0) # 360会拦截虚拟按键,可以添加信任或者关闭360
    win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0, 0, 0)


open_by_grab()



