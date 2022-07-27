# -*- coding:utf-8 -*-
#@Time : 2022/2/15 16:53
#@Author: Geoff Nie
#@File : model17.py
aa = ["平台2", "平台2", "平台3", "应用1", "平台1", "应用2", "应用1"]

print(aa)
# print(aa.sort())
print(sorted(aa))
print(sorted(list(set(aa))))


import itchat
# itchat.auto_login()#python会自动生成二维码登陆
# def get_friends():
#     friends = itchat.get_friends(update=True)#获取好友信息，如果设置update=True将从服务器刷新列表
#     print(friends)
#     for i in friends:
#         print(i)
#     return friends
#
#
# def main():
#     itchat.auto_login(hotReload=True) #登录
#     friends = get_friends() #获取朋友
#     chatrooms = itchat.get_chatrooms(update=True)
#     print(chatrooms)
#     for i in chatrooms:
#         print(i)
#     # itchat.send("test111", toUserName="@1fe8833eb15aa7feee397eda616c83f77514a549f360f7f03f5299e8d322d3b4") #好像不可行
#     userName=friends[0]['UserName']
#     print(userName)
#     # itchat.send("test111", toUserName=userName) #可行
#
# # main()

import requests
from bs4 import BeautifulSoup
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
}
# url = 'http://q.10jqka.com.cn/gn/'
# r = requests.get(url, headers=headers)
# soup = BeautifulSoup(r.text, "lxml")
# html_list = soup.find('div', attrs={'class': 'boxShadow'}).find_all('a', attrs={'target': '_blank'})
# name_list = [item.text for item in html_list]
# url_list = [item['href'] for item in html_list]
# # temp_df = pd.DataFrame([name_list, url_list], index=['name', 'url']).T
# print(html_list)

import random
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


# print(temp_df)
url = 'http://q.10jqka.com.cn/gn/index/field/addtime/order/desc/page/8/ajax/1/'
# url = 'http://q.10jqka.com.cn/gn/'
headers = {
    'User-Agent': ua_random()
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 Edg/98.0.1108.62'
}
t = requests.get(url, headers=headers)
soup = BeautifulSoup(t.text, "lxml")
print(soup)
# 历史交易日
# code_lhb_details = soup.select('#maincont > table > tbody > tr > td')
code_lhb_details = soup.select('body > table > tbody > tr > td')



print(soup)
print(code_lhb_details)
print(len(code_lhb_details))
print(code_lhb_details[0])
print(code_lhb_details[0].text)
print(code_lhb_details[1])

print(code_lhb_details[1].text)

aa = code_lhb_details[1]
print(aa)
print(type(aa))
print(aa.a['href'])
print(code_lhb_details[1].a['href'] )













