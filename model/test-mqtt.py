# -*- coding:utf-8 -*-
#@Time : 2022/4/20 17:39
#@Author: Geoff Nie
#@File : test-mqtt.py



# import random
# from paho.mqtt import client as mqtt_client
#
#
# from apscheduler.schedulers.blocking import BlockingScheduler
#
# scheduler = BlockingScheduler()
#
# # port = 1883
# topic = "gateway/monitor/status"
# # generate client ID with pub prefix randomly
# client_id = f'python-mqtt-{random.randint(0, 100)}'
#
#
# def connect_mqtt() -> mqtt_client:
#     def on_connect(client, userdata, flags, rc):
#         if rc == 0:
#             print("Connected to MQTT Broker!")
#         else:
#             print("Failed to connect, return code %d\n", rc)
#
#     client = mqtt_client.Client(client_id)
#     client.on_connect = on_connect
#     client.connect(host='192.168.1.81', port=30413)
#     return client
#
#
# def subscribe(client: mqtt_client):
#     def on_message(client, userdata, msg):
#         print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#
#     client.subscribe(topic)
#     client.on_message = on_message
#
#
# def run():
#     client = connect_mqtt()
#     subscribe(client)
#     client.loop_forever()
#
# global aa
#
# aa = 10
# @scheduler.scheduled_job('cron', second='*/5', coalesce=False, misfire_grace_time=60, max_instances=20)
# def test():
#     print("----------------")
#     aa = 120
#     print(aa)
#     aa = 888
#     print(aa)
#
#
# @scheduler.scheduled_job('cron', second='*/5', coalesce=False, misfire_grace_time=60, max_instances=20)
# def test2():
#     print("----------------")
#     # global aa
#     aa = 120
#     print(aa)
#     aa = 888
#     print(aa)
#
# import redis
#
# redis_conn = redis.Redis(host='127.0.0.1', port=6379, password='your pw', db=0)
# redis_conn.set('name_1', 'Zarten_2')
# v = redis_conn.get('name_1')
# print(v)

# if __name__ == '__main__':
#     # run()#
#     scheduler.start()


import os
# try:
#     file = os.system('ls -t -A /root/CIFS/tobedelete/ | head -3')
#     # file = os.system('ls -t -A /root/CIFS/tobedelete/ ')
#     print(file)
#     print(str(file)[0])
#     # print(type(file))
# except Exception as e:
#     print(e)


# p=os.popen('ls -t -A /root/CIFS/tobedelete/ | head -3')
# x=p.readlines()
# print(type(x))
# print(x[0].replace("\n",""))
# print("-------------")
# for line in x:
#     print(line.replace("\n",""))


def get_last_file():
    try:
        p = os.popen('ls -t -A /root/CIFS/tobedelete/ | head -1')
        files = p.readlines()
        print(files[0].replace("\n", ""))
        file = files[0].replace("\n", "")
    except Exception as e:
        print(e)
    return "/root/CIFS/tobedelete/" + file


# print(get_last_file())

str1 = '0'
str2 = 'f'
str3 = '<'
str4 = 'F'

print(str1.isalpha())
print(str2.isalpha())
print(str3.isalpha())
print(str4.isalpha())









