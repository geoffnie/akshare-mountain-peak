# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 09:41:01 2021

@author: jiuxin
"""

# from kafka import KafkaConsumer
# import time
# # from log.log import logger

# #  与kafka建立连接
# # consumer = KafkaConsumer('com.2000000557.1002072216.collect.0',bootstrap_servers=['192.168.1.95:9092,192.168.1.28:9092,192.168.1.163:9092'])
# # consumer = KafkaConsumer('ods.motr.pnt.data.ip.tab',bootstrap_servers=['192.168.1.95:9092', '192.168.1.28:9092', '192.168.1.163:9092'])
# consumer = KafkaConsumer('com.2000000557.1002072216.collect.0', group_id='123456',bootstrap_servers=['192.168.1.95:9092', '192.168.1.28:9092', '192.168.1.163:9092'])
# print("连接成功")
# for message in consumer:   #开始消费消息
#     # logger.info("%s value=%s" %(message.topic,message.value))
#     print(message.topic,message.value)
#     # print ("%s %s value=%s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),message.topic,message.value))


from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# topic = 'com.2000000557.1002072216.collect.0'
topic = 'ods.motr.pnt.data.ip.tab'


consumer = KafkaConsumer( group_id='123456',bootstrap_servers=['192.168.1.95:9092', '192.168.1.28:9092', '192.168.1.163:9092'])
consumer.assign([TopicPartition(topic=topic, partition=0), TopicPartition(topic=topic, partition=1), TopicPartition(topic=topic, partition=2)])
print(consumer.partitions_for_topic(topic))  # 获取test主题的分区信息
print(consumer.assignment())
print(consumer.beginning_offsets(consumer.assignment()))
consumer.seek(TopicPartition(topic=topic, partition=1), 132912858)
print(consumer)
for msg in consumer:
    recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    print(recv)
    
 
# # pip install kafka-python
# from kafka import KafkaConsumer
# luyang_kafka_setting = {
#     'host':'192.168.1.95:9092,192.168.1.28:9092,192.168.1.163:9092',
#     'topic':'ods.motr.pnt.data.ip.tab',
#     'groupid':'luyang1'
# }
# conf = luyang_kafka_setting

# consumer = KafkaConsumer(bootstrap_servers=conf['host'], group_id=conf['groupid'])

# print('consumer start to consuming...')
# consumer.subscribe((conf['topic'], ))
# for message in consumer:
#     print(message.topic, message.offset, message.key, message.value, message.value, message.partition)    



# #pip install kafka-python
# import hashlib
# import os
# import time
# import pymysql
# import json
# import requests
# from kafka import KafkaConsumer
# from kafka import TopicPartition

# consumer = KafkaConsumer(
#     # bootstrap_servers = "192.168.1.95:9092,192.168.1.28:9092,192.168.1.163:9092", # kafka集群地址
#     # group_id = "newConsumerTest1", # 消费组id
#     bootstrap_servers = "192.168.1.95:9092,192.168.1.28:9092,192.168.1.163:9092", # kafka集群地址
#     group_id = "newConsumerTest1", # 消费组id
#     client_id = '8eaa8c81edfd41f28a50f9121ad14572',
#     auto_offset_reset="latest",
#     max_poll_records=10, # 每次最大消费数量
#     enable_auto_commit = True, # 每过一段时间自动提交所有已消费的消息（在迭代时提交）
#     auto_commit_interval_ms = 5000, # 自动提交的周期（毫秒）
# )

# partition = TopicPartition('com.2000000557.1002072216.collect.0', 1)
# partition = TopicPartition('com.2000000557.1002072216.collect.0', 1)
# res = consumer.poll(10)
# start = 0
# end = 20000000
# consumer.assign([partition])
# consumer.seek(partition, offset=start)
# #consumer.seek_to_end() 默认读取最新数据
# print(consumer)
# for msg in consumer: # 迭代器，等待下一条消息
#     offset, value = msg.offset, msg.value
#     print(offset, value)
#     if msg.offset > end:
#         break

#     jdate = json.loads(value)
#     print(jdate)
#     # print(offset,"====>>>>",jdate.get("crawler_time"), jdate.get("taskId")," url_md5:", jdate.get("url_md5"))





    