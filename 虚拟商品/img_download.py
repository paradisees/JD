# -*- coding: utf-8 -*-
import requests
import os
import time
import threading
import queue
import numpy as np
import cv2
import tensorflow as tf
import sys

content = []
global all_num_load
all_num_load=0
all_num = 0
#输入数据的格式（100303  jfs/t15145/195/543264896/292066/e672de05/5a30b19cN5832a818.jpg）
for line in open('./valid_sku_small_path.txt'):
    all_num+=1
    if line is None:
        continue
    cons = line.strip().split("\t")
    if len(cons) == 1:
        continue
    content.append([cons[0], cons[1]])
print('sku import done')
# 使用Queue来线程通信
q = queue.Queue()
for url in content:
    q.put(url)

print('图片下载开始')
start = time.time()
def fetch_img_func(q):
    global all_num_load
    while q.qsize()>0:
        try:
            urls = q.get_nowait()
            # i = q.qsize()
            url = urls[1]
            sku = urls[0]
            # print ('Current Thread Name Runing %s ... 11' % threading.currentThread().name)
            url = url.strip()
            imgresponse = requests.get('http://img10.360buyimg.local/n3/' + url, stream=True)
            image = imgresponse.content
            with open('./small_pic/' + str(sku) + '.jpg', 'wb') as jpg:
                all_num_judge+=1
                if all_num_judge%100000==0:
                    end_time=time.time()
                    print('已下载：',all_num_judge)
                    print('time:',end_time-start_time)
                    start_time=end_time
                jpg.write(image)
            jpg.close()
        except Exception as e:
            continue
t = []
for i in range(11):
    t.append('t' + str(i))
for i in range(11):
    t[i] = threading.Thread(target=fetch_img_func, args=(q,), name="child_thread_" + str(i))
for i in range(11):
    t[i].start()
for i in range(11):
    t[i].join()