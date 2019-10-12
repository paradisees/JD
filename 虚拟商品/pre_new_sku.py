#输入需要预测的图片sku及对应的下载路径，下载图片并预测
#需要提供输入图片数据valid_sku_path，已经train好的模型路径./model.ckpt_214.meta
#输出数据中，阈值设置为0.15，即概率小于0.15才会被认为虚拟商品，可以修改
import requests
import os
import time
import threading
import queue
import numpy as np
import cv2
import tensorflow as tf
import sys

data_path='./valid_sku_path.txt'
out_path='./upload.txt'

content = []
global all_num_judge,write_limit,all_num_pre
all_num_pre=0
write_limit=10000
all_num_judge=0
all_num = 0
flag=0
for line in open(data_path):
    all_num+=1
    if line is None:
        continue
    cons = line.strip().split("\t")
    if len(cons) == 1:
        continue
    content.append([cons[0], cons[1]])
print('sku import done, The whole num is :',all_num)
q = queue.Queue()
for url in content:
    q.put(url)

print('图片下载开始')
start = time.time()
global res,skus,sku_label
res_queue=queue.Queue()
skus_queue=queue.Queue()
sku_label={}
with tf.Session() as sess:
    saver = tf.train.import_meta_graph('./model.ckpt_214.meta')
    saver.restore(sess, './model.ckpt_214')
    inputs = tf.get_default_graph().get_tensor_by_name('inputs:0')
    is_training=tf.get_default_graph().get_tensor_by_name('is_training:0')
    prediction = tf.get_default_graph().get_tensor_by_name('Softmax:0')
    keep_prob_new = tf.get_default_graph().get_tensor_by_name('keep_prob:0')
    print('model initianize done')
    
    def fetch_img_func(q):
        #get图片，放入队列中
        global res_queue,skus_queue,all_num_judge
        while q.qsize()>0:
            try:
                all_num_judge+=1
                urls = q.get_nowait()
                # i = q.qsize()
                url = urls[1]
                sku = urls[0]
                # print ('Current Thread Name Runing %s ... 11' % threading.currentThread().name)
                url = url.strip()
                imgresponse = requests.get('http://img1.360buyimg.local/n3/' + url, stream=True)
                image = imgresponse.content
                img = cv2.imdecode(np.frombuffer(image, np.uint8), cv2.IMREAD_COLOR)
                rgb_img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
                img = cv2.resize(rgb_img, (224, 224))
                res_queue.put_nowait(img)
                skus_queue.put_nowait(sku)
            except Exception as e:
                all_num_judge+=1
                continue
                
    def predict():
        #判断队列中数据是否够，够的话启动model_pre，每次预测1024个数据
        global res_queue,skus_queue,all_num_judge
        while True:
            try:
                while res_queue.qsize()>=1024:
                    time0=time.time()
                    res_new=[]
                    skus_new=[]
                    for dd in range(1024):
                        res_new.append(res_queue.get_nowait())
                        skus_new.append(skus_queue.get_nowait())
                    print('--------------------------')
                    print(len(skus_new))
                    model_pre(res_new,skus_new)
                    time1=time.time()
                    print('time:',time1-time0,time1)
            except Exception as e:
                continue
            if all_num_judge>=all_num:
                print('yes')
                res_new=[]
                skus_new=[]
                for dd in range(res_queue.qsize()):
                    res_new.append(res_queue.get_nowait())
                    skus_new.append(skus_queue.get_nowait())
                print('--------------------------')
                print(len(skus_new))
                model_pre(res_new,skus_new)
                break
                
    def model_pre(images,skus):
        global sku_label,write_limit,all_num_pre
        print('Start prediction')
        print('sku_label len is {}'.format(len(sku_label)))
        pred = sess.run(prediction, feed_dict={inputs: images,is_training: False, keep_prob_new: 1.0})
        all_num_pre+=1024
        for id in range(len(pred)):
            #输出阈值可修改
            if pred[id][0]<=0.15:
                sku_label[skus[id]]=str(pred[id][0])
        if len(sku_label)>write_limit:
            print('write done')
            file=open(out_path,'w',encoding='utf8')
            file.write(str(len(sku_label)))
            file.write('------------------------')
            for key in sku_label.keys():
                file.write(str(key))
                file.write('\t')
                file.write(str(sku_label[key]))
                file.write('\n')
            file.close()
            write_limit+=10000
        print('all_num_predict:',all_num_pre)
        print('prediction done')
        
    t = []
    for i in range(11):
        t.append('t' + str(i))
    t.append('t_pre')
    for i in range(11):
        t[i] = threading.Thread(target=fetch_img_func, args=(q,), name="child_thread_" + str(i))
    t[11] = threading.Thread(target=predict,  name="predict")
    for i in range(12):
        t[i].start()
    for i in range(12):
        t[i].join()

    end = time.time()
    print(end - start)
    print(len(sku_label))
    file=open(out_path,'w',encoding='utf8')
    for key in sku_label.keys():
        file.write(str(key))
        file.write('\t')
        file.write(str(sku_label[key]))
        file.write('\n')
