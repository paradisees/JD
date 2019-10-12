# -*- coding: utf-8 -*-
# coding:UTF-8
from __future__ import division
from math import exp
import numpy as np
from random import normalvariate 
from datetime import datetime
import pandas as pd
import random
trainData = ['data_24_01.csv']
testData = ['data_12_02.csv']  
def data_process(data,label):
    random.seed(1)
    data_out,label_out=[],[]
    flag_0,flag_1=[],[]
    positive,negtive=0,0
    for item in label:
        if item==1:
            positive+=1
        else:
            negtive+=1
    for i in range(len(label)):
        if label[i]==-1:
            flag_0.append(i)
        else:
            flag_1.append(i)
    flag_0_new=random.sample(flag_0,positive)
    index=flag_1+flag_0_new
    for num in index:
        data_out.append(data[num])
        label_out.append(label[num])
    return data_out,label_out
def import_data(data,label,path):
    for path_tmp in path:
        file = open(path_tmp)
        num = 0
        for line in file:
            #if num > 5000:
            #    break
            cons = line.strip().split("\t")
            data.append(list(map(float, cons[7].split(',')))
               )
            label.append(1 if float(cons[-2])==1.0 else -1)
            num+=1
    return data,label

def preprocessData(data,label):
    feature = np.array(data)  
    label = np.array(label)    
    from sklearn import preprocessing
    min_max_scaler = preprocessing.MinMaxScaler()
    feature = min_max_scaler.fit_transform(feature)
    #zmax, zmin = feature.max(axis=0), feature.min(axis=0)
    #feature = (feature - zmin) / (zmax - zmin)
    #print(feature[0])

    return feature, label


def sigmoid(inx):
    # return 1. / (1. + exp(-max(min(inx, 15.), -15.)))
    return 1.0 / (1 + exp(-inx))


def SGD_FM(dataMatrix, classLabels, k, iter):
    m, n = np.shape(dataMatrix) 
    alpha = 0.01
    w = np.zeros((n, 1))  
    w_0 = 0.
    v = normalvariate(0, 0.2) * np.ones((n, k)) 

    for it in range(iter):
        for x in range(m):  
            inter_1 = dataMatrix[x] * v
            inter_2 = np.multiply(dataMatrix[x], dataMatrix[x]) * np.multiply(v, v)  
            interaction = sum(np.multiply(inter_1, inter_1) - inter_2) / 2.

            p = w_0 + dataMatrix[x] * w + interaction  
            loss = 1 - sigmoid(classLabels[x] * p[0, 0])  

            w_0 = w_0 + alpha * loss * classLabels[x]

            for i in range(n):
                if dataMatrix[x, i] != 0:
                    w[i, 0] = w[i, 0] + alpha * loss * classLabels[x] * dataMatrix[x, i]
                    for j in range(k):
                        v[i, j] = v[i, j] + alpha * loss * classLabels[x] * (
                                dataMatrix[x, i] * inter_1[0, j] - v[i, j] * dataMatrix[x, i] * dataMatrix[x, i])
        print("loss after {} is {}".format(it, loss))
    return w_0, w, v


def getAccuracy(dataMatrix, classLabels, w_0, w, v):
    m, n = np.shape(dataMatrix)
    allItem = 0
    error = 0
    result = []
    for x in range(m):  
        allItem += 1
        inter_1 = dataMatrix[x] * v
        inter_2 = np.multiply(dataMatrix[x], dataMatrix[x]) * np.multiply(v, v)
        interaction = sum(np.multiply(inter_1, inter_1) - inter_2) / 2.
        p = w_0 + dataMatrix[x] * w + interaction  
        pre = sigmoid(p[0, 0])
        result.append(pre)

        if pre < 0.5 and classLabels[x] == 1:
            error += 1
        elif pre >= 0.5 and classLabels[x] == -1:
            error += 1
        else:
            continue
    return float(error) / allItem


if __name__ == '__main__':
    train_data, train_label = import_data([], [], trainData)
    train_data, train_label = data_process(train_data, train_label)

    test_data, test_label = import_data([], [], testData)
    test_data, test_label = data_process(test_data, test_label)

    dataTrain, labelTrain = preprocessData(train_data,train_label)
    dataTest, labelTest = preprocessData(test_data,test_label)
    #print(dataTest,labelTest)
    date_startTrain = datetime.now()
    print("开始训练")
    w_0, w, v = SGD_FM(np.mat(dataTrain), labelTrain, 20, 20)
    print("w_0 is {0}\nw is {1}\nv is {2}".format(w_0,w,v))
    print(
        "训练准确性为：%f" % (1 - getAccuracy(np.mat(dataTrain), labelTrain, w_0, w, v)))
    date_endTrain = datetime.now()
    print(
        "训练用时为：%s" % (date_endTrain - date_startTrain))
    print("开始测试")
    print(
        "测试准确性为：%f" % (1 - getAccuracy(np.mat(dataTest), labelTest, w_0, w, v)))