# -*- coding: utf-8 -*-
# coding:UTF-8
import xlearn as xl
import numpy as np
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
    print(positive,negtive)
    for i in range(len(label)):
        if label[i]==0:
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
            if num > 1000000:
                break
            cons = line.strip().split("\t")
            data.append(list(map(float, cons[7].split(',')))
               )
            label.append(1 if float(cons[-2])==1.0 else 0)
            num+=1
    return data,label
def import_data1(data,label,path):
    for path_tmp in path:
        file = open(path_tmp)
        num = 0
        for line in file:
            if num > 2000:
                break
            cons = line.strip().split("\t")
            data.append(list(map(float, cons[7].split(',')))
               )
            label.append(1 if float(cons[-2])==1.0 else 0)
            num+=1
    return data,label
def preprocessData(data,label):
    feature = np.array(data)  # 取特征
    label = np.array(label)    # 将数组按行进行归一化
    from sklearn import preprocessing
    min_max_scaler = preprocessing.MinMaxScaler()
    feature = min_max_scaler.fit_transform(feature)
    #print(feature[0])

    return feature, label
train_data, train_label = import_data([], [], trainData)
train_data, train_label = data_process(train_data, train_label)

test_data, test_label = import_data1([], [], testData)
test_data, test_label = data_process(test_data, test_label)

dataTrain, labelTrain = preprocessData(train_data,train_label)
dataTest, labelTest = preprocessData(test_data,test_label)

'''ffm_model=xl.create_ffm()
ffm_model.setTrain(dataTrain)
ffm_model.setValidate(dataTest)

param={'task':'binary',
       'lr':0.2,
       'lambda':0.002,
       'metric':'acc'}

ffm_model.fit(param,"./model.out")
ffm_model.setTest(dataTest)
ffm_model.predict("./model.out","model./output.txt")
'''
linear_model = xl.FMModel(task='binary',lr=0.2,k=10,init=0.1,epoch=500,reg_lambda=0.002,metric='acc')

linear_model.fit(dataTrain,labelTrain,eval_set=[dataTest, labelTest])

y_pred = linear_model.predict(dataTest)
y_pred=list(y_pred)
print(y_pred)
error=0
item=0
for i in range(len(y_pred)):
    item+=1
    if y_pred[i]<0.5 and labelTest[i]==1:
        error+=1
    elif y_pred[i]>=0.5 and labelTest[i]==0:
        error+=1
    else:
        continue
print('acc:',float(item-error)/item)
#print(y_pred)
