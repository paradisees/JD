import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import xgboost as xgb
from random import shuffle
from xgboost.sklearn import XGBClassifier
from sklearn.cross_validation import cross_val_score
import pickle
import time
import random
from hyperopt import fmin, tpe, hp,space_eval,rand,Trials,partial,STATUS_OK
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
        file.readline()  # skip the header
        for line in file:
            cons = line.strip().split("\t")
            data.append(list(map(float, cons[7].split(',')))
               )
            label.append(float(cons[-2]))
    return data,label
path = ['data_24_01.csv']
data, label = import_data([], [], path)
data, label = data_process(data, label)
print('data_done')

attr_train  = np.array(data)
label_train = np.array(label)
#print (np.mat(label_train).reshape((-1,1)).shape)


def GBM(argsDict):
    max_depth = argsDict["max_depth"] + 1
    n_estimators = argsDict['n_estimators'] * 50 + 300
    learning_rate = argsDict["learning_rate"] * 0.01 + 0.01
    subsample = argsDict["subsample"] * 0.1 + 0.7
    min_child_weight = argsDict["min_child_weight"]+1
    print(max_depth,n_estimators,learning_rate,subsample,min_child_weight)
    global attr_train,label_train
    gbm = xgb.XGBClassifier(nthread=4,    
                            max_depth=max_depth,  
                            n_estimators=n_estimators,   
                            learning_rate=learning_rate, 
                            subsample=subsample,      
                            min_child_weight=min_child_weight,   
                            max_delta_step = 10,  
                            objective="binary:logistic")

    metric = cross_val_score(gbm,attr_train,label_train,cv=5,scoring="roc_auc").mean()
    print (metric)
    return -metric

space = {"max_depth":hp.randint("max_depth",10),
         "n_estimators":hp.randint("n_estimators",20),  # * 50 + 300
         "learning_rate":hp.randint("learning_rate",10),  # * 0.01 + 0.01
         "subsample":hp.randint("subsample",4),#[0,1,2,3] -> [0.7,0.8,0.9,1.0]
         "min_child_weight":hp.randint("min_child_weight",10), 
        }
best = fmin(GBM,space,algo=tpe.suggest,max_evals=50)

print (best)
print (GBM(best))