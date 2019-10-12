import xgboost as xgb
from sklearn.datasets import load_svmlight_file
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_curve, auc, roc_auc_score
from sklearn.externals import joblib
import numpy as np
from scipy.sparse import hstack
from sklearn.preprocessing.data import OneHotEncoder
def import_data(data,label,path):
    for path_tmp in path:
        file = open(path_tmp)
        file.readline()  # skip the header
        for line in file:
            cons = line.strip().split("\t")
            data.append(list(map(float, cons[5].split(',')))
               )
            label.append(float(cons[-2]))
    return np.array(data),np.array(label)
def xgboost_lr_train():
    path = [
        'data_11_02.csv','data_11_03.csv']
    data, label = import_data([], [], path)

    X_train, X_test, y_train, y_test = train_test_split(data, label, test_size = 0.1, random_state = 42)


    xgboost = xgb.XGBClassifier(nthread=4, learning_rate=0.02,min_child_weight=5,tree_method='gpu_hist',
                            n_estimators=500, max_depth=5)
    global model_xgb,model_tmp,model_lr
    model_xgb=xgboost.fit(X_train, y_train)

    y_pred_test = xgboost.predict_proba(X_test)[:, 1]
    xgb_test_auc = roc_auc_score(y_test, y_pred_test)
    print('xgboost test auc: %.5f' % xgb_test_auc)

    X_train_leaves = xgboost.apply(X_train)
    X_test_leaves = xgboost.apply(X_test)


    All_leaves = np.concatenate((X_train_leaves, X_test_leaves), axis=0)
    All_leaves = All_leaves.astype(np.int32)

    xgbenc = OneHotEncoder()
    X_trans = xgbenc.fit_transform(All_leaves)

    (train_rows, cols) = X_train_leaves.shape

    lr = LogisticRegression()
    model_tmp=lr.fit(X_trans[:train_rows, :], y_train)
    y_pred_xgblr1 = lr.predict_proba(X_trans[train_rows:, :])[:, 1]
    xgb_lr_auc1 = roc_auc_score(y_test, y_pred_xgblr1)
    print('LR based on XGB AUC: %.5f' % xgb_lr_auc1)

    lr = LogisticRegression(n_jobs=-1)
    X_train_ext = hstack([X_trans[:train_rows, :], X_train])
    X_test_ext = hstack([X_trans[train_rows:, :], X_test])

    model_lr=lr.fit(X_train_ext, y_train)
    joblib.dump(model_lr,'lr')
    y_pred_xgblr2 = lr.predict_proba(X_test_ext)[:, 1]
    xgb_lr_auc2 = roc_auc_score(y_test, y_pred_xgblr2)
    print('AUC bsed on combined features of lr: %.5f' % xgb_lr_auc2)

if __name__ == '__main__':
    xgboost_lr_train()
'''for model in model_all:
    def value(features):
        # print(features)
        val = 0
        val += model.predict_proba(np.array(features).reshape(1, -1))[:, 1]
        # print(val)
        # val=val/10
        return val'''
