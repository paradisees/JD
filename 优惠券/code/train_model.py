import xgboost as xgb
def data_process(label):
    positive,negtive=0,0
    for item in label:
        if item==1:
            positive+=1
        else:
            negtive+=1
    print(positive)
    w=negtive/positive
    print(w)
    return w
def import_data(data,label,path):
    for path_tmp in path:
        file = open(path_tmp)
        for line in file:
            cons = line.strip().split("\t")
            features_111=list(map(float, cons[7].split(',')))
            if float(cons[8])==0.0 or cons[8]=='NULL':
                continue
            data.append(list(map(float, cons[7].split(',')))
              +[round(abs((float(cons[8])-float(features_111[-7]))/float(cons[8])),3)])
            label.append(float(cons[-2]))
    return data,label
def features_importance(model):
    bst = model
    featureImpFile = open('purchasing_new_topN', 'w')
    gainImp = bst.get_score(fmap='', importance_type='gain')
    weightImp = bst.get_score(fmap='', importance_type='weight')
    coverImp = bst.get_score(fmap='', importance_type='cover')
    sortFeatImp = sorted(gainImp.items(), key=lambda gainImp: gainImp[1], reverse=True)
    maxGain = (float)(sortFeatImp[0][1])
    maxWeight = float(max(weightImp.values()))
    maxCover = float(max(coverImp.values()))
    featureImpFile.write('rank\tgain\tweight\tcover\tname\n')
    #print(len(sortFeatImp))
    for i in range(len(sortFeatImp)):
        featureImpFile.write(str(i + 1) + '\t' + str(round((sortFeatImp[i][1] / maxGain), 4))
                             + '\t' + str(round((weightImp[sortFeatImp[i][0]] / maxWeight), 4))
                             + '\t' + str(round((coverImp[sortFeatImp[i][0]] / maxCover), 4))
                             + '\t' + features[(int)(sortFeatImp[i][0][1:])] + '\n')
    featureImpFile.close()
    print('feature_importance over')
def train():
    path=['data_purchasing_train_new.csv']
    data,label=import_data([],[],path)
    #scale_pos_weight = data_process(label)
    print('data_process done')
    X_train = data
    y_train = label
    dtrain = xgb.DMatrix(X_train, label=y_train)
    params = {'booster': 'gbtree',
              'objective': 'binary:logistic',
              'learning_rate': 0.05,
              'max_depth': 5,
              'seed': 1113,
              'silent': 0,
              'min_child_weight':4,
              'subsample':0.8,
              'colsample_bytree':0.7,
              'eval_metric': 'auc',
              #'scale_pos_weight':scale_pos_weight,
              'tree_method':'gpu_hist'}
    model = xgb.train(params, dtrain, num_boost_round=900)
    #features_importance(model)
    model.save_model('train.gbdt')
    return model
features=[]
file = open('features_purchasing.txt')
for line in file:
    features.append(line)
torch.cuda.manual_seed(7)
model=train()
print('ending')