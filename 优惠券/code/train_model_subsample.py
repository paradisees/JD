import xgboost as xgb
import random
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
def import_data(data, label, path):
    for path_tmp in path:
        file = open(path_tmp)
        file.readline()  # skip the header
        for line in file:
            cons = line.strip().split("\t")
            data.append(list(map(float, cons[7].split(',')))
                        )
            label.append(float(cons[-2]))
    return data, label
def features_importance(model):
    bst = model
    featureImpFile = open('features_importance_1205', 'w')
    gainImp = bst.get_score(fmap='', importance_type='gain')
    weightImp = bst.get_score(fmap='', importance_type='weight')
    coverImp = bst.get_score(fmap='', importance_type='cover')
    sortFeatImp = sorted(gainImp.items(), key=lambda gainImp: gainImp[1], reverse=True)
    maxGain = (float)(sortFeatImp[0][1])
    maxWeight = float(max(weightImp.values()))
    maxCover = float(max(coverImp.values()))
    featureImpFile.write('rank\tgain\tweight\tcover\tname\n')
    for i in range(len(sortFeatImp)):
        featureImpFile.write(str(i + 1) + '\t' + str(round((sortFeatImp[i][1] / maxGain), 4))
                             + '\t' + str(round((weightImp[sortFeatImp[i][0]] / maxWeight), 4))
                             + '\t' + str(round((coverImp[sortFeatImp[i][0]] / maxCover), 4))
                             + '\t' + features[(int)(sortFeatImp[i][0][1:])] + '\n')
    featureImpFile.close()
    print('feature_importance over')
def train(param,depth,child_weight,rate):
    X_train = data
    y_train = label
    dtrain = xgb.DMatrix(X_train, label=y_train)
    params = {'booster': 'gbtree',
              'objective': 'binary:logistic',
              'learning_rate': rate,
              'max_depth': depth,
              'seed': 1113,
              'silent': 0,
              'min_child_weight': child_weight,
              #'subsample':0.8,
              #'colsample_bytree':0.8,
              'eval_metric': 'auc',
              'tree_method':'gpu_hist'}
    model = xgb.train(params, dtrain, param)
    features_importance(model)
    model.save_model('tmp/'+str(param)+'_'+str(depth)+'_'+str(child_weight)+'_'+str(rate))
    return model

features = []
file = open('features.txt')
for line in file:
    features.append(line)

path = ['data_24_01.csv']
data, label = import_data([], [], path)
data, label = data_process(data,label)
print('train_data_process done')

models = []
params = [800]
learning_rate=[0.08]
max_depth=[7]
min_child_weight=[7]

for param in params:
  for depth in max_depth:
    for child_weight in min_child_weight:
      for rate in learning_rate:
          models.append(train(param,depth,child_weight,rate))
print('models_prepare ending')
'''params = [1300]
learning_rate=[0.11]
max_depth=[5]
min_child_weight=[5]
'subsample':0.8,
'colsample_bytree':0.93'''