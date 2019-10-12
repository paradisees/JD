import multiprocessing
from gensim import models
import os
root_path = os.getcwd()
####### Word2vec params ########
size = 200
window = 5
min_count = 10
draw_topn = 20
word2vec_model_path = os.path.join(root_path, "word2vec")

'''
class MySentences(object):
    def __init__(self, dirname):
        self.dirname = dirname

    def __iter__(self):
        for fname in os.listdir(self.dirname):
            for line in open(os.path.join(self.dirname, fname)):
                cons = [pw for pw in line.strip().split(",")]
                #print(cons)
                yield cons
sentences = MySentences('./data')  # a memory-friendly iterator
model = models.Word2Vec(sentences)
words = model.most_similar(positive='1')
for word, similarity in words:
    print(word, similarity)

'''

class data_process(object):
    def __init__(self, corpus: list, pw2id_filename: str):
        self.corpus_list = corpus
        self.pw2id_filename = pw2id_filename
        self.id2pw = {}
        self.pw2id = {}
        self.pws = []
        self._getId2pw()
    def _getId2pw(self):
        file = open(self.pw2id_filename, encoding='utf8')
        for line in file:
            cons = line.strip().split("\t")
            self.id2pw[str(cons[1])] = cons[0]
            self.pw2id[cons[0]] = int(cons[1])
            self.pws.append(cons[0])
        file.close()
    def __iter__(self):
        #print(self.id2pw['1296'])
        for filename in self.corpus_list:
            with open(filename, encoding='utf8') as file:
                try:
                    file.readline()
                except:
                    print("read header wrong")
                for line in file:
                    cons = [pw for pw in line.strip().split(",")]
                    try:
                        yield [self.id2pw[pw_idx] for pw_idx in cons]
                    except:
                        print('error:',line)
                        continue
class Word2Vec(object):
    def __init__(self, data):
        self.data = data
        self.model_name = word2vec_model_path
    def train_model(self):
        self.word2vec_model = models.Word2Vec(sentences=self.data,
                                              size=size,
                                              window=window,
                                              min_count=min_count,
                                              workers=min(multiprocessing.cpu_count(), 8)
                                              )
        return self.word2vec_model
    def save_mode(self):
        self.word2vec_model.save(self.model_name)
lis=[]
for i in range(0,1347,1):
    lis.append(str(i).zfill(6)+'_0')
a=data_process(lis,
           './id2pw.txt')

b=Word2Vec(a)
model=b.train_model()
'''words = model.most_similar(positive='9987-手机')
for word, similarity in words:
    print(word, similarity)'''
model.save_mode('Word2vec_model')
