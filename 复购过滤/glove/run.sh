iter=475
corpus_size=18  #以亿为单位
./vocab_count -verbose 2 -max-vocab 6000000 -min-count 10 < corpus-$corpus_size > vocab-${corpus_size}.txt
./cooccur -verbose 2 -symmetric 0 -window-size 10 -vocab-file vocab-${corpus_size}.txt -memory 8.0 -overflow-file tempoverflow < corpus-$corpus_size > cooccurrence-${corpus_size}.bin
./shuffle -verbose 2 -memory 8.0 < cooccurrence-${corpus_size}.bin > cooccurrence-${corpus_size}.shuf.bin
./glove -input-file cooccurrence-${corpus_size}.shuf.bin -vocab-file vocab-${corpus_size}.txt -save-file vectors-iter_$iter-corpus_size-$corpus_size -gradsq-file gradsq -verbose 2 -vector-size 200 -threads 24 -iter $iter -alpha 0.75 -x-max 100.0 -eta 0.05 -binary 0 -model 2
