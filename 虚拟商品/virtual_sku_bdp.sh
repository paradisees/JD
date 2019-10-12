hdfs dfs -get /user/recsys/rec/hehaoyang/pack/* .

#记录新的需要预测的sku
hive -e"select item_sku_id ,image_path from (select item_sku_id from recall.m_sku_information where cnt_click>=1 group by item_sku_id )a 
left join (select * from app.m_sku_to_imageurl)b on a.item_sku_id=b.sku_id 
left join (select * from tmpr.pic_sku_tmp)c on a.item_sku_id=c.sku
where b.sku_id is not null
and c.sku is null">>valid_sku_path.txt;

#记录已经预测过的所有sku
hive -e"select item_sku_id ,image_path from (select item_sku_id from recall.m_sku_information where cnt_click>=1 group by item_sku_id )a 
left join (select * from app.m_sku_to_imageurl)b on a.item_sku_id=b.sku_id 
where b.sku_id is not null">>valid_sku_pre.txt;

#更新已经预测过的sku
HQL="
drop table if exists tmpr.pic_sku_tmp;
create table if not exists tmpr.pic_sku_tmp
(sku string,
 url string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
location 'hdfs://ns1013/user/recsys/rec/tmpr.db/pic_sku_tmp';
load data local inpath 'valid_sku_pre.txt' into table tmpr.pic_sku_tmp;"
echo "$HQL"
hive -e "$HQL"


python small_sku_bdp.py

HQL="
drop table if exists tmpr.pic_label_small1;
create table if not exists tmpr.pic_label_small1
(sku string,
weight string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
location 'hdfs://ns1013/user/recsys/rec/tmpr.db/pic_label_small1';
load data local inpath 'upload.txt' into table tmpr.pic_label_small1;"
echo "$HQL"
hive -e "$HQL"


HQL="
drop table if exists tmpr.virtual_sku_upload;
create table tmpr.virtual_sku_upload as 
select a.sku,55 from 
(select * from tmpr.pic_label_small1)a 
left join
(select item_sku_id from app.pzh_filter_total where dt='$dt2')b 
on a.sku=b.item_sku_id 
where b.item_sku_id is null"
echo "$HQL"
hive -e "$HQL"