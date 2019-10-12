base_day="2019-03-07"
database="tmpr"
HADOOP_PATH="hdfs://ns1013/user/recsys/rec/${database}.db"
dt1=`date -d "162 day ago ${base_day}" +%Y-%m-%d` 
dt2=`date -d "2 day ago ${base_day}" +%Y-%m-%d` 
dt3=`date -d "62 day ago ${base_day}" +%Y-%m-%d` 
# dt4=`date -d "-62 day" +%Y-%m-%d` 
setcof="
set hive.optimize.skewjoin=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=268435456;
set hive.cli.print.header=true;
set mapreduce.input.fileinputformat.split.maxsize=1147483648;
set mapreduce.input.fileinputformat.split.minsize=1147483648;
set mapreduce.job.reduce.slowstart.completedmaps=1;
set hive.auto.convert.join=true;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set hive.groupby.skewindata=true;
use ${database};
"
#'套装/礼盒','工具套装','洗护套装','彩妆套装','套装','美妆工具套装','身体护理套装'
#select distinct(item_third_cate_cd) from recall.m_sku_information where item_third_cate_name = '套装/礼盒';

HQL="
$setcof
drop table if exists samples_with_cid1_by_search_clk_rmcid3;
CREATE EXTERNAL TABLE if not exists samples_with_cid1_by_search_clk_rmcid3
(
  pvid string COMMENT 'query log id',
  list_pws string COMMENT '',
  set_pws string COMMENT ''
) COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS ORC
location '${HADOOP_PATH}/samples_with_cid1_by_search_clk_rmcid3'
tblproperties ('orc.compress'='SNAPPY');


drop table if exists samples_tmp_cid3;
create table samples_tmp_cid3
 as 
select pvid, concat(cid1, '-', pw) as pw, cid3
from search_clk_data
lateral view explode(pws) ps as pw
where pw <> '-1' and pw is not null
and cid3 not in ('9767','1396','16850','16869','16761','6739','14119','16794','11929','11930','11222','1426');


insert overwrite table samples_with_cid1_by_search_clk_rmcid3
select pvid, 
       concat_ws(',', collect_list(cast(rn as string))) as list_pws,
       concat_ws(',', collect_set(cast(rn as string))) as set_pws
from (select a.pvid, c.rn
      from samples_tmp_cid3 a
           left join
           pwwithcid12id_tmp c
           on a.pw=c.pw
      where c.pw is not null
     ) d
group by pvid
;"
echo "$HQL"
if hive -e "$HQL"; then
    echo "create data_pre done."
else
    echo "create data_pre failed..."
    exit 2;
fi

hive -e " select list_pws 
          from tmpr.samples_with_cid1_by_search_clk_rmcid3 
          where size(split(list_pws, ','))>=5 and size(split(set_pws, ','))>=3
        ;" > corpus_with_cid1_rmcid3.csv
        
        
factor=12
base_corpus_size=`expr 100 \* 1000000`
corpus_size=`expr $factor \* $base_corpus_size`     
hive -e "
select regexp_replace(list_pws, ',', ' ')
from tmpr.samples_with_cid1_by_search_clk_rmcid3
where size(split(list_pws, ','))>=5 and size(split(set_pws,','))>=3
limit $corpus_size
;" > corpus-$factor


drop table tmpr.hhy_ci_pw;
create table tmpr.hhy_ci_pw_7
location 'hdfs://ns1013/user/recsys/rec/tmpr.db/hhy_ci_pw_5'
as
select cid1,cid1_name,pw,num,rn,n_rows from(
select cid1,pw,num,rn,n_rows from (
select cid1,pw,num,row_number () over(partition by cid1 order by num asc) as rn,
count(*) over(partition by cid1) as n_rows
 from (
select cid1,pw, count(*) as num from tmpr.search_clk_data lateral view explode(pws)tmp as pw
group by cid1,pw )tmp)out
where rn<=round(n_rows * 70 / 100))tmp1
left join(
select item_first_cate_cd,item_first_cate_name as cid1_name from recall.m_sku_information group by item_first_cate_cd,item_first_cate_name)tmp2
on tmp1.cid1=tmp2.item_first_cate_cd
;


select count(*) from (
select sku from (select sku,concat(cid1,'-',pw) as c1_pw from (select * from tmpr.search_clk_data 
        lateral view explode(pws)tmp as pw)v)a
left join (select concat(cid1,'-',pw) as c1_pw from tmpr.hhy_ci_pw_7)b
on a.c1_pw=b.c1_pw
where b.c1_pw is null
group by sku)out;

select num, count(1) as freq
from (select pw,count(*) as num from 
(select pw,cid1 from tmpr.search_clk_data lateral view explode(pws)tmp as pw group by pw,cid1)a
group by pw order by num asc) b
group by num;


hive -e"
select pw,count(*) as num from 
(select pw,cid1 from tmpr.search_clk_data lateral view explode(pws)tmp as pw group by pw,cid1)a
group by pw order by num asc;">pw_num_incid1.txt

select set_pws,max(pws) from tmpr.samples_with_cid1_by_search_clk_rmcid3 lateral view explode(set_pws)tmp as pws group by set_pws limit 5;

create table tmpr.samples_with_cid1_by_search_clk_rmcid3_withflag
location 'hdfs://ns1013/user/recsys/rec/tmpr.db/samples_with_cid1_by_search_clk_rmcid3_withflag'
as
select pvid, 
       list_pws,
       set_pws,
       if (min(pws)>222040,1000,if (min(pws) > 119893,100,1))as flag
from (
select pvid, 
       concat_ws(',', collect_list(cast(rn as string))) as list_pws,
       concat_ws(',', collect_set(cast(rn as string))) as set_pws
from (select a.pvid, c.rn
      from tmpr.samples_tmp_cid3 a
           left join
           tmpr.pwwithcid12id_tmp c
           on a.pw=c.pw
           left join
           tmpr.codeMin10_code d
           on c.rn=d.code_id
      where c.pw is not null
      and d.code_id is null
     ) d
group by pvid)out
lateral view explode(split(set_pws,','))tmp as pws
group by pvid, 
       list_pws,
       set_pws
;

sleep 4h;
hive -e "
select regexp_replace(list_pws, ',', ' '),flag
from tmpr.samples_with_cid1_by_search_clk_rmcid3_withflag
where size(split(list_pws, ','))>=5 and size(split(set_pws,','))>=3
;" > corpus-12_done_data_4h

