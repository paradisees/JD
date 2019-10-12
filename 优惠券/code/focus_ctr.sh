i=3
dt=`date -d "-${i} day" +%Y-%m-%d`
dtt=`date -d "-${i} day" +%Y%m%d`
dt30=`date -d "-$[${i}+30] day" +%Y-%m-%d`

setcof="
set hive.optimize.skewjoin = true;
set hive.cli.print.header=true;
set mapreduce.input.fileinputformat.split.maxsize=1147483648;
set mapreduce.input.fileinputformat.split.minsize=1147483648;
set mapreduce.job.reduce.slowstart.completedmaps=1;
set hive.auto.convert.join=true;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
"

HQL="
create table if not exists tmp.hhy_focus_ctr (
    clk_pv   string       
   ,clk_uv     string          
   ,tab	        string
   ,expo_uv             string        
   ,expo_pv			string
   ,ctr		string
   ,click_people		string
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/hhy_focus_ctr';

create table if not exists tmp.hhy_focus_ctr_other (
    clk_pv   string       
   ,clk_uv     string          
   ,tab	        string
   ,expo_uv             string        
   ,expo_pv			string
   ,ctr		string
   ,click_people		string
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/hhy_focus_ctr_other';
"
echo "$HQL"
hive -e "$HQL"


HQL="
$setcof
insert overwrite table tmp.hhy_focus_ctr partition (dt='$dt')
select a.clk_pv, 
       a.clk_uv, 
	   a.tab,
       b.expo_uv, 
       b.expo_pv, 
       a.clk_pv/b.expo_pv as ctr,
       a.clk_pv/b.expo_uv as click_people 
from (select tab,
             count(distinct browser_uniq_id) as clk_uv,
             count(1) as clk_pv
      from tmp.focus_click_info_new
	  where uid_hash>=50 and dt='$dt'
      group by tab
     ) a
join
     (select tab,
             count(distinct browser_uniq_id) as expo_uv,
			 count(1) as expo_pv
      from tmp.focus_expo_info_new
	  where uid_hash>=50 and dt='$dt'
      group by tab
     ) b
on a.tab=b.tab
where a.tab='PLUS专区';"
echo "$HQL"
hive -e "$HQL"


HQL="
$setcof
insert overwrite table tmp.hhy_focus_ctr_other partition (dt='$dt')
select a.clk_pv, 
       a.clk_uv, 
	   a.tab,
       b.expo_uv, 
       b.expo_pv, 
       a.clk_pv/b.expo_pv as ctr,
       a.clk_pv/b.expo_uv as click_people 
from (select tab,
             count(distinct browser_uniq_id) as clk_uv,
             count(1) as clk_pv
      from tmp.focus_click_info_new
	  where uid_hash<=50 and dt='$dt'
      group by tab
     ) a
join
     (select tab,
             count(1) as expo_pv,
             count(distinct browser_uniq_id) as expo_uv 
      from tmp.focus_expo_info_new
	  where uid_hash<=50 and dt='$dt'
      group by tab
     ) b
on a.tab=b.tab
where a.tab='PLUS专区';"
echo "$HQL"
hive -e "$HQL"