i=45
dt=`date -d "-${i} day" +%Y-%m-%d`
dtt=`date -d "-${i} day" +%Y%m%d`
dt15=`date -d "-$[${i}+15] day" +%Y-%m-%d`
dt45=`date -d "-$[${i}+45] day" +%Y-%m-%d`
dt115=`date -d "-$[${i}+115] day" +%Y-%m-%d`
setcof="
set hive.optimize.skewjoin = true;
set hive.cli.print.header=true;
set mapreduce.input.fileinputformat.split.maxsize=2294967296;
set mapreduce.input.fileinputformat.split.minsize=2294967296;
set mapreduce.job.reduce.slowstart.completedmaps=1;
set hive.auto.convert.join=true;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.job.queue.name=root.bdp_jmart_recsys.recsys_rec;

add jar recsys-hive-udf-1.0-SNAPSHOT-jar-with-dependencies.jar;
create temporary function to_expid as 'com.jd.si.diviner.udf.MurmurHashExpid';
create temporary function to_uid as 'com.jd.si.diviner.udf.MurmurHash';
"
HQL="
create table if not exists tmp.re_purchasing_rbcid_base (
    uuid                    string       
   ,cid4                    string
   ,request_tm              string      
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid_base';

create table if not exists tmp.re_purchasing_rbcid_period (
   cid4                     string
  ,period                   string
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid_period';

create table if not exists tmp.re_purchasing_rbcid_cart (
    uuid                    string       
   ,cid4                    string
   ,cart_pv                 string  
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid_cart';

create table if not exists tmp.re_purchasing_rbcid_search_click (
    uuid                    string       
   ,cid4                    string
   ,search_click_pv         string  
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid_search_click';

create table if not exists tmp.re_purchasing_rbcid_interval (
    uuid                    string       
   ,cid4                    string
   ,avg_interval            string  
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid_interval';

create table if not exists tmp.re_purchasing_rbcid_count (
    uuid                    string       
   ,cid4                    string
   ,purchased_num           string  
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid_count';

create table if not exists tmp.re_purchasing_rbcid_order_after (
    uuid                    string       
   ,cid4                    string  
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid_order_after';

create table if not exists tmp.re_purchasing_rbcid (
    uuid                    string       
   ,cid4                    string
   ,request_tm              string      comment '订单时间'
   ,period                  string      comment '再次曝光周期'
   ,cart_pv                 string      comment '加购pv'
   ,search_click_pv         string      comment '搜索点击pv'  
   ,purchased_data_period   string      comment '已购天数/曝光周期'
   ,interval_period         string      comment '历史购买平均间隙/曝光周期'
   ,purchased_num           string      comment '历史购买次数'
   ,label                   string
) partitioned by (dt string)
location 'hdfs://ns1013/user/recsys/rec/tmp.db/re_purchasing_rbcid';
"
echo "$HQL"
hive -e "$HQL"


HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid_base partition (dt='$dt')
select uuid,
       cid4,
       request_tm from (
    select uuid,cid4,request_tm,row_number() over (partition by uuid,cid4 order by request_tm desc) num
        from ( 
        select uuid,item_sku_id as sku,request_tm
            from app.recsys_up_behavior_ord_app where dt>='$dt45' and dt<='$dt15') order_before
left join (
    select sku,split(cid4_period,',')[0] as cid4 from app.sku_pw_period_da) sku_cid4
    on order_before.sku=sku_cid4.sku)last   
where last.num = 1
     ;"
echo "$HQL"
hive -e "$HQL"

HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid_period partition (dt='$dt') 
select distinct split(cid4_period,',')[0] as cid4,split(cid4_period,',')[1] as period 
    from app.sku_pw_period_da
;"
echo "$HQL"
hive -e "$HQL"

HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid_cart partition (dt='$dt') 
select a.uuid,sku_cid4.cid4,count(*) as cart_pv from (
    select uuid,item_sku_id as sku,request_tm from app.recsys_up_behavior_cart_app
        where dt>='$dt45' and dt<='$dt15' )a
    left join (
        select sku,split(cid4_period,',')[0] as cid4 from app.sku_pw_period_da)sku_cid4
        on a.sku=sku_cid4.sku
	left join(
		select uuid,cid4,request_tm from tmp.re_purchasing_rbcid_base) base
		on a.uuid=base.uuid
		and sku_cid4.cid4=base.cid4
where a.request_tm>if(base.request_tm is null,string(0),base.request_tm)
group by a.uuid,sku_cid4.cid4
     ;"
echo "$HQL"
hive -e "$HQL"

HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid_search_click partition (dt='$dt') 
select a.uuid,sku_cid4.cid4,count(*) as search_click_pv from (
    select browser_uniq_id as uuid,sku,click_time from app.app_idata_bdl_search_click_log
        where dt>='$dt45' and dt<='$dt15')a
    left join (
        select sku,split(cid4_period,',')[0] as cid4 from app.sku_pw_period_da)sku_cid4
        on a.sku=sku_cid4.sku
	left join(
		select uuid,cid4,request_tm from tmp.re_purchasing_rbcid_base) base
		on a.uuid=base.uuid
		and sku_cid4.cid4=base.cid4
where string(unix_timestamp(a.click_time))>if(base.request_tm is null,string(0),base.request_tm)
group by a.uuid,sku_cid4.cid4
     ;"
echo "$HQL"
hive -e "$HQL"

HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid_interval partition (dt='$dt')
select uuid,cid4,avg(interval_tmp) as avg_interval from (
    select  uuid,
            cid4,
            request_tm,
            time,
            datediff(time,lag(time,1,0) over (partition by uuid,cid4 order by time asc))as interval_tmp 
        from (
        select uuid,
               item_sku_id,
               request_tm,
               from_unixtime(bigint(request_tm),'yyyy-MM-dd HH:mm:ss') as time
             from app.recsys_up_behavior_ord_app 
             where dt>='$dt115' and dt<='$dt15')a
    left join(
        select sku,split(cid4_period,',')[0] as cid4 from app.sku_pw_period_da)sku_cid4
            on a.item_sku_id=sku_cid4.sku)order_tmp
group by uuid,cid4
     ;"
echo "$HQL"
hive -e "$HQL"

HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid_count partition (dt='$dt')
select uuid,cid4,count(distinct request_tm) as purchased_num from (
    select uuid,item_sku_id as sku,request_tm from app.recsys_up_behavior_ord_app 
        where dt>='$dt115' and dt<='$dt15')ord
left join(
    select sku,split(cid4_period,',')[0] as cid4 from app.sku_pw_period_da) sku_cid4
    on ord.sku=sku_cid4.sku
group by uuid,
         cid4
     ;"
echo "$HQL"
hive -e "$HQL"

HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid_order_after partition (dt='$dt') 
select uuid,cid4 from (
    select uuid,item_sku_id as sku from app.recsys_up_behavior_ord_app 
        where dt>='$dt15' and dt<='$dt') order_after
    left join (
        select sku,split(cid4_period,',')[0] as cid4 from app.sku_pw_period_da)sku_cid4
        on order_after.sku=sku_cid4.sku
     ;"
echo "$HQL"
hive -e "$HQL"

HQL="
$setcof
insert overwrite table tmp.re_purchasing_rbcid partition (dt='$dt') 
select uuid, 
       cid4,
       request_tm,
       period,
       cart_pv,
       search_click_pv,
       purchased_data_period,
       interval_period, 
       purchased_num,       
       label    
from(
select order_before.uuid as uuid, 
       order_before.cid4 as cid4,
       order_before.request_tm as request_tm,
       cid4_period.period as period,
       if(cart.cart_pv is not null,cart.cart_pv,0) as cart_pv,
       if(search_click.search_click_pv is not null,search_click.search_click_pv,0) as search_click_pv,
       abs((unix_timestamp('$dt15','yyyy-MM-dd')-order_before.request_tm)/(cid4_period.period*24*60*60)) as purchased_data_period,
       if(order_all.avg_interval is not null,order_all.avg_interval/(cid4_period.period),0) as interval_period,
       order_count.purchased_num as purchased_num,
       if (order_after.uuid is null,0,1) as label
from ( 
    select uuid,cid4,request_tm from tmp.re_purchasing_rbcid_base where dt='$dt') order_before
left join (
    select cid4,period from tmp.re_purchasing_rbcid_period where dt='$dt') cid4_period
        on order_before.cid4=cid4_period.cid4       
left join ( 
    select uuid,cid4,cart_pv from tmp.re_purchasing_rbcid_cart where dt='$dt') cart
        on order_before.uuid=cart.uuid
        and order_before.cid4=cart.cid4 
left join (
    select uuid,cid4,search_click_pv from tmp.re_purchasing_rbcid_search_click where dt='$dt') search_click
        on order_before.uuid=search_click.uuid
        and order_before.cid4=search_click.cid4
left join (
    select uuid,cid4,avg_interval from tmp.re_purchasing_rbcid_interval where dt='$dt')order_all
        on order_before.uuid=order_all.uuid  
        and order_before.cid4=order_all.cid4
left join (
    select uuid,cid4,purchased_num from tmp.re_purchasing_rbcid_count where dt='$dt')order_count  
        on order_before.uuid=order_count.uuid  
        and order_before.cid4=order_count.cid4
left join (
    select uuid,cid4 from tmp.re_purchasing_rbcid_order_after where dt='$dt') order_after
        on order_before.uuid=order_after.uuid  
        and order_before.cid4=order_after.cid4) tmp
where cid4<>'手机话费充值'
and 100/period>purchased_num
and purchased_data_period*period>1
and purchased_data_period<1
group by uuid, 
         cid4,
         request_tm,
         period,
         cart_pv,
         search_click_pv,
         purchased_data_period,
         interval_period,   
         purchased_num,     
         label                  
     ;"
echo "$HQL"
hive -e "$HQL"


