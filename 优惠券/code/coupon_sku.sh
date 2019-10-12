source ./conf.sh

HQL="
$setcof;
create external table if not exists coupon_available (
  batch_id                bigint,
  cps_name                string comment '优惠券名称',
  cps_type_cd             bigint comment '0=京券 1=东券 2=免运费券', 
  cps_cate_cd             bigint comment '0-全品类,1-限品类,11-限品类商品,12-不可用商品,2-限店铺,3-限店铺三级类,31-限店铺商品,32-店铺不可用商品',
  allow_overlap           int    comment '是否允许叠加,1 不允许 2允许 默认不允许，0或者空）京券的叠加属性',
  dong_overlap            string comment '东券叠加的属性',
  vender_id               string,
  shop_id                 string comment '店铺id', 
  valid_start_tm          string comment '优惠券有效期开始时间', 
  valid_end_tm            string comment '优惠券有效期结束时间',
  cps_face_value          double comment '满减力度', 
  consume_lim             double comment '满减使用阈值',
  activity_applicant_erp_acct string,
  platform_type bigint comment '平台类型',
  second_biz_type_id bigint
) COMMENT '限店铺券'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS ORC
location '${HADOOP_PATH}/coupon_available'
tblproperties ('orc.compress'='SNAPPY');

create external table if not exists coupon_sku_batch (
    batch_id             string
   ,second_biz_type_id   bigint          
   ,sku                  string 
   ,ext                  string
) COMMENT '中间表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS ORC
location '${HADOOP_PATH}/coupon_sku_batch'
tblproperties ('orc.compress'='SNAPPY');
"
echo "$HQL"
if hive -e "$HQL"; then
    echo "create table done."
else
    echo "create table failed..."
    exit 2;
fi


HQL="
$setcof
insert overwrite table coupon_available
select batch_id, cps_name, cps_type_cd, cps_cate_cd, allow_overlap,
       dong_overlap, vender_id, shop_id, valid_start_tm, 
       valid_end_tm, cps_face_value, consume_lim,
       activity_applicant_erp_acct, platform_type, second_biz_type_id
from (select batch_id, cps_name, cps_type_cd, cps_cate_cd, allow_overlap,
             dong_overlap, vender_id, shop_id, valid_start_tm, 
             valid_end_tm, cps_face_value, consume_lim,
             activity_applicant_erp_acct, platform_type, second_biz_type_id,
             row_number() over(partition by batch_id order by batch_id) as rn
      from gdm.gdm_m07_cps_batch_da
      where dt = '$dt' and 
            (valid_end_tm >= '$dt' or val_time_limit=1) and 
            check_status_cd = 3 and 
            activity_end_tm >= '$dt' and  
            second_biz_type_id in (9, 10) and 
            batch_cps_total_qtty > batch_cps_putout_qtty and 
            batch_id is not null and 
            coupon_style in (0, 3) and 
            activity_applicant_erp_acct not in ('yubin11','bjmachun','xiaoyi29')
     ) a
where rn=1
;"
echo "$HQL"
if hive -e "$HQL"; then
    echo "done."
else
    echo "failed..."
    exit 2;
fi


# 店铺券
HQL="
$setcof
insert overwrite table coupon_sku_batch
select base.batch_id, base.second_biz_type_id, 
       shop.item_sku_id,
       'shop_coupon'
from (select batch_id, shop_id, second_biz_type_id
      from coupon_available
      where cps_cate_cd=2 and 
            vender_id <> 0 and 
            shop_id <> 0 
     ) base
     left join 
     sku_cid3_shop_brand_info shop
     on base.shop_id=shop.shop_id
where shop.shop_id is not null
;"
echo "$HQL"
if hive -e "$HQL"; then
    echo "done."
else
    echo "failed..."
    exit 2;
fi


# 店铺限sku券
HQL="
$setcof
insert into table coupon_sku_batch
select base.batch_id, base.second_biz_type_id, sku.sku,
       'shop_sku_coupon'
from (select batch_id, shop_id, second_biz_type_id
      from coupon_available
      where cps_cate_cd <> 2
     ) base
     left join 
     (select batch_id, sku 
      from fdm.fdm_coupon_batch_batch_limit_type_chain 
      where yn=1 and sku<>0
     ) sku
     on base.batch_id=sku.batch_id
where sku.batch_id is not null
group by base.batch_id, sku.sku, base.shop_id, base.second_biz_type_id
"
echo "$HQL"
if hive -e "$HQL"; then
    echo "done."
else
    echo "failed..."
    exit 2;
fi

# 限品类券
HQL="
$setcof
insert into table coupon_sku_batch
select batch.batch_id, t1.second_biz_type_id, tmp.sku,
       'limit_cid3_coupon'
from (select b.batch_id, count(1) as num 
      from (select batch_id 
            from fdm.fdm_coupon_batch_batch_chain 
            where dp='ACTIVE' and 
                  yn=1 and 
                  end_time>='$dt'
           ) b
           left join 
           (select batch_id 
            from fdm.fdm_coupon_batch_batch_ext_chain 
            where dp='ACTIVE' and yn=1 and 
                  ext_type=11 and 
                  ext_value in ('0','2','3')
           ) e
           on b.batch_id=e.batch_id 
      where e.batch_id is not null  
      group by b.batch_id
     ) batch
     left join 
     (select batch_id, sku 
      from fdm.fdm_coupon_batch_batch_limit_type_chain 
      where yn=1 and dp='ACTIVE' and 
            sku is not null and
            sku<>0
     ) tmp
     on batch.batch_id=tmp.batch_id
     left join
     (select * from coupon_available where cps_cate_cd<>2) t1
     on batch.batch_id=t1.batch_id
where tmp.sku is not null and t1.batch_id is not null
"
echo "$HQL"
if hive -e "$HQL"; then
    echo "done."
else
    echo "failed..."
    exit 2;
fi

# 限品类券
HQL="
$setcof
insert into table coupon_sku_batch
select batch_present.batch_id, t1.second_biz_type_id, present_sku.sku,
       'limit_cid3_coupon' 
from (select batch_id,
             if(ext_value is not null,ext_value,batch_id) as present_id 
      from (select ext_a.batch_id as batch_id,
                   ext_b.ext_value as ext_value
            from (select batch_id, ext_type, ext_value 
                  from fdm.fdm_coupon_batch_batch_ext_chain
                  where dp='ACTIVE' and 
                        yn=1 and 
                        ext_type=11 and 
                        ext_value in ('5','6')
                 ) ext_a
                 left outer join
                 (select batch_id, ext_type, ext_value 
                  from fdm.fdm_coupon_batch_batch_ext_chain
                  where dp='ACTIVE' and 
                        yn=1 and 
                        ext_type = 30
                 ) ext_b
                 on ext_a.batch_id=ext_b.batch_id
           ) c 
     ) batch_present
     left join
     (select present_id, biz_id as sku 
      from fdm.fdm_coupon_present_1_present_rule_chain
      where biz_type=1 and 
            use_rule=1 and 
            yn=1 and 
            dp='ACTIVE' and 
            biz_id<>0
     ) present_sku
    on batch_present.present_id=present_sku.present_id
    left join
    (select * from coupon_available where cps_cate_cd<>2) t1
    on batch_present.batch_id=t1.batch_id
where t1.batch_id is not null and present_sku.sku is not null
;"
echo "$HQL"
if hive -e "$HQL"; then
    echo "done."
else
    echo "failed..."
    exit 2;
fi
