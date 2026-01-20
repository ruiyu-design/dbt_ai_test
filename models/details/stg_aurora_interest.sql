with combined_data as (
	select
	    id
		,uid
		,workspace_id
		,order_sn
		,order_source
		,goods_id
		,goods_type
		,goods_plan_type
		,consume_interest
		,common_interest
		,start_valid_time
		,flush_time
		,extension_period
		,create_time
		,update_time
	from `notta-data-analytics.notta_aurora.notta_mall_interest_interest`
union all
    select
	    id
		,uid
		,workspace_id
		,order_sn
		,order_source
		,goods_id
		,goods_type
		,goods_plan_type
		,consume_interest
		,common_interest
		,start_valid_time
		,flush_time
		,extension_period
		,create_time
		,update_time
	from `notta-data-analytics.notta_aurora.notta_mall_interest_interest_history`
),

interest as (
select *,row_number() over(partition by id order by create_time) as rn
from combined_data

)

select
        id
		,uid
		,workspace_id
		,order_sn
		,order_source
		,goods_id
		,goods_type
		,goods_plan_type
		,consume_interest
		,common_interest
		,start_valid_time
		,flush_time
		,extension_period
		,create_time
		,update_time
from interest
where rn=1