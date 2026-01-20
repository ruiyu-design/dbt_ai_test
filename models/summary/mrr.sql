select
	substr(cast(calendar_date as string),1,7) as month
	,pay_channel
	,case when pay_currency in ('JPY','USD') then pay_currency else 'Other' end as pay_currency
	,case when signup_country in ('Japan','United States','unknown') then signup_country else 'Other' end as signup_country
	,profession
	,plan_type_name
	,seat_type
	,mrr_type_detail
	,mrr_type
	,round(sum(case when mrr_type='Churned' then 0-daily_payment_amount else daily_payment_amount end),2) as mrr
	,round(sum(case when mrr_type='New' then daily_payment_amount else 0 end),2) as new_mrr
	,round(sum(case when mrr_type='Recurring' then daily_payment_amount else 0 end),2) as recurring_mrr
	,round(sum(case when mrr_type!='Churned' then daily_payment_amount else 0 end),2) as total_mrr
	,round(sum(case when mrr_type='Churned' then daily_payment_amount else 0 end),2) as churned_mrr
from `dbt_models_details.notta_mrr`
where substr(cast(calendar_date as string),1,7)<=substr(cast(date_add(current_date(),interval -1 day) as string),1,7)
group by
	substr(cast(calendar_date as string),1,7)
	,pay_channel
	,case when pay_currency in ('JPY','USD') then pay_currency else 'Other' end
	,case when signup_country in ('Japan','United States','unknown') then signup_country else 'Other' end
	,profession
	,plan_type_name
	,seat_type
	,mrr_type_detail
	,mrr_type