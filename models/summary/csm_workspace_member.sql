SELECT
CAST(workspace_id AS STRING) AS workspace_id,
CAST(uid AS STRING) AS uid,
role,
TIMESTAMP_MILLIS(cast(create_time as integer)) as create_time,
TIMESTAMP_MILLIS(cast(login_time as integer)) as login_time,
bot_name
FROM `notta-data-analytics.notta_aurora.langogo_user_space_member`
WHERE CAST(workspace_id AS STRING) IN (
SELECT DISTINCT workspace_id from notta-data-analytics.dbt_models_details.stg_aurora_interest usage
WHERE usage.goods_plan_type IN (2,3)
and usage.goods_type not in (2,5,7,8,9)
and COALESCE(CAST(JSON_VALUE(usage.common_interest, '$.seats') AS INT64),0)>1

)