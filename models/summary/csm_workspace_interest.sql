select
  usage.workspace_id,
  TIMESTAMP_SECONDS(start_valid_time) AS start_t,
  TIMESTAMP_SECONDS(flush_time) AS flush_t,
  CASE usage.goods_plan_type
  WHEN 2 THEN 'Biz'
  WHEN 3 THEN 'Enterprise'
  ELSE 'Others'
  END AS plan_type,
  -- notta_interest_import_audio
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_import_audio.total') AS INT64),0) AS `import_file_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_import_audio.used') AS INT64),0) AS `import_file_used`,

  -- notta_interest_ai_summary
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_ai_summary.total') AS INT64),0) AS `ai_summary_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_ai_summary.used') AS INT64),0) AS `ai_summary_used`,

  -- notta_interest_chat_bot
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_new_chat_bot.total') AS INT64),0) AS `ai_chat_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_new_chat_bot.used') AS INT64),0) AS `ai_chat_used`,

  -- duration
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.duration.total') AS INT64),0) AS `duration_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.duration.used') AS INT64),0) AS `duration_used`,

  COALESCE(CAST(JSON_VALUE(usage.common_interest, '$.seats') AS INT64),0) as seats_size,

  row_number() over (partition by concat(workspace_id,start_valid_time) order by create_time desc) as rn

FROM
	`notta-data-analytics.dbt_models_details.stg_aurora_interest` usage
WHERE
    usage.goods_plan_type IN (2,3) -- biz和enterprise
	and usage.goods_type not in (2,5,7,8,9) -- 排除add on
	and start_valid_time<flush_time --排除已回收数据
	and TIMESTAMP_SECONDS(start_valid_time)<current_timestamp() --排除未开始权益
	and COALESCE(CAST(JSON_VALUE(usage.common_interest, '$.seats') AS INT64),0)>1 -- 筛选2席位以上数据
ORDER BY workspace_id, start_t