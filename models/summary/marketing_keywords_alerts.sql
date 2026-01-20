WITH
-- =================================================================================
-- 1. 核心聚合：按 [日期 + 国家 + 关键词文本] 汇总数据
--    (因为要监测的是这个词在整个国家的表现，所以先要把不同 Campaign/AdGroup 里的同一个词加起来)
-- =================================================================================
daily_grouped_base AS (
    SELECT
        date,
        week,
        country,
        ad_group_criterion_keyword_text,
        -- 关键词分类通常也是跟文本绑定的，取一个即可
        MAX(keyword_category) AS keyword_category,

        -- 拼接来源，方便定位 (因为聚合了，可能来自多个计划)
        STRING_AGG(DISTINCT campaign_name, ', ') AS campaign_names,
        STRING_AGG(DISTINCT ad_group_name, ', ') AS ad_group_names,

        -- 聚合指标
        SUM(total_cost) AS total_cost,
        SUM(total_clicks) AS total_clicks,
        SUM(total_conversions_value) AS total_conversions_value
    FROM dbt_models_summary.marketing_keywords_report_daily
    GROUP BY 1, 2, 3, 4
),

-- =================================================================================
-- 2. 数据增强：计算 CPC，标记工作日/周末
-- =================================================================================
daily_enhanced AS (
    SELECT
        *,
        -- 基于聚合后的数据计算 CPC
        SAFE_DIVIDE(total_cost, total_clicks) AS current_cpc,

        -- 标记日期类型：1=周日, 7=周六 (BigQuery标准) -> Weekend, 其他 -> Weekday
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type
    FROM daily_grouped_base
),

-- =================================================================================
-- 3. 窗口函数计算：过去5个同类型日期的均值
--    (PARTITION BY 改为 country, keyword_text, day_type)
-- =================================================================================
historical_stats AS (
    SELECT
        *,
        -- 计算过去5个同类型日(工作日vs工作日, 周末vs周末)的平均消耗
        AVG(total_cost) OVER (
            PARTITION BY country, ad_group_criterion_keyword_text, day_type
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_cost_5d,

        -- 计算过去5个同类型日的平均 CPC
        AVG(current_cpc) OVER (
            PARTITION BY country, ad_group_criterion_keyword_text, day_type
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_cpc_5d
    FROM daily_enhanced
),

-- =================================================================================
-- 4. 规则一 & 二：生成每日异常报告 (Daily Alerts)
-- =================================================================================
daily_alerts AS (
    SELECT
        date,
        'Daily Monitor' AS monitor_type,
        country,
        campaign_names AS campaign_name, -- 聚合后的字段
        ad_group_names AS ad_group_name, -- 聚合后的字段
        ad_group_criterion_keyword_text,
        keyword_category,
        total_cost,
        current_cpc,
        avg_cost_5d,
        avg_cpc_5d,

        -- 生成具体的报警原因
        ARRAY_TO_STRING([
            CASE
                WHEN total_cost < (avg_cost_5d * 0.5) THEN 'Cost Drop > 50%'
                WHEN total_cost > (avg_cost_5d * 1.5) THEN 'Cost Surge > 50%'
                ELSE NULL
            END,
            CASE
                WHEN current_cpc > (avg_cpc_5d * 1.3) THEN 'CPC Increase > 30%'
                ELSE NULL
            END
        ], ' | ') AS alert_message

    FROM historical_stats
    WHERE
        -- 基础门槛：聚合后的单日消耗 > 50
        total_cost > 50
        AND avg_cost_5d > 0
        AND (
            -- 规则一：消耗异常
            (total_cost < avg_cost_5d * 0.5 OR total_cost > avg_cost_5d * 1.5)
            OR
            -- 规则二：CPC 异常
            (current_cpc > avg_cpc_5d * 1.3)
        )
),

-- =================================================================================
-- 5. 规则三：生成每周异常报告 (Weekly ROAS Alerts)
--    (直接基于 daily_grouped_base 再次按周聚合，不需要回到原表)
-- =================================================================================
weekly_agg AS (
    SELECT
        week,
        country,
        STRING_AGG(DISTINCT campaign_names, ', ') AS campaign_names,
        STRING_AGG(DISTINCT ad_group_names, ', ') AS ad_group_names,
        ad_group_criterion_keyword_text,
        MAX(keyword_category) AS keyword_category,
        SUM(total_cost) AS weekly_cost,
        SUM(total_conversions_value) AS weekly_conv_value
    FROM daily_grouped_base
    GROUP BY 1, 2, 5
),

weekly_alerts AS (
    SELECT
        week AS date,
        'Weekly Monitor' AS monitor_type,
        country,
        campaign_names AS campaign_name,
        ad_group_names AS ad_group_name,
        ad_group_criterion_keyword_text,
        keyword_category,
        weekly_cost AS total_cost,
        NULL AS current_cpc,
        NULL AS avg_cost_5d,
        NULL AS avg_cpc_5d,

        CASE
            WHEN country = 'Japan' AND SAFE_DIVIDE(weekly_conv_value, weekly_cost) < 0.5
                THEN FORMAT('Japan ROAS Alert (%.2f < 0.5)', SAFE_DIVIDE(weekly_conv_value, weekly_cost))
            WHEN country != 'Japan' AND SAFE_DIVIDE(weekly_conv_value, weekly_cost) < 0.4
                THEN FORMAT('Non Japan ROAS Alert (%.2f < 0.4)', SAFE_DIVIDE(weekly_conv_value, weekly_cost))
        END AS alert_message

    FROM weekly_agg
    WHERE
        weekly_cost > 350
        AND (
            (country = 'Japan' AND SAFE_DIVIDE(weekly_conv_value, weekly_cost) < 0.5)
            OR
            (country != 'Japan' AND SAFE_DIVIDE(weekly_conv_value, weekly_cost) < 0.4)
        )
)

-- =================================================================================
-- 6. 最终输出：合并日报和周报报警信息
-- =================================================================================
SELECT * FROM daily_alerts
UNION ALL
SELECT * FROM weekly_alerts
ORDER BY date DESC, total_cost DESC