WITH campaign_weekly_data AS (
    SELECT
        weekdate_friday AS date,
        max_paid_platform,
        max_paid_country,
        source,
        campaign,
        -- 基础消耗与流量
        ROUND(SUM(cost), 2) AS cost,
        SUM(clicks) AS clicks,
        SUM(installs) AS installs,
        ROUND(SAFE_DIVIDE(SUM(cost), SUM(clicks)), 2) AS cpc,
        ROUND(SAFE_DIVIDE(SUM(cost), SUM(installs)), 2) AS cpi,
        ROUND(SAFE_DIVIDE(SUM(signup_users), SUM(clicks)), 4) AS cvr,

        -- 漏斗转化指标
        SUM(signup_users) AS signup_users,
        ROUND(SAFE_DIVIDE(SUM(cost), SUM(signup_users)), 2) AS cpr, -- 注册成本
        SUM(create_record_users) AS create_record_users,
        ROUND(SAFE_DIVIDE(SUM(cost), SUM(create_record_users)), 2) AS cpa, -- 激活成本

        -- 变现指标
        SUM(new_paid_users) AS new_paid_users,
        ROUND(SAFE_DIVIDE(SUM(new_paid_users), SUM(signup_users)), 4) AS paid_rate,
        ROUND(SAFE_DIVIDE(SUM(new_paid_users), SUM(installs)), 4) AS install_to_paid_rate,
        ROUND(SAFE_DIVIDE(SUM(cost), SUM(new_paid_users)), 2) AS cac,
        ROUND(SUM(first_payment_rev), 2) AS first_payment_rev,
        ROUND(SAFE_DIVIDE(SUM(first_payment_rev), SUM(cost)), 4) AS first_payment_roas

    FROM `dbt_models_summary.marketing_report_by_campaign`
    GROUP BY 1, 2, 3, 4, 5
    HAVING Cost > 0 -- 仅保留有花费的记录
)

SELECT
    a.*,
    b.weekly_change_description
FROM campaign_weekly_data a
LEFT JOIN dbt_models_details.marketing_googleads_campaign_weekly_change_log b on a.date=b.week_start_date and a.campaign=b.campaign_name