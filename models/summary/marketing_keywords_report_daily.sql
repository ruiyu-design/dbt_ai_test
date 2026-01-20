WITH
-- =================================================================================
-- 1. 维度查找：账户 2936848719 的 Campaign, AdGroup, Keyword 最新信息
-- =================================================================================
camp_dim_2936 AS (
    SELECT
       campaign_id
       ,MAX_BY(campaign_name, campaign_start_date) AS campaign_name
    FROM `notta-data-analytics.notta_google_ads.p_ads_Campaign_2936848719`
    GROUP BY 1
),
ag_dim_2936 AS (
    SELECT
        campaign_id,
        ad_group_id,
        ad_group_name
    FROM `notta-data-analytics.notta_google_ads.ads_AdGroup_2936848719`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id, ad_group_id ORDER BY _LATEST_DATE DESC) = 1
),
kw_dim_2936 AS (
    SELECT
        campaign_id,
        ad_group_id,
        ad_group_criterion_criterion_id,
        ad_group_criterion_keyword_match_type,
        ad_group_criterion_keyword_text
    FROM `notta-data-analytics.notta_google_ads.ads_Keyword_2936848719`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id, ad_group_id, ad_group_criterion_criterion_id ORDER BY _LATEST_DATE DESC) = 1
),

-- =================================================================================
-- 2. 基础关键词统计数据
-- =================================================================================
keyword_stats AS (
    SELECT
        t1.segments_date AS date,
        DATE_TRUNC(t1.segments_date, WEEK(FRIDAY)) AS week,
        t1.campaign_id,
        t1.ad_group_id,
        t1.ad_group_criterion_criterion_id,
        t1.metrics_cost_micros,
        t1.metrics_impressions,
        t1.metrics_clicks,
        t1.metrics_conversions,
        t1.metrics_conversions_value
    FROM `notta_google_ads.ads_KeywordStats_2936848719` AS t1
),

-- =================================================================================
-- 3. 按 [日期 + 维度] 分组并聚合指标
-- =================================================================================
grouped_stats AS (
    SELECT
        date,
        week,
        campaign_id,
        ad_group_id,
        ad_group_criterion_criterion_id,
        SUM(metrics_cost_micros) AS total_cost_micros,
        SUM(metrics_impressions) AS total_impressions,
        SUM(metrics_clicks) AS total_clicks,
        SUM(metrics_conversions) AS total_conversions,
        SUM(metrics_conversions_value) AS total_conversions_value
    FROM keyword_stats
    GROUP BY 1, 2, 3, 4, 5
),

-- =================================================================================
-- 4. 基础数据关联 + 国家字段计算
-- =================================================================================
base_data_with_country AS (
    SELECT
        t1.date,
        t1.week,

        -- 国家分类规则
        CASE
            -- 1. 区域/组合
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%NL/BE%' THEN 'Europe'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%ES/MX%' OR UPPER(t_camp_2936.campaign_name) LIKE '%MX/CO%' THEN 'T2'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%BR/PT%' THEN 'T2'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%MY/TH%' THEN 'T2'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%/CA/%'
                 OR UPPER(t_camp_2936.campaign_name) LIKE '%/AU/%'
                 OR UPPER(t_camp_2936.campaign_name) LIKE '%/UK/%'
                 OR UPPER(t_camp_2936.campaign_name) LIKE '%CA/UK%'
                 OR UPPER(t_camp_2936.campaign_name) LIKE '%US/CA%' THEN 'T1'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_EN_MENA_%' THEN 'T2'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_EN_OTHER_%' THEN 'T2'

            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_JP_%' THEN 'Japan'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_US_%' THEN 'United States'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_UK_%' OR UPPER(t_camp_2936.campaign_name) LIKE '%_GB_%' THEN 'United Kingdom'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_NL_%' THEN 'The Netherlands'

            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_EU_%' THEN 'Europe'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_DE_%' THEN 'Germany'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_FR_%' THEN 'France'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_IT_%' THEN 'Italy'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_MX_%' THEN 'Mexico'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_ES_%' THEN 'Spain'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_BR_%' THEN 'Brazil'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_PT_%' THEN 'Portugal'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_BR_%' THEN 'Brazil'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_AU_%' THEN 'Australia'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_SG_%' THEN 'Singapore'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_ID_%' THEN 'Indonesia'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_TH_%' THEN 'Thailand'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_HK_%' THEN 'HongKong'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_TW_%' THEN 'Taiwan'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_VI_%' THEN 'Vietnam'
            WHEN UPPER(t_camp_2936.campaign_name) LIKE '%_KR_%' THEN 'South Korea'

            ELSE 'Others'
        END AS country,
        t1.ad_group_criterion_criterion_id,
        t_kw_2936.ad_group_criterion_keyword_text,
        t_kw_2936.ad_group_criterion_keyword_match_type,
        t1.ad_group_id,
        t_ag_2936.ad_group_name,
        t1.campaign_id,
        t_camp_2936.campaign_name,

        -- 基础指标
        t1.total_cost_micros / 1000000 AS total_cost,
        t1.total_impressions,
        t1.total_clicks,
        t1.total_conversions,
        t1.total_conversions_value

    FROM grouped_stats AS t1

    -- 关联维度表
    LEFT JOIN kw_dim_2936 AS t_kw_2936
        ON t1.campaign_id = t_kw_2936.campaign_id
        AND t1.ad_group_id = t_kw_2936.ad_group_id
        AND t1.ad_group_criterion_criterion_id = t_kw_2936.ad_group_criterion_criterion_id
    LEFT JOIN ag_dim_2936 AS t_ag_2936
        ON t1.campaign_id = t_ag_2936.campaign_id
        AND t1.ad_group_id = t_ag_2936.ad_group_id
    LEFT JOIN camp_dim_2936 AS t_camp_2936
        ON t1.campaign_id = t_camp_2936.campaign_id
)

-- =================================================================================
-- 5. 最终输出：添加会议词判断字段
-- =================================================================================
SELECT
    *,
    CASE
        WHEN LOWER(ad_group_criterion_keyword_text) LIKE '%whisper%' THEN 'Whisper Keyword'
        WHEN country IN ('Japan', 'United States')
        AND (
            ad_group_criterion_keyword_text LIKE '%議事%'
            OR ad_group_criterion_keyword_text LIKE '%要約%'
            OR ad_group_criterion_keyword_text LIKE '%会議%'
            -- 使用 LOWER 确保匹配 meeting, Meeting, MEETING 等大小写情况
            OR LOWER(ad_group_criterion_keyword_text) LIKE '%meeting%'
            OR LOWER(ad_group_criterion_keyword_text) LIKE '%minutes%'
            OR LOWER(ad_group_criterion_keyword_text) LIKE '%summary%'
        )
        -- 排除词也做小写处理以防万一
        AND LOWER(ad_group_criterion_keyword_text) NOT LIKE '%video%'
        AND LOWER(ad_group_criterion_keyword_text) NOT LIKE '%youtube%'
        AND LOWER(ad_group_criterion_keyword_text) NOT LIKE '%podcast%'
        THEN 'Meeting Keyword'
        ELSE 'Else'
    END AS keyword_category -- 这里给新字段起名为 keyword_category
FROM base_data_with_country




