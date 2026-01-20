WITH
-- =================================================
-- 1. 基础维表：用户画像 (User Profile)
-- =================================================
user_basic AS (
    SELECT
        CAST(u.uid AS INT64) AS uid,
        CASE
            WHEN u.signup_country IN ('Japan','United States','unknown','United Kingdom','France','Canada')  THEN u.signup_country
            ELSE 'Other'
        END AS country_group,
        CASE
            WHEN raw_u.register_platform IS NULL THEN 'meeting'
            WHEN TRIM(CAST(raw_u.register_platform AS STRING)) = '' THEN 'meeting'
            WHEN LOWER(TRIM(CAST(raw_u.register_platform AS STRING))) = 'null' THEN 'meeting'
            ELSE CAST(raw_u.register_platform AS STRING)
        END AS register_platform
    FROM `notta-data-analytics.dbt_models_details.user_details` u
    LEFT JOIN `notta-data-analytics.notta_aurora.langogo_user_space_users` raw_u
        ON CAST(u.uid AS INT64) = CAST(raw_u.uid AS INT64)
    WHERE u.pt = date_add(date(current_timestamp()), interval -1 DAY)
),

-- =================================================
-- 2. 基础维表：每日权益 (Daily Plan)
-- =================================================
daily_plans AS (
    SELECT
        CAST(workspace_id AS INT64) AS workspace_id,
        calendar_date,
        CASE
            WHEN goods_plan_type IS NULL THEN 'Free'
            WHEN goods_plan_type = 0 THEN 'Starter'
            WHEN goods_plan_type = 1 THEN 'Pro'
            WHEN goods_plan_type = 2 THEN 'Biz'
            WHEN goods_plan_type = 3 THEN 'Enterprise'
            ELSE 'Other'
        END AS plan_type
    FROM `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
),

-- =================================================
-- 3. 录音行为标签 (Meeting User)
-- =================================================
daily_record_stats AS (
    SELECT
        DATE(create_date) AS stat_date,
        CAST(creator_uid AS INT64) AS uid,
        COUNT(record_id) AS total_record_cnt,
        SUM(audio_duration) / 60.0 as total_duration_min
    FROM `notta-data-analytics.dbt_models_details.stg_aurora_record`
    WHERE TIMESTAMP(create_date) >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 780 DAY)
    GROUP BY 1, 2
),

daily_meeting_labels AS (
    SELECT
        stat_date,
        uid,
        CASE
            WHEN SAFE_DIVIDE(total_duration_min, total_record_cnt) > 30 THEN 'Meeting User'
            ELSE 'Non-Meeting User'
        END AS meeting_status
    FROM daily_record_stats
),

-- =================================================
-- 4. 核心数据准备 (Brain Requests)
-- =================================================
raw_brain_data AS (
    SELECT
        request_id,
        session_id,
        CAST(uid AS INT64) AS uid,
        CAST(workspace_id AS INT64) AS workspace_id,
        DATE(raw_timestamp) AS stat_date
    FROM `notta-data-analytics.dbt_models_details.active_notta_brain_request`
    WHERE uid IS NOT NULL
      AND uid != ''
      AND DATE(raw_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)
),

-- =================================================
-- 5. 构造 Daily 数据 和 Last 30 Days 数据
-- =================================================
combined_activity AS (
    -- --- Part A: 正常的每日数据 (Daily) ---
    SELECT
        'Daily' AS period_type,    -- [新增] 标识为每日数据
        stat_date,                 -- 具体的自然日
        stat_date AS dimension_date, -- 用于关联维度的日期 (当天)
        uid,
        workspace_id,
        request_id,
        session_id
    FROM raw_brain_data
    WHERE stat_date < CURRENT_DATE()

    UNION ALL

    -- --- Part B: 过去30天汇总 (Last 30 Days) ---
    SELECT
        'Last 30 Days' AS period_type, -- [新增] 标识为30天汇总
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS stat_date, -- 锚定到昨天(T-1)
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS dimension_date, -- 维度取 T-1 的快照
        uid,
        workspace_id,
        request_id,
        session_id
    FROM raw_brain_data
    -- 【核心逻辑】只取 T-1 到 T-30 的数据
    WHERE stat_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

-- =================================================
-- 6. 最终输出
-- =================================================
SELECT
    -- --- 标识字段 ---
    base.period_type, -- 用于筛选是 'Daily' 还是 'Last 30 Days'
    base.stat_date,   -- 均为 DATE 类型，方便排序

    -- --- 维度 (Dimensions) ---
    COALESCE(u.country_group, 'unknown') AS country_group,
    COALESCE(u.register_platform, 'meeting') AS register_platform,
    COALESCE(p.plan_type, 'Free/unknown') AS plan_type,
    COALESCE(ml.meeting_status, 'Non-Meeting User') AS meeting_user_status,

    -- --- 指标 (Metrics) ---
    COUNT(DISTINCT base.request_id) AS total_requests,
    COUNT(DISTINCT base.session_id) AS total_sessions,
    COUNT(DISTINCT base.uid) AS active_users,

    -- --- 衍生指标 ---
    SAFE_DIVIDE(COUNT(base.request_id), COUNT(DISTINCT base.uid)) AS avg_requests_per_user,
    SAFE_DIVIDE(COUNT(base.request_id), COUNT(DISTINCT base.session_id)) AS avg_requests_per_session

FROM combined_activity base
-- 关联用户画像
LEFT JOIN user_basic u ON base.uid = u.uid
-- 关联权益 (关联 dimension_date: Daily对应当日, L30对应T-1)
LEFT JOIN daily_plans p
    ON base.workspace_id = p.workspace_id AND base.dimension_date = p.calendar_date
-- 关联会议用户标签 (关联 dimension_date)
LEFT JOIN daily_meeting_labels ml
    ON base.uid = ml.uid AND base.dimension_date = ml.stat_date

GROUP BY 1, 2, 3, 4, 5, 6

-- 排序逻辑:
-- 1. 按 period_type 降序 ('Last 30 Days' > 'Daily')，让汇总行排在最前面
-- 2. 按 stat_date 降序，让最近的日期排在前面
ORDER BY 1 DESC, 2 DESC