{{
    config(
        materialized='incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "alert_date",
            "data_type": "date"
        }
    )
}}
-- 监控的业务单元：
-- Web端 GoogleAds 日本/美国
-- Web端 BingAds 日本
-- IOS端 ASA/AppierAds/MolocoAds/MintegralAds 日本
-- ANDROID端 GoogleAds 日本/美国/印尼/菲律宾/马来

-- 日监控
-- 规则1: 核心表现 - Campaign CPR/CPA, 目的: 快速定位导致成本变化的广告系列
-- 	触发条件: 日消耗 > $200, WEB端的GoogleAds
-- 	1.1 偏离大盘: 其当日成本高于或低于所属业务单元（国家-渠道-平台）过去7日加权平均成本的30%。
-- 	1.2 自身恶化: 其当日成本对比自身过去7日的平均成本，下降超过20%。

-- 规则2: 核心表现 - Unit D1/D4 ROAS, 目的: 及时发现单日渠道级别的短期回报率恶化或异动
-- 	 Web端对比该业务单元过去7天的加权平均 D1/D4 ROAS，波动超过 ±20%。
-- 	 IOS和ANDROID端对比该业务单元过去7天的加权平均 D1/D7 ROAS，波动超过 ±20%。
-- 	 WEB端的GoogleAds D4 ROAS 低于 35% 的绝对目标。
-- 	 WEB端的GoogleAds D1 ROAS 低于 20% 的绝对目标。
-- 	 日本WEB端的BingAds D1 ROAS 低于 25% 的绝对目标。
-- 	 日本WEB端的BingAds D4 ROAS 低于 45% 的绝对目标。
-- 	 日本IOS端 D1 ROAS 低于 50% 的绝对目标。
-- 	 日本IOS端 D7 ROAS 低于 80% 的绝对目标。
-- 	 日本IOS端 D7 ROAS 高于 90% 的绝对目标。
-- 	 日本ANDROID端 GoogleAds D1 ROAS 低于 40% 的绝对目标。
-- 	 日本ANDROID端 GoogleAds D7 ROAS 低于 55% 的绝对目标。
-- 	 日本ANDROID端 GoogleAds D7 ROAS 高于 90% 的绝对目标。
-- 	 美国ANDROID端 GoogleAds D1 ROAS 低于 50% 的绝对目标。
-- 	 美国ANDROID端 GoogleAds D1 ROAS 高于 80% 的绝对目标。
-- 	 印尼ANDROID端 GoogleAds D1 ROAS 低于 50% 的绝对目标。
-- 	 印尼ANDROID端 GoogleAds D1 ROAS 高于 80% 的绝对目标。
-- 	 菲律宾ANDROID端 GoogleAds D1 ROAS 低于 50% 的绝对目标。
-- 	 菲律宾ANDROID端 GoogleAds D1 ROAS 高于 70% 的绝对目标。
-- 	 马来ANDROID端 GoogleAds D1 ROAS 低于 55% 的绝对目标。
-- 	 马来ANDROID端 GoogleAds D1 ROAS 高于 80% 的绝对目标。


-- 规则3: 过程诊断 - Campaign CTR/CVR, 目的: 漏斗前端是否健康，判断素材创意或落地页是否存在问题
-- 	触发条件: 日消耗 > $100, WEB端的GoogleAds
-- 	3.1 对比其自身过去7天的加权平均值下降超过20%。

-- 规则4: 趋势变化 - Campaign CPR, 目的: 捕捉成本持续、渐进式恶化的广告系列
-- 	触发条件: 日消耗 > $100, WEB端的GoogleAds
-- 	4.1 连续3天，每天都比前一天上涨超过10%。

-- 规则5: 趋势变化 - Unit D1 ROAS, 捕捉渠道级别的持续性表现衰退
-- 	5.1 连续3天，每天都比前一天下降超过10%。


-- 周监控
-- 规则6: 长期价值 - Unit 周度D30 ROAS, 目的: 评估渠道的长期健康度和真实盈利水平
-- 	6.1 该业务单元的D30完全转化的最近一个完整周 D30 ROAS 低于
-- 		WEB端的GoogleAds: 50%
-- 		日本WEB端的BingAds: 70%
-- 		日本IOS端: 90%
-- 		日本ANDROID端 GoogleAds: 65%
-- 		美国ANDROID端 GoogleAds: 65%
-- 		印尼ANDROID端 GoogleAds: 55%
-- 		菲律宾ANDROID端 GoogleAds: 50%
-- 		马来ANDROID端 GoogleAds: 65%


WITH
  -- Campaign日聚合
  campaign_daily_metrics_base AS (
    SELECT
      date,
      platform,
      country,
      source,
      CONCAT(platform, '-', country, '-', source) AS business_unit,
      campaign,
      cost,
      impressions,
      clicks,
      signup_users AS signups,
      create_record_users AS actives,
      `24hours_revenue` AS revenue24h,
      `96hours_revenue` AS revenue96h,
      `7days_revenue` AS revenue7d,
      `30days_revenue` AS revenue30d,
      SAFE_DIVIDE(clicks, impressions) AS ctr,
      SAFE_DIVIDE(signup_users, clicks) AS cvr,
      SAFE_DIVIDE(cost, signup_users) AS cpr,
      SAFE_DIVIDE(cost, create_record_users) AS cpa,
      SAFE_DIVIDE(`24hours_revenue`, cost) AS roas24h, -- D1 ROAS
      SAFE_DIVIDE(`96hours_revenue`, cost) AS roas96h, -- D4 ROAS
      SAFE_DIVIDE(`7days_revenue`, cost) AS roas7d     -- D7 ROAS
    FROM
      `dbt_models_summary.marketing_funnel_report`
    WHERE
      date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND (
        (platform = 'WEB' AND source = 'GoogleAds' AND country IN ('Japan', 'United States'))
        OR (platform = 'WEB' AND source = 'BingAds' AND country = 'Japan')
        OR (platform = 'IOS' AND source IN ('ASA', 'AppierAds', 'MolocoAds', 'MintegralAds') AND country = 'Japan')
        OR (platform = 'ANDROID' AND source = 'GoogleAds' AND country IN ('Japan', 'United States', 'Indonesia', 'Philippines', 'Malaysia'))
      )
  ),

  -- Campaign：仅为WEB-JP-GoogleAds计算计算7天平均值和连续变化 (用于规则1, 3, 5)
  campaign_daily_metrics_enhanced AS (
    SELECT
      b.*,
      -- 【规则3】计算过去7日的加权平均CTR和CVR
      SAFE_DIVIDE(
          SUM(b.clicks) OVER w_prev_7d,
          SUM(b.impressions) OVER w_prev_7d
      ) AS prev_7d_weighted_avg_ctr,
      SAFE_DIVIDE(
          SUM(b.signups) OVER w_prev_7d,
          SUM(b.clicks) OVER w_prev_7d
      ) AS prev_7d_weighted_avg_cvr,

      -- 【规则1】计算包含当日的CPR算术平均值
      AVG(b.cpr) OVER w_prev_7d AS cpr_7d_avg_campaign,
      AVG(b.cpa) OVER w_prev_7d AS cpa_7d_avg_campaign,

      -- 【规则5】计算CPR的LAG值
      LAG(b.cpr, 1) OVER (PARTITION BY b.business_unit, b.campaign ORDER BY b.date) AS prev_day_1_cpr,
      LAG(b.cpr, 2) OVER (PARTITION BY b.business_unit, b.campaign ORDER BY b.date) AS prev_day_2_cpr
    FROM
      campaign_daily_metrics_base b
    WHERE
      business_unit in ('WEB-Japan-GoogleAds', 'WEB-United States-GoogleAds')
    -- 定义一个“过去7天”的窗口，不包含今天
    WINDOW w_prev_7d AS (PARTITION BY b.business_unit, b.campaign ORDER BY b.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
  ),

  -- 业务单元日聚合
  unit_daily_aggregates AS (
    SELECT
      date,
      business_unit,
      platform,
      country,
      source,
      SUM(cost) as total_cost,
      SUM(impressions) as total_impressions,
      SUM(clicks) as total_clicks,
      SUM(signups) as total_signups,
      SUM(actives) as total_actives,
      SUM(revenue24h) as total_revenue24h,
      SUM(revenue96h) as total_revenue96h,
      SUM(revenue7d) as total_revenue7d
    FROM
      campaign_daily_metrics_base
    GROUP BY
      date,
      business_unit,
      platform,
      country,
      source
  ),

  -- 业务单元：当日7天加权平均 D1/D7ROAS/CPR/CPA (用于规则1, 2, 3)
  unit_metrics_with_history AS (
    SELECT
      u.date,
      u.business_unit,
      u.platform,
      u.country,
      u.source,
      -- 计算当日的指标
      SAFE_DIVIDE(u.total_revenue24h, u.total_cost) AS daily_unit_roas24h,
      SAFE_DIVIDE(u.total_revenue96h, u.total_cost) AS daily_unit_roas96h,
      SAFE_DIVIDE(u.total_revenue7d, u.total_cost) AS daily_unit_roas7d,
      -- 计算过去7日的滚动加权平均指标
      SAFE_DIVIDE(SUM(u.total_revenue24h) OVER w_prev_7d, SUM(u.total_cost) OVER w_prev_7d) AS hist_7d_weighted_avg_roas24h,
      SAFE_DIVIDE(SUM(u.total_revenue96h) OVER w_prev_7d, SUM(u.total_cost) OVER w_prev_7d) AS hist_7d_weighted_avg_roas96h,
      SAFE_DIVIDE(SUM(u.total_revenue7d) OVER w_prev_7d, SUM(u.total_cost) OVER w_prev_7d) AS hist_7d_weighted_avg_roas7d,
      SAFE_DIVIDE(SUM(u.total_cost) OVER w_prev_7d, SUM(u.total_signups) OVER w_prev_7d) AS cpr_7d_weighted_avg_unit,
      SAFE_DIVIDE(SUM(u.total_cost) OVER w_prev_7d, SUM(u.total_actives) OVER w_prev_7d) AS cpa_7d_weighted_avg_unit,
      SAFE_DIVIDE(SUM(u.total_clicks) OVER w_prev_7d, SUM(u.total_impressions) OVER w_prev_7d) AS ctr_7d_weighted_avg_unit,
      SAFE_DIVIDE(SUM(u.total_signups) OVER w_prev_7d, SUM(u.total_clicks) OVER w_prev_7d) AS cvr_7d_weighted_avg_unit
    FROM
      unit_daily_aggregates u
      WINDOW w_prev_7d AS (PARTITION BY u.business_unit ORDER BY u.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
  ),

  -- 业务单元连续变化 (用于规则6)
  unit_metrics_for_trends AS (
    SELECT
      *,
      LAG(daily_unit_roas24h, 1) OVER (PARTITION BY business_unit ORDER BY date) AS prev_day_1_unit_roas24h,
      LAG(daily_unit_roas24h, 2) OVER (PARTITION BY business_unit ORDER BY date) AS prev_day_2_unit_roas24h
    FROM
      unit_metrics_with_history
  ),

  -- 规则1: Campaign CPR/CPA 触发条件：Cost > 200
  rule1_alerts AS (
    SELECT
      '核心表现: Campaign CPR/CPA' AS rule_type,
      c.campaign,
      c.business_unit,
      CURRENT_DATE() AS alert_date,
      c.date AS metric_period,
      CASE
        WHEN c.cpr < u.cpr_7d_weighted_avg_unit * 0.7 THEN '正反馈'
        WHEN c.cpr > u.cpr_7d_weighted_avg_unit * 1.3 THEN '一般负反馈'
        WHEN c.cpr < c.cpr_7d_avg_campaign * 0.8 THEN '正反馈'
        WHEN c.cpa < u.cpa_7d_weighted_avg_unit * 0.7 THEN '正反馈'
        WHEN c.cpa > u.cpa_7d_weighted_avg_unit * 1.3 THEN '一般负反馈'
        WHEN c.cpa < c.cpa_7d_avg_campaign * 0.8 THEN '正反馈'
      END AS alert_category,
      CASE
        WHEN c.cpr < u.cpr_7d_weighted_avg_unit * 0.7 THEN FORMAT('CPR(%.2f) 低于业务单元7日加权均值(%.2f)的30%%以上', c.cpr, u.cpr_7d_weighted_avg_unit)
        WHEN c.cpr > u.cpr_7d_weighted_avg_unit * 1.3 THEN FORMAT('CPR(%.2f) 高于业务单元7日加权均值(%.2f)的30%%以上', c.cpr, u.cpr_7d_weighted_avg_unit)
        WHEN c.cpr < c.cpr_7d_avg_campaign * 0.8 THEN FORMAT('CPR(%.2f) 低于自身7日均值(%.2f)的20%%以上', c.cpr, c.cpr_7d_avg_campaign)
        WHEN c.cpa < u.cpa_7d_weighted_avg_unit * 0.7 THEN FORMAT('CPA(%.2f) 低于业务单元7日加权均值(%.2f)的30%%以上', c.cpa, u.cpa_7d_weighted_avg_unit)
        WHEN c.cpa > u.cpa_7d_weighted_avg_unit * 1.3 THEN FORMAT('CPA(%.2f) 高于业务单元7日加权均值(%.2f)的30%%以上', c.cpa, u.cpa_7d_weighted_avg_unit)
        WHEN c.cpa < c.cpa_7d_avg_campaign * 0.8 THEN FORMAT('CPA(%.2f) 低于自身7日均值(%.2f)的20%%以上', c.cpa, c.cpa_7d_avg_campaign)
      END AS comparison_data
    FROM campaign_daily_metrics_enhanced AS c
    JOIN unit_metrics_with_history AS u ON c.date = u.date AND c.business_unit = u.business_unit
    WHERE c.cost > 200
      AND (
      	(c.country='Japan' AND c.platform='WEB' AND c.date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      	OR
      	(c.country='United States' AND c.platform='WEB' AND c.date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      	)
      AND (
      	c.cpr < u.cpr_7d_weighted_avg_unit * 0.7
      	OR c.cpr > u.cpr_7d_weighted_avg_unit * 1.3
      	OR c.cpr < c.cpr_7d_avg_campaign * 0.8
      	OR c.cpa < u.cpa_7d_weighted_avg_unit * 0.7
      	OR c.cpa > u.cpa_7d_weighted_avg_unit * 1.3
      	OR c.cpa < c.cpa_7d_avg_campaign * 0.8
      	)
  ),

  -- 规则2: 业务单元 ROAS 表现监控
  rule2_alerts AS (
    SELECT
      '核心表现: Unit D1/D4/D7 ROAS' AS rule_type
      , CAST(NULL AS STRING) AS campaign
      , business_unit
      , CURRENT_DATE() AS alert_date
      , date AS metric_period,
      CASE
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND platform = 'WEB' AND source = 'GoogleAds' AND daily_unit_roas96h < 0.35 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND platform = 'WEB' AND source = 'GoogleAds' AND daily_unit_roas24h < 0.20 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'WEB-Japan-BingAds' AND daily_unit_roas24h < 0.25 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND business_unit = 'WEB-Japan-BingAds' AND daily_unit_roas96h < 0.45 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND platform = 'IOS' AND country = 'Japan' AND daily_unit_roas24h < 0.50 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND platform = 'IOS' AND country = 'Japan' AND daily_unit_roas7d < 0.80 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND platform = 'IOS' AND country = 'Japan' AND daily_unit_roas7d > 0.90 THEN '正反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Japan-GoogleAds' AND daily_unit_roas24h < 0.40 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND business_unit = 'ANDROID-Japan-GoogleAds' AND daily_unit_roas7d < 0.55 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND business_unit = 'ANDROID-Japan-GoogleAds' AND daily_unit_roas7d > 0.90 THEN '正反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-United States-GoogleAds' AND daily_unit_roas24h < 0.50 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-United States-GoogleAds' AND daily_unit_roas24h > 0.80 THEN '正反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Indonesia-GoogleAds' AND daily_unit_roas24h < 0.50 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Indonesia-GoogleAds' AND daily_unit_roas24h > 0.80 THEN '正反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Philippines-GoogleAds' AND daily_unit_roas24h < 0.50 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Philippines-GoogleAds' AND daily_unit_roas24h > 0.70 THEN '正反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Malaysia-GoogleAds' AND daily_unit_roas24h < 0.55 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Malaysia-GoogleAds' AND daily_unit_roas24h > 0.80 THEN '正反馈'
        WHEN platform='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND daily_unit_roas96h < hist_7d_weighted_avg_roas96h * 0.8 THEN '紧急负反馈'
        WHEN platform='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND daily_unit_roas96h > hist_7d_weighted_avg_roas96h * 1.2 THEN '正反馈'
        WHEN platform!='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND daily_unit_roas7d < hist_7d_weighted_avg_roas7d * 0.8 THEN '紧急负反馈'
        WHEN platform!='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND daily_unit_roas7d > hist_7d_weighted_avg_roas7d * 1.2 THEN '正反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND daily_unit_roas24h < hist_7d_weighted_avg_roas24h * 0.8 THEN '紧急负反馈'
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND daily_unit_roas24h > hist_7d_weighted_avg_roas24h * 1.2 THEN '正反馈'
      END as alert_category,
      CASE
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND platform = 'WEB' AND source = 'GoogleAds' AND daily_unit_roas96h < 0.35 THEN FORMAT('WEB GoogleAds D4 ROAS(%.2f%%)低于目标(35%%)', daily_unit_roas96h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND platform = 'WEB' AND source = 'GoogleAds' AND daily_unit_roas24h < 0.20 THEN FORMAT('WEB GoogleAds D1 ROAS(%.2f%%)低于目标(20%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'WEB-Japan-BingAds' AND daily_unit_roas24h < 0.25 THEN FORMAT('WEB JP Bing D1 ROAS(%.2f%%)低于目标(25%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND business_unit = 'WEB-Japan-BingAds' AND daily_unit_roas96h < 0.45 THEN FORMAT('WEB JP Bing D4 ROAS(%.2f%%)低于目标(45%%)', daily_unit_roas96h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND platform = 'IOS' AND country = 'Japan' AND daily_unit_roas24h < 0.50 THEN FORMAT('IOS JP D1 ROAS(%.2f%%)低于目标(50%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND platform = 'IOS' AND country = 'Japan' AND daily_unit_roas7d < 0.80 THEN FORMAT('IOS JP D7 ROAS(%.2f%%)低于目标(80%%)', daily_unit_roas7d*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND platform = 'IOS' AND country = 'Japan' AND daily_unit_roas7d > 0.90 THEN FORMAT('IOS JP D7 ROAS(%.2f%%)高于目标(90%%)', daily_unit_roas7d*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Japan-GoogleAds' AND daily_unit_roas24h < 0.40 THEN FORMAT('ANDROID JP D1 ROAS(%.2f%%)低于目标(40%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND business_unit = 'ANDROID-Japan-GoogleAds' AND daily_unit_roas7d < 0.55 THEN FORMAT('ANDROID JP D7 ROAS(%.2f%%)低于目标(55%%)', daily_unit_roas7d*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND business_unit = 'ANDROID-Japan-GoogleAds' AND daily_unit_roas7d > 0.90 THEN FORMAT('ANDROID JP D7 ROAS(%.2f%%)高于目标(90%%)', daily_unit_roas7d*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-United States-GoogleAds' AND daily_unit_roas24h < 0.50 THEN FORMAT('ANDROID US D1 ROAS(%.2f%%)低于目标(50%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-United States-GoogleAds' AND daily_unit_roas24h > 0.80 THEN FORMAT('ANDROID US D1 ROAS(%.2f%%)高于目标(80%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Indonesia-GoogleAds' AND daily_unit_roas24h < 0.50 THEN FORMAT('ANDROID ID D1 ROAS(%.2f%%)低于目标(50%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Indonesia-GoogleAds' AND daily_unit_roas24h > 0.80 THEN FORMAT('ANDROID ID D1 ROAS(%.2f%%)高于目标(80%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Philippines-GoogleAds' AND daily_unit_roas24h < 0.50 THEN FORMAT('ANDROID PH D1 ROAS(%.2f%%)低于目标(50%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Philippines-GoogleAds' AND daily_unit_roas24h > 0.70 THEN FORMAT('ANDROID PH D1 ROAS(%.2f%%)高于目标(70%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Malaysia-GoogleAds' AND daily_unit_roas24h < 0.55 THEN FORMAT('ANDROID MY D1 ROAS(%.2f%%)低于目标(55%%)', daily_unit_roas24h*100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND business_unit = 'ANDROID-Malaysia-GoogleAds' AND daily_unit_roas24h > 0.80 THEN FORMAT('ANDROID MY D1 ROAS(%.2f%%)高于目标(80%%)', daily_unit_roas24h*100)

        WHEN platform='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND daily_unit_roas96h < hist_7d_weighted_avg_roas96h * 0.8 THEN FORMAT('D4 ROAS(%.2f%%) 低于历史7日加权均值(%.2f%%)的20%%以上', daily_unit_roas96h * 100, hist_7d_weighted_avg_roas96h * 100)
        WHEN platform='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AND daily_unit_roas96h > hist_7d_weighted_avg_roas96h * 1.2 THEN FORMAT('D4 ROAS(%.2f%%) 高于历史7日加权均值(%.2f%%)的20%%以上', daily_unit_roas96h * 100, hist_7d_weighted_avg_roas96h * 100)

        WHEN platform!='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND daily_unit_roas7d < hist_7d_weighted_avg_roas7d * 0.8 THEN FORMAT('D7 ROAS(%.2f%%) 低于历史7日加权均值(%.2f%%)的20%%以上', daily_unit_roas7d * 100, hist_7d_weighted_avg_roas7d * 100)
        WHEN platform!='WEB' and date = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND daily_unit_roas7d > hist_7d_weighted_avg_roas7d * 1.2 THEN FORMAT('D7 ROAS(%.2f%%) 高于历史7日加权均值(%.2f%%)的20%%以上', daily_unit_roas7d * 100, hist_7d_weighted_avg_roas7d * 100)

        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND daily_unit_roas24h < hist_7d_weighted_avg_roas24h * 0.8 THEN FORMAT('D1 ROAS(%.2f%%) 低于历史7日加权均值(%.2f%%)的20%%以上', daily_unit_roas24h * 100, hist_7d_weighted_avg_roas24h * 100)
        WHEN date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND daily_unit_roas24h > hist_7d_weighted_avg_roas24h * 1.2 THEN FORMAT('D1 ROAS(%.2f%%) 高于历史7日加权均值(%.2f%%)的20%%以上', daily_unit_roas24h * 100, hist_7d_weighted_avg_roas24h * 100)

      END AS comparison_data
    FROM unit_metrics_with_history
    WHERE date IN (DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY), DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY))
  ),

  -- 规则3: Campaign CTR/CVR 触发条件：Cost > 100
  rule3_alerts AS (
    SELECT
      '过程漏斗: Camapgin CTR/CVR' AS rule_type,
      campaign,
      business_unit,
      CURRENT_DATE() AS alert_date,
      date AS metric_period,
      '一般负反馈' as alert_category,
      CASE
          WHEN ctr < prev_7d_weighted_avg_ctr * 0.8 THEN FORMAT('CTR(%.2f%%)低于过去7日加权均值(%.2f%%)的20%%以上', ctr*100, prev_7d_weighted_avg_ctr*100)
          WHEN cvr < prev_7d_weighted_avg_cvr * 0.8 THEN FORMAT('CVR(%.2f%%)低于过去7日加权均值(%.2f%%)的20%%以上', cvr*100, prev_7d_weighted_avg_cvr*100)
      END AS comparison_data
    FROM
      campaign_daily_metrics_enhanced
    WHERE
      date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
      AND cost > 100
      AND (ctr < prev_7d_weighted_avg_ctr * 0.8 OR cvr < prev_7d_weighted_avg_cvr * 0.8)
  ),

  -- 规则4: 趋势 - CPR 连续3天上涨 (无变化)
  rule4_alerts AS (
    SELECT
      '趋势变化: Campaign CPR' AS rule_type,
      campaign,
      business_unit,
      CURRENT_DATE() AS alert_date,
      date AS metric_period,
      '一般负反馈' as alert_category,
      FORMAT('CPR连续3天上涨超10%% (D-2: %.2f, D-1: %.2f, D-0: %.2f)', prev_day_2_cpr, prev_day_1_cpr, cpr) AS comparison_data
    FROM
      campaign_daily_metrics_enhanced
    WHERE
      date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
      AND cpr > prev_day_1_cpr * 1.1
      AND prev_day_1_cpr > prev_day_2_cpr * 1.1
  ),

  -- 规则5: 趋势 - Unit D1 ROAS 连续3天下降 (无变化)
  rule5_alerts AS (
    SELECT
      '趋势变化: Unit D1 ROAS' AS rule_type,
      CAST(NULL AS STRING) as campaign,
      business_unit,
      CURRENT_DATE() AS alert_date,
      date AS metric_period,
      '紧急负反馈' as alert_category,
      FORMAT('D1 ROAS连续3天下降超10%% (D-2: %.2f%%, D-1: %.2f%%, D-0: %.2f%%)', prev_day_2_unit_roas24h*100, prev_day_1_unit_roas24h*100, daily_unit_roas24h*100) AS comparison_data
    FROM
      unit_metrics_for_trends
    WHERE
      date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
      AND daily_unit_roas24h < prev_day_1_unit_roas24h * 0.9
      AND prev_day_1_unit_roas24h < prev_day_2_unit_roas24h * 0.9
  ),

  -- 规则6: 长期价值 - 周度D30 ROAS
  rule6_alerts AS (
    WITH target_week_cte AS (
      -- 计算需要分析的目标周的开始日期(周一)
      SELECT
        CASE
          -- 如果 N-31 是周日
          WHEN FORMAT_DATE('%u', DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)) = '7'
            THEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY), WEEK(MONDAY))
          -- 如果 N-31 是周一到周六
          ELSE DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY), WEEK(MONDAY)), INTERVAL 7 DAY)
        END as target_week_start_date
    ),
	  thresholds AS (
	  -- D30 ROAS阈值
	    SELECT 'WEB-Japan-GoogleAds' as bu, 0.50 as th UNION ALL
	    SELECT 'WEB-United States-GoogleAds', 0.50 UNION ALL
	    SELECT 'WEB-Japan-BingAds', 0.70 UNION ALL
	    SELECT 'IOS-Japan-ASA', 0.90 UNION ALL
	    SELECT 'IOS-Japan-AppierAds', 0.90 UNION ALL
	    SELECT 'IOS-Japan-MolocoAds', 0.90 UNION ALL
	    SELECT 'IOS-Japan-MintegralAds', 0.90 UNION ALL
	    SELECT 'ANDROID-Japan-GoogleAds', 0.65 UNION ALL
	    SELECT 'ANDROID-United States-GoogleAds', 0.65 UNION ALL
	    SELECT 'ANDROID-Indonesia-GoogleAds', 0.55 UNION ALL
	    SELECT 'ANDROID-Philippines-GoogleAds', 0.50 UNION ALL
	    SELECT 'ANDROID-Malaysia-GoogleAds', 0.65
	  )
    SELECT
      '长期价值: Unit D30 ROAS' AS rule_type,
      CAST(NULL AS STRING) AS campaign,
      b.business_unit,
      CURRENT_DATE() AS alert_date,
      DATE(t.target_week_start_date) AS metric_period,
      '紧急负反馈' as alert_category,
      FORMAT('周D30 ROAS(%.2f%%)低于目标(%.2f%%)', SAFE_DIVIDE(SUM(b.revenue30d), SUM(b.cost)) * 100, th.th * 100) AS comparison_data
    FROM campaign_daily_metrics_base AS b
    JOIN thresholds th ON b.business_unit = th.bu
    CROSS JOIN target_week_cte AS t
    WHERE DATE_TRUNC(b.date, WEEK(MONDAY)) = t.target_week_start_date
    GROUP BY
    	b.business_unit,
    	t.target_week_start_date,
    	th.th
    HAVING
      EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 2 -- 仅在周一运行
      AND SAFE_DIVIDE(SUM(b.revenue30d), SUM(b.cost)) < th.th
     )

SELECT * FROM rule1_alerts
UNION ALL
SELECT * FROM rule2_alerts
where comparison_data is not null
UNION ALL
SELECT * FROM rule3_alerts
UNION ALL
SELECT * FROM rule4_alerts
UNION ALL
SELECT * FROM rule5_alerts
UNION ALL
SELECT * FROM rule6_alerts