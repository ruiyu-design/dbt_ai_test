/*
 * @field plan_type
 * @description Plan type
 * @type integer
 * @value 0 Free
 * @value 1 Pro Monthly
 * @value 2 Pro Annual
 * @value 3 Bussiness Monthly
 * @value 4 Bussiness Annual
 * @value 5 Enterprise Monthly
 * @value 6 Enterprise Annual
 * @value 99 Other
 *
 * @field data_learning_disabled
 * @description Whether to switch on AI learning
 * @type integer
 * @value 0 Enabled
 * @value 1 Disabled
 */
WITH goods AS (
    SELECT
        goods_id,
        CASE
            WHEN plan_type = 0 THEN 0
            WHEN plan_type = 1 AND type IN (1, 3, 4, 7) AND period_count = 1 AND period_unit = 2 THEN 1
            WHEN plan_type = 1 AND type IN (1, 3, 4, 7) AND (( period_count = 1 AND period_unit = 1 ) OR ( period_count = 12 AND period_unit = 2 )) THEN 2
            WHEN plan_type = 2 AND type IN (1, 3, 4, 7) AND period_count = 1 AND period_unit = 2 THEN 3
            WHEN plan_type = 2 AND type IN (1, 3, 4, 7) AND (( period_count = 1 AND period_unit = 1 ) OR ( period_count = 12 AND period_unit = 2 )) THEN 4
            WHEN plan_type = 3 AND type IN (1, 3, 4, 7) AND period_count = 1 AND period_unit = 1 THEN 5
            WHEN plan_type = 3 AND type IN (1, 3, 4, 7) AND (( period_count = 1 AND period_unit = 2 ) OR ( period_count = 12 AND period_unit = 2 )) THEN 6
            ELSE 99
        END AS plan_type,
        CASE
            WHEN JSON_EXTRACT(interest_json, '$.notta_interest_no_data_learning') IS NOT NULL THEN 1
            ELSE 0
        END AS data_learning_disabled
    FROM
        {{ source('Aurora', 'notta_mall_interest_goods') }}
)

SELECT
    goods_id,
    plan_type,
    data_learning_disabled
FROM
    goods
WHERE
    goods_id IS NOT NULL