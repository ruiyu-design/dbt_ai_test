/*
 * @field status
 * @description Order Status
 * @type integer
 * @value 4 Payment successful
 * @value 13 Upgrade Orders
 * @value 10 Refund
 */

WITH source_data AS (
    SELECT 
        order_sn, 
        origin_order_sn,
        uid, 
        goods_id, 
        pay_channel, 
        status, 
        is_trial, 
        pay_amount, 
        pay_currency, 
        create_time, 
        entry_time,
        workspace_id, 
        seats_size,
        failure_time
    FROM {{ source('Aurora', 'payment_center_order_table') }}
    WHERE 
        uid IS NOT NULL AND
        order_sn IS NOT NULL AND
        origin_order_sn IS NOT NULL AND
        goods_id IS NOT NULL AND
        pay_channel IN (6, 7, 8) AND
        is_trial IN (0, 1) AND
        status IN (4, 10, 13) AND
        create_time > UNIX_SECONDS(TIMESTAMP('2021-01-02'))
),

transformed_data AS (
    SELECT 
        uid,
        goods_id,
        workspace_id,
        CASE
            WHEN origin_order_sn is NULL OR origin_order_sn = "" THEN order_sn
            ELSE origin_order_sn
        END AS subscription_id,
        is_trial,
        order_sn AS invoice_id,
        status,
        
        -- [修改点] 将支付渠道 ID 转换为可读名称
        CASE 
            WHEN pay_channel = 6 THEN 'Apple Pay'
            WHEN pay_channel = 7 THEN 'Google Pay'
            WHEN pay_channel = 8 THEN 'Stripe'
            ELSE 'Unknown'
        END AS channel,

        pay_amount AS amount,
        pay_currency AS currency,
        seats_size,
        TIMESTAMP_SECONDS(create_time) AS created_at,
        TIMESTAMP_SECONDS(entry_time) AS period_start,
        TIMESTAMP_SECONDS(failure_time) AS period_end
    FROM source_data
)

SELECT
    uid,
    goods_id,
    workspace_id,
    subscription_id,
    is_trial,
    invoice_id,
    status,
    channel,
    amount,
    currency,
    seats_size,
    created_at,
    period_start,
    period_end
FROM
    transformed_data
