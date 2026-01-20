/*
 * @field currency
 * @description Currency type
 * @type integer
 * @value 1 Currency is JPY
 * @value 2 Non-JPY
 */

WITH invoices AS (
    SELECT
        inv.uid,
        inv.status,
        inv.subscription_id,
        inv.invoice_id,
        g.plan_type AS plan,
        inv.channel,
        inv.amount / er.rate / 100 AS amount,
        inv.created_at,
        CASE
            WHEN UPPER(inv.currency) = 'JPY' THEN 1
            ELSE 99
        END AS currency
    FROM {{ ref('stg_aurora_invoice') }} inv
    LEFT JOIN {{ source('Summary', 'exchange_rates') }} er ON UPPER(inv.currency) = er.currency
    LEFT JOIN {{ ref('stg_aurora_goods') }} g ON inv.goods_id = g.goods_id
)

SELECT * FROM invoices
