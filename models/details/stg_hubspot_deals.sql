/*
 * @field sales_channel_code
 * @description Sales channels
 * @type integer
 * @value 0 Direct sales
 * @value 1 Agent
 */

WITH hubspot_deals AS (
    SELECT
        createdAt AS created_at,
        CASE
            WHEN INSTR(properties_notta_ws_id, ',') > 0 THEN SUBSTRING(properties_notta_ws_id, 0 , INSTR(properties_notta_ws_id, ',') - 1)
            WHEN LENGTH(properties_notta_ws_id) = 0 THEN NULL
            ELSE properties_notta_ws_id
        END AS workspace_id,
        properties_dealname AS workspace_name,
        CASE
            WHEN LOWER(properties_pipeline) = 'default' THEN 0
            ELSE 1
        END AS sales_channel_code,
        properties_amount AS amount,
        properties_deal_currency_code AS currency_code,
        LOWER(CAST(properties_dealstage AS STRING)) AS properties_dealstage,
        properties_paidusers
    FROM
        {{ source('Hubspot', 'deals') }}
),

/*
 * @field plan_type
 * @description Subscribe type
 * @type integer
 * @value 1 Direct sales + contractsent = Enterprise annual payment paid
 * @value 2 Direct sales + 149849408 = Business annual paid
 * @value 3 Direct sales + appointmentscheduled = Business monthly payment paid
 * @value 4 Agent + 145257875 = Enterprise annual payment paid
 * @value 99 Other
 */

deal_stage AS (
    SELECT
        created_at,
        CAST(workspace_id AS INT64) AS workspace_id,
        workspace_name,
        sales_channel_code,
        amount,
        currency_code,
        CASE
            WHEN sales_channel_code = 1 THEN
                CASE
                    WHEN properties_dealstage = '145257875' THEN 4
                    ELSE 99
                END
            WHEN sales_channel_code = 0 THEN
                CASE
                    WHEN properties_dealstage = 'contractsent' THEN 1
                    WHEN properties_dealstage = '149849408' THEN 2
                    WHEN properties_dealstage = 'appointmentscheduled' THEN 3
                    ELSE 99
                END
            ELSE NULL
        END AS plan_type,
        properties_paidusers AS seats_count
    FROM
        hubspot_deals
    WHERE
        properties_dealstage IN ('145257875', 'contractsent', '149849408', 'appointmentscheduled')
)

SELECT
    created_at,
    workspace_id,
    workspace_name,
    sales_channel_code,
    amount,
    currency_code,
    plan_type,
    seats_count
FROM
    deal_stage
WHERE
    workspace_id IS NOT NULL