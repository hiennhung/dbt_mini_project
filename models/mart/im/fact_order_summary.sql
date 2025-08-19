-- This file defines the fact table for order summaries with incremental logic.
 
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy = 'merge'

) }}
 
WITH order_data AS (
    SELECT
        o.ORDER_ID,
        o.CUSTOMER_ID,
        o.ORDER_DATE,
        o.ORDER_STATUS,
        o.TOTAL_AMOUNT AS ORDER_TOTAL_AMOUNT,
        COUNT(od.ORDER_DETAIL_ID) AS TOTAL_ITEMS_COUNT,
        COUNT(DISTINCT od.PRODUCT_ID) AS TOTAL_PRODUCT_TYPES,
        AVG(od.UNIT_PRICE) AS AVERAGE_UNIT_PRICE,
        SUM(od.UNIT_PRICE * od.QUANTITY * od.DISCOUNT_PERCENT / 100) AS TOTAL_DISCOUNT_AMOUNT,
        AVG(od.DISCOUNT_PERCENT) AS TOTAL_DISCOUNT_PERCENT,
        CURRENT_TIMESTAMP() AS CREATED_ON,
        CURRENT_TIMESTAMP() AS UPDATED_ON
    FROM
        {{ ref('stg_order') }} o
    LEFT JOIN
        {{ ref('stg_order_detail') }} od ON o.ORDER_ID = od.ORDER_ID
    WHERE
        o.IS_DELETED = FALSE
    GROUP BY
        o.ORDER_ID, o.CUSTOMER_ID, o.ORDER_DATE, o.ORDER_STATUS, o.TOTAL_AMOUNT
)
 
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_STATUS,
    ORDER_TOTAL_AMOUNT,
    TOTAL_ITEMS_COUNT,
    TOTAL_PRODUCT_TYPES,
    AVERAGE_UNIT_PRICE,
    TOTAL_DISCOUNT_AMOUNT,
    TOTAL_DISCOUNT_PERCENT,
    CREATED_ON,
    UPDATED_ON
FROM
    order_data
 
{% if is_incremental() %}
WHERE
    ORDER_ID NOT IN (SELECT ORDER_ID FROM {{ this }})
{% endif %}