from helper import run_stage_to_dw

table_name = "monthly_product_summary"

sql_update = """
    UPDATE {dw_schema}.{table_name} mp
    SET 
      customer_apd            = CASE WHEN mp.customer_apd = 1 THEN 1 ELSE dp.customer_apd END,
      customer_apm            = CASE WHEN mp.customer_apm = 1 THEN 1 ELSE dp.customer_apd END,

      product_cost_amount     = mp.product_cost_amount + dp.product_cost_amount,
      product_mrp_amount      = mp.product_mrp_amount  + dp.product_mrp_amount,

      cancelled_product_qty   = mp.cancelled_product_qty + dp.cancelled_product_qty,
      cancelled_cost_amount   = mp.cancelled_cost_amount + dp.cancelled_cost_amount,
      cancelled_mrp_amount    = mp.cancelled_mrp_amount  + dp.cancelled_mrp_amount,

      cancelled_order_apd     = CASE WHEN mp.cancelled_order_apd = 1 THEN 1 ELSE dp.cancelled_order_apd END,
      cancelled_order_apm     = CASE WHEN mp.cancelled_order_apm = 1 THEN 1 ELSE dp.cancelled_order_apd END,

      dw_update_timestamp     = CURRENT_TIMESTAMP,
      etl_batch_no            = dp.etl_batch_no,
      etl_batch_date          = dp.etl_batch_date
    FROM {dw_schema}.daily_product_summary dp
    WHERE mp.start_of_the_month_date = DATE_TRUNC('month', dp.summary_date)::date
      AND mp.dw_product_id           = dp.dw_product_id
      AND dp.summary_date            >= %s::date;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name}
    (
      start_of_the_month_date,
      dw_product_id,
      customer_apd,
      customer_apm,
      product_cost_amount,
      product_mrp_amount,
      cancelled_product_qty,
      cancelled_cost_amount,
      cancelled_mrp_amount,
      cancelled_order_apd,
      cancelled_order_apm,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT 
      DATE_TRUNC('month', dp.summary_date)::date AS start_of_the_month_date,
      dp.dw_product_id,
      MAX(dp.customer_apd)              AS customer_apd,
      MAX(dp.customer_apd)              AS customer_apm,
      SUM(dp.product_cost_amount)       AS product_cost_amount,
      SUM(dp.product_mrp_amount)        AS product_mrp_amount,
      SUM(dp.cancelled_product_qty)     AS cancelled_product_qty,
      SUM(dp.cancelled_cost_amount)     AS cancelled_cost_amount,
      SUM(dp.cancelled_mrp_amount)      AS cancelled_mrp_amount,
      MAX(dp.cancelled_order_apd)       AS cancelled_order_apd,
      MAX(dp.cancelled_order_apd)       AS cancelled_order_apm,
      CURRENT_TIMESTAMP                 AS dw_create_timestamp,
      CURRENT_TIMESTAMP                 AS dw_update_timestamp,
      MAX(dp.etl_batch_no)              AS etl_batch_no,
      MAX(dp.etl_batch_date)            AS etl_batch_date
    FROM {dw_schema}.daily_product_summary dp
    LEFT JOIN {dw_schema}.{table_name} mp
      ON DATE_TRUNC('month', dp.summary_date)::date = mp.start_of_the_month_date
     AND dp.dw_product_id = mp.dw_product_id
    WHERE mp.start_of_the_month_date IS NULL
      AND dp.summary_date >= %s::date
    GROUP BY DATE_TRUNC('month', dp.summary_date)::date, dp.dw_product_id;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("date",),  
        insert_param_order=("date",),  
    )
