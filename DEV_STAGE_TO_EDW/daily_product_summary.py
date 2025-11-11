from helper import run_stage_to_dw

table_name = "daily_product_summary"

sql_update = f"""
    UPDATE {{dw_schema}}.{table_name}
    SET dw_update_timestamp = dw_update_timestamp
    WHERE 1 = 0;
"""

sql_insert = f"""
    INSERT INTO {{dw_schema}}.{table_name}
    (
      summary_date,
      dw_product_id,
      customer_apd,
      product_cost_amount,
      product_mrp_amount,
      cancelled_product_qty,
      cancelled_cost_amount,
      cancelled_mrp_amount,
      cancelled_order_apd,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    WITH params AS (
      SELECT %s::integer AS etl_no, %s::date AS etl_dt
    ),
    cte AS (
      -- Ordered Products Summary
      SELECT
        CAST(o.orderdate AS date) AS summary_date,
        p.dw_product_id,
        1 AS customer_apd,
        SUM(od.quantityordered * od.priceeach) AS product_cost_amount,
        SUM(od.quantityordered * p.msrp) AS product_mrp_amount,
        0 AS cancelled_product_qty,
        0 AS cancelled_cost_amount,
        0 AS cancelled_mrp_amount,
        0 AS cancelled_order_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        par.etl_no AS etl_batch_no,
        par.etl_dt AS etl_batch_date
      FROM {{dw_schema}}.orders o
      JOIN {{dw_schema}}.orderdetails od ON o.dw_order_id = od.dw_order_id
      JOIN {{dw_schema}}.products p ON p.dw_product_id = od.dw_product_id
      JOIN params par ON 1=1
      WHERE CAST(o.orderdate AS date) >= par.etl_dt
      GROUP BY CAST(o.orderdate AS date), p.dw_product_id, par.etl_no, par.etl_dt

      UNION ALL

      -- Cancelled Products Summary
      SELECT
        CAST(o.cancelleddate AS date) AS summary_date,
        p.dw_product_id,
        1 AS customer_apd,
        0 AS product_cost_amount,
        0 AS product_mrp_amount,
        SUM(od.quantityordered) AS cancelled_product_qty,
        SUM(od.quantityordered * od.priceeach) AS cancelled_cost_amount,
        SUM(od.quantityordered * p.msrp) AS cancelled_mrp_amount,
        1 AS cancelled_order_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        par.etl_no AS etl_batch_no,
        par.etl_dt AS etl_batch_date
      FROM {{dw_schema}}.orders o
      JOIN {{dw_schema}}.orderdetails od ON o.dw_order_id = od.dw_order_id
      JOIN {{dw_schema}}.products p ON p.dw_product_id = od.dw_product_id
      JOIN params par ON 1=1
      WHERE o.cancelleddate IS NOT NULL
        AND CAST(o.cancelleddate AS date) >= par.etl_dt
      GROUP BY CAST(o.cancelleddate AS date), p.dw_product_id, par.etl_no, par.etl_dt
    )
    SELECT
      summary_date,
      dw_product_id,
      MAX(customer_apd) AS customer_apd,
      SUM(product_cost_amount) AS product_cost_amount,
      SUM(product_mrp_amount) AS product_mrp_amount,
      SUM(cancelled_product_qty) AS cancelled_product_qty,
      SUM(cancelled_cost_amount) AS cancelled_cost_amount,
      SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
      MAX(cancelled_order_apd) AS cancelled_order_apd,
      MAX(dw_create_timestamp) AS dw_create_timestamp,
      MAX(dw_update_timestamp) AS dw_update_timestamp,
      par.etl_no AS etl_batch_no,
      par.etl_dt AS etl_batch_date
    FROM cte
    JOIN params par ON 1=1
    GROUP BY summary_date, dw_product_id, par.etl_no, par.etl_dt
    ORDER BY summary_date, dw_product_id;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=(),              
        insert_param_order=("no", "date")   
    )
