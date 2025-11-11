from helper import run_stage_to_dw

table_name = "daily_customer_summary"

sql_update = """
    UPDATE {dw_schema}.{table_name}
    SET dw_update_timestamp = dw_update_timestamp
    WHERE 1 = 0;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name}
    (
      summary_date,
      dw_customer_id,
      order_count,
      order_apd,
      order_cost_amount,
      cancelled_order_count,
      cancelled_order_amount,
      cancelled_order_apd,
      shipped_order_count,
      shipped_order_apd,
      shipped_order_amount,
      payment_apd,
      payment_amount,
      products_ordered_qty,
      products_items_qty,
      order_mrp_amount,
      new_customer_apd,
      new_customer_paid_apd,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    WITH params AS (
      SELECT %s::integer AS etl_no, %s::date AS etl_dt
    ),
    cte AS (
      SELECT 
        CAST(c.src_create_timestamp AS date) AS summary_date,
        c.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_cost_amount,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        0 AS order_mrp_amount,
        1 AS new_customer_apd,
        0 AS new_customer_paid_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        p.etl_no  AS etl_batch_no,
        p.etl_dt  AS etl_batch_date
      FROM {dw_schema}.customers c
      JOIN params p ON 1=1
      WHERE CAST(c.src_create_timestamp AS date) >= p.etl_dt
      GROUP BY CAST(c.src_create_timestamp AS date), c.dw_customer_id, p.etl_no, p.etl_dt

      UNION ALL

      SELECT
        CAST(o.orderdate AS date) AS summary_date,
        o.dw_customer_id,
        COUNT(DISTINCT o.dw_order_id) AS order_count,
        1 AS order_apd,
        SUM(od.quantityordered * od.priceeach) AS order_cost_amount,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        SUM(od.quantityordered) AS products_ordered_qty,
        COUNT(DISTINCT od.dw_product_id) AS products_items_qty,
        SUM(od.quantityordered * pdt.msrp) AS order_mrp_amount,
        0 AS new_customer_apd,
        0 AS new_customer_paid_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        p.etl_no  AS etl_batch_no,
        p.etl_dt  AS etl_batch_date
      FROM {dw_schema}.orders o
      JOIN {dw_schema}.orderdetails od ON o.dw_order_id = od.dw_order_id
      JOIN {dw_schema}.products     pdt ON pdt.dw_product_id = od.dw_product_id
      JOIN params p ON 1=1
      WHERE CAST(o.orderdate AS date) >= p.etl_dt
      GROUP BY CAST(o.orderdate AS date), o.dw_customer_id, p.etl_no, p.etl_dt

      UNION ALL

      SELECT
        CAST(o.cancelleddate AS date) AS summary_date,
        o.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_cost_amount,
        COUNT(DISTINCT o.dw_order_id) AS cancelled_order_count,
        SUM(od.quantityordered * od.priceeach) AS cancelled_order_amount,
        1 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        0 AS order_mrp_amount,
        0 AS new_customer_apd,
        0 AS new_customer_paid_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        p.etl_no  AS etl_batch_no,
        p.etl_dt  AS etl_batch_date
      FROM {dw_schema}.orders o
      JOIN {dw_schema}.orderdetails od ON o.dw_order_id = od.dw_order_id
      JOIN params p ON 1=1
      WHERE CAST(o.cancelleddate AS date) >= p.etl_dt
        AND o.cancelleddate IS NOT NULL
        AND LOWER(TRIM(o.status)) = 'cancelled'
      GROUP BY CAST(o.cancelleddate AS date), o.dw_customer_id, p.etl_no, p.etl_dt

      UNION ALL

      SELECT
        CAST(o.shippeddate AS date) AS summary_date,
        o.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_cost_amount,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        COUNT(DISTINCT o.dw_order_id) AS shipped_order_count,
        SUM(od.quantityordered * od.priceeach) AS shipped_order_amount,
        1 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        0 AS order_mrp_amount,
        0 AS new_customer_apd,
        0 AS new_customer_paid_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        p.etl_no  AS etl_batch_no,
        p.etl_dt  AS etl_batch_date
      FROM {dw_schema}.orders o
      JOIN {dw_schema}.orderdetails od ON o.dw_order_id = od.dw_order_id
      JOIN params p ON 1=1
      WHERE CAST(o.shippeddate AS date) >= p.etl_dt
        AND o.shippeddate IS NOT NULL
        AND LOWER(TRIM(o.status)) = 'shipped'
      GROUP BY CAST(o.shippeddate AS date), o.dw_customer_id, p.etl_no, p.etl_dt

      UNION ALL

      SELECT
        CAST(pmt.paymentdate AS date) AS summary_date,
        pmt.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_cost_amount,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        1 AS payment_apd,
        SUM(pmt.amount) AS payment_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        0 AS order_mrp_amount,
        0 AS new_customer_apd,
        MAX(CASE WHEN CAST(pmt.paymentdate AS date) = first_pay.minimumpaymentdate THEN 1 ELSE 0 END) AS new_customer_paid_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        p.etl_no  AS etl_batch_no,
        p.etl_dt  AS etl_batch_date
      FROM {dw_schema}.payments pmt
      JOIN (
        SELECT dw_customer_id, MIN(CAST(paymentdate AS date)) AS minimumpaymentdate
        FROM {dw_schema}.payments
        WHERE paymentdate IS NOT NULL
        GROUP BY dw_customer_id
      ) first_pay ON pmt.dw_customer_id = first_pay.dw_customer_id
      JOIN params p ON 1=1
      WHERE CAST(pmt.paymentdate AS date) >= p.etl_dt
        AND pmt.paymentdate IS NOT NULL
      GROUP BY CAST(pmt.paymentdate AS date), pmt.dw_customer_id, p.etl_no, p.etl_dt
    )
    SELECT 
      summary_date,
      dw_customer_id,
      SUM(order_count)              AS order_count,
      MAX(order_apd)                AS order_apd,
      SUM(order_cost_amount)        AS order_cost_amount,
      SUM(cancelled_order_count)    AS cancelled_order_count,
      SUM(cancelled_order_amount)   AS cancelled_order_amount,
      MAX(cancelled_order_apd)      AS cancelled_order_apd,
      SUM(shipped_order_count)      AS shipped_order_count,
      MAX(shipped_order_apd)        AS shipped_order_apd,
      SUM(shipped_order_amount)     AS shipped_order_amount,
      MAX(payment_apd)              AS payment_apd,
      SUM(payment_amount)           AS payment_amount,
      SUM(products_ordered_qty)     AS products_ordered_qty,
      SUM(products_items_qty)       AS products_items_qty,
      SUM(order_mrp_amount)         AS order_mrp_amount,
      MAX(new_customer_apd)         AS new_customer_apd,
      MAX(new_customer_paid_apd)    AS new_customer_paid_apd,
      MAX(dw_create_timestamp)      AS dw_create_timestamp,
      MAX(dw_update_timestamp)      AS dw_update_timestamp,
      p.etl_no                      AS etl_batch_no,
      p.etl_dt                      AS etl_batch_date
    FROM cte
    JOIN params p ON 1=1
    GROUP BY summary_date, dw_customer_id, p.etl_no, p.etl_dt
    ORDER BY summary_date, dw_customer_id;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=(),
        insert_param_order=("no", "date")
    )
