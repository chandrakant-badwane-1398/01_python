# orderdetails.py
from helper import run_stage_to_dw

table_name = "orderdetails"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      quantityordered       = s.quantityordered,
      priceeach             = s.priceeach,
      orderlinenumber       = s.orderlinenumber,
      src_update_timestamp  = s.update_timestamp,
      dw_update_timestamp   = CURRENT_TIMESTAMP,
      etl_batch_no          = %s,
      etl_batch_date        = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.src_ordernumber = s.ordernumber
      AND d.src_productcode = s.productcode;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      dw_order_id,
      dw_product_id,
      src_ordernumber,
      src_productcode,
      quantityordered,
      priceeach,
      orderlinenumber,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      o.dw_order_id,
      p.dw_product_id,
      s.ordernumber,
      s.productcode,
      s.quantityordered,
      s.priceeach,
      s.orderlinenumber,
      s.create_timestamp,
      s.update_timestamp,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      %s,
      %s
    FROM {stage_schema}.{table_name} s
    LEFT JOIN {dw_schema}.orders   o ON s.ordernumber = o.src_ordernumber
    LEFT JOIN {dw_schema}.products p ON s.productcode = p.src_productcode
    LEFT JOIN {dw_schema}.{table_name} d
           ON s.ordernumber = d.src_ordernumber
          AND s.productcode = d.src_productcode
    WHERE d.src_ordernumber IS NULL
      AND d.src_productcode IS NULL
      AND o.dw_order_id IS NOT NULL
      AND p.dw_product_id IS NOT NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no", "date"),
        insert_param_order=("no", "date"),
    )
