# orders.py
from helper import run_stage_to_dw

table_name = "orders"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      orderdate             = s.orderdate,
      requireddate          = s.requireddate,
      shippeddate           = s.shippeddate,
      status                = s.status,
      comments              = s.comments,
      src_customernumber    = s.customernumber,
      cancelleddate         = s.cancelleddate,
      src_update_timestamp  = s.update_timestamp,
      dw_update_timestamp   = CURRENT_TIMESTAMP,
      etl_batch_no          = %s,
      etl_batch_date        = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.src_ordernumber = s.ordernumber;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      dw_customer_id,
      src_ordernumber,
      orderdate,
      requireddate,
      shippeddate,
      status,
      comments,
      src_customernumber,
      cancelleddate,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      c.dw_customer_id,
      s.ordernumber,
      s.orderdate,
      s.requireddate,
      s.shippeddate,
      s.status,
      s.comments,
      s.customernumber,
      s.cancelleddate,
      s.create_timestamp,
      s.update_timestamp,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      %s,
      %s
    FROM {stage_schema}.{table_name} s
    LEFT JOIN {dw_schema}.customers c
      ON s.customernumber = c.src_customernumber
    LEFT JOIN {dw_schema}.{table_name} d
      ON s.ordernumber = d.src_ordernumber
    WHERE d.src_ordernumber IS NULL
      AND c.dw_customer_id IS NOT NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no", "date"),      # UPDATE has 2 %s
        insert_param_order=("no", "date")  # INSERT has 3 %s
    )
