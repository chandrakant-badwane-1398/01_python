# payments.py
from helper import run_stage_to_dw

table_name = "payments"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      paymentdate           = s.paymentdate,
      amount                = s.amount,
      src_update_timestamp  = s.update_timestamp,
      dw_update_timestamp   = CURRENT_TIMESTAMP,
      etl_batch_no          = %s,
      etl_batch_date        = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.src_customernumber = s.customernumber
      AND d.checknumber        = s.checknumber;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      dw_customer_id,
      src_customernumber,
      checknumber,
      paymentdate,
      amount,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      c.dw_customer_id,
      s.customernumber,
      s.checknumber,
      s.paymentdate,
      s.amount,
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
      ON s.customernumber = d.src_customernumber
     AND s.checknumber    = d.checknumber
    WHERE d.src_customernumber IS NULL
      AND d.checknumber    IS NULL
      AND c.dw_customer_id IS NOT NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no", "date"),
        insert_param_order=("no", "date"),
    )
