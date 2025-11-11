# productlines.py
from helper import run_stage_to_dw

table_name = "productlines"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      textdescription       = s.textdescription,
      htmldescription       = s.htmldescription,
      image                 = s.image,
      src_update_timestamp  = s.update_timestamp,
      dw_update_timestamp   = CURRENT_TIMESTAMP,
      etl_batch_no          = %s,
      etl_batch_date        = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.productline = s.productline;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      productline,
      textdescription,
      htmldescription,
      image,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      s.productline,
      s.textdescription,
      s.htmldescription,
      s.image,
      s.create_timestamp,
      s.update_timestamp,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      %s,
      %s
    FROM {stage_schema}.{table_name} s
    LEFT JOIN {dw_schema}.{table_name} d
      ON s.productline = d.productline
    WHERE d.productline IS NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no", "date"),
        insert_param_order=("no", "date"),
    )
