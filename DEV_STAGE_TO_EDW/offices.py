# offices.py
from helper import run_stage_to_dw

table_name = "offices"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      city                 = s.city,
      phone                = s.phone,
      addressline1         = s.addressline1,
      addressline2         = s.addressline2,
      state                = s.state,
      country              = s.country,
      postalcode           = s.postalcode,
      territory            = s.territory,
      src_update_timestamp = s.update_timestamp,
      dw_update_timestamp  = CURRENT_TIMESTAMP,
      etl_batch_no         = %s,
      etl_batch_date       = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.officecode = s.officecode;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      officecode,
      city,
      phone,
      addressline1,
      addressline2,
      state,
      country,
      postalcode,
      territory,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      s.officecode,
      s.city,
      s.phone,
      s.addressline1,
      s.addressline2,
      s.state,
      s.country,
      s.postalcode,
      s.territory,
      s.create_timestamp,
      s.update_timestamp,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      %s,
      %s
    FROM {stage_schema}.{table_name} s
    LEFT JOIN {dw_schema}.{table_name} d
      ON s.officecode = d.officecode
    WHERE d.officecode IS NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no", "date"),
        insert_param_order=("no", "date")
    )
