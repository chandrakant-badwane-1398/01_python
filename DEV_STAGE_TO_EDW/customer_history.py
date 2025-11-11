from helper import run_stage_to_dw

table_name = "customer_history"

sql_update = """
    UPDATE {dw_schema}.{table_name} c
    SET 
      dw_active_record_ind   = 0,
      update_etl_batch_no    = %s,
      effective_to_date      = DATEADD('day', -1, %s),
      update_etl_batch_date  = %s,
      dw_update_timestamp    = CURRENT_TIMESTAMP
    FROM (
        SELECT a.dw_customer_id
        FROM {dw_schema}.customers a
        JOIN {dw_schema}.{table_name} b 
          ON a.dw_customer_id = b.dw_customer_id
        WHERE b.dw_active_record_ind = 1
          AND a.creditlimit IS DISTINCT FROM b.creditlimit
    ) d
    WHERE c.dw_customer_id = d.dw_customer_id
      AND c.dw_active_record_ind = 1;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      dw_customer_id,
      creditlimit,
      effective_from_date,
      dw_active_record_ind,
      dw_create_timestamp,
      dw_update_timestamp,
      create_etl_batch_no,
      create_etl_batch_date
    )
    SELECT 
      a.dw_customer_id,
      a.creditlimit,
      %s AS effective_from_date,
      1  AS dw_active_record_ind,
      CURRENT_TIMESTAMP AS dw_create_timestamp,
      CURRENT_TIMESTAMP AS dw_update_timestamp,
      %s AS create_etl_batch_no,
      %s AS create_etl_batch_date
    FROM {dw_schema}.customers a
    LEFT JOIN (
      SELECT dw_customer_id
      FROM {dw_schema}.{table_name}
      WHERE dw_active_record_ind = 1
    ) b 
      ON a.dw_customer_id = b.dw_customer_id
    WHERE b.dw_customer_id IS NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no","date","date"),  
        insert_param_order=("date","no","date")   
    )
