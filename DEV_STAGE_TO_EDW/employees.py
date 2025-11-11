from helper import run_stage_to_dw

table_name = "employees"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      lastname              = s.lastname,
      firstname             = s.firstname,
      extension             = s.extension,
      email                 = s.email,
      officecode            = s.officecode,
      reportsto             = s.reportsto,
      jobtitle              = s.jobtitle,
      src_update_timestamp  = s.update_timestamp,
      dw_update_timestamp   = CURRENT_TIMESTAMP,
      etl_batch_no          = %s,
      etl_batch_date        = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.employeenumber = s.employeenumber;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      employeenumber,
      lastname,
      firstname,
      extension,
      email,
      officecode,
      reportsto,
      jobtitle,
      dw_office_id,
      dw_reporting_employee_id,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      s.employeenumber,
      s.lastname,
      s.firstname,
      s.extension,
      s.email,
      s.officecode,
      s.reportsto,
      s.jobtitle,
      o.dw_office_id,
      r.dw_employee_id AS dw_reporting_employee_id,
      s.create_timestamp,
      s.update_timestamp,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      %s,
      %s
    FROM {stage_schema}.{table_name} s
    LEFT JOIN {dw_schema}.offices o
      ON s.officecode = o.officecode
    LEFT JOIN {dw_schema}.{table_name} d
      ON s.employeenumber = d.employeenumber
    LEFT JOIN {dw_schema}.{table_name} r
      ON s.reportsto = r.employeenumber
    WHERE d.employeenumber IS NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no", "date"),
        insert_param_order=("no", "date")
    )
