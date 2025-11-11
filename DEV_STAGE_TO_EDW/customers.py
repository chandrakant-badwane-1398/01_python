from helper import run_stage_to_dw

table_name = "customers"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      customername             = s.customername,
      contactlastname          = s.contactlastname,
      contactfirstname         = s.contactfirstname,
      phone                    = s.phone,
      addressline1             = s.addressline1,
      addressline2             = s.addressline2,
      city                     = s.city,
      state                    = s.state,
      postalcode               = s.postalcode,
      country                  = s.country,
      salesrepemployeenumber   = s.salesrepemployeenumber,
      creditlimit              = s.creditlimit,
      src_update_timestamp     = s.update_timestamp,
      dw_update_timestamp      = CURRENT_TIMESTAMP,
      etl_batch_no             = %s,
      etl_batch_date           = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.src_customernumber = s.customernumber;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      src_customernumber,
      customername,
      contactlastname,
      contactfirstname,
      phone,
      addressline1,
      addressline2,
      city,
      state,
      postalcode,
      country,
      salesrepemployeenumber,
      creditlimit,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      s.customernumber,
      s.customername,
      s.contactlastname,
      s.contactfirstname,
      s.phone,
      s.addressline1,
      s.addressline2,
      s.city,
      s.state,
      s.postalcode,
      s.country,
      s.salesrepemployeenumber,
      s.creditlimit,
      s.create_timestamp,
      s.update_timestamp,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      %s,
      %s
    FROM {stage_schema}.{table_name} s
    LEFT JOIN {dw_schema}.{table_name} d
      ON s.customernumber = d.src_customernumber
    WHERE d.src_customernumber IS NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no","date"),   
        insert_param_order=("no","date")  
    )

