# products.py
from helper import run_stage_to_dw

table_name = "products"

sql_update = """
    UPDATE {dw_schema}.{table_name} d
    SET
      productname           = s.productname,
      productline           = s.productline,
      productscale          = s.productscale,
      productvendor         = s.productvendor,
      productdescription    = s.productdescription,
      quantityinstock       = s.quantityinstock,
      buyprice              = s.buyprice,
      msrp                  = s.msrp,
      src_update_timestamp  = s.update_timestamp,
      dw_update_timestamp   = CURRENT_TIMESTAMP,
      etl_batch_no          = %s,
      etl_batch_date        = %s
    FROM {stage_schema}.{table_name} s
    WHERE d.src_productcode = s.productcode;
"""

sql_insert = """
    INSERT INTO {dw_schema}.{table_name} (
      src_productcode,
      productname,
      productline,
      productscale,
      productvendor,
      productdescription,
      quantityinstock,
      buyprice,
      msrp,
      dw_product_line_id,
      src_create_timestamp,
      src_update_timestamp,
      dw_create_timestamp,
      dw_update_timestamp,
      etl_batch_no,
      etl_batch_date
    )
    SELECT
      s.productcode,
      s.productname,
      s.productline,
      s.productscale,
      s.productvendor,
      s.productdescription,
      s.quantityinstock,
      s.buyprice,
      s.msrp,
      p.dw_product_line_id,
      s.create_timestamp,
      s.update_timestamp,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      %s,
      %s
    FROM {stage_schema}.{table_name} s
    LEFT JOIN {dw_schema}.productlines p
      ON s.productline = p.productline
    LEFT JOIN {dw_schema}.{table_name} d
      ON s.productcode = d.src_productcode
    WHERE d.src_productcode IS NULL
      AND p.dw_product_line_id IS NOT NULL;
"""

if __name__ == "__main__":
    run_stage_to_dw(
        sql_update,
        sql_insert,
        table_name,
        update_param_order=("no", "date"),
        insert_param_order=("no", "date"),
    )
