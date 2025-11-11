import os
import psycopg2
from dotenv import load_dotenv

def run_stage_to_dw(sql_update, sql_insert, table_name, 
                    update_param_order=("no","date","date"),
                    insert_param_order=("no","date","date")):
    load_dotenv()

    host     = os.getenv("REDSHIFT_HOST")
    port     = os.getenv("REDSHIFT_PORT")
    database = os.getenv("REDSHIFT_DB")
    user     = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")

    metadata_schema = os.getenv("REDSHIFT_METADATA_SCHEMA")
    stage_schema    = os.getenv("REDSHIFT_STAGE_SCHEMA", "j25chandrakant_devstage")
    dw_schema       = os.getenv("REDSHIFT_DW_SCHEMA", "j25chandrakant_devdw")

    if not metadata_schema:
        raise SystemExit("REDSHIFT_METADATA_SCHEMA is not set")

    with psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT etl_batch_no, etl_batch_date FROM {metadata_schema}.batch_control LIMIT 1;")
            row = cur.fetchone()
            if not row:
                raise SystemExit("No row found in batch_control")
            etl_batch_no, etl_batch_date = row[0], str(row[1])

    print(f"[{table_name.upper()}] Batch Info : no={etl_batch_no}, date={etl_batch_date}")

    sql_update = sql_update.format(stage_schema=stage_schema, dw_schema=dw_schema, table_name=table_name)
    sql_insert = sql_insert.format(stage_schema=stage_schema, dw_schema=dw_schema, table_name=table_name)

    def build_params(order_tuple):
        mapping = {"no": etl_batch_no, "date": etl_batch_date}
        return [mapping[k] for k in order_tuple]

    update_params = build_params(update_param_order)
    insert_params = build_params(insert_param_order)

    with psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(sql_update, update_params)
                updated = cur.rowcount

                cur.execute(sql_insert, insert_params)
                inserted = cur.rowcount

            conn.commit()
            print(f"[{table_name.upper()}] Upsert done : Updated={updated}, Inserted={inserted}")
        except Exception as e:
            conn.rollback()
            raise SystemExit(f"[{table_name.upper()}] Upsert failed: {e}")
