import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

rs_host = os.getenv("REDSHIFT_HOST")
rs_port = os.getenv("REDSHIFT_PORT")
rs_db   = os.getenv("REDSHIFT_DB")
rs_user = os.getenv("REDSHIFT_USER")
rs_pass = os.getenv("REDSHIFT_PASSWORD")
rs_schema = os.getenv("REDSHIFT_METADATA_SCHEMA")

conn = psycopg2.connect(
    host=rs_host, port=rs_port, dbname=rs_db,
    user=rs_user, password=rs_pass
)
cur = conn.cursor()

cur.execute(f"""
select etl_batch_no, etl_batch_date 
from {rs_schema}.batch_control
order by etl_batch_no desc
limit 1
""")

row = cur.fetchone()
etl_batch_no = row[0]
etl_batch_date = row[1]

cur.execute(f"""
insert into {rs_schema}.batch_control_log(
    etl_batch_no,
    etl_batch_date,
    etl_batch_status,
    etl_batch_start_time,
    etl_batch_end_time
)
values (
    {etl_batch_no},
    '{etl_batch_date}',
    'R',
    current_timestamp,
    null
)
""")

conn.commit()
cur.close()
conn.close()

print("start log inserted")
