# end_log.py
import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

rs_host = os.getenv("REDSHIFT_HOST")
rs_port = os.getenv("REDSHIFT_PORT")
rs_db   = os.getenv("REDSHIFT_DB")
rs_user = os.getenv("REDSHIFT_USER")
rs_pass = os.getenv("REDSHIFT_PASSWORD")
rs_schema = os.getenv("REDSHIFT_METADATA_SCHEMA")

status = sys.argv[1] if len(sys.argv) > 1 else "S"   # default S

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
etl_batch_no, etl_batch_date = row[0], row[1]

cur.execute(f"""
update {rs_schema}.batch_control_log
set etl_batch_status = '{status}',
    etl_batch_end_time = current_timestamp
where etl_batch_no = {etl_batch_no}
  and etl_batch_date = '{etl_batch_date}'
""")

conn.commit()
cur.close()
conn.close()

print(f"end log updated with status={status}")
