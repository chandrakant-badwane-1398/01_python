import os
import oracledb
import pandas as pd
import psycopg2
import boto3
from io import StringIO
from dotenv import load_dotenv

def export_products_to_s3():
    load_dotenv()

    # Oracle local
    ora_user = os.getenv("ORACLE_USER")
    ora_pass = os.getenv("ORACLE_PASSWORD")
    ora_dsn  = os.getenv("ORACLE_DSN")
    schema   = os.getenv("ORACLE_SCHEMA")

    # Redshift (to get etl_batch_date)
    rs_host = os.getenv("REDSHIFT_HOST")
    rs_port = os.getenv("REDSHIFT_PORT")
    rs_db   = os.getenv("REDSHIFT_DB")
    rs_user = os.getenv("REDSHIFT_USER")
    rs_pass = os.getenv("REDSHIFT_PASSWORD")
    rs_schema = os.getenv("REDSHIFT_SCHEMA")

    # S3
    bucket = os.getenv("S3_BUCKET_NAME")

    # dblink (remote)
    dblink_name    = os.getenv("DBLINK_NAME")
    dblink_user    = os.getenv("DBLINK_USER")
    dblink_pass    = os.getenv("DBLINK_PASSWORD")
    dblink_host    = os.getenv("DBLINK_HOST")
    dblink_port    = os.getenv("DBLINK_PORT")
    dblink_service = os.getenv("DBLINK_SERVICE")

    table_name = "PRODUCTS"
    columns = ("PRODUCTCODE,PRODUCTNAME,PRODUCTLINE,PRODUCTSCALE,PRODUCTVENDOR,"
               "PRODUCTDESCRIPTION,QUANTITYINSTOCK,BUYPRICE,MSRP")
    csv_file = "product.csv"

    # 1. get etl_batch_date from redshift
    etl_batch_date = None
    try:
        rs_conn = psycopg2.connect(host=rs_host, port=rs_port, dbname=rs_db,
                                   user=rs_user, password=rs_pass)
        rs_cur = rs_conn.cursor()
        rs_cur.execute(f"select etl_batch_date from {rs_schema}.batch_control limit 1")
        row = rs_cur.fetchone()
        if row:
            etl_batch_date = str(row[0])  # something like '2025-10-13'
        rs_cur.close()
        rs_conn.close()
    except Exception as e:
        raise SystemExit(f"Failed to read etl_batch_date from Redshift: {e}")

    # 2. connect to oracle and create db link
    con = oracledb.connect(user=ora_user, password=ora_pass, dsn=ora_dsn)
    cur = con.cursor()

    # set session schema (use ORACLE_SCHEMA exactly as provided)
    cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA={schema}")

    # create DBLINK (drop + create in same block)
    cur.execute(f"""
    BEGIN
      BEGIN EXECUTE IMMEDIATE 'DROP PUBLIC DATABASE LINK {dblink_name}';
      EXCEPTION WHEN OTHERS THEN NULL;
      END;

      EXECUTE IMMEDIATE '
        CREATE PUBLIC DATABASE LINK {dblink_name}
        CONNECT TO {dblink_user} IDENTIFIED BY "{dblink_pass}"
        USING ''(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={dblink_host})(PORT={dblink_port}))
        (CONNECT_DATA=(SERVICE_NAME={dblink_service})))''
      ';
    END;
    """)
    con.commit()

    # 3. query remote table via dblink and upload
    query = f"""
        SELECT {columns}
        FROM {table_name}@{dblink_name}
        WHERE TO_CHAR(UPDATE_TIMESTAMP,'YYYY-MM-DD') >= '{etl_batch_date}'
    """
    df = pd.read_sql(query, con)

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)

    s3 = boto3.client("s3")
    s3_key = f"{table_name}/{etl_batch_date}/{csv_file}"
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buf.getvalue())

    print(f"Uploaded: s3://{bucket}/{s3_key}")

    cur.close()
    con.close()

if __name__ == "__main__":
    export_products_to_s3()
