import oracledb
import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

def export_table_to_s3():
    load_dotenv()

    # Database and AWS main connection
    username = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn      = os.getenv("ORACLE_DSN")
    schema   = os.getenv("ORACLE_SCHEMA")
    bucket_name    = os.getenv("S3_BUCKET_NAME")
    etl_batch_date = os.getenv("ETL_BATCH_DATE")

    # DB LINK variables
    link_user = os.getenv("DBLINK_USER")
    link_pass = os.getenv("DBLINK_PASSWORD")
    link_host = os.getenv("DBLINK_HOST")
    link_port = os.getenv("DBLINK_PORT")
    link_svc  = os.getenv("DBLINK_SERVICE")
    link_name = os.getenv("DBLINK_NAME")

    table_name = "PAYMENTS"
    csv_file_name = "payment.csv"
    columns = "CUSTOMERNUMBER,CHECKNUMBER,PAYMENTDATE,AMOUNT"

    connection = None

    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        # 1) DROP DBLINK
        try:
            cursor.execute(f"DROP PUBLIC DATABASE LINK {link_name}")
        except:
            pass

        # 2) CREATE DBLINK
        link_sql = f"""
        CREATE PUBLIC DATABASE LINK {link_name}
        CONNECT TO {link_user} IDENTIFIED BY "{link_pass}"
        USING '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={link_host})(PORT={link_port}))
        (CONNECT_DATA=(SERVICE_NAME={link_svc})))'
        """
        cursor.execute(link_sql)

        # 3) QUERY
        query = f"""
            SELECT {columns}
            FROM {schema}.{table_name}
            WHERE TO_CHAR(UPDATE_TIMESTAMP, 'YYYY-MM-DD') >= '{etl_batch_date}'
        """
        print("Running query:", query.strip())

        df = pd.read_sql(query, connection)

        # 4) CSV in-memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")

        # 5) UPLOAD TO S3
        s3 = boto3.client("s3")
        s3_key = f"{table_name}/{etl_batch_date}/{csv_file_name}"
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

        print(f"{csv_file_name} uploaded successfully to s3://{bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Error exporting {table_name.lower()}:", e)

    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    export_table_to_s3()
