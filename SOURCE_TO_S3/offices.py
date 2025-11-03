import oracledb
import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

def export_table_to_s3():
    load_dotenv()

    # LOCAL DB (where code running)
    username = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")

    # REMOTE DB LINK CREDS
    dblink_user = os.getenv("DBLINK_USER")
    dblink_pass = os.getenv("DBLINK_PASSWORD")
    dblink_name = os.getenv("DBLINK_NAME")
    dblink_service = os.getenv("DBLINK_SERVICE")
    dblink_host = os.getenv("DBLINK_HOST")
    dblink_port = os.getenv("DBLINK_PORT")

    schema = os.getenv("ORACLE_SCHEMA")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    etl_batch_date = os.getenv("ETL_BATCH_DATE")

    table_name = "OFFICES"
    columns = "OFFICECODE,CITY,PHONE,ADDRESSLINE1,ADDRESSLINE2,STATE,COUNTRY,POSTALCODE,TERRITORY"

    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        # DROP if exists
        try:
            cursor.execute(f"DROP PUBLIC DATABASE LINK {dblink_name}")
        except:
            pass

        # CREATE
        create_sql = f"""
        CREATE PUBLIC DATABASE LINK {dblink_name}
        CONNECT TO {dblink_user} IDENTIFIED BY "{dblink_pass}"
        USING '(DESCRIPTION=
            (ADDRESS=(PROTOCOL=TCP)(HOST={dblink_host})(PORT={dblink_port}))
            (CONNECT_DATA=(SERVICE_NAME={dblink_service}))
        )'
        """
        cursor.execute(create_sql)
        connection.commit()

        # QUERY through dblink
        query = f"""
            SELECT {columns}
            FROM {schema}.{table_name}@{dblink_name}
            WHERE TO_CHAR(UPDATE_TIMESTAMP,'YYYY-MM-DD') >= '{etl_batch_date}'
        """

        df = pd.read_sql(query, connection)

        # Convert to csv
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")

        # Upload to S3
        s3 = boto3.client("s3")
        s3_key = f"{table_name}/{etl_batch_date}/office.csv"
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

        print(f"office.csv uploaded successfully to s3://{bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Error exporting {table_name}:", e)

    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    export_table_to_s3()
