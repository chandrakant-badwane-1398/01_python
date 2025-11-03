import oracledb
import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

def export_table_to_s3():
    # Load environment variables
    load_dotenv()

    # Database and AWS configuration
    username = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")
    schema = os.getenv("ORACLE_SCHEMA")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    etl_batch_date = os.getenv("ETL_BATCH_DATE")

    # Table setup
    table_name = "ORDERDETAILS"
    columns = "ORDERNUMBER,PRODUCTCODE,QUANTITYORDERED,PRICEEACH,ORDERLINENUMBER"

    try:
        # Connect to Oracle
        connection = oracledb.connect(user=username, password=password, dsn=dsn)

        # Run SQL query (fetch updated/new records for the ETL batch date)
        query = f"""
            SELECT {columns}
            FROM {schema}.{table_name}
            WHERE TO_CHAR(UPDATE_TIMESTAMP, 'YYYY-MM-DD') >= '{etl_batch_date}'
        """
        print("Running query:", query.strip())

        # Load query result into DataFrame
        df = pd.read_sql(query, connection)

        # Convert DataFrame to in-memory CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")

        # Upload CSV to S3
        s3 = boto3.client("s3")
        s3_key = f"{table_name}/{schema}/orderdetail.csv"
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

        print(f"orderdetail.csv uploaded successfully to s3://{bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Error exporting {table_name.lower()}:", e)

    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    export_table_to_s3()
