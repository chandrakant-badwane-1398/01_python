import oracledb
import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

def export_table_to_s3():
    load_dotenv()

    username = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn      = os.getenv("ORACLE_DSN")

    bucket_name    = os.getenv("S3_BUCKET_NAME")
    etl_batch_date = os.getenv("ETL_BATCH_DATE")

    dblink_user    = os.getenv("DBLINK_USER")
    dblink_pass    = os.getenv("DBLINK_PASSWORD")
    dblink_host    = os.getenv("DBLINK_HOST")
    dblink_port    = os.getenv("DBLINK_PORT")
    dblink_service = os.getenv("DBLINK_SERVICE")
    dblink_name    = os.getenv("DBLINK_NAME")

    table_name = "PAYMENTS"
    csv_file_name = "payment.csv"
    columns = "CUSTOMERNUMBER,CHECKNUMBER,PAYMENTDATE,AMOUNT"

    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        cursor.execute(f"""
            BEGIN
                BEGIN EXECUTE IMMEDIATE 'DROP PUBLIC DATABASE LINK {dblink_name}';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;

                EXECUTE IMMEDIATE '
                    CREATE PUBLIC DATABASE LINK {dblink_name}
                    CONNECT TO {dblink_user} IDENTIFIED BY "{dblink_pass}"
                    USING ''(DESCRIPTION=
                                (ADDRESS=(PROTOCOL=TCP)(HOST={dblink_host})(PORT={dblink_port}))
                                (CONNECT_DATA=(SERVICE_NAME={dblink_service}))
                             )''
                ';
            END;
        """)
        connection.commit()
        print(f"DBLINK {dblink_name} created successfully")

        query = f"""
            SELECT {columns}
            FROM {table_name}@{dblink_name}
            WHERE TO_CHAR(UPDATE_TIMESTAMP,'YYYY-MM-DD') >= '{etl_batch_date}'
        """

        df = pd.read_sql(query, connection)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")

        s3 = boto3.client("s3")
        s3_key = f"{table_name}/{etl_batch_date}/{csv_file_name}"
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

        print(f"{csv_file_name} uploaded successfully to s3://{bucket_name}/{s3_key}")

    except Exception as e:
        print(f"Error exporting {table_name.lower()} :", e)

    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    export_table_to_s3()
