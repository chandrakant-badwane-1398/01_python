import oracledb
import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

def export_table_to_s3():
    load_dotenv()

    username   = os.getenv("ORACLE_USER")
    password   = os.getenv("ORACLE_PASSWORD")
    dsn        = os.getenv("ORACLE_DSN")

    schema     = os.getenv("ORACLE_SCHEMA")
    bucket     = os.getenv("S3_BUCKET_NAME")
    etl_batch_date   = os.getenv("ETL_BATCH_DATE")

    dblink_name     = os.getenv("DB_LINK")
    dblink_user     = os.getenv("DBLINK_USER")
    dblink_pass     = os.getenv("DBLINK_PASSWORD")
    dblink_host     = os.getenv("DBLINK_HOST")
    dblink_port     = os.getenv("DBLINK_PORT")
    dblink_service  = os.getenv("DBLINK_SERVICE")

    table = "CUSTOMERS"
    columns = ("CUSTOMERNUMBER, CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME, PHONE, "
               "ADDRESSLINE1, ADDRESSLINE2, CITY, STATE, POSTALCODE, COUNTRY, "
               "SALESREPEMPLOYEENUMBER, CREDITLIMIT")

    try:
        conn = oracledb.connect(user=username, password=password, dsn=dsn)
        cur = conn.cursor()

        cur.execute(f"""
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
        conn.commit()
        print(f"DBLINK {dblink_name} created successfully")

        query = f"""
            SELECT {columns}
            FROM {table}@{dblink_name}
            WHERE TO_CHAR(UPDATE_TIMESTAMP,'YYYY-MM-DD') >= '{etl_batch_date}'
        """
        print("Running:", query.strip())

        df = pd.read_sql(query, conn)
        df.drop_duplicates(inplace=True)

        csv_buf = StringIO()
        df.to_csv(csv_buf, index=False, encoding="utf-8")

        s3 = boto3.client("s3")
        s3_key = f"{table}/{etl_batch_date}/customer.csv"
        s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buf.getvalue())

        print(f"customer.csv uploaded to s3://{bucket}/{s3_key}")

    except Exception as e:
        print("error:", e)

    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    export_table_to_s3()
