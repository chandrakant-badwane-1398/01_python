import os
import psycopg2
from dotenv import load_dotenv

def load_offices_to_redshift():
    load_dotenv()

    host     = os.getenv("REDSHIFT_HOST")
    port     = os.getenv("REDSHIFT_PORT")
    database = os.getenv("REDSHIFT_DB")
    user     = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")

    stage_schema     = os.getenv("REDSHIFT_SCHEMA")
    metadata_schema  = os.getenv("REDSHIFT_METADATA_SCHEMA")

    role     = os.getenv("REDSHIFT_IAM_ROLE")
    bucket   = os.getenv("S3_BUCKET_NAME")

    # get batch date once
    with psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select etl_batch_date from {metadata_schema}.batch_control limit 1")
            etl_batch_date = str(cur.fetchone()[0])

    s3_key = f"OFFICES/{etl_batch_date}/offices.csv"
    s3_path = f"s3://{bucket}/{s3_key}"

    print("Loading from:", s3_path)

    copy_sql = f"""
        COPY {stage_schema}.offices
        FROM '{s3_path}'
        IAM_ROLE '{role}'
        IGNOREHEADER 1
        BLANKSASNULL
        EMPTYASNULL
        FORMAT AS CSV
        DELIMITER ','
        QUOTE '"';
    """

    with psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(copy_sql)
                conn.commit()
                print("OFFICES LOAD DONE")
            except Exception as e:
                print("COPY ERROR:", e)

if __name__ == "__main__":
    load_offices_to_redshift()
