import os
import oracledb
from dotenv import load_dotenv

load_dotenv()

ora_user = os.getenv("ORACLE_USER")
ora_pass = os.getenv("ORACLE_PASSWORD")
ora_dsn  = os.getenv("ORACLE_DSN")
schema   = os.getenv("ORACLE_SCHEMA")

dblink_name    = os.getenv("DB_LINK")
dblink_user    = os.getenv("DBLINK_USER")
dblink_pass    = os.getenv("DBLINK_PASSWORD")
dblink_host    = os.getenv("DBLINK_HOST")
dblink_port    = os.getenv("DBLINK_PORT")
dblink_service = os.getenv("DBLINK_SERVICE")

con = oracledb.connect(user=ora_user, password=ora_pass, dsn=ora_dsn)
cur = con.cursor()

cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA={schema}")

cur.execute(f"""
BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP PUBLIC DATABASE LINK {dblink_name}';
  EXCEPTION WHEN OTHERS THEN NULL;
  END;

  EXECUTE IMMEDIATE '
    CREATE PUBLIC DATABASE LINK {dblink_name}
    CONNECT TO {dblink_user} IDENTIFIED BY "{dblink_pass}"
    USING ''(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={dblink_host})(PORT={dblink_port}))
    (CONNECT_DATA=(SERVICE_NAME={dblink_service})))''';
END;
""")

con.commit()
cur.close()
con.close()

print("DB LINK CREATED SUCCESSFULLY")
