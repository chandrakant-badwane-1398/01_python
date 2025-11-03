import os
import subprocess
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get table list from .env (example: TABLES=CUSTOMERS,EMPLOYEES,ORDERS)
tables = os.getenv("TABLES", "").split(",")

# Run each table script
for table in tables:
    table = table.strip().upper()
    if not table:
        continue

    script_name = f"{table.lower()}.py"

    if os.path.exists(script_name):
        print(f"Running script for {table}...")
        result = subprocess.run(["python", script_name], capture_output=True, text=True)
        print(result.stdout)
        if result.stderr.strip():
            print(f"Error in {table}: {result.stderr.strip()}")
    else:
        print(f"Script not found: {script_name}")

print("All listed table scripts executed successfully!")
