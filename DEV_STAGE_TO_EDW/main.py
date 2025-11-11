import os
import subprocess
from dotenv import load_dotenv

load_dotenv()

tables = os.getenv("ALLTABLES", "").split(",")

for table in tables:
    table = table.strip()
    if not table:
        continue

    script = f"{table}.py"

    if os.path.exists(script):
        print(f"running {script} ...")
        result = subprocess.run(["python", script], capture_output=True, text=True)
        print(result.stdout)
        if result.stderr.strip():
            print("ERROR:", result.stderr.strip())
        print(f"{script} completed")
        print()
    else:
        print(f"file not found: {script}")

print("ALL TABLES COMPLETED")
