import os
import subprocess
from dotenv import load_dotenv

load_dotenv()

tables = os.getenv("TABLES", "").split(",")

procs = []

for table in tables:
    table = table.strip().upper()
    if not table:
        continue

    script_name = f"{table.lower()}.py"

    if os.path.exists(script_name):
        print(f"launching {table} in background ...")
        p = subprocess.Popen(["python", script_name])
        procs.append((table, p))
    else:
        print(f"Script not found: {script_name}")

# wait for all children to finish
for table, p in procs:
    p.wait()
    print(f"{table} finished with exit code {p.returncode}")

print("ALL DONE!")
