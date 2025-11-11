import subprocess
import sys
import os

steps = [
    ("start_log.py", ""),
    ("main.py", "SOURCE_TO_S3"),
    ("main.py", "S3_TO_DEV_STAGE"),
    ("main.py", "DEV_STAGE_TO_EDW"),
    ("end_log.py", "")
]

for script, folder in steps:
    print(f"Running {folder}/{script} ...")
    try:
        if folder:
            subprocess.check_call([sys.executable, script], cwd=folder)
        else:
            subprocess.check_call([sys.executable, script])
    except Exception as e:
        print(f"FAILED → {folder}/{script}  {e}")
        # here run end_log with 'R'
        subprocess.call([sys.executable, "end_log.py"], cwd="")
        sys.exit(1)

# success → run end_log with S
subprocess.call([sys.executable, "end_log.py"])
print("MASTER DONE")
