import shutil
from pathlib import Path

for p in Path(".").rglob("__pycache__"):
    print(f"Removing {p}")
    shutil.rmtree(p)
