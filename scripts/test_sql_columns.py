#!/usr/bin/env python
import os
import sys

print("Starting...", flush=True)

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

user = os.environ.get("SQL_USER", "")
pwd = os.environ.get("SQL_PASSWORD", "")
print(f"User set: {bool(user)}", flush=True)

driver = "ODBC Driver 17 for SQL Server"
server = "engineeringdatabase.database.windows.net"
database = "WideWorldImporters-Standard"

conn_str = f"mssql+pyodbc://{user}:{quote_plus(pwd)}@{server}:1433/{database}?driver={quote_plus(driver)}&Encrypt=yes&TrustServerCertificate=no"
print("Creating engine...", flush=True)
engine = create_engine(conn_str, connect_args={"timeout": 30})
print("Engine created", flush=True)

try:
    with engine.connect() as conn:
        print("Connected!", flush=True)
        result = conn.execute(
            text("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'Sales' AND TABLE_NAME = 'Customers'
            ORDER BY ORDINAL_POSITION
        """)
        )
        rows = result.fetchall()
        print(f"Got {len(rows)} columns:", flush=True)
        for row in rows:
            print(f"  {row[0]}: {row[1]}", flush=True)
except Exception as e:
    print(f"Error: {e}", flush=True)
    sys.exit(1)
