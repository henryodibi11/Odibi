#!/usr/bin/env python
"""Quick diagnostic to test MCP connection initialization."""

import os
import sys

# Add the repo to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    print("=== MCP Connection Diagnostic ===\n")
    
    # 1. Check environment variables
    print("1. Environment Variables:")
    sql_user = os.environ.get("SQL_USER")
    sql_password = os.environ.get("SQL_PASSWORD")
    odibi_config = os.environ.get("ODIBI_CONFIG")
    
    print(f"   SQL_USER: {'SET' if sql_user else 'NOT SET'}")
    print(f"   SQL_PASSWORD: {'SET' if sql_password else 'NOT SET'}")
    print(f"   ODIBI_CONFIG: {odibi_config or 'NOT SET'}")
    
    if not sql_user or not sql_password:
        print("\n   ❌ Missing SQL credentials! Set SQL_USER and SQL_PASSWORD env vars.")
        print("   Example (PowerShell):")
        print('   $env:SQL_USER = "your_username"')
        print('   $env:SQL_PASSWORD = "your_password"')
        return
    
    # 2. Check ODBC driver
    print("\n2. ODBC Driver:")
    try:
        import pyodbc
        drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
        if drivers:
            print(f"   OK Found drivers: {drivers}")
        else:
            print("   FAIL No SQL Server ODBC drivers found!")
            print("   Install: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
    except ImportError:
        print("   FAIL pyodbc not installed! Run: pip install pyodbc")
        return
    
    # 3. Try loading the config
    print("\n3. Loading Config:")
    config_path = odibi_config or r"C:\Users\hodibi\Downloads\discover.yaml"
    print(f"   Using: {config_path}")
    
    try:
        from odibi_mcp.context import MCPProjectContext
        ctx = MCPProjectContext.from_exploration_config(config_path)
        print(f"   OK Config loaded: {ctx.project_name}")
        print(f"   Connections defined: {list(ctx.config.get('connections', {}).keys())}")
    except Exception as e:
        print(f"   FAIL Failed to load config: {e}")
        return
    
    # 4. Try initializing connections
    print("\n4. Initializing Connections:")
    try:
        ctx.initialize_connections()
        print(f"   OK Initialized: {list(ctx.connections.keys())}")
        
        if not ctx.connections:
            print("   WARN No connections were initialized! Check logs above.")
    except Exception as e:
        print(f"   FAIL Failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 5. Test the SQL connection
    conn_name = "WideWorldImporters-Standard"
    if conn_name in ctx.connections:
        print(f"\n5. Testing Connection '{conn_name}':")
        conn = ctx.connections[conn_name]
        print(f"   Type: {type(conn).__name__}")
        print(f"   Has execute_query: {hasattr(conn, 'execute_query')}")
        
        if hasattr(conn, 'execute_query'):
            try:
                result = conn.execute_query("SELECT 1 AS test")
                print(f"   OK Query succeeded: {result}")
            except Exception as e:
                print(f"   FAIL Query failed: {e}")
    else:
        print(f"\n5. Connection '{conn_name}' not found in initialized connections.")
        print("   This means the factory failed silently. Check error logs.")

if __name__ == "__main__":
    main()
