@echo off
SETLOCAL

:: 1. Setup Paths
set "PROJECT_ROOT=%~dp0.."
set "HADOOP_HOME=%PROJECT_ROOT%\tools\hadoop"
set "VENV_DIR=%PROJECT_ROOT%\.venv"

:: 2. Check Hadoop
if not exist "%HADOOP_HOME%\bin\winutils.exe" (
    echo [ERROR] winutils.exe not found in %HADOOP_HOME%\bin
    echo Please run: python setup/setup_local_spark.py
    exit /b 1
)

:: 3. Setup Environment Variables
cd /d "%PROJECT_ROOT%"
set "PATH=%HADOOP_HOME%\bin;%VENV_DIR%\Scripts;%PATH%"
set "PYSPARK_PYTHON=python"
:: Do NOT set PYSPARK_DRIVER_PYTHON when running script directly with python
set "PYSPARK_DRIVER_PYTHON="

:: 4. Run Playground
echo [INFO] Running Spark Playground with spark-submit...
call "%VENV_DIR%\Scripts\spark-submit.cmd" examples/playground_spark_local.py

ENDLOCAL
