# WSL Setup for Odibi Campaign Runner

This guide sets up WSL (Windows Subsystem for Linux) for running Odibi campaigns with Spark support.

## Prerequisites

- Windows 10/11 with WSL2 enabled
- Ubuntu 20.04 or 22.04 in WSL

## Quick Setup Script

Run this entire script in WSL to set everything up:

```bash
#!/bin/bash
# Odibi WSL Setup Script

set -e

echo "=== Odibi WSL Setup ==="

# Update system
echo "Updating packages..."
sudo apt update && sudo apt upgrade -y

# Install Python 3.9 (Ubuntu 20.04) or use system Python (Ubuntu 22.04+)
echo "Installing Python..."
sudo apt install -y python3.9 python3.9-venv python3.9-dev curl

# Install pip
echo "Installing pip..."
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

# Install Java (required for Spark)
echo "Installing Java..."
sudo apt install -y openjdk-11-jdk

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Spark
echo "Installing Spark..."
SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
cd /tmp
wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
sudo tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt/
sudo mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark
rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Set SPARK_HOME
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin

# Install Odibi dependencies
echo "Installing Odibi dependencies..."
cd /mnt/d/odibi
python3.9 -m pip install -e ".[dev]"

# Verify installations
echo ""
echo "=== Verification ==="
echo "Python: $(python3.9 --version)"
echo "Java: $(java -version 2>&1 | head -1)"
echo "Spark: $(spark-submit --version 2>&1 | head -1)"
echo "pytest: $(python3.9 -m pytest --version)"
echo "ruff: $(python3.9 -m ruff --version)"

echo ""
echo "=== Setup Complete ==="
echo "WSL is ready for Odibi campaigns!"
```

## Manual Step-by-Step

### 1. Install Python 3.9+

```bash
# Ubuntu 20.04
sudo apt update
sudo apt install -y python3.9 python3.9-venv python3.9-dev
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

# Ubuntu 22.04+ (Python 3.10+ is default)
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
```

### 2. Install Java

```bash
sudo apt install -y openjdk-11-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
java -version
```

### 3. Install Spark

```bash
cd /tmp
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
sudo tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt/
sudo mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
spark-submit --version
```

### 4. Install Odibi

```bash
cd /mnt/d/odibi
python3.9 -m pip install -e ".[dev]"
```

### 5. Verify Everything

```bash
python3.9 --version          # Python 3.9.x
java -version                # openjdk 11.x
spark-submit --version       # Spark 3.5.0
python3.9 -m pytest --version
python3.9 -m ruff --version
python3.9 -c "import pyspark; print('PySpark OK')"
```

## Campaign UI Settings

Use these settings in the Campaign Runner UI:

| Setting | Value |
|---------|-------|
| Run through WSL | ✅ Checked |
| WSL Distribution | `Ubuntu-20.04` |
| WSL Python Command | `python3.9` |
| SPARK_HOME | `/opt/spark` |
| JAVA_HOME | `/usr/lib/jvm/java-11-openjdk-amd64` |
| Shell Init (optional) | `source ~/.bashrc` |

## Troubleshooting

### "Python not found"
```bash
# Check available Python versions
ls /usr/bin/python*
# Use the correct one in UI (e.g., python3.9, python3.10)
```

### "WSL distribution not found"
```bash
# List available distros (run in Windows PowerShell)
wsl --list
# Use exact name in UI (e.g., Ubuntu-20.04, not Ubuntu)
```

### "SPARK_HOME not found"
```bash
# Check Spark installation
ls /opt/spark
# If missing, reinstall Spark (see step 3)
```

### "Permission denied"
```bash
# Fix permissions
sudo chown -R $USER:$USER /opt/spark
```

### "Module not found" (pyspark, pytest, ruff)
```bash
# Reinstall with the correct Python
python3.9 -m pip install pyspark pytest ruff
```

## Quick Test

Run this to test WSL is working:

```bash
wsl -d Ubuntu-20.04 -- python3.9 -c "import pyspark; print('WSL + PySpark OK')"
```
