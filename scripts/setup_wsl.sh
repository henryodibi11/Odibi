#!/bin/bash
# Odibi WSL Setup Script
# Run this inside WSL: bash /mnt/d/odibi/scripts/setup_wsl.sh

set -e

echo "=========================================="
echo "  Odibi WSL Setup for Campaign Runner"
echo "=========================================="
echo ""

# Detect Ubuntu version
UBUNTU_VERSION=$(lsb_release -rs 2>/dev/null || echo "unknown")
echo "Detected Ubuntu: $UBUNTU_VERSION"

# Update system
echo ""
echo "[1/6] Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Determine Python version to install
if [[ "$UBUNTU_VERSION" == "20.04" ]]; then
    PYTHON_CMD="python3.9"
    echo ""
    echo "[2/6] Installing Python 3.9..."
    sudo apt install -y python3.9 python3.9-venv python3.9-dev curl
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9
else
    PYTHON_CMD="python3"
    echo ""
    echo "[2/6] Installing Python 3..."
    sudo apt install -y python3 python3-pip python3-venv python3-dev curl
fi

# Install Java
echo ""
echo "[3/6] Installing Java 11..."
sudo apt install -y openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Add to bashrc if not already there
if ! grep -q "JAVA_HOME" ~/.bashrc; then
    echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
fi

# Install Spark
echo ""
echo "[4/6] Installing Apache Spark 3.5.0..."
SPARK_VERSION="3.5.0"
if [ ! -d "/opt/spark" ]; then
    cd /tmp
    if [ ! -f "spark-${SPARK_VERSION}-bin-hadoop3.tgz" ]; then
        wget -q --show-progress "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
    fi
    sudo tar -xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" -C /opt/
    sudo mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark
    sudo chown -R $USER:$USER /opt/spark
    echo "Spark installed to /opt/spark"
else
    echo "Spark already installed at /opt/spark"
fi

export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin

# Add to bashrc if not already there
if ! grep -q "SPARK_HOME" ~/.bashrc; then
    echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
    echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
fi

# Install Odibi dependencies
echo ""
echo "[5/6] Installing Odibi and dependencies..."
cd /mnt/d/odibi
$PYTHON_CMD -m pip install --upgrade pip
$PYTHON_CMD -m pip install -e ".[dev]"
$PYTHON_CMD -m pip install pyspark ruff pytest

# Verify
echo ""
echo "[6/6] Verifying installation..."
echo ""
echo "=========================================="
echo "  Installation Summary"
echo "=========================================="
echo ""
echo "Python:    $($PYTHON_CMD --version 2>&1)"
echo "Java:      $(java -version 2>&1 | head -1)"
echo "Spark:     $($SPARK_HOME/bin/spark-submit --version 2>&1 | grep version | head -1 || echo 'Spark 3.5.0')"
echo "pytest:    $($PYTHON_CMD -m pytest --version 2>&1 | head -1)"
echo "ruff:      $($PYTHON_CMD -m ruff --version 2>&1)"
echo "PySpark:   $($PYTHON_CMD -c 'import pyspark; print(pyspark.__version__)' 2>&1)"
echo ""
echo "=========================================="
echo "  Campaign UI Settings"
echo "=========================================="
echo ""
echo "WSL Distribution:    $(lsb_release -cs 2>/dev/null || echo 'Ubuntu-20.04')"
echo "WSL Python Command:  $PYTHON_CMD"
echo "SPARK_HOME:          /opt/spark"
echo "JAVA_HOME:           /usr/lib/jvm/java-11-openjdk-amd64"
echo ""
echo "=========================================="
echo "  Setup Complete! ✅"
echo "=========================================="
echo ""
echo "Restart your terminal or run: source ~/.bashrc"
