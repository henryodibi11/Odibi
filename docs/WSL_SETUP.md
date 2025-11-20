# WSL 2 Setup Guide for Odibi

You have **Ubuntu-20.04** installed, which is great!
However, it comes with Python 3.8, and Odibi requires **Python 3.9+**.

## 1. Access Your Project
Your D: drive is mounted at `/mnt/d`.
```bash
cd /mnt/d/odibi
```

## 2. Install Python 3.10 and Venv
Run these commands inside your WSL terminal:

```bash
# Update package list
sudo apt update

# Install Python 3.10
sudo apt install -y python3.10 python3.10-venv python3.10-dev

# Verify
python3.10 --version
```

## 3. Create Virtual Environment
It is best to keep the WSL venv separate from Windows.

```bash
# Create venv named .venv_wsl
python3.10 -m venv .venv_wsl

# Activate
source .venv_wsl/bin/activate

# Upgrade pip
pip install --upgrade pip
```

## 4. Install Odibi
```bash
# Install with Spark extras
pip install -e ".[spark]"
```

## 5. Verify Setup
You can now run the playgrounds without Windows compatibility issues!

```bash
# Pandas
python examples/playground_pandas.py

# Spark (Should work perfectly in WSL!)
python examples/playground_spark_local.py
```
