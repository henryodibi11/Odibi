# 🐧 The Definitive Odibi WSL Guide

This guide explains how to setup, develop, and run Odibi using Windows Subsystem for Linux (WSL 2).

## 🧠 Mental Model

Think of WSL as a "magic terminal" on your Windows machine.
*   **Files:** You can edit files in Windows (VS Code) and run them in Linux (WSL).
*   **Environment:** It has its own brain. It installs tools (Python, Java) separately from Windows.
*   **Golden Rule:** **Edit code in Windows. Run code in WSL.**

---

## 1. Initial Setup (One-Time)

### Step 1: Access Your Project
In WSL, your drives are mounted at `/mnt/`.
```bash
cd /mnt/d/odibi
```

### Step 2: Install System Dependencies
Run these commands inside your WSL terminal:
```bash
# Update package list
sudo apt update

# Install Python 3.10 and Java (for Spark)
sudo apt install -y python3.10 python3.10-venv python3.10-dev openjdk-17-jdk

# Verify
python3.10 --version
java -version
```

### Step 3: Create Virtual Environment
It is best to keep the WSL venv separate from Windows.
```bash
# Create venv named .venv_wsl
python3.10 -m venv .venv_wsl

# Activate
source .venv_wsl/bin/activate

# Upgrade pip
pip install --upgrade pip
```

### Step 4: Install Odibi
```bash
# Install with Spark extras
pip install -e ".[spark]"
```

### Step 5: Configure Environment Variables
Add these to your `~/.bashrc` so Spark works every time:
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

---

## 2. Daily Workflow

Every time you open a new terminal to work on Odibi:

1. **Enter WSL** (from PowerShell/CMD):
   ```powershell
   wsl
   ```

2. **Go to Project**:
   ```bash
   cd /mnt/d/odibi
   ```

3. **Activate Environment**:
   ```bash
   source .venv_wsl/bin/activate
   ```
   *(Or `conda activate odibi` if you used Conda)*

4. **Run Your Code**:
   ```bash
   python examples/playground_spark_local.py
   ```

---

## 3. VS Code Integration

1.  Open your project in the WSL terminal: `cd /mnt/d/odibi`
2.  Type: `code .`
3.  VS Code will open in Windows, but it will be **connected** to WSL.
    *   The bottom-left corner will show a green box: **[WSL: Ubuntu]**.
    *   The terminal inside VS Code will be your Linux terminal.

---

## 4. Troubleshooting

### "Java version mismatch" / "UnsupportedClassVersionError"
*   **Cause:** Spark is finding Java 11 instead of Java 17.
*   **Fix:** Verify `JAVA_HOME` is set correctly in `~/.bashrc`.

### "Command not found: conda"
*   **Cause:** Miniconda isn't in your path.
*   **Fix:** Run `source ~/miniconda3/bin/activate`.

### "Permission denied"
*   **Fix:** Try adding `sudo` before the command (e.g., `sudo apt update`).

### "My internet isn't working in WSL"
*   **Cause:** VPN conflicts.
*   **Fix:** Run `wsl --shutdown` in PowerShell, then open `wsl` again.
