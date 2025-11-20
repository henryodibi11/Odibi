# The Definitive Odibi WSL Guide 🐧

This guide explains how to develop and run Odibi using Windows Subsystem for Linux (WSL 2).
Since you have it set up and working, follow this reference for your daily workflow.

---

## ⚡ Quick Start (Daily Workflow)

Every time you open a new terminal to work on Odibi:

1. **Enter WSL** (from PowerShell/CMD):
   ```powershell
   wsl
   ```

2. **Go to Project**:
   ```bash
   cd /mnt/d/odibi
   ```

3. **Activate Environment** (if using Conda):
   ```bash
   conda activate odibi
   ```

4. **Run Your Code**:
   ```bash
   python examples/playground_spark_local.py
   ```

---

## 🔧 Setup Reference (One-Time Config)

You have already done this, but here is the reference configuration for your machine (`CommandCenter`).

### 1. Environment Variables (`~/.bashrc`)
Ensure these lines are in your `~/.bashrc` file so you don't have to type them every time.

Run this once to append them if missing:
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

### 2. Conda Environment
Your environment is named `odibi`.

*   **Create:** `conda create -y -n odibi python=3.10`
*   **Activate:** `conda activate odibi`
*   **Install:** `pip install -e ".[spark]"`

---

## 📂 Mental Model: Where are my files?

*   **Windows:** `D:\odibi`
*   **Linux (WSL):** `/mnt/d/odibi`

They are the **exact same folder**.
*   Edit code in **VS Code on Windows**.
*   Run code in **WSL Terminal**.

---

## 🛠 Troubleshooting

### "Java version mismatch" / "UnsupportedClassVersionError"
*   **Cause:** Spark is finding Java 11 instead of Java 17.
*   **Fix:** Verify `JAVA_HOME` is set correctly.
    ```bash
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    ```

### "Command not found: conda"
*   **Cause:** Miniconda isn't in your path.
*   **Fix:**
    ```bash
    source ~/miniconda3/bin/activate
    conda init
    ```

### "Python worker exited unexpectedly"
*   **Cause:** Often memory issues or mismatched PySpark/Python versions.
*   **Fix:** Ensure you are using the `odibi` conda env with Python 3.10.
