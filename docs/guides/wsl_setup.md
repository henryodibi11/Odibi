# 🐧 WSL Setup Guide

If you are on Windows, **Windows Subsystem for Linux (WSL 2)** is the only supported way to develop with Odibi (especially for Spark compatibility).

---

## The Golden Rule

> **Edit code in Windows. Run code in WSL.**

*   **VS Code:** Runs in Windows, but connects to WSL.
*   **Terminal:** You type commands in the WSL (Ubuntu) terminal.
*   **Files:** Live in the Linux file system (or `/mnt/d/`).

---

## 1. Install Requirements (Inside WSL)

Open your Ubuntu terminal and run:

```bash
# 1. Update system
sudo apt update && sudo apt upgrade -y

# 2. Install Python 3.10
sudo apt install -y python3.10 python3.10-venv python3.10-dev

# 3. Install Java (Required for Spark)
sudo apt install -y openjdk-17-jdk
```

---

## 2. Setup Environment

1.  **Clone/Go to your project:**
    ```bash
    cd /mnt/d/odibi  # Accessing D: drive from Linux
    ```

2.  **Create Virtual Environment:**
    ```bash
    python3.10 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install Odibi:**
    ```bash
    pip install "odibi[spark]"
    ```

---

## 3. Configure VS Code

1.  Install the **"WSL"** extension in VS Code.
2.  Open your folder in Windows.
3.  Click the green button in the bottom-left corner ("Open a Remote Window").
4.  Select **"Reopen in WSL"**.

Now your terminal inside VS Code is a Linux terminal!

---

## Troubleshooting

**"Java not found"**
Make sure you installed `openjdk-17-jdk`. Add this to your `~/.bashrc`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

**"Command not found: odibi"**
Did you activate your virtual environment?
```bash
source .venv/bin/activate
```
