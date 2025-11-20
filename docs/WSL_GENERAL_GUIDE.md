# 🐧 The Pragmatic Developer's Guide to WSL 2

WSL (Windows Subsystem for Linux) lets you run a full Linux environment directly on Windows, without dual-booting or slow virtual machines.

This guide focuses on **practical usage**—how to actually live and work in it.

---

## 🧠 Mental Model

Think of WSL as a "magic terminal" on your Windows machine.
*   **It shares your files.** You can edit files in Windows (VS Code) and run them in Linux (WSL).
*   **It has its own brain.** It installs tools (Python, Node, Java) separately from Windows. Installing Python in Windows does *not* install it in WSL.

### The Golden Rule
> **Edit code in Windows. Run code in WSL.**

---

## ⚡ Daily Commands (Cheat Sheet)

| Action | Command (PowerShell) | Command (Inside WSL) |
| :--- | :--- | :--- |
| **Start WSL** | `wsl` | N/A |
| **Go to D: drive** | `wsl` then `cd /mnt/d` | `cd /mnt/d` |
| **Open current folder in Explorer** | N/A | `explorer.exe .` |
| **Open current folder in VS Code** | `code .` | `code .` |
| **Paste from Windows** | `Ctrl+V` | `Right-Click` (usually) |
| **Copy from WSL** | Select text + `Enter` | Highlight text (auto-copy) |

---

## 📂 Navigating Drives

In Linux, there are no drive letters (`C:`, `D:`). Instead, drives are mounted folders.

*   `C:\`  ➡️  `/mnt/c/`
*   `D:\`  ➡️  `/mnt/d/`

**Example:**
To go to `D:\projects\odibi`:
```bash
cd /mnt/d/projects/odibi
```

**Pro Tip:** create a shortcut alias.
Running `alias d='cd /mnt/d'` lets you just type `d` to jump to your drive.

---

## 📦 Installing Stuff (The "apt" way)

In Windows, you download `.exe` installers. In Linux (Ubuntu), you use a package manager called `apt`.

**1. Update the catalog (Do this before installing anything)**
```bash
sudo apt update
```

**2. Install a tool (e.g., git, curl, zip)**
```bash
sudo apt install git curl zip -y
```

**3. Remove a tool**
```bash
sudo apt remove git
```

---

## 🐍 Python & Environments (Conda)

Since you are doing data engineering, **Miniconda** is your best friend in WSL. It handles Python versions better than the system `apt` does.

**Create a new environment:**
```bash
conda create -n my_project python=3.10
```

**Activate it (do this every time you work):**
```bash
conda activate my_project
```

**Install packages:**
```bash
pip install pandas pyspark
# OR
conda install pandas
```

---

## 🛠️ VS Code Integration (The Killer Feature)

You don't need to use `vim` or `nano` inside the terminal.

1.  Open your project in the WSL terminal: `cd /mnt/d/odibi`
2.  Type: `code .`
3.  VS Code will open in Windows, but it will be **connected** to WSL.
    *   The bottom-left corner will show a green box: **[WSL: Ubuntu]**.
    *   The terminal inside VS Code will be your Linux terminal.

This is the best way to work.

---

## 🛑 Troubleshooting

**"Command not found"**
*   Did you install it? (`sudo apt install <name>`)
*   Is it in your path? (Restart terminal)

**"Permission denied"**
*   Try adding `sudo` (SuperUser DO) before the command.
*   Example: `sudo apt update`

**"My internet isn't working in WSL"**
*   This happens sometimes with VPNs.
*   Fix: Run `wsl --shutdown` in PowerShell, then open `wsl` again.

---

## 🎓 Practice Exercise

Try this sequence right now to lock it in:

1.  Open PowerShell.
2.  Type `wsl` (Enter).
3.  Go to your D drive: `cd /mnt/d`
4.  Create a test folder: `mkdir wsl_test`
5.  Go inside: `cd wsl_test`
6.  Open it in Windows Explorer: `explorer.exe .`
7.  See the folder? Great. Now delete it: `cd ..` then `rmdir wsl_test`

Welcome to Linux! 🐧
