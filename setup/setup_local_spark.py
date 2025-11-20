import os
import platform
import urllib.request
from pathlib import Path

# Constants
HADOOP_VERSION = "3.3.5"
BASE_URL = f"https://github.com/cdarlint/winutils/raw/master/hadoop-{HADOOP_VERSION}/bin/"
FILES = ["winutils.exe", "hadoop.dll"]


def setup_windows_spark():
    """Download Hadoop binaries for Windows."""
    if platform.system() != "Windows":
        print("-> Not running on Windows. Skipping winutils setup.")
        return

    print(f"-> Windows detected. Checking for Hadoop binaries (v{HADOOP_VERSION})...")

    # Determine project root
    project_root = Path(__file__).parent.parent.absolute()
    tools_dir = project_root / "tools" / "hadoop" / "bin"

    # Create directories
    tools_dir.mkdir(parents=True, exist_ok=True)
    print(f"-> Target directory: {tools_dir}")

    # Download files
    for filename in FILES:
        target_path = tools_dir / filename
        if target_path.exists():
            print(f"-> {filename} already exists.")
            continue

        url = BASE_URL + filename
        print(f"-> Downloading {filename}...")
        try:
            urllib.request.urlretrieve(url, target_path)
            print(f"-> Downloaded {filename}")
        except Exception as e:
            print(f"X Failed to download {filename}: {e}")
            print("Please download manually see docs/SPARK_LOCAL_SETUP.md")
            return

    # Instructions
    hadoop_home = tools_dir.parent
    print("\n-> Setup complete!")
    print("\nTo run Spark locally, you must set HADOOP_HOME:")
    print(f"\n    $env:HADOOP_HOME = '{hadoop_home}'")
    print(f"    $env:PATH = $env:PATH + ';{tools_dir}'")
    print("\nOr in Python before initializing Spark:")
    print(f"    os.environ['HADOOP_HOME'] = r'{hadoop_home}'")


def main():
    print("--- Odibi Local Spark Setup ---")

    # Check Java
    print("\n1. Checking Java...")
    res = os.system("java -version")
    if res != 0:
        print("X Java not found! Please install JDK 11 or 17.")
        print("   Download: https://adoptium.net/")
    else:
        print("-> Java found.")

    # Setup Windows binaries
    print("\n2. Checking Hadoop/Winutils (Windows only)...")
    setup_windows_spark()


if __name__ == "__main__":
    main()
