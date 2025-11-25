import random
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

IPS = ["192.168.1.1", "10.0.0.5", "172.16.0.1", "66.249.66.1"]
URLS = ["/index.html", "/login", "/api/data", "/admin", "/wp-login.php"]
AGENTS = ["Mozilla/5.0", "Googlebot", "Python-urllib"]


def generate_logs(n=1000):
    logs = []
    start = datetime.now()
    for i in range(n):
        ip = random.choice(IPS)
        timestamp = (start + timedelta(seconds=i)).strftime("%d/%b/%Y:%H:%M:%S +0000")
        method = "GET"
        url = random.choice(URLS)

        # Inject attack
        if i % 50 == 0:
            url += "?id=1 OR 1=1"  # SQL Injection
        elif i % 100 == 0:
            url = "/etc/passwd"  # LFI

        status = 200
        if "admin" in url or "passwd" in url:
            status = 403
        elif "wp-login" in url:
            status = 404

        size = random.randint(100, 5000)
        ua = random.choice(AGENTS)

        line = f'{ip} - - [{timestamp}] "{method} {url} HTTP/1.1" {status} {size} "-" "{ua}"'
        logs.append(line)

    with open(DATA_DIR / "access.log", "w") as f:
        f.write("\n".join(logs))
    print(f"Generated {n} logs.")


if __name__ == "__main__":
    generate_logs()
