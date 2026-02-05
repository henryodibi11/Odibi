"""Debug script to check .env loading and key format."""

import os
from pathlib import Path
from dotenv import load_dotenv
import base64

# Load .env from project root
env_path = Path(__file__).parent / ".env"
print(f"Loading .env from: {env_path}")
print(f"File exists: {env_path.exists()}")

load_dotenv(env_path, override=True)

key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "")
account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "")

print(f"\nAZURE_STORAGE_ACCOUNT_NAME: {account or '(not set)'}")
print("\nAZURE_STORAGE_ACCOUNT_KEY:")
print(f"  Length: {len(key)} (should be 88)")
print(f"  Ends with '==': {key.endswith('==')}")
print(f"  First 10 chars: {key[:10] if key else '(empty)'}...")
print(f"  Last 10 chars: ...{key[-10:] if key else '(empty)'}")
has_quotes = key.startswith('"') or key.endswith('"')
print(f"  Has quotes: {has_quotes}")
print(f"  Has whitespace: {key != key.strip()}")

# Try to decode it as Base64
if key:
    try:
        decoded = base64.b64decode(key)
        print("\n✅ Base64 decoding successful! Key is valid.")
    except Exception as e:
        print(f"\n❌ Base64 decoding FAILED: {e}")
        print("   This is your problem! The key is malformed.")
