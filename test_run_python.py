from odibi_mcp.tools.execute import run_python
import time

start = time.time()
result = run_python(
    """
from pathlib import Path
print("Writing file...")
Path("D:/projects/test.txt").write_text("hello")
print("Done!")
""",
    timeout=30,
)

print(f"Took {time.time() - start:.1f}s")
print("Success:", result.success)
print("Output:", result.stdout)
print("Error:", result.stderr if result.stderr else "None")
