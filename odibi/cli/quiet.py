"""CLI-only quiet presentation; keep native output in a full per-run log."""

from collections import deque
from contextlib import contextmanager
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import uuid


class _DiagnosticFormatter(logging.Formatter):
    def __init__(self, marker):
        super().__init__()
        self.marker = marker

    def format(self, record):
        # One framed line survives Rich wrapping, native progress, and multiline errors.
        return (
            "\n"
            + self.marker
            + json.dumps({"level": record.levelname, "message": record.getMessage()})
        )


@contextmanager
def quiet_diagnostics():
    """Forward emitted Python warnings/errors without changing logger levels."""
    marker = os.environ.pop("_ODIBI_QUIET_RUN", None)
    if not marker:
        yield
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(_DiagnosticFormatter(marker))
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)
        handler.close()


def quiet_run_command(argv):
    """Run with this exact interpreter; capture JVM output as well as Python output."""
    # Only remove the option, never a positional filename following '--'.
    separator = argv.index("--") if "--" in argv else len(argv)
    child_argv = [
        arg for arg in argv[:separator] if not (arg.startswith("--") and "--quiet".startswith(arg))
    ] + argv[separator:]
    marker = "ODIBI_DIAGNOSTIC_" + uuid.uuid4().hex + ":"
    child_env = os.environ.copy()
    child_env["_ODIBI_QUIET_RUN"] = marker
    tail = deque(maxlen=40)
    diagnostics = 0

    with tempfile.NamedTemporaryFile(prefix="odibi-run-", suffix=".log", delete=False) as log:
        print(f"Run log: {log.name}", flush=True)
        try:
            with subprocess.Popen(
                [sys.executable, "-u", "-X", "utf8", "-m", "odibi", *child_argv],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=child_env,
            ) as child:
                try:
                    for raw in child.stdout:
                        log.write(raw)
                        log.flush()
                        line = raw.decode("utf-8", errors="replace").rstrip().rsplit("\r", 1)[-1]
                        diagnostic = None
                        if line.startswith(marker):
                            diagnostic = json.loads(line[len(marker) :])
                        elif line.startswith("{"):
                            # Structured Odibi logging prints JSON instead of logging records.
                            try:
                                record = json.loads(line)
                            except ValueError:
                                record = None
                            if isinstance(record, dict) and record.get("level") in (
                                "WARNING",
                                "ERROR",
                                "CRITICAL",
                                "FATAL",
                            ):
                                diagnostic = record
                        elif line.startswith("Warning:"):
                            diagnostic = {"level": "WARNING", "message": line}
                        elif re.match(r"^(?:\d[\d:/T.,+Z -]*\s+)?(?:ERROR|FATAL)\b", line):
                            # Native JVM errors do not pass through Python's logging handlers.
                            diagnostic = {"level": "ERROR", "message": line}

                        if diagnostic:
                            diagnostics += 1
                            message = diagnostic.get("message", diagnostic.get("msg", ""))
                            line = f"{diagnostic['level']}: {message}"
                            print(line, file=sys.stderr, flush=True)
                        tail.append(line)
                    code = child.wait()
                except KeyboardInterrupt:
                    child.terminate()
                    child.wait()
                    print("Run interrupted; full output is in the run log.", file=sys.stderr)
                    return 130
        except OSError as error:
            print(f"Unable to run pipeline: {error}", file=sys.stderr)
            return 1

    if code:
        print(f"Run failed (exit {code}). Full details are in the run log.", file=sys.stderr)
        print("Last log lines:\n" + "\n".join(tail), file=sys.stderr)
    elif diagnostics:
        print("Run finished with warning/error diagnostics; inspect the run log.")
    else:
        print("Run finished (exit 0).")
    return code if code >= 0 else 128 - code
