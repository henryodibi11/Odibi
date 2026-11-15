"""Public CLI quiet-output contracts, using local fixtures and real child processes."""

import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def project(tmp_path):
    (tmp_path / "input.csv").write_text("id,value\n1,7\n2,11\n3,-4\n", encoding="utf-8")
    (tmp_path / "odibi.yaml").write_text(
        """project: quiet-test
engine: pandas
connections:
  local:
    type: local
    base_path: .
story:
  connection: local
  path: stories
system:
  connection: local
  path: _system
  async_lineage: false
  async_derived_updates: false
pipelines:
  - pipeline: copy
    nodes:
      - name: source
        read:
          connection: local
          path: input.csv
          format: csv
        write:
          connection: local
          path: output.csv
          format: csv
          mode: overwrite
""",
        encoding="utf-8",
    )
    (tmp_path / "transforms.py").write_text(
        """import os
import subprocess
import sys
from odibi.utils.logging import logger

print('python-info-chatter', flush=True)
subprocess.run([sys.executable, '-c',
    "import os; os.write(1, b'native-ivy-chatter' + bytes([10])); "
    "os.write(2, b'WARNING: native-startup-chatter' + bytes([10]))"], check=True)
mode = os.environ.get('QUIET_TEST_MODE')
if mode == 'warning':
    logger.warning('Failed to save synthetic metadata; main work may still succeed')
elif mode == 'structured':
    from odibi.utils.logging_context import StructuredLogger
    StructuredLogger(structured=True).error('Synthetic structured diagnostic')
elif mode == 'native-error':
    print('26/11/14 12:34:56 ERROR NativeControl: synthetic JVM failure', flush=True)
elif mode == 'native-json':
    import json
    print(json.dumps({'level': 'ERROR', 'msg': 'Synthetic native JSON diagnostic'}), flush=True)
elif mode == 'crash':
    print('synthetic raw crash details', flush=True)
    os._exit(23)
""",
        encoding="utf-8",
    )
    return tmp_path


def invoke(project, *options, mode=""):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env["QUIET_TEST_MODE"] = mode
    # Keep test logs disposable and away from the developer's normal run logs.
    for name in ("TEMP", "TMP", "TMPDIR"):
        env[name] = str(project)
    result = subprocess.run(
        [sys.executable, "-X", "utf8", "-m", "odibi", "run", "odibi.yaml", *options],
        cwd=project,
        env=env,
        capture_output=True,
        encoding="utf-8",
        timeout=90,
    )
    return result, result.stdout + result.stderr


@pytest.mark.parametrize("option", [None, "--quiet", "--qui"])
def test_run_preserves_values_and_keeps_full_native_output(project, option):
    result, output = invoke(project, *([option] if option else []))
    assert result.returncode == 0, output
    assert pd.read_csv(project / "output.csv").to_dict("list") == {
        "id": [1, 2, 3],
        "value": [7, 11, -4],
    }
    if option:
        assert "python-info-chatter" not in output
        assert "native-ivy-chatter" not in output
        assert "native-startup-chatter" not in output
        log_path = Path(result.stdout.splitlines()[0].removeprefix("Run log: "))
        assert log_path.is_file()
        full = log_path.read_text(encoding="utf-8")
        assert "python-info-chatter" in full
        assert "native-ivy-chatter" in full
        assert "native-startup-chatter" in full
        assert "Run finished" in output
    else:
        assert "python-info-chatter" in output
        assert "native-ivy-chatter" in output
        assert "native-startup-chatter" in output
        assert not list(project.glob("odibi-run-*.log"))


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("warning", "Failed to save synthetic metadata"),
        ("structured", "Synthetic structured diagnostic"),
        ("native-error", "synthetic JVM failure"),
        ("native-json", "Synthetic native JSON diagnostic"),
    ],
)
def test_quiet_does_not_misreport_success_when_diagnostics_exist(project, mode, message):
    result, output = invoke(project, "--quiet", mode=mode)
    assert result.returncode == 0, output
    assert message in output
    assert "finished with warning/error diagnostics" in output
    assert "Run finished (exit 0)" not in output
    assert "native-ivy-chatter" not in output


@pytest.mark.parametrize("crash", [False, True])
def test_quiet_keeps_failure_exit_code_and_diagnostics(project, crash):
    if not crash:
        (project / "input.csv").unlink()
    result, output = invoke(project, "--quiet", mode="crash" if crash else "")
    assert result.returncode == (23 if crash else 1), output
    assert "Run failed" in output
    assert ("synthetic raw crash details" if crash else "input.csv") in output
    assert "Run finished (exit 0)" not in output
    assert "Run log:" in output


def test_quiet_retains_legacy_simulation_warning(project):
    result, output = invoke(project, "--quiet", "--dry-run")
    assert result.returncode == 0, output
    assert "legacy late runtime simulation; may initialize runtime" in output
    assert "side effects" in output
