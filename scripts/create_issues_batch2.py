"""Create GitHub Issues batch 2 - assign to Copilot Coding Agent."""
import subprocess
import time

REPO = "henryodibi11/Odibi"

ISSUES = [
    {
        "title": "Fix failing test: test_get_password_fetches_from_key_vault",
        "body": (
            "## Task\n"
            "Fix the failing test in `tests/unit/connections/test_azure_sql.py`::\n"
            "`TestGetPassword::test_get_password_fetches_from_key_vault`\n\n"
            "## Instructions\n"
            "- Run: `pytest tests/unit/connections/test_azure_sql.py::TestGetPassword::test_get_password_fetches_from_key_vault -v`\n"
            "- Read the error output and fix the test\n"
            "- The test should mock Key Vault calls properly\n"
            "- Do NOT modify source code, only fix the test\n\n"
            "## Verify\n"
            "```\npytest tests/unit/connections/test_azure_sql.py -v\n"
            "ruff check tests/unit/connections/test_azure_sql.py\n```"
        ),
    },
    {
        "title": "Regenerate unit tests for connections/azure_adls.py",
        "body": (
            "## Task\n"
            "Add unit tests for `odibi/connections/azure_adls.py`.\n\n"
            "## Important\n"
            "The source code was recently updated:\n"
            "- `_cached_key` was renamed to `_cached_storage_key`\n"
            "- A new `_cached_client_secret` field was added\n"
            "- `get_client_secret()` now uses `_cached_client_secret`\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/connections/test_azure_adls.py`\n"
            "- Use mocks for Azure SDK calls (do NOT make real Azure connections)\n"
            "- Test all auth modes: key_vault, direct_key, sas_token, service_principal, managed_identity\n"
            "- Test that `_cached_storage_key` and `_cached_client_secret` are separate\n"
            "- Test `get_client_secret()` returns client_secret, not storage key\n"
            "- Test `validate()` for each auth_mode\n"
            "- Test error handling for missing credentials\n\n"
            "## Verify\n"
            "```\npytest tests/unit/connections/test_azure_adls.py -v\n"
            "ruff check tests/unit/connections/test_azure_adls.py\n```"
        ),
    },
    {
        "title": "Regenerate unit tests for diagnostics/delta.py and diagnostics/diff.py",
        "body": (
            "## Task\n"
            "Add unit tests for `odibi/diagnostics/delta.py` and `odibi/diagnostics/diff.py`.\n\n"
            "## Important\n"
            "The source code was recently fixed:\n"
            "- `operations_between` was renamed to `operations` in `DeltaDiffResult`\n"
            "- Make sure tests use the correct field name `operations`\n\n"
            "## Instructions\n"
            "- Create: `tests/unit/test_diagnostics_delta.py` and `tests/unit/test_diagnostics_diff.py`\n"
            "- Test `DeltaDiffResult` dataclass fields\n"
            "- Test `detect_drift` function\n"
            "- Test `_get_delta_diff_pandas` with mocked DeltaTable\n"
            "- Test diff utilities in diff.py\n"
            "- Use pandas DataFrames, mock any Delta Lake dependencies\n\n"
            "## Verify\n"
            "```\npytest tests/unit/test_diagnostics_delta.py tests/unit/test_diagnostics_diff.py -v\n"
            "ruff check tests/unit/test_diagnostics_delta.py tests/unit/test_diagnostics_diff.py\n```"
        ),
    },
    {
        "title": "Add docstrings to all public methods in engine/pandas_engine.py",
        "body": (
            "## Task\n"
            "Add Google-style docstrings to all public methods in `odibi/engine/pandas_engine.py`.\n\n"
            "## Instructions\n"
            "- Only add docstrings to methods that are MISSING them\n"
            "- Do NOT modify existing docstrings\n"
            "- Use Google-style format:\n"
            "```python\n"
            "def method(self, arg: str) -> pd.DataFrame:\n"
            '    """Short description.\n\n'
            "    Args:\n"
            "        arg: Description of arg.\n\n"
            "    Returns:\n"
            "        Description of return value.\n\n"
            "    Raises:\n"
            '        ValueError: If arg is invalid.\n    """\n```\n'
            "- Do NOT change any logic or code\n\n"
            "## Verify\n"
            "```\npytest tests/test_pandas_engine_full_coverage.py -v\n"
            "ruff check odibi/engine/pandas_engine.py\n```"
        ),
    },
    {
        "title": "Add docstrings to all public methods in engine/polars_engine.py",
        "body": (
            "## Task\n"
            "Add Google-style docstrings to all public methods in `odibi/engine/polars_engine.py`.\n\n"
            "## Instructions\n"
            "- Only add docstrings to methods that are MISSING them\n"
            "- Do NOT modify existing docstrings\n"
            "- Use Google-style format (see pandas_engine.py for examples)\n"
            "- Do NOT change any logic or code\n\n"
            "## Verify\n"
            "```\npytest tests/test_polars_engine.py -v\n"
            "ruff check odibi/engine/polars_engine.py\n```"
        ),
    },
    {
        "title": "Add type hints to functions missing them in transformers/",
        "body": (
            "## Task\n"
            "Add type hints to all public functions in `odibi/transformers/` that are missing them.\n\n"
            "## Instructions\n"
            "- Check each file in `odibi/transformers/`: advanced.py, delete_detection.py, "
            "manufacturing.py, merge_transformer.py, relational.py, scd.py, sql_core.py, "
            "thermodynamics.py, units.py, validation.py\n"
            "- Add parameter type hints and return type hints to functions missing them\n"
            "- Use types from `typing` module and `pandas` as needed\n"
            "- Common patterns: `context: EngineContext`, `params: SomeParams`, `-> EngineContext`\n"
            "- Do NOT change any logic or code\n\n"
            "## Verify\n"
            "```\npytest tests/unit/transformers/ -v\n"
            "ruff check odibi/transformers/\n```"
        ),
    },
    {
        "title": "Audit engine parity: list methods missing from polars_engine.py",
        "body": (
            "## Task\n"
            "Compare `odibi/engine/pandas_engine.py` and `odibi/engine/polars_engine.py` "
            "and identify all public methods in pandas that are missing from polars.\n\n"
            "## Instructions\n"
            "- Read both files\n"
            "- List all public methods (not starting with `_`) in each\n"
            "- Create a markdown report at `docs/engine_parity_report.md` with:\n"
            "  - Methods in both engines\n"
            "  - Methods only in pandas_engine\n"
            "  - Methods only in polars_engine\n"
            "  - Recommended priority for implementing missing methods\n"
            "- Do NOT implement any missing methods, only create the report\n\n"
            "## Verify\n"
            "```\npython -c \"import pathlib; assert pathlib.Path('docs/engine_parity_report.md').exists()\"\n```"
        ),
    },
    {
        "title": "Create GitHub Actions CI workflow for pytest and ruff",
        "body": (
            "## Task\n"
            "Create a GitHub Actions workflow that runs pytest and ruff on every PR.\n\n"
            "## Instructions\n"
            "- Create `.github/workflows/ci.yml`\n"
            "- Trigger on: push to main, pull_request to main\n"
            "- Python version: 3.9\n"
            "- Steps:\n"
            "  1. Checkout code\n"
            "  2. Set up Python 3.9\n"
            "  3. Install dependencies: `pip install -e .[dev]`\n"
            "  4. Run ruff: `ruff check .`\n"
            "  5. Run pytest: `pytest tests/ -v --tb=short` (skip spark/delta tests)\n"
            "- Set env var `SKIP_SPARK=1` if needed\n"
            "- Do NOT include any secrets or tokens\n"
            "- Use `continue-on-error: false` for both steps\n\n"
            "## Verify\n"
            "```\npython -c \"import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))\"\n```"
        ),
    },
    {
        "title": "Improve error messages in pattern classes with context",
        "body": (
            "## Task\n"
            "Improve error messages in `odibi/patterns/` to include context (node name, pipeline).\n\n"
            "## Instructions\n"
            "- Check all files in `odibi/patterns/`\n"
            "- Find `raise` statements with generic messages\n"
            "- Add context like node name, pipeline name, column names where available\n"
            '- Example: change `raise ValueError("Missing column")` to '
            "`raise ValueError(f\"Missing column '{col}' in node '{node_name}'\")`\n"
            "- Use `get_logging_context()` for structured logging where appropriate\n"
            "- Do NOT change logic, only improve error messages\n\n"
            "## Verify\n"
            "```\npytest tests/ -v --tb=short -k 'pattern'\n"
            "ruff check odibi/patterns/\n```"
        ),
    },
    {
        "title": "Update README.md with current test count and feature list",
        "body": (
            "## Task\n"
            "Update `README.md` with current project stats.\n\n"
            "## Instructions\n"
            "- Run `pytest tests/ --co -q` to count total tests\n"
            "- Run `odibi list transformers` to count transformers\n"
            "- Run `odibi list patterns` to count patterns\n"
            "- Update README.md with:\n"
            "  - Current test count (2700+)\n"
            "  - Number of transformers (50+)\n"
            "  - Number of patterns (6)\n"
            "  - Supported engines: Pandas, Polars, Spark\n"
            "- Keep existing README structure, just update numbers/stats\n"
            "- Do NOT remove any existing content\n\n"
            "## Verify\n"
            "```\nruff check README.md 2>/dev/null; echo 'README updated'\n```"
        ),
    },
]


def main():
    print(f"Creating {len(ISSUES)} GitHub Issues and assigning to Copilot...\n")

    for i, issue in enumerate(ISSUES, 1):
        print(f"[{i}/{len(ISSUES)}] {issue['title']}")
        result = subprocess.run(
            [
                "gh",
                "issue",
                "create",
                "--repo",
                REPO,
                "--title",
                issue["title"],
                "--body",
                issue["body"],
                "--label",
                "copilot",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            url = result.stdout.strip()
            issue_num = url.split("/")[-1]
            print(f"  -> {url}")
            # Assign to Copilot
            assign_result = subprocess.run(
                [
                    "gh",
                    "issue",
                    "edit",
                    issue_num,
                    "--repo",
                    REPO,
                    "--add-assignee",
                    "copilot-swe-agent[bot]",
                ],
                capture_output=True,
                text=True,
            )
            if assign_result.returncode == 0:
                print("  -> Assigned to Copilot")
            else:
                print(f"  -> Could not auto-assign: {assign_result.stderr.strip()}")
                print(f"     Manually assign at: {url}")
        else:
            print(f"  -> ERROR: {result.stderr.strip()}")
        time.sleep(2)

    print(f"\nDone! Check https://github.com/{REPO}/issues")
    print("If auto-assign failed, manually assign each issue to Copilot on GitHub.")


if __name__ == "__main__":
    main()
