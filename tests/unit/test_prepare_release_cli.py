"""Public subprocess contract for the offline release draft helper."""

import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "prepare_release.py"
pytestmark = pytest.mark.skipif(sys.version_info < (3, 11), reason="helper requires stdlib tomllib")


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


@pytest.fixture
def candidate(tmp_path):
    git(tmp_path, "init")
    git(tmp_path, "config", "user.name", "Release CLI Test")
    git(tmp_path, "config", "user.email", "release-test@example.invalid")
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\nname = "odibi"\nversion = "1.0.0"\n', encoding="utf-8")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-m", "prior release")
    git(tmp_path, "tag", "-a", "v1.0.0", "-m", "prior")
    project.write_text('[project]\nname = "odibi"\nversion = "1.0.1"\n', encoding="utf-8")
    (tmp_path / "CHANGELOG.md").write_text(
        "# Changelog\n\n## [Unreleased]\n\n- Fix useful behavior\n\n## [1.0.0]\nOld notes\n",
        encoding="utf-8",
    )
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-m", "fix useful behavior")
    return tmp_path


def run(repo, *args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo), "--version", "1.0.1", *args],
        capture_output=True,
        text=True,
    )


def test_draft_is_read_only_and_binds_exact_commits(candidate, tmp_path_factory):
    head = git(candidate, "rev-parse", "HEAD")
    prior = git(candidate, "rev-parse", "v1.0.0^{commit}")
    result = run(candidate)
    assert result.returncode == 0, result.stderr
    assert f"{prior}..{head}" in result.stdout
    assert "NOT RUN / unavailable" in result.stdout
    assert "Fix useful behavior" in result.stdout
    assert "Old notes" not in result.stdout
    assert git(candidate, "status", "--porcelain") == ""
    assert git(candidate, "tag", "--list") == "v1.0.0"
    output = tmp_path_factory.mktemp("draft") / "release.md"
    written = run(candidate, "--output", str(output))
    assert written.returncode == 0, written.stderr
    assert output.read_text(encoding="utf-8").strip() == result.stdout.strip()
    refused = run(candidate, "--output", str(output))
    assert refused.returncode != 0
    assert output.read_text(encoding="utf-8").strip() == result.stdout.strip()


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--tag", "v1.0.2"], "Proposed tag must"),
        (["--version", "1.0.2"], "must match committed"),
        (["--previous-tag", "HEAD"], "exact stable ancestor"),
    ],
)
def test_rejects_inconsistent_release_identity(candidate, args, message):
    result = run(candidate, *args)
    assert result.returncode != 0
    assert message in result.stderr


def test_dirty_candidate_cannot_write_a_draft(candidate):
    (candidate / "untracked.txt").write_text("unfinished fix", encoding="utf-8")
    output = candidate / "release.md"
    result = run(candidate, "--output", str(output))
    assert result.returncode != 0
    assert "must be clean" in result.stderr
    assert not output.exists()


def test_existing_proposed_tag_is_never_reused(candidate):
    git(candidate, "tag", "v1.0.1")
    result = run(candidate)
    assert result.returncode != 0
    assert "already exists locally" in result.stderr
