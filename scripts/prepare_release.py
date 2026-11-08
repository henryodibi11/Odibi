"""Local-only release draft/check. Requires Python 3.11+ and Git; never publishes."""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def git(repo, *args):
    result = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode:
        raise ValueError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def version_tuple(value):
    if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value):
        raise ValueError("Only stable MAJOR.MINOR.PATCH versions are supported")
    return tuple(map(int, value.split(".")))


def draft(repo, version, tag, previous):
    import tomllib

    version_tuple(version)
    if tag != f"v{version}":
        raise ValueError("Proposed tag must be v followed by the proposed version")
    repo = Path(git(repo, "rev-parse", "--show-toplevel"))
    head = git(repo, "rev-parse", "--verify", "HEAD^{commit}")
    if git(repo, "rev-parse", "--is-shallow-repository") != "false":
        raise ValueError("Shallow history cannot establish the release range; obtain full history")
    if git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise ValueError("Checkout must be clean (including untracked files) before drafting")
    project = tomllib.loads(git(repo, "show", f"{head}:pyproject.toml"))["project"]
    if project["name"] != "odibi" or project["version"] != version:
        raise ValueError("Proposed version must match committed Odibi pyproject.toml version")
    tags = git(repo, "tag", "--list").splitlines()
    if tag in tags:
        raise ValueError(f"Proposed tag {tag} already exists locally; never reuse a release tag")
    ancestors = git(repo, "tag", "--merged", head).splitlines()
    stable = [
        t
        for t in ancestors
        if re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", t)
    ]
    if not stable:
        raise ValueError("No stable ancestor release tag available; verify local tag history")
    latest = max(stable, key=lambda t: version_tuple(t[1:]))
    previous = previous or latest
    if previous not in stable:
        raise ValueError(
            "Previous release must be an exact stable ancestor tag, not a branch or SHA"
        )
    if previous != latest:
        raise ValueError(
            f"Previous release must be latest stable ancestor {latest}; avoid omitted fixes"
        )
    if version_tuple(version) <= version_tuple(previous[1:]):
        raise ValueError("Proposed version must be greater than the prior release")
    prior_sha = git(repo, "rev-parse", "--verify", f"refs/tags/{previous}^{{commit}}")
    git(repo, "merge-base", "--is-ancestor", prior_sha, head)
    if prior_sha == head:
        raise ValueError("No commits since the prior release")
    commit_range = f"{prior_sha}..{head}"
    commits = git(repo, "log", "--reverse", "--format=- %H %s", commit_range)
    changes = git(repo, "diff", "--stat", prior_sha, head)
    try:
        changelog = git(repo, "show", f"{head}:CHANGELOG.md")
    except ValueError:
        changelog = ""
    unreleased = re.search(r"^## \[Unreleased\][^\n]*\n(.*?)(?=^## |\Z)", changelog, re.M | re.S)
    notes = (
        unreleased.group(1).strip() if unreleased else "No Unreleased section found; curate notes."
    )
    if (
        git(repo, "rev-parse", "--verify", f"refs/tags/{previous}^{{commit}}") != prior_sha
        or git(repo, "tag", "--list").splitlines() != tags
    ):
        raise ValueError("Release tags changed during inspection; rerun with stable refs")
    if git(repo, "rev-parse", "HEAD^{commit}") != head or git(
        repo, "status", "--porcelain=v1", "--untracked-files=all"
    ):
        raise ValueError("Checkout changed during inspection; rerun on a stable clean checkout")
    return f"""# Odibi {version} release draft

LOCAL SOURCE CHECKS PASSED. NOT publication approval or evidence of test success.

- Proposed tag: `{tag}` (not created)
- Exact candidate HEAD: `{head}`
- Prior release: `{previous}` at `{prior_sha}`
- Commit range: `{commit_range}`
- Committed project version: `{project["version"]}`
- Checkout: clean; full local history
- Remote tag/release and PyPI state: UNAVAILABLE (not queried); local tags may be stale.

## Required evidence (NOT RUN / unavailable to this helper)

- [ ] Targeted regression tests for every included fix: commands, results, environment, exact SHA.
- [ ] CI on exact candidate SHA: Quality (Python 3.11), Test Base (Python 3.9,
      3.10, 3.11, 3.12), MCP Security (Python 3.11), and Build Status; record run URLs.
- [ ] Review Test Extras and SQL results and any relevant engine-specific evidence;
      do not silently waive failures or use older-SHA results.
- [ ] Build wheel/sdist from this SHA; install wheel outside the source checkout in
      an isolated environment; pip check, metadata/version, CLI and representative
      installed-package smoke. Record artifact hashes and results (see runbook).
- [ ] Confirm full remote tag history and last public release; confirm version absent
      from PyPI and tag absent remotely. This helper cannot prove either condition.
- [ ] Curate user-facing notes, compatibility/migration risks and rollback guidance.
- [ ] Owner explicitly approves this version, exact tag/SHA, notes and evidence before
      publication. Publishing a GitHub release triggers PyPI; publish.yml runs no tests.

## Changelog draft (Unreleased may contain older entries; reconcile with range)

{notes}

## Included commits (review that product fixes are merged)

{commits}

## Changed-file summary

```text
{changes}
```

After owner-authorized publication: verify PyPI artifacts and a fresh installed-package
smoke, then update and validate the separate harness explicitly. No harness change is
implied by this draft. See docs/RELEASE_PREPARATION.md.
"""


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--version", required=True, help="Must match committed project version")
    parser.add_argument("--tag", help="Defaults to vVERSION; must match exactly")
    parser.add_argument(
        "--previous-tag", help="Exact latest stable ancestor; otherwise auto-selected"
    )
    parser.add_argument(
        "--output", type=Path, help="Explicit new UTF-8 Markdown path; never overwritten"
    )
    args = parser.parse_args(argv)
    if sys.version_info < (3, 11):
        parser.exit(1, "error: release preparation requires Python 3.11+ (stdlib tomllib)\n")
    try:
        report = draft(args.repo, args.version, args.tag or f"v{args.version}", args.previous_tag)
        if args.output:
            with args.output.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(report)
            print(f"Draft written to {args.output}; evidence and owner approval remain required.")
        else:
            print(report)
    except (OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"error: {exc}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
