# Preparing an Odibi release (not publishing it)

Batch merged, regression-tested bug fixes into a meaningful patch release: a coherent
set of user-visible corrections with useful notes, not one release per commit or an
arbitrary commit-count threshold. Do not delay a blocking production regression or
security fix to fill a batch: prepare the smallest reviewed hotfix immediately, with
the same evidence and owner gate. Handle security details through the owner's private
process rather than putting exploitable details in a public draft prematurely.

## Local source check and draft

Use **Python 3.11+**, Git and the standard library only. This is a repository helper,
not a second release system or an installed `odibi` command. It never fetches, installs,
changes versions, creates tags/releases, commits, pushes or publishes. No credentials
are needed. A successful exit means **local source prerequisites only**, never that
tests passed or publication is authorized.

1. Review merged fixes on main and their regression evidence. Confirm the last public
   GitHub release/tag and PyPI version with the owner. Obtain complete up-to-date local
   history/tags through your normal authorized checkout process; this offline helper
   cannot detect missing/stale tags or unpublished local tags. Do not assume the
   highest local tag is necessarily the last public release.
2. Separately propose and integrate a `pyproject.toml` version bump and curated
   `CHANGELOG.md` notes through normal review. `odibi.__version__` comes from installed
   distribution metadata: do not edit it or trust an editable install as release proof.
   This task itself does not bump the current version or prepare a real release.
3. On the **clean, committed final candidate checkout**, run (example next patch only):

   ```powershell
   python scripts\prepare_release.py --version 3.15.2 --previous-tag v3.15.1
   python scripts\prepare_release.py --version 3.15.2 --tag v3.15.2 --previous-tag v3.15.1 --output C:\path\to\existing\review-folder\odibi-3.15.2.md
   ```

   Default output is stdout, with no writes. `--output` requires an existing parent
   directory and a nonexistent file; it refuses overwrite (including symlinks).
   Keep drafts in an ignored review-artifact folder so repeated checks stay clean.
   `--repo` can select the candidate checkout explicitly. Invalid prerequisites exit
   nonzero without creating a draft. Stable `MAJOR.MINOR.PATCH` / `vMAJOR.MINOR.PATCH`
   only; prereleases require a separately reviewed procedure, not guessed ordering.
   The automatic prior tag is the highest-version stable ancestor; an explicit tag
   must match it to avoid silently omitting fixes. No prior tag, shallow history,
   dirty files, an existing proposed local tag, a non-increasing/mismatched version,
   or an empty range blocks preparation.
4. Review exact full HEAD and prior tag's peeled commit, included commit range,
   changed-file summary, and Unreleased notes. Notes are copied, **not** certified:
   the existing Unreleased section can contain entries from older releases. Reconcile
   it against included fixes; identify merged PRs, user impact, migrations and risks.

## Evidence gate before asking for publication

Keep commands, environment, results and CI URLs bound to the **exact final SHA**.
Changing notes, version or source changes the SHA: regenerate and requalify it.

- Run bounded regression tests for included fixes. Do not install broad test extras or
  claim full-suite success from a focused local run. Unavailable evidence is a blocker
  to obtain from CI or a qualified environment, not a passing result.
- Require the CI Quality job (pinned Ruff `0.14.6` full formatting/lint policy), all
  Test Base Python 3.9/3.10/3.11/3.12 cells, MCP Security, and Build Status to pass on
  that SHA. Consult `.github/workflows/ci.yml`, not stale comments: Extras currently
  has `continue-on-error: false` but is not in Build Status; SQL is allowed to fail.
  Review both and resolve failures relevant to the batch explicitly. Branch protection
  configuration is external and is not inferred from the workflow.
- Build wheel **and sdist** from the exact clean candidate in an isolated build
  environment (`python -m build` when the build frontend is available). Record SHA256
  hashes, build logs and versions. Install the wheel into a fresh environment with
  approved dependencies; run **outside** the repository, with no source `PYTHONPATH`:

  ```powershell
  python -m pip check
  python -c "from importlib.metadata import version; import odibi; assert version('odibi') == '3.15.2'; assert odibi.__version__ == '3.15.2'; print(odibi.__file__)"
  odibi --help
  ```

  Verify `odibi.__file__` is in that environment's site-packages, not any checkout.
  Add a tiny local fixture pipeline smoke representative of the included fix and its
  engine; no live connections, cloud resources or secrets. For MCP changes, retain the
  installed-root-wheel MCP Security CI results and relevant authorization/smoke tests.
  Smoke an sdist-built wheel too when packaging changed. Record actual results; this
  helper intentionally does not execute builds, tests or dependency installations.

## Owner gate and post-publication handoff

Present the draft, version, exact **new tag -> full commit SHA**, fix/PR list, evidence,
known limitations and rollback/forward-fix plan. Confirm the proposed version is not
already on PyPI and tag is not remote. Require explicit owner approval for publication
of that exact candidate. A draft document is not a GitHub release, and approval to
implement/merge changes is **not** approval to publish a release/package.

Only an explicitly authorized owner/operator may subsequently create/push the exact
tag and publish its GitHub release. Verify the remote tag peels to the approved SHA,
not a moving branch tip. **Publishing the GitHub release triggers
`.github/workflows/publish.yml`, which builds and uploads to PyPI and runs no tests.**
It is not the evidence gate. Do not rely on it to catch a bad version or failed CI.
Never move/reuse an existing release tag to repair a release; coordinate a new version.

After publication, inspect the Publish workflow result and actual PyPI wheel/sdist
version and hashes/provenance. Repeat the fresh-environment installed-package smoke
using `odibi==<approved-version>` from PyPI (outside source); record artifact/version,
`pip check`, CLI and representative pipeline results. A green upload is not smoke
evidence. Stop and inform the owner on discrepancies; use a reviewed forward fix.

Finally hand the verified package/version and smoke evidence to the separate harness
workstream. Update its pin/environment and run its acceptance checks **separately**
under its own approval/scope. Neither this helper nor an Odibi publication updates
the harness or any live environment automatically.
