# Git & Azure DevOps — Complete Guide for Data Engineers

> Written for someone who knows nothing about Git. Every concept is explained before the command.

---

## Table of Contents

1. [What Is Git and Why Should I Care?](#1-what-is-git-and-why-should-i-care)
2. [Key Concepts (The Mental Model)](#2-key-concepts-the-mental-model)
3. [One-Time Setup](#3-one-time-setup)
4. [Starting a Project](#4-starting-a-project)
5. [The Daily Workflow](#5-the-daily-workflow)
6. [Branching](#6-branching)
7. [Working with Remotes (Azure DevOps)](#7-working-with-remotes-azure-devops)
8. [Pull Requests](#8-pull-requests)
9. [Undoing Mistakes](#9-undoing-mistakes)
10. [Viewing History & Investigating](#10-viewing-history--investigating)
11. [Writing Professional Commit Messages](#11-writing-professional-commit-messages)
12. [.gitignore — Keeping Junk Out](#12-gitignore--keeping-junk-out)
13. [Common Scenarios (Recipes)](#13-common-scenarios-recipes)
14. [Troubleshooting Common Errors](#14-troubleshooting-common-errors)
15. [Glossary](#15-glossary)
16. [Quick Reference Card](#16-quick-reference-card)

---

## 1. What Is Git and Why Should I Care?

**Git** is a version control system. Think of it as an **unlimited undo button** for your entire project that also lets you:

- **Track every change** you ever made (who changed what, when, and why)
- **Go back in time** to any previous version of your code
- **Experiment safely** on a separate branch without breaking what works
- **Collaborate** with others without overwriting each other's work
- **Back up your code** to a remote server (Azure DevOps, GitHub, etc.)

**Without Git:** You end up with `pipeline_v2_final_FINAL_fixed.py` on your desktop.
**With Git:** You have one file, and Git remembers every version automatically.

### Git vs Azure DevOps

| Tool | What It Is |
|------|-----------|
| **Git** | The tool on your computer that tracks changes (runs in PowerShell) |
| **Azure DevOps** | A website/server that stores your Git repos online and adds features like pull requests, CI/CD, and permissions |

Git works completely offline. Azure DevOps is where you **push** your code so it's backed up and visible to your team.

---

## 2. Key Concepts (The Mental Model)

### The Three Areas

Every Git project has three areas on your computer:

```
┌─────────────────┐     git add     ┌─────────────────┐    git commit    ┌─────────────────┐
│  Working        │ ──────────────> │  Staging Area    │ ──────────────> │  Repository      │
│  Directory      │                 │  (Index)         │                 │  (History)       │
│                 │                 │                  │                 │                  │
│  Your actual    │                 │  "Ready to       │                 │  Permanent       │
│  files on disk  │                 │   commit" pile   │                 │  snapshots       │
└─────────────────┘                 └─────────────────┘                 └─────────────────┘
```

1. **Working Directory** — Your actual files. Edit them normally.
2. **Staging Area** — A "loading dock" where you place files you want to save. Like putting items in a box before shipping.
3. **Repository** — The permanent history. Each commit is a snapshot you can always go back to.

**Analogy:** Writing a book.
- Working Directory = your draft on the desk
- Staging Area = pages you've selected to send to the publisher
- Repository = published editions (v1, v2, v3...)

### What Is a Commit?

A **commit** is a snapshot of your project at a specific point in time. Each commit has:
- A unique ID (a hash like `a1b2c3d`)
- A message describing what changed ("fix null handling in reader")
- A timestamp
- A pointer to the previous commit

Think of commits as **save points** in a video game. You can always reload from any save point.

### What Is a Branch?

A **branch** is a parallel line of development. By default, you have one branch called `main`.

```
main:      A ── B ── C
                      \
feature:               D ── E     ← you're working here
```

- You create a branch to work on something without affecting `main`
- When you're done, you **merge** the branch back into `main`
- If something goes wrong, `main` is untouched

### What Is a Remote?

A **remote** is a copy of your repo on a server (Azure DevOps). Your local repo is linked to it.

```
Your Computer                    Azure DevOps
┌──────────────┐    git push     ┌──────────────┐
│  Local Repo  │ ──────────────> │  Remote Repo │
│              │ <────────────── │  (origin)    │
└──────────────┘    git pull     └──────────────┘
```

- `git push` — sends your commits to the server
- `git pull` — downloads new commits from the server
- `origin` — the default name for your remote (just a bookmark to the URL)

---

## 3. One-Time Setup

### Install Git

Download from https://git-scm.com/download/win and install with defaults.

### Tell Git Who You Are

Git stamps every commit with your name and email. Set this once:

```powershell
git config --global user.name "Your Name"
git config --global user.email "your.email@company.com"
```

### Set Default Branch Name

Modern convention is `main` instead of `master`:

```powershell
git config --global init.defaultBranch main
```

### Check Your Config

```powershell
git config --list
```

---

## 4. Starting a Project

You have two starting points:

### Option A: Start Fresh (New Project)

```powershell
mkdir my-project
cd my-project
git init
```

`git init` creates a hidden `.git/` folder that tracks everything. Your folder is now a Git repo.

### Option B: Clone an Existing Repo

```powershell
git clone https://dev.azure.com/YOUR_ORG/YOUR_PROJECT/_git/repo-name
cd repo-name
```

`git clone` downloads the entire repo (all files + all history) and automatically sets `origin` to point back at the server.

### When to Use Which?

| Situation | Use |
|-----------|-----|
| You have code locally, repo is empty on Azure DevOps | `git init` + `git remote add` + `git push` |
| Repo already exists on Azure DevOps with code | `git clone` |
| Starting from scratch, nothing exists yet | Either works |

---

## 5. The Daily Workflow

This is what you'll do 90% of the time:

### Step 1: Check What's Changed

```powershell
git status
```

**What it shows:**
- 🔴 Red files = changed but not staged
- 🟢 Green files = staged and ready to commit
- ❓ Untracked files = new files Git doesn't know about yet

**Run this constantly.** It's your dashboard.

### Step 2: Stage Files

```powershell
# Stage specific files (recommended)
git add datakit/core/enums.py
git add datakit/tests/test_reader.py

# Stage all changes in a specific folder
git add datakit/tests/

# Stage everything (use carefully!)
git add .
```

**⚠️ Rule:** Prefer staging specific files. `git add .` grabs everything, including files you might not want (temp files, secrets, etc.).

### Step 3: Verify What's Staged

```powershell
git status          # see which files are staged
git diff --staged   # see the actual line-by-line changes that will be committed
```

### Step 4: Commit

```powershell
git commit -m "fix: resolve null handling in pandas reader"
```

See [Section 11: Writing Professional Commit Messages](#11-writing-professional-commit-messages) for full guidance and templates.

### Step 5: Push to Azure DevOps

```powershell
git push
```

That's it. Your commits are now backed up on the server.

### The Complete Daily Flow

```powershell
# 1. Start of day — get latest changes
git pull

# 2. Do your work... edit files normally

# 3. Check what changed
git status

# 4. Stage the files you want to save
git add datakit/validation/gate.py
git add datakit/tests/test_gate.py

# 5. Commit with a message
git commit -m "feat: add quality gate thresholds"

# 6. Push to Azure DevOps
git push
```

---

## 6. Branching

### Why Branch?

- **Protection:** `main` always stays in a working state
- **Isolation:** Your half-finished work doesn't affect anyone
- **Required:** Azure DevOps branch policies often block direct pushes to `main`

### Creating and Switching Branches

```powershell
# Create a new branch AND switch to it
git checkout -b feature/add-delta-merge

# Switch to an existing branch
git checkout main

# See all branches (* = current)
git branch
```

### Branch Naming Conventions

```
feature/add-scd2-pattern      ← new feature
fix/null-handling-reader       ← bug fix
test/validation-coverage       ← adding tests
refactor/flatten-project       ← restructuring
```

### The Branch Workflow

```powershell
# 1. Start from main (make sure it's up to date)
git checkout main
git pull

# 2. Create your branch
git checkout -b feature/add-delta-merge

# 3. Do your work... edit files, test, etc.

# 4. Stage and commit (repeat as needed)
git add .
git commit -m "feat: implement delta merge manager"

# 5. Push the branch to Azure DevOps
git push -u origin feature/add-delta-merge
#   -u = sets upstream tracking (only needed the first push of a new branch)
#   After this first push, just use: git push

# 6. Create a Pull Request on Azure DevOps (in the browser)

# 7. After PR is merged, clean up locally
git checkout main
git pull
git branch -d feature/add-delta-merge    # delete local branch
```

### Visualizing Branches

```powershell
git log --oneline --graph --all
```

Shows a visual tree of all branches and commits.

---

## 7. Working with Remotes (Azure DevOps)

### What Is a Remote?

A remote is a bookmark to a server URL. `origin` is the default name.

```powershell
# See your remotes
git remote -v

# Add a remote
git remote add origin https://dev.azure.com/ORG/PROJECT/_git/REPO

# Change a remote's URL
git remote set-url origin https://dev.azure.com/ORG/PROJECT/_git/NEW-REPO

# Remove a remote
git remote remove origin
```

### Push and Pull

```powershell
# Push your commits to the remote
git push

# Pull (download) new commits from the remote
git pull

# First push of a new branch (sets tracking)
git push -u origin branch-name
```

### What Does `-u` Do?

`-u` (or `--set-upstream`) tells Git "remember that this local branch tracks this remote branch." After setting it once, you can just type `git push` and `git pull` without specifying the remote and branch name.

### Force Push

```powershell
git push --force
```

**What it does:** Overwrites the remote history with your local history.
**When to use:** Only when the remote has junk you want to replace (like a default README on a brand new repo).
**⚠️ Warning:** Never force push to a shared branch unless you know what you're doing — it can erase other people's work.

---

## 8. Pull Requests

### What Is a Pull Request (PR)?

A PR is a **request to merge your branch into another branch** (usually `main`). It:

- Shows all the changes you made (the diff)
- Lets reviewers comment and approve
- Runs automated checks (if configured)
- Creates a merge commit when completed

### The PR Workflow

1. **Push your branch** to Azure DevOps
2. **Go to Azure DevOps** → Repos → Pull Requests → "New Pull Request"
3. **Select** source branch (yours) → target branch (`main`)
4. **Add a title and description** explaining your changes
5. **Review the diff** — make sure only your intended changes are there
6. **Complete the PR** (merge)
7. **Delete the source branch** (Azure DevOps offers this option)

### After Merging a PR

```powershell
git checkout main        # switch back to main
git pull                 # download the merged changes
git branch -d old-branch # delete the old local branch
```

---

## 9. Undoing Mistakes

### "I edited a file and want to undo it" (before staging)

```powershell
git checkout -- filename.py
# or (newer syntax)
git restore filename.py
```

⚠️ This permanently discards your changes to that file.

### "I staged a file but don't want to commit it yet" (unstage)

```powershell
git reset HEAD filename.py
# or (newer syntax)
git restore --staged filename.py
```

The file stays modified, just removed from the staging area.

### "I committed but want to change the message"

```powershell
git commit --amend -m "better message here"
```

⚠️ Only do this if you **haven't pushed yet**.

### "I committed but forgot a file"

```powershell
git add forgotten_file.py
git commit --amend --no-edit
```

Adds the file to the previous commit without changing the message. ⚠️ Only before pushing.

### "I committed and want to undo it completely"

```powershell
# Undo the last commit but keep the changes in your working directory
git reset --soft HEAD~1

# Undo the last commit AND discard the changes (⚠️ permanent)
git reset --hard HEAD~1
```

`HEAD~1` means "one commit before the current one." `HEAD~3` = three commits back.

### "I pushed something bad to the remote"

```powershell
# Create a new commit that undoes the bad commit (safe for shared branches)
git revert <commit-hash>
git push
```

`revert` doesn't erase history — it creates a new commit that undoes the changes. This is safe because it doesn't rewrite history.

---

## 10. Viewing History & Investigating

### See Commit History

```powershell
# Simple log
git log --oneline

# Detailed log
git log

# Visual branch graph
git log --oneline --graph --all

# Last 5 commits
git log --oneline -5
```

### See What Changed in a Commit

```powershell
git show <commit-hash>
```

### See Changes Not Yet Staged

```powershell
git diff
```

### See Changes That Are Staged

```powershell
git diff --staged
```

### See Who Changed a Specific Line (Blame)

```powershell
git blame filename.py
```

Shows each line with who last changed it and when. Useful for "who wrote this and why?"

### Search Commit Messages

```powershell
git log --grep="delta merge"
```

---

## 11. Writing Professional Commit Messages

Your commit messages are a **permanent record** of why your code changed. Six months from now, when something breaks, the commit log is the first place people look. Good messages save hours of debugging.

### The Format

```
<type>: <short summary in present tense>
```

**One line, under 72 characters.** That's it for most commits.

For complex changes, add a body:

```
<type>: <short summary>

<blank line>
<detailed explanation — what changed and WHY>
<optional: bullet points for multiple changes>
```

Example with body:

```powershell
git commit -m "fix: handle null columns in validation gate" -m "" -m "The gate was counting null rows as passing, which inflated the pass rate. Now nulls in monitored columns are counted as failures."
```

> The `-m "" -m "body text"` trick adds a blank line then a body paragraph.

### Types (Prefixes)

Always start with a type. This makes the log scannable at a glance.

| Type | When to Use | Example |
|------|-------------|---------|
| `feat:` | New functionality | `feat: add foreign key validation module` |
| `fix:` | Bug fix | `fix: resolve off-by-one in date dimension generator` |
| `refactor:` | Code restructuring, no behavior change | `refactor: flatten project from nested to flat hierarchy` |
| `test:` | Adding or fixing tests | `test: add unit tests for quarantine split logic` |
| `docs:` | Documentation only | `docs: add getting started guide with code examples` |
| `chore:` | Maintenance, dependencies, configs | `chore: update pyspark dependency to 3.5.0` |
| `perf:` | Performance improvement | `perf: replace row-by-row loop with vectorized operation` |
| `style:` | Formatting, whitespace (no logic change) | `style: apply ruff formatting to validation module` |
| `ci:` | CI/CD pipeline changes | `ci: add pytest step to Azure DevOps pipeline` |
| `build:` | Build system or dependencies | `build: add fastavro to pandas extras in pyproject.toml` |

### The Rules

1. **Use present tense** — `"add feature"` not `"added feature"`
2. **Use imperative mood** — `"fix bug"` not `"fixes bug"` or `"fixed bug"`
3. **Don't end with a period** — `"add validation"` not `"add validation."`
4. **Be specific** — describe *what*, not *that* you did something
5. **One commit = one logical change** — don't bundle unrelated changes

Think of it as completing the sentence: *"If applied, this commit will \_\_\_\_\_\_."*

- ✅ "If applied, this commit will **add SCD2 pattern for dimension tables**"
- ❌ "If applied, this commit will **updated stuff**" ← doesn't make sense

### Good vs Bad Examples

**❌ Bad — vague, says nothing:**
```
"update"
"changes"
"fix stuff"
"WIP"
"asdfasdf"
"final version"
"misc updates"
```

**✅ Good — specific, scannable, professional:**
```
feat: add quality gate with configurable pass thresholds
fix: handle missing columns in schema validator
refactor: extract SQL builder into standalone module
test: add edge case tests for empty DataFrame in quarantine
docs: update README with Databricks setup instructions
chore: remove unused odibi_de_v2 imports
fix: correct file path resolution for Windows in pandas saver
```

### Templates by Scenario

Copy-paste and fill in the blanks:

**Adding a new feature:**
```
feat: add <what> for <purpose>
```
```
feat: add date dimension generator with fiscal year support
feat: add Avro reader to PandasDataReader
feat: add surrogate key assignment in dimension builder
```

**Fixing a bug:**
```
fix: <what was wrong> in <where>
```
```
fix: resolve null handling in quarantine split
fix: correct ABFS path format for Pandas framework
fix: prevent duplicate rows in SCD2 merge output
```

**Refactoring:**
```
refactor: <what you restructured> in <where>
```
```
refactor: flatten nested project structure to single level
refactor: replace decorator stack with plain function in validation
refactor: extract common join logic into shared helper
```

**Tests:**
```
test: add <what kind> tests for <what module>
```
```
test: add unit tests for foreign key validation
test: fix mock path in azure blob connector test
test: add edge cases for empty and null DataFrames
```

**Multiple related changes in one commit:**
```powershell
git commit -m "fix: resolve 17 test failures across test suite" -m "" -m "- Fix mock patch paths for azure blob connector
- Add missing ErrorType enum values (PERMISSION_ERROR, IO_ERROR)
- Create test data fixtures for pandas reader tests
- Fix positional args in wrap_read_errors decorator
- Use tempfile for Windows-compatible test paths"
```

### What Your Log Should Look Like

After following these conventions, `git log --oneline` reads like a changelog:

```
a3f1d2e feat: add quality gate with configurable thresholds
b7c4e8a fix: handle null columns in schema validator
c9d2f1b test: add unit tests for quarantine module
d1e3a4c refactor: flatten project from nested to flat hierarchy
e5f6b7d docs: add getting started guide
f8a9c0e chore: update pyspark to 3.5.0
g2h3i4j feat: add SCD2 pattern for dimension tables
```

Anyone can read this and understand the project's evolution without opening a single file.

### Commit Frequency

- **Commit often** — small, focused commits are easier to review and revert
- **Each commit should compile/run** — don't commit broken code
- **One logical change per commit** — split "add feature + fix bug" into two commits

**Too big:** `"refactor entire project, add tests, fix bugs, update docs"`
**Just right:** Four separate commits, one for each concern.

---

## 12. .gitignore — Keeping Junk Out

A `.gitignore` file tells Git which files to **never track**. Put it in your repo root.

### Example .gitignore for Python/Data Engineering

```
# Python
__pycache__/
*.pyc
*.pyo
.mypy_cache/
.pytest_cache/
*.egg-info/
dist/
build/

# Virtual environments
.venv/
.env

# IDE
.vscode/
.idea/

# OS junk
.DS_Store
Thumbs.db

# Secrets (NEVER commit these)
*.key
*.pem
secrets.yaml

# Data files (usually too large for Git)
*.csv
*.parquet
*.avro
data/raw/

# Logs
*.log
```

### Rules

| Pattern | Matches |
|---------|---------|
| `*.pyc` | All .pyc files everywhere |
| `__pycache__/` | All __pycache__ directories |
| `data/` | The data directory at the repo root |
| `**/data/` | Any directory named data, anywhere |
| `!important.csv` | Exception — DO track this file |

### Already Tracked a File You Want to Ignore?

```powershell
# Remove from Git tracking (keeps the file on disk)
git rm --cached filename.py
# Then add it to .gitignore and commit
```

---

## 13. Common Scenarios (Recipes)

### "I want to start tracking an existing project"

```powershell
cd my-project
git init
git add .
git commit -m "initial commit"
git remote add origin https://dev.azure.com/ORG/PROJECT/_git/REPO
git push -u origin main
```

### "I need to work on two things at once"

```powershell
# Save current work on a branch
git checkout -b feature/thing-one
git add .
git commit -m "wip: thing one progress"

# Switch to a new branch for thing two
git checkout main
git checkout -b feature/thing-two
# ... work on thing two ...

# Come back to thing one later
git checkout feature/thing-one
```

### "I want to temporarily save my work without committing"

```powershell
# Stash your changes (hides them away)
git stash

# Do other stuff...

# Bring your changes back
git stash pop

# See all stashes
git stash list
```

**When to use stash:** You're in the middle of something but need to quickly switch branches. You don't want to commit half-done work.

### "I need to get a specific file from another branch"

```powershell
git checkout other-branch -- path/to/file.py
```

### "Someone else pushed changes and I need to get them"

```powershell
git pull
```

If you have local changes that conflict:
```powershell
git stash          # hide your changes
git pull           # get the remote changes
git stash pop      # bring your changes back
# resolve any conflicts if they appear
```

### "A reviewer asked me to split a big PR into multiple smaller PRs"

This happens when your branch touches multiple scopes (e.g., pipeline code, framework updates, and config changes) and a reviewer wants separate, focused PRs.

**Step 1: See what files your branch changed**
```powershell
git diff --name-only main..your-branch | sort
```

**Step 2: Plan the split — mentally group files into scopes**

Example:
- `queue-automation/**` → PR #1
- `datakit/**`, `pyproject.toml` → PR #2
- `agents/**`, `genie/**` → PR #3

**Step 3: Create a clean branch per scope, pulling only those files**
```powershell
# Branch 1: queue automation
git checkout -b pr/queue-automation main
git checkout your-branch -- queue-automation/
git commit -m "feat: queue automation pipelines and docs"

# Branch 2: datakit updates
git checkout -b pr/datakit-updates main
git checkout your-branch -- datakit/ pyproject.toml
git commit -m "feat(datakit): read_auto, io strategies, utils updates"

# Branch 3: genie skills
git checkout -b pr/genie-skills main
git checkout your-branch -- agents/
git commit -m "feat: restructure skills, add skills 28-46"
```

**What's happening:**
- `git checkout -b pr/xxx main` — creates a new branch from main (clean slate)
- `git checkout your-branch -- folder/` — grabs ONLY those files from your big branch, stages them automatically
- `git commit` — saves them as one clean commit

**Step 4: Push each branch**
```powershell
git push -u origin pr/queue-automation
git push -u origin pr/datakit-updates
git push -u origin pr/genie-skills
```

**Step 5: Create 3 PRs in Azure DevOps, each targeting `main`**

**Tips:**
- If a file was **deleted** on your branch, you need `git rm path/to/file` manually after checking out
- If your big branch renamed/moved a folder (e.g., `genie/` → `agents/`), handle both: `git rm -r genie/` + `git checkout your-branch -- agents/`
- The original big branch is untouched — you can keep it or delete it after all PRs merge

### "My branch is behind main and I need to update it"

```powershell
git checkout main
git pull
git checkout my-branch
git merge main
```

This brings main's latest changes into your branch.

### "I want to see what a file looked like 5 commits ago"

```powershell
git show HEAD~5:path/to/file.py
```

### "I want to go back to a previous commit temporarily"

```powershell
git checkout <commit-hash>    # "detached HEAD" — just looking around
git checkout main             # go back to current
```

---

## 14. Troubleshooting Common Errors

### `error: src refspec main does not match any`

**Meaning:** Your local branch isn't called `main`.
**Fix:**
```powershell
git branch              # check your actual branch name
git branch -M main      # rename to main
git push -u origin main
```

### `! [rejected] ... (non-fast-forward)`

**Meaning:** The remote has commits you don't have locally.
**Fix:**
```powershell
git pull --rebase     # get remote changes, replay yours on top
git push
```

### `! [remote rejected] ... must use a pull request`

**Meaning:** Branch policies prevent direct pushes to this branch.
**Fix:** Push to a feature branch instead:
```powershell
git checkout -b feature/my-changes
git push -u origin feature/my-changes
# Then create a PR on Azure DevOps
```

### `CONFLICT (content): Merge conflict in file.py`

**Meaning:** Two people changed the same lines. Git doesn't know which to keep.
**Fix:** Open the file — you'll see markers:
```
<<<<<<< HEAD
your version of the code
=======
their version of the code
>>>>>>> branch-name
```
1. Edit the file to keep what you want (remove the markers)
2. `git add file.py`
3. `git commit -m "resolve merge conflict in file.py"`

### `fatal: not a git repository`

**Meaning:** You're not inside a Git repo folder.
**Fix:** `cd` into the right folder, or run `git init`.

### `error: Pulling is not possible because you have unmerged files`

**Meaning:** You're in the middle of a merge/rebase that has conflicts.
**Fix:** Either resolve the conflicts or abort:
```powershell
git merge --abort     # if you were merging
git rebase --abort    # if you were rebasing
```

### `warning: LF will be replaced by CRLF`

**Meaning:** Windows/Linux line ending difference. Usually harmless.
**Fix (optional):**
```powershell
git config --global core.autocrlf true
```

---

## 15. Glossary

| Term | Meaning |
|------|---------|
| **repo (repository)** | A project folder tracked by Git |
| **commit** | A saved snapshot of your project |
| **branch** | A parallel line of development |
| **main** | The default/primary branch |
| **remote** | A server copy of your repo (Azure DevOps) |
| **origin** | The default name for your remote |
| **clone** | Download a repo from a server |
| **push** | Upload your commits to the server |
| **pull** | Download commits from the server |
| **fetch** | Download info from server WITHOUT merging (just look) |
| **merge** | Combine two branches together |
| **rebase** | Replay your commits on top of another branch (cleaner history) |
| **stage** | Mark a file as "ready to commit" |
| **stash** | Temporarily hide your uncommitted changes |
| **HEAD** | Pointer to your current commit/branch |
| **diff** | The difference between two versions |
| **hash** | The unique ID of a commit (like `a1b2c3d`) |
| **upstream** | The remote branch your local branch tracks |
| **PR (Pull Request)** | A request to merge a branch (reviewed on Azure DevOps) |
| **conflict** | When Git can't auto-merge because the same lines were changed |
| **.gitignore** | File listing patterns Git should never track |
| **fast-forward** | A simple merge where no divergence happened |
| **detached HEAD** | You're looking at a specific commit, not on a branch |

---

## 16. Quick Reference Card

### Setup
```powershell
git config --global user.name "Name"     # set your name
git config --global user.email "email"   # set your email
git init                                  # new repo
git clone <url>                           # download repo
```

### Daily Commands
```powershell
git status                    # what's changed?
git add <file>                # stage a file
git commit -m "message"       # save a snapshot
git push                      # upload to remote
git pull                      # download from remote
```

### Branching
```powershell
git branch                         # list branches
git checkout -b <name>             # create + switch
git checkout <name>                # switch branch
git branch -d <name>               # delete branch
git push -u origin <name>          # push new branch
```

### Investigating
```powershell
git log --oneline             # compact history
git diff                      # unstaged changes
git diff --staged             # staged changes
git show <hash>               # inspect a commit
git blame <file>              # who changed each line
```

### Undoing
```powershell
git restore <file>              # discard working changes
git restore --staged <file>     # unstage
git commit --amend              # fix last commit
git reset --soft HEAD~1         # undo commit, keep changes
git revert <hash>               # undo a pushed commit safely
git stash / git stash pop       # temporarily shelve work
```

### Splitting a Branch
```powershell
git diff --name-only main..branch          # see all changed files
git checkout -b pr/scope-name main         # new branch from main
git checkout branch -- folder/             # grab specific files
git commit -m "feat: scoped change"        # commit them
git push -u origin pr/scope-name           # push for PR
```

### Remotes
```powershell
git remote -v                              # show remotes
git remote add origin <url>                # add remote
git remote set-url origin <url>            # change URL
git push -u origin main --force            # force push (⚠️)
```

---

*Last updated: April 2026 — tailored for a data engineer using PowerShell + Azure DevOps*
