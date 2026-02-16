# Code Review Skill

## When to Use
Before finalizing changes or when asked to review.

## Review Sequence

### 1. Get the Code
```
read_file(path)
```
Or for changes:
```
git_diff()
```

### 2. Call Review
```
review_code(path, focus="specific area to focus on")
```

### 3. Apply Suggestions
Review response will have actionable suggestions. Apply them:
```
edit_file(path, old, new)
```

### 4. Verify
```
run_command("pytest")           # Run tests
run_command("ruff check .")     # Run linter
```

## What to Look For

### Security
- Hardcoded secrets
- SQL injection
- Path traversal

### Quality
- Error handling
- Edge cases
- Type hints

### Style
- Naming conventions
- Code duplication
- Function length

### Odibi-Specific
- `read:`/`write:` not `source:`/`sink:`
- `format:` always present
- Node names with underscores

## Review Response Format

When reporting review results:
```
Found 3 issues:
1. [file:line] Issue description → Fix applied
2. [file:line] Issue description → Fix applied  
3. [file:line] Issue description → Needs discussion
```

## DON'T

- ❌ Long explanations of what could be wrong
- ❌ Ask "should I fix this?"
- ❌ List issues without fixing them

## DO

- ✅ Find issues
- ✅ Fix them immediately
- ✅ Report what was fixed
