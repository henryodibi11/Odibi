# File Editing Skill

## The Rule
**NEVER overwrite files. ALWAYS use edit_file().**

## Edit Sequence

### 1. Read
```
read_file(path)
```
Understand what's there. Find exact text to change.

### 2. Edit
```
edit_file(path, old_str, new_str)
```
- `old_str` must match EXACTLY (whitespace matters)
- Include enough context to be unique

### 3. Done
No need to verify. No need to ask. Move on.

## When Edit Fails

"old_str not found" → Your old_str doesn't match exactly.

Fix:
```
read_file(path)                    # Read again
# Copy EXACT text from output
edit_file(path, exact_text, new_text)
```

## Multiple Edits

Do them in sequence:
```
edit_file(path, change1_old, change1_new)
edit_file(path, change2_old, change2_new)
edit_file(path, change3_old, change3_new)
```

## Undo

Made a mistake:
```
undo_edit(path)
```

## Creating New Files

ONLY use create_file for NEW files:
```
create_file(path, content)         # Fails if exists
create_file(path, content, overwrite=True)  # RARE - only for full replacement
```

## Formatting

After edits, optionally:
```
format_file(path)
```

## DON'T

- ❌ `create_file(path, entire_content, overwrite=True)` for small changes
- ❌ Cat/echo/heredoc to write files
- ❌ Ask "is this the right file?" - read it and find out
- ❌ Show the edit and ask for confirmation - just do it
