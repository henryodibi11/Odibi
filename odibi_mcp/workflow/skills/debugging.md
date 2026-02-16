# Debugging Skill

## When to Use
Call `debug_issue()` when stuck on an error.

## Debugging Sequence

### 1. Capture the Error
Get the full error message. Run the command again if needed:
```
run_command("python -m odibi run X.yaml")
```

### 2. Get AI Analysis
```
debug_issue("paste error message here", "context about what you were doing")
```

### 3. Apply the Fix
The debug_issue response will suggest fixes. Apply them:
```
edit_file(path, old_str, new_str)
```

### 4. Retry
```
run_command("same command that failed")
```

### 5. Still Failing?
Different error → Repeat from step 1
Same error → Read more context:
```
read_file(relevant_file)
grep("related_term", ".")
```

## Common Debugging Actions

### Import Error
```
grep("import X", ".")           # Find where it's imported
read_file(file_with_import)     # Read context
```

### File Not Found
```
find_path("filename")           # Find the file
glob_files("**/*filename*")     # Try patterns
list_dir("expected/path")       # Check directory
```

### YAML Error
```
read_file("pipeline.yaml")      # Read the YAML
explain("field_name")           # Get docs for field
```

### Test Failure
```
run_command("pytest path -v")   # Run with verbose
read_file("test_file.py")       # Read the test
read_file("source_file.py")     # Read the source
```

## Rules

- **Don't give up.** Keep trying different approaches.
- **Read more context.** When stuck, read more files.
- **Try things.** Run commands to see what happens.
- **No asking.** Don't ask user what to do. Try something.
