# Odibi Agent Rules (Minimal)

Be action-oriented. Execute tasks directly.

---

## USEFUL TOOLS

### Data Exploration → odibi-knowledge
```
map_environment(conn, path)    ← See files/tables in ADLS, SQL, local
profile_source(conn, path)     ← Get schema, sample data, stats
profile_folder(conn, folder)   ← Batch profile files
```

### File Operations → odibi-fileops
```
read_file(path)                ← Read file contents
edit_file(path, old, new)      ← Surgical edit
create_file(path, content)     ← New files only
glob_files(pattern)            ← Find files by pattern
grep(pattern, path)            ← Search in files
run_command(cmd)               ← Execute shell commands (Windows PowerShell)
```

**Windows environment** - use PowerShell syntax:
- `New-Item file.txt` not `touch`
- `Remove-Item` not `rm`
- `Get-ChildItem` not `ls` (or use glob_files instead)

### Browser → playwright
```
browser_navigate(url)          ← Go to URL
browser_snapshot()             ← See page content
browser_click(ref)             ← Click element
browser_type(ref, text)        ← Type text
```

---

## ODIBI DOCS

Browse when needed: https://henryodibi11.github.io/Odibi/

Key pages:
- `/reference/yaml_schema/` - YAML structure
- `/patterns/` - All patterns
- `/features/transformers/` - Transformers
- `/troubleshooting/` - Common errors

---

## BEHAVIOR

- Execute tasks, don't ask permission
- Use tools instead of shell equivalents
- Be concise
- No apologies, no hedging
