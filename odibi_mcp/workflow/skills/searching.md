# Searching Skill

## The Rule
**NEVER say "I can't find it". CHAIN search tools until found.**

## Search Sequence

### 1. find_path (Smart)
```
find_path("config.yaml")
```
Tries multiple approaches automatically.

### 2. glob_files (Pattern)
```
glob_files("**/*config*")
glob_files("**/*.yaml")
glob_files("src/**/*.py")
```

### 3. grep (Content)
```
grep("def function_name", ".")
grep("class ClassName", "src/")
grep("import module", "**/*.py")
```

### 4. list_dir (Explore)
```
list_dir(".")
list_dir("src/")
list_dir("likely/location/")
```

### 5. Keep Going
Not found? Try different patterns:
```
glob_files("**/*partial*")
glob_files("**/test*")
grep("keyword", ".")
```

## Common Searches

### Find a file
```
find_path("exact_name.ext")
glob_files("**/*partial*")
```

### Find a function/class
```
grep("def function_name", ".")
grep("class ClassName", ".")
```

### Find where something is used
```
grep("function_name(", ".")
grep("import module", ".")
```

### Find config files
```
glob_files("**/*.yaml")
glob_files("**/*.json")
glob_files("**/config*")
```

### Explore unknown codebase
```
list_dir(".")
glob_files("**/*.md")        # Find docs
grep("main", ".")            # Find entry points
```

## DON'T

- ❌ "I can't find the file"
- ❌ "The file doesn't seem to exist"
- ❌ Ask user where file is
- ❌ Give up after one search

## DO

- ✅ Try find_path first
- ✅ Try multiple glob patterns
- ✅ Try grep for content
- ✅ Explore with list_dir
- ✅ Report what you found (even partial matches)
