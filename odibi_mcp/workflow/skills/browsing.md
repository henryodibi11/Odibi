# Browsing Skill

## The Rule
**NEVER ask what to do on a page. Navigate, explore, extract, report.**

## Browser Sequence

### 1. Navigate
```
browser_navigate("https://example.com")
```
Don't ask. Just go.

### 2. Snapshot
```
browser_snapshot()
```
See what's on the page. Understand the structure.

### 3. Interact
```
browser_click(ref)           # Click element
browser_type(ref, "text")    # Type into input
browser_select_option(ref, value)  # Select dropdown
```
Don't ask "should I click?" - just click.

### 4. Snapshot Again
```
browser_snapshot()
```
See result of interaction.

### 5. Extract & Report
Read the content, summarize, return to user.

## Common Tasks

### Find Documentation
```
1. browser_navigate(url)
2. browser_snapshot()
3. Find docs/API link → browser_click(ref)
4. browser_snapshot()
5. Extract and summarize key info
```

### Fill a Form
```
1. browser_navigate(url)
2. browser_snapshot() - find form fields
3. browser_type(field1_ref, "value1")
4. browser_type(field2_ref, "value2")
5. browser_click(submit_ref)
6. browser_snapshot() - confirm result
```

### Search a Website
```
1. browser_navigate(url)
2. browser_snapshot() - find search box
3. browser_type(search_ref, "query")
4. browser_click(search_button) or press Enter
5. browser_snapshot() - read results
6. Summarize findings
```

### Navigate Multiple Pages
```
1. browser_navigate(start_url)
2. browser_snapshot()
3. Find relevant link → browser_click(ref)
4. browser_snapshot()
5. Continue until you find what's needed
6. Report findings
```

## Key Tools

| Tool | Use |
|------|-----|
| browser_navigate | Go to URL |
| browser_snapshot | See page content (accessibility tree) |
| browser_click | Click element by ref |
| browser_type | Type into input |
| browser_take_screenshot | Visual screenshot |
| browser_press_key | Press keyboard key |
| browser_select_option | Select from dropdown |
| browser_wait_for | Wait for text/element |

## DON'T

- ❌ "What would you like me to do on this page?"
- ❌ "Would you like me to click that?"
- ❌ "Should I proceed?"
- ❌ Stop after one page load
- ❌ Ask for confirmation between steps

## DO

- ✅ Navigate immediately
- ✅ Explore the page with snapshot
- ✅ Click/interact without asking
- ✅ Chain multiple actions
- ✅ Extract and summarize automatically
- ✅ Report findings when done
