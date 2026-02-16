# Planning Skill

## When to Use
Call `plan_task()` BEFORE any task with 3+ steps.

## How to Plan

### 1. Call plan_task
```
plan_task("Clear description of what needs to be done")
```

### 2. Review the Plan
The plan will have numbered steps. Each step should be:
- Concrete (specific action, not vague)
- Executable (you can do it with available tools)
- Verifiable (you'll know when it's done)

### 3. Execute
Do each step. Don't stop between steps. Don't ask for confirmation.

```
Step 1 → Do it
Step 2 → Do it  
Step 3 → Do it
Done → Report result
```

### 4. Handle Failures
Step failed? Fix it. Continue to next step.
```
Step 2 failed → Fix → Retry Step 2 → Continue to Step 3
```

## Plan Structure

Good plan:
```
1. read_file("src/config.py") to understand current structure
2. edit_file() to add new configuration option
3. read_file("tests/test_config.py") to find related tests
4. edit_file() to add test for new option
5. run_command("pytest tests/test_config.py") to verify
```

Bad plan:
```
1. Look at the code
2. Make some changes
3. Test it
```

## Rules

- **No pauses.** Execute all steps without stopping.
- **No questions.** Don't ask "should I continue?"
- **Fix and continue.** Errors don't stop the plan.
- **Report at end.** One summary when done.
