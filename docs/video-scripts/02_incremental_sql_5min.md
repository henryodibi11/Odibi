# Video Script: Incremental SQL in 5 Minutes

**Target Length:** 5 minutes  
**Audience:** Junior and senior data engineers  
**Goal:** Show how stateful HWM works and why it matters

---

## Script

### Opening (0:00 - 0:20)

**[Screen: Desktop]**

**Voiceover:**
"Loading a million-row table every day is wasteful. Let me show you how Odibi loads only the rows that changed since yesterday—automatically."

**[Title card: "Incremental SQL in 5 Minutes | High-Water Mark Explained"]**

---

### Section 1: The Problem (0:20 - 1:00)

**[Screen: Diagram]**

**[Show table visualization with dates]:**
```
Day 1: Load 1,000,000 rows (5 minutes)
Day 2: Load 1,000,000 rows AGAIN (5 minutes)  ← Wasteful!
Day 3: Load 1,000,000 rows AGAIN (5 minutes)  ← Only 100 new rows!
```

**Voiceover:**
"Full loads are simple but inefficient. On Day 3, we're reloading 999,900 rows we already have. There's a better way: incremental loading."

**[Diagram transitions to incremental]:**
```
Day 1: Load 1,000,000 rows (5 minutes)
Day 2: Load 100 new rows (2 seconds)  ← Much better!
Day 3: Load 150 new rows (2 seconds)
```

**Voiceover:**
"Incremental loading tracks where you left off—called a High-Water Mark or HWM—and only loads rows after that point."

---

### Section 2: Configuration (1:00 - 2:30)

**[Screen: VS Code with YAML]**

**Voiceover:**
"Here's how simple it is in Odibi. Add an 'incremental' block to your read config."

**[Show YAML]:**
```yaml
read:
  connection: source_db
  table: orders
  format: sql
  options:
    incremental:
      mode: stateful        # Track high-water mark
      column: updated_at    # Which column to track
      initial_value: "2025-01-01"  # First run: load from this date
```

**[Highlight each line as you explain]:**

**Voiceover:**
"Mode stateful tells Odibi to remember the last value. Column updated_at is your timestamp. Initial value is where to start on the first run. That's it—three lines."

---

### Section 3: First Run (2:30 - 3:30)

**[Screen: Terminal]**

**Voiceover:**
"Let's run it. First time, there's no state, so Odibi loads everything from January 1st."

**[Type and run]:**
```bash
odibi run incremental_demo.yaml
```

**[Show output]:**
```
📊 Incremental loading: mode=stateful, column=updated_at
📖 No previous state found
📥 Loading all rows WHERE updated_at >= '2025-01-01'
✅ Loaded 1,000,000 rows
💾 State saved: updated_at = '2025-01-10 23:59:59'
```

**Voiceover:**
"One million rows loaded. And look—Odibi saved the state. The highest updated_at value it saw."

**[Screen: Show .odibi/system/state.json]**

```json
{
  "ingest_orders": {
    "updated_at": "2025-01-10 23:59:59"
  }
}
```

---

### Section 4: Second Run (3:30 - 4:30)

**[Screen: Terminal]**

**Voiceover:**
"Now let's run it again the next day. New data arrived in the source."

**[Type and run]:**
```bash
odibi run incremental_demo.yaml
```

**[Show output]:**
```
📊 Incremental loading: mode=stateful, column=updated_at
📖 Previous HWM: '2025-01-10 23:59:59'
📥 Loading rows WHERE updated_at > '2025-01-10 23:59:59'
✅ Loaded 150 new rows
💾 State saved: updated_at = '2025-01-11 14:30:00'
```

**[Highlight "150 new rows"]**

**Voiceover:**
"Only 150 new rows. Same config, but this time it used the saved state to load only what's new. No duplicates, no wasted work."

**[Screen: Show Data Story side-by-side comparison]**

**[Two browser windows]:**
- Left: First run (1M rows)
- Right: Second run (150 rows)

**Voiceover:**
"The Data Story shows exactly what happened each run. First run: full load. Second run: incremental."

---

### Section 5: How It Works (4:30 - 5:00)

**[Screen: Animated diagram]**

**[Show sequence]:**
```
1. Read state catalog → last HWM = '2025-01-10 23:59:59'
2. Query: SELECT * WHERE updated_at > '2025-01-10 23:59:59'
3. Process 150 rows
4. Calculate new HWM = MAX(updated_at) = '2025-01-11 14:30:00'
5. Save to state catalog
```

**Voiceover:**
"Behind the scenes: Odibi reads the saved HWM, builds a SQL query with a WHERE clause, processes only new rows, then updates the HWM for next time. Fully automatic."

---

### Closing (5:00 - 5:20)

**[Screen: Terminal with summary]**

**Voiceover:**
"That's incremental loading. Add three lines to your YAML and never waste time on full loads again."

**[Show on screen]:**
```yaml
incremental:
  mode: stateful
  column: updated_at
```

**Voiceover:**
"Want to learn more? Check out the Incremental Decision Tree in the docs to choose between stateful, rolling window, and other patterns. Link in description. Thanks for watching!"

**[Fade to end card with links]**

---

## Key Visuals

1. **Title Card:** "Incremental SQL in 5 Minutes | Stateful HWM"
2. **Comparison Diagram:** Full load vs Incremental (before/after)
3. **State Visualization:** JSON file with HWM highlighted
4. **Sequence Diagram:** How HWM tracking works
5. **Side-by-Side Stories:** First run vs Second run

---

## Screen Recording Notes

- **Before Recording:** Prepare source database with dated records
- **Database:** Use local SQLite or PostgreSQL (easy to demo)
- **Sample Data:** Orders table with clear updated_at timestamps
- **Timing:** Actual pipeline runs should be < 5 seconds for video flow

---

## On-Screen Text Callouts

**[At 1:30, show callout]:**
> 💡 Stateful = Exact incremental (no guessing)

**[At 2:30, show callout]:**
> 📊 First Run: Loads all data from initial_value

**[At 4:00, show callout]:**
> ⚡ Second Run: Only new rows (based on HWM)

**[At 4:45, show callout]:**
> 🔄 Automatic: No manual tracking needed!

---

## B-Roll Ideas

- Quick glimpse of state.json file structure
- Animated HWM line moving up over time
- Terminal output with highlighted row counts
- Data Story validation section (green checkmarks)

---

## Call to Action

"Try it yourself! Clone example 02_incremental_sql from the docs. Run it twice and watch the row counts change. Share your results in GitHub Discussions!"

**[On-screen]:**
```
📖 Docs: odibi.io/examples/02_incremental_sql
💬 Discuss: github.com/henryodibi11/Odibi/discussions
```

---

## Related Videos

- 01: Zero to Story in 7 Minutes (prerequisite)
- 03: SCD2 Pitfalls in 10 Minutes (next)
- 04: Debug Toolkit in 5 Minutes (troubleshooting)

---

## Common Questions to Address (Bonus Segment)

If time allows, add a 30-second Q&A:

**Q: "What if data arrives late?"**  
**A:** "Use rolling_window mode instead of stateful. See the Incremental Decision Tree guide."

**Q: "Can I reset the HWM?"**  
**A:** "Yes! Run `odibi catalog reset` to force a full reload."
