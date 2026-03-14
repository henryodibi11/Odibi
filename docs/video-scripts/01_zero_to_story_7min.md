# Video Script: Zero to Story in 7 Minutes

**Target Length:** 7 minutes  
**Audience:** Complete beginners, business analysts, junior data engineers  
**Goal:** Show how easy it is to run your first pipeline and see results

---

## Script

### Opening (0:00 - 0:30)

**[Screen: Desktop, terminal closed]**

**Voiceover:**
"In the next 7 minutes, you'll go from zero to your first data pipeline with a beautiful audit report. No prior data engineering experience needed. Let's go."

**[Show title card: "Zero to Story in 7 Minutes"]**

---

### Section 1: Installation (0:30 - 1:30)

**[Screen: Terminal]**

**Voiceover:**
"First, install Odibi. It's just one command."

**[Type and run]:**
```bash
pip install odibi
```

**[Show installation output scrolling]**

**Voiceover:**
"That's it. While that installs, let me show you what we're building today."

**[Screen: Diagram appears]**

**[Simple diagram showing: CSV → Odibi → Parquet + Story]**

**Voiceover:**
"We'll read a CSV file, convert it to Parquet—a better format for analytics—and generate an HTML audit report called a Data Story. Three outputs from one YAML file."

---

### Section 2: Create Project (1:30 - 3:00)

**[Screen: Terminal]**

**Voiceover:**
"Verify installation:"

**[Type and run]:**
```bash
odibi --version
```

**[Show output: "odibi 2.4.0"]**

**Voiceover:**
"Perfect. Now create your project using the built-in template."

**[Type and run]:**
```bash
odibi init my_first_pipeline --template star-schema
```

**[Show output: directory structure being created]**

**Voiceover:**
"Odibi just created a complete project structure with sample data included. Let's look inside."

**[Type and run]:**
```bash
cd my_first_pipeline
ls
```

**[Show output: odibi.yaml, data/, README.md]**

**[Screen: Open odibi.yaml in VS Code with syntax highlighting]**

**Voiceover:**
"This YAML file defines your entire pipeline. See how readable it is? Connections, pipelines, nodes. Everything in one place."

**[Scroll through YAML highlighting key sections: connections, nodes, write]**

---

### Section 3: Run the Pipeline (3:00 - 4:30)

**[Screen: Terminal]**

**Voiceover:**
"Let's run it."

**[Type and run]:**
```bash
odibi run odibi.yaml
```

**[Show output scrolling: green checkmarks, progress indicators]**

**[Highlight key lines:]**
```
✅ Executing node: dim_customer
✅ Wrote 1,234 rows to data/gold/dim_customer
✅ Executing node: fact_sales
✅ Wrote 5,678 rows to data/gold/fact_sales
📖 Generated Data Story: stories/main_documentation.html
✅ Pipeline completed in 2.3s
```

**Voiceover:**
"Done! In 2 seconds, we built a complete star schema—3 dimension tables and 1 fact table. But the real magic is the Data Story. Let's open it."

---

### Section 4: View the Data Story (4:30 - 6:30)

**[Screen: Terminal]**

**[Type and run]:**
```bash
odibi story last
```

**[Screen: Browser opens with HTML report]**

**Voiceover:**
"This is your Data Story. An automatic audit report for every pipeline run."

**[Scroll through Story, pausing on each section]**

**[Section 1: Pipeline Summary]**

**Voiceover:**
"At the top: when it ran, how long it took, success status."

**[Section 2: Lineage Graph]**

**Voiceover:**
"Here's the visual dependency graph. You can see which tables depend on which—customers and products loaded first, then the fact table joined them together."

**[Section 3: Data Profile]**

**[Click on "fact_sales" node]**

**Voiceover:**
"Click any node to see details. Row count: 5,678. Schema with all column types. And sample data—the first 10 rows."

**[Show sample data table]**

**Voiceover:**
"Notice the surrogate keys—customer_sk, product_sk. Those were automatically looked up from the dimension tables. No manual joins required."

**[Section 4: Validation Results]**

**Voiceover:**
"Scroll down for validation results. All checks passed. Green checkmarks mean your data quality is good."

---

### Section 5: What's Next (6:30 - 7:00)

**[Screen: Back to terminal, then show Golden Path doc]**

**Voiceover:**
"You just built your first star schema with full audit trail in under 10 minutes. Ready to go deeper?"

**[Show quick list on screen]:**
```
Next Steps:
✓ Adapt this pipeline for your own CSV
✓ Add validation gates for data quality
✓ Learn incremental loading (only new data)
✓ Scale to production with Spark engine
```

**Voiceover:**
"Check out the Learning Journeys—we have paths for business analysts, junior engineers, and senior engineers. Pick your role and start learning."

**[Show journeys page briefly]**

**Voiceover:**
"All the docs, examples, and scripts are linked in the description. Happy building!"

**[Fade to end card with links]**

---

## Key Visuals to Include

1. **Title Card:** "Zero to Story in 7 Minutes | Odibi Framework"
2. **Diagram:** CSV → Odibi → Parquet + Story (simple flowchart)
3. **Terminal:** Clean, easy-to-read font (16pt minimum)
4. **Browser:** Full Data Story HTML with annotations
5. **End Card:**
   - Links to docs
   - GitHub repo
   - Learning Journeys

---

## Recording Notes

- **Screen Resolution:** 1920x1080 minimum
- **Terminal Font:** Fira Code or JetBrains Mono, 16pt
- **Speed:** Type at moderate speed, pause on outputs
- **Highlighting:** Use yellow highlight boxes for key concepts
- **Annotations:** Add arrows/callouts when showing Data Story sections
- **Music:** Light, upbeat background music (low volume)

---

## B-Roll Ideas

- Quick glimpses of other Data Stories
- Mermaid diagrams from docs
- Terminal commands from cheatsheet
- Success checkmarks animations

---

## Call to Action

"Try it yourself! Install Odibi, run the Golden Path, and share your first Data Story in our GitHub Discussions. We want to see what you build!"

---

## Related Videos (To Produce)

- 02: Incremental SQL in 5 Minutes
- 03: SCD2 Pitfalls in 10 Minutes
- 04: Debug Toolkit in 5 Minutes
- 05: Quality Gates in 6 Minutes
