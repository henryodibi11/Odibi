# Visual Guides & Diagrams

> Complex concepts explained visually with Mermaid diagrams and step-by-step flows.

---

## 📊 All Visual Guides

### **System Architecture**

**[Odibi in One Picture](odibi_architecture.md)** - Complete system overview
- 3-layer architecture (Config → Execution → Data)
- Data flow sequence
- Engine parity principle
- Quality layer detail

**Best for:** Understanding how Odibi works end-to-end

---

### **Decision Trees**

**[Incremental Decision Tree](incremental_decision_tree.md)** - Choose your incremental pattern
- Stateful HWM vs Rolling Window vs Skip If Unchanged
- Real-world scenarios (SCADA, lab results, batch reports)
- Configuration examples
- State management guide

**Best for:** Deciding how to load data incrementally

---

### **Data Patterns**

**[SCD2 Timeline](scd2_timeline.md)** - Slowly Changing Dimension Type 2 explained
- Timeline visualization
- Table evolution through changes
- Current vs historical queries
- SCD2 vs SCD1 comparison

**Best for:** Understanding dimension history tracking

**[Fact Build Flow](fact_build_flow.md)** - How fact tables are built
- Surrogate key lookups
- Orphan handling (unknown, quarantine, fail)
- Grain validation
- FK validation

**Best for:** Understanding star schema fact tables

---

### **Execution**

**[Run Lifecycle](run_lifecycle.md)** - What happens when you run a pipeline
- 6-phase execution flow
- Error handling strategies
- Performance optimization
- Debugging tools

**Best for:** Understanding pipeline execution from CLI to Story

---

## 🗺️ Visual Guide by Use Case

### "I'm new to Odibi"
Start here: [Odibi in One Picture](odibi_architecture.md)

### "I need to load only new data"
Go to: [Incremental Decision Tree](incremental_decision_tree.md)

### "I need to track dimension changes"
Go to: [SCD2 Timeline](scd2_timeline.md)

### "I'm building a fact table"
Go to: [Fact Build Flow](fact_build_flow.md)

### "My pipeline failed, how do I debug?"
Go to: [Run Lifecycle](run_lifecycle.md) → Error Handling section

---

## 🎨 Diagram Types Used

All diagrams use **Mermaid** syntax (rendered automatically in MkDocs):

- **Flowcharts** - Decision trees, process flows
- **Sequence Diagrams** - Step-by-step interactions
- **Gantt Charts** - Timelines (SCD2 validity windows)
- **State Diagrams** - Lifecycle states
- **Graph Diagrams** - Relationships and dependencies

---

## 📚 Related Guides

After understanding visuals, apply them:

- **[Patterns](../patterns/README.md)** - Implementation guides for each pattern
- **[Canonical Examples](../examples/canonical/README.md)** - Working YAML configs
- **[Learning Journeys](../journeys/README.md)** - Persona-based learning paths
- **[How-to Guides](../guides/README.md)** - Task-oriented guides

---

## 💡 Using These Diagrams

### **In Presentations**
- Copy Mermaid code blocks into slides
- Export as SVG (MkDocs renders Mermaid)
- Use for onboarding new team members

### **In Documentation**
- Link to visuals from your YAML explanations
- Include diagram snippets in pull requests
- Reference in code review comments

### **In Learning**
- Print diagrams for reference
- Annotate with your own notes
- Use as templates for your pipelines

---

[← Back to Documentation Home](../README.md)
