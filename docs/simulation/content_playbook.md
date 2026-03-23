---
title: "Content Playbook (Internal)"
---

# Content Playbook - Internal

**How to systematically extract LinkedIn posts, YouTube videos, and teaching content from the 38 simulation patterns.**

This is an internal reference. It documents the repeatable process for turning any pattern into content without creative overhead.

---

## The Extraction Template

Every pattern has (or should have) a structured content block. Use this template to extract content from any pattern in under 10 minutes:

### 1. Core Insight
What is the one non-obvious thing this example teaches? Not "how to use odibi" but the domain insight underneath.

**Formula:** "Most people think [common assumption]. Actually, [insight from this pattern]."

### 2. Real-World Problem
Where does this exact scenario show up in industry? Be specific - name the role, the system, the failure mode.

**Formula:** "[Role] at [company type] needs [thing]. Without it, [consequence]."

### 3. Why It Matters
What breaks, fails, or costs money if this concept is ignored?

**Formula:** "If you skip [this concept], your [system/pipeline/dashboard] will [specific failure]."

### 4. Hook (LinkedIn-ready)
One strong, opinionated sentence that would stop a scrolling engineer. No jargon unless it's the right jargon.

**Rules:**
- Under 20 words
- First-person ("I built...", "I simulated...", "I learned...")
- Specific, not generic ("80 lines of YAML" not "a simple config")
- Opinionated ("most data engineers don't know..." not "it's useful to know...")

### 5. YouTube Angle
How this would be explained in a 5-10 minute video. Include the visual hook.

**Formula:** "[Visual element] + [domain lesson] + [framework demo]"

---

## LinkedIn Post Template

Use this structure for any pattern:

```
[Hook - 1 sentence, first person, specific]

[2-3 sentences of context: what the pattern models and why it matters]

[The "most people don't realize" insight - 1-2 sentences]

[What odibi does differently - 2-3 sentences with a code snippet if short]

[Call to action: link to docs, ask a question, or tease the next post]

#dataengineering #[domain] #[relevant tag]
```

**Example (Pattern 9 - Wastewater):**

```
I simulated a wastewater treatment plant in 80 lines of YAML.

Five treatment stages, each one referencing the upstream stage's output.
Realistic BOD removal rates. A 2 AM storm event that tests hydraulic overload.

Most synthetic data tools would generate independent random numbers for each stage.
But a secondary clarifier can't be dirtier than the influent - that's physically impossible.
Cross-entity references enforce that constraint automatically.

Odibi's simulation engine builds a dependency DAG across entities and generates them in order.
No Python. No custom code. Just YAML that respects the physics.

Full pattern with explanation: [link]

#dataengineering #watertreatment #simulation
```

---

## YouTube Video Template

**Target length:** 5-10 minutes per pattern

### Structure

1. **Hook (0:00 - 0:30):** Start with the domain visual or the "most people don't know" insight. Show the final output first (the generated data), then explain how it works.

2. **Domain lesson (0:30 - 2:00):** Explain the real-world system. Use the mermaid diagram from the pattern docs. Keep it accessible - "if you've ever flushed a toilet, you've used this system."

3. **The YAML walkthrough (2:00 - 5:00):** Walk through the config line by line. Explain *why* each parameter has its value ("volatility 0.3 because pressure transmitters are well-tuned instruments").

4. **Run it live (5:00 - 6:00):** `odibi run` on camera. Show the output. Point out the realistic features (autocorrelation, cross-entity dependencies, chaos effects).

5. **The bigger picture (6:00 - 7:00):** "What would you do with this data?" Connect to downstream analytics, dashboards, validation. Mention the one-line swap to production data.

6. **CTA (7:00 - 7:30):** "Try it yourself - link in description. Next week: [next pattern]."

### Visual Hooks by Category

| Category | Visual Hook |
|----------|-------------|
| Process Engineering | Show a P&ID, then show the YAML that simulates it |
| Manufacturing | Show an assembly line photo, then the entity overrides |
| IoT / Energy | Show a sensor dashboard, then reveal it's synthetic data |
| Business | Show a POS receipt, then the simulation config behind it |
| Data Engineering | Show a broken dashboard, then the chaos config that tests it |

### The Recurring Angle

Every video should reinforce the positioning: **"I'm a chemical engineer who built a data engineering framework. Let me teach you both."**

This is the unique angle. No other data engineering content creator has this combination. Lean into it.

---

## Content Calendar

38 patterns = 38 weeks of content at one per week. Group by theme for seasonal relevance:

| Weeks | Category | Patterns | Theme |
|-------|----------|----------|-------|
| 1-8 | Foundations | 1-8 | "From zero to simulation" series |
| 9-15 | Process Engineering | 9-15 | "ChemE meets data engineering" |
| 16-20 | Energy & Utilities | 16-20 | "Renewable energy data" |
| 21-25 | Manufacturing | 21-25 | "Factory floor data" |
| 26-28 | Environmental | 26-28 | "Environmental monitoring" |
| 29-30 | Healthcare | 29-30 | "Healthcare data simulation" |
| 31-35 | Business & IT | 31-35 | "Business data patterns" |
| 36-38 | Data Engineering | 36-38 | "Testing your data platform" |

**LinkedIn cadence:** 2-3 posts per week (pattern highlight + general insight + engagement post)

**YouTube cadence:** 1 video per week, released on a consistent day

---

## Repurposing Matrix

Each pattern can generate multiple content pieces:

| Content Type | Time to Produce | From What |
|--------------|-----------------|-----------|
| LinkedIn post | 10 min | Content card in pattern docs |
| YouTube script | 30 min | Pattern narrative + YAML walkthrough |
| Twitter/X thread | 15 min | Core insight + 5 supporting points |
| Blog post | 45 min | Full pattern narrative + screenshots |
| Conference talk slide | 5 min | Mermaid diagram + hook |
| README example | 5 min | Minimal YAML snippet from pattern |

---

## Quality Checklist

Before publishing any content piece from a pattern:

- [ ] Hook is first-person, specific, and under 20 words
- [ ] Domain insight is non-obvious (not "simulation generates data")
- [ ] Real-world problem names a role and a consequence
- [ ] Code snippet is minimal and self-contained
- [ ] No em dashes (use regular hyphens)
- [ ] Links point to live documentation
- [ ] CTA directs to the pattern page or getting started guide
- [ ] Positioning reinforced: ChemE + DE, solo builder, YAML-first
