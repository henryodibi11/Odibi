# How I Taught Myself Data Engineering While Working in Operations

*5 years: from night shifts to leading data engineering*

---

## TL;DR

Today marks my 5-year anniversary at my company. I graduated during COVID, joined a rotational leadership program in operations, and taught myself data engineering through 1000+ hours of self-study. No CS degree. No formal training. Just real problems and the willingness to figure things out. Here's how it happened and what I learned.

---

## The Beginning: 2020

I graduated from Texas Tech University in 2020, right as the world shut down.

Like many graduates that year, my plans changed overnight. Job offers evaporated. The economy was uncertain. But I had an opportunity-a rotational leadership development program at a global manufacturing company.

The catch? It started in operations.

## Rotation 1: Production Supervision

My first assignment was supervising a production line. Night shifts. 12 hours at a time. Three nights on, three nights off.

At first, I didn't see the connection to where I wanted to end up. I was there to learn leadership, not to become a plant manager.

But looking back, those 18 months taught me things I couldn't have learned anywhere else:

- **How businesses actually run.** Not from a PowerPoint, but from the floor.
- **How to lead people.** Real people, with real problems, at 3am.
- **How to operate in ambiguity.** Production doesn't wait for perfect information.

I didn't know it yet, but this operational perspective would become my superpower in data.

## Rotation 2: The Pivot

After 18 months, I moved to an engineering role at our largest global facility. The assignment: map energy usage across the entire plant.

Nobody handed me a tool or a method. Just a problem.

I started in Excel. Built some charts. They were... fine.

But I wanted more. I started exploring Power BI. Then I realized the data I needed wasn't easily accessible. That led me to Azure. Which led to SQL. Which led to Python.

One problem unlocked the next skill.

## The Learning Phase: 1000+ Hours

Here's the truth about self-teaching: it's not glamorous.

I watched YouTube tutorials during lunch. Took Udemy courses on weekends. Did LinkedIn Learning modules whenever I had time. Stayed up way too late reading documentation.

In one year, I logged over 1000 hours of learning outside of work.

Not because I'm special. Because I was hungry to learn. And because the problems in front of me were real-I couldn't half-solve them.

### What I Studied (In Rough Order)

1. **Excel** - Where everyone starts
2. **Power BI** - Visualization and basic data modeling
3. **SQL** - The foundation I should have learned first
4. **Azure fundamentals** - Cloud storage, basic services
5. **Azure Data Factory** - Building pipelines
6. **Python** - Scripting, automation, data manipulation
7. **Databricks/Spark** - Large-scale processing

Each skill unlocked the next problem I could solve.

### Resources That Helped

- **YouTube** - Free, visual, endless content. Great for getting started.
- **Udemy** - Structured courses, cheap during sales. Good for going deeper.
- **LinkedIn Learning** - Solid for fundamentals, often free through employers.
- **Documentation** - The hardest but most valuable. Microsoft Docs, Databricks docs, etc.
- **AI assistants** - ChatGPT became invaluable for debugging and learning faster.

## Rotation 3: Corporate Engineering

My third rotation took me to the global engineering team at corporate headquarters.

Same pattern: they needed data work, I figured it out.

I built:
- A global cycle time report
- A global energy report
- A global yield report

Each project was harder than the last. Each one taught me something new.

I also did a 9-month training program that touched on AI/ML. But I quickly realized: the foundations weren't solid enough. I needed to focus on data engineering before data science.

## The Current Role: Analytics & Data Engineering

After my rotational program ended, I joined a global digital team focused on analytics.

My official title was "data analyst." But I told my boss: I'd add more value on the backend, managing the data.

So that's what I did.

I took over a report with terrible performance. The ask: push complexity to the backend so dashboards load faster.

The challenge: I didn't have direct access to the source databases. Security requirements meant I had to get creative.

### Getting Creative With Constraints

In large organizations, data governance exists for good reason. Security matters. Compliance matters.

But I still needed to deliver value.

So I found solutions:
- Queried semantic models instead of source tables
- Used Power Automate to extract data in chunks
- Built pipelines in Azure Data Factory
- Partnered with the IT team, built trust, and eventually got my own Databricks workspace

The Azure admin who helped me became a key ally. That relationship opened doors.

**Lesson learned:** Constraints force creativity. And relationships open doors that tools can't.

## What I Do Now

Today, exactly 5 years in, I'm the go-to data person for global operations at my company.

I manage the data infrastructure for our team. I build pipelines, maintain data models, and support reporting across multiple regions. I train my peers in SQL and engineering fundamentals.

I'm not in IT. I'm in operations. Which gives me a unique perspective: I understand both the business problems and the technical solutions.

## What I Learned Along The Way

### 1. You Don't Need Permission to Learn

Nobody asked me to learn Python. Nobody assigned me Azure training. I just started.

The most valuable career decisions I've made were things I chose to do, not things I was told to do.

### 2. SQL is 80% of the Job

I spent months learning fancy tools. I should have mastered SQL first.

Everything-Power BI, Spark, dbt, whatever-eventually talks to data through SQL or something SQL-like. Learn it deeply.

### 3. The Hard Part Isn't Technical

Getting access to data is hard. Understanding what the business actually needs is hard. Communicating with non-technical stakeholders is hard.

The code is the easy part.

### 4. Rewriting is Normal

I've rebuilt my pipelines four or five times. Each time because I learned a better approach.

That's not failure. That's growth.

### 5. Documentation is Not Optional

Future you will forget why you did something. Write it down.

When you're the only one who understands a system, documentation is your safety net.

### 6. Build Systems, Not Scripts

A script solves one problem one time. A system solves many problems many times.

When you're solo, you can't maintain 50 one-off notebooks. You need reusable patterns.

## Why I'm Sharing This

I learned data engineering the hard way-piecing together YouTube videos, documentation, and trial-and-error at 2am.

There's no single resource that covers the practical, end-to-end journey from "I have no idea what I'm doing" to "I can build production data pipelines."

So I'm creating that resource.

Over the coming months, I'll be sharing:
- How I approach data engineering problems
- Patterns and anti-patterns I've learned
- Code and configurations you can reuse
- A framework I built to make this easier

If you're on a similar journey-or thinking about starting one-I hope this helps.

## The Bottom Line

You don't need a CS degree to become a data engineer.

You need:
- A real problem to solve
- The willingness to look stupid while you figure it out
- 1000 hours of focus

The path isn't linear. The learning never stops. But if you're hungry enough, you can teach yourself almost anything.

---

*I'm Henry-a data engineer who started in operations. I write about data engineering, building in public, and the tools I create along the way. Follow along if that's interesting to you.*

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
