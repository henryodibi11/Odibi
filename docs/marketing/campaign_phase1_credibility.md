# LinkedIn Campaign - Phase 1: Credibility Building

**Duration:** Weeks 1-2 (6 posts)
**Goal:** Establish yourself as someone worth following before mentioning Odibi
**Tone:** Humble, teaching from experience, relatable

---

## Week 1

### Post 1.1 - Your Origin Story (Monday)

**Hook:**
Today marks 5 years at my company. I started on night shifts. Now I lead data engineering for our analytics team.

**Body:**
In 2020, I graduated into a pandemic.

I joined a rotational leadership program that started me in operations — 12-hour night shifts, 3 on, 3 off, supervising production lines.

It was hard, but it taught me how a business actually runs from the ground up.

After 18 months, I moved into an engineering role. They asked me to map energy usage across an entire plant.

I had no idea what I was doing.

So I learned:
- YouTube tutorials at 2am
- Udemy courses on weekends
- LinkedIn Learning during lunch

Excel → Power BI → Azure → Python → Databricks

1000+ hours of self-study in one year. Not because I'm special — because I was hungry to learn.

5 years later, I'm the go-to data person for global operations. I train my peers in SQL. I build the pipelines everyone depends on.

The lesson? You don't need a CS degree. You need a problem you care about solving and the willingness to look stupid while you figure it out.

What skill did you teach yourself the hard way?

**CTA:** Engagement question

---

### Post 1.2 - The Biggest Mistake (Wednesday)

**Hook:**
My best work became my biggest headache.

**Body:**
Early in my data engineering journey, I built a pipeline that everyone loved.

It pulled telemetry from 50+ sources, cleaned it, transformed it, and loaded it into a dashboard. One script. One run. Done.

Leadership was impressed. I was proud.

Then 3 months later: "Can we add a new metric?"

I opened the script. 500+ lines. No functions. No comments. Just a wall of nested logic I barely recognized.

I spent a week reverse-engineering my own code. Then I rewrote the entire thing.

Here's what I learned:

**Monolithic code is a loan with interest.** Fast to write, expensive to maintain.

Now I follow three rules:
1. One function, one job. If it does two things, split it.
2. Name things like you'll forget what they do. Because you will.
3. If you can't explain a block of code in one sentence, it's too complex.

The goal isn't clever code. It's code your future self won't hate you for.

I wrote about my full journey from night shifts to data engineering on Medium. Link in comments.

What's a shortcut that came back to haunt you?

**CTA:** Link to Article 1 + Engagement question

---

### Post 1.3 - What I Wish I Knew (Friday)

**Hook:**
5 things I wish I knew when I started in data engineering:

**Body:**
1. **SQL is 80% of the job.** I spent months learning fancy tools. Should have mastered SQL first.

2. **The hardest part isn't technical.** It's getting access to data, understanding what business actually needs, and communicating with non-technical stakeholders.

3. **Documentation is not optional.** Future you will forget why you did something. Write it down.

4. **Rewriting code is normal.** I've rebuilt my pipelines 4+ times as I learned better approaches. That's growth, not failure.

5. **You don't need permission to learn.** Nobody asked me to learn Python or Azure. I just started. Most valuable career decision I've made.

What would you add to this list?

**CTA:** Engagement question

---

## Week 2

### Post 2.1 - Working With Constraints (Monday)

**Hook:**
I built a data platform with almost no direct database access.

**Body:**
When I started managing data for operations, I had to get creative.

In large organizations, data access is governed carefully-and for good reason. Security matters.

But I still needed to deliver value.

So I found solutions:
- Queried semantic models instead of source tables
- Used Power Automate to move data in chunks
- Built pipelines in Azure Data Factory
- Partnered with IT and eventually earned a Databricks workspace

Constraints force creativity. And building relationships opened doors.

3 things I learned:
1. **Build relationships.** The Azure admin who helped me provision resources became a key ally.
2. **Show value first.** Every small win built trust for bigger asks.
3. **Document everything.** When you're doing things unconventionally, you need a paper trail.

Resources are always limited. The question is: what can you build with what you have?

**CTA:** Engagement question

---

### Post 2.2 - The Solo Data Engineer (Wednesday)

**Hook:**
Being the only data engineer on a team of analysts is a unique challenge.

**Body:**
When you're the only one who does what you do:
- You make architecture decisions independently
- You set your own standards
- You're the expert by default

It pushes you to:
- Document obsessively (future you is your only teammate)
- Build systems, not scripts (you can't maintain 50 one-off notebooks)
- Learn to explain technical concepts simply (your stakeholders aren't technical)
- Automate everything (you don't have time to babysit pipelines)

The upside? You own the entire stack. You see problems end-to-end. You learn faster because there's no one else to hand things off to.

If you're a solo data person on your team: you're not alone. Many of us are building in isolation.

What's the hardest part of being the only data person on your team?

**CTA:** Engagement question

---

### Post 2.3 - Building in Public Announcement (Friday)

**Hook:**
I'm going to build a data warehouse in public.

**Body:**
Over the next few months, I'm going to take a public dataset and walk through the entire process:

📦 **Bronze** - Landing raw data
🧹 **Silver** - Cleaning and validating
⭐ **Gold** - Building dimensions and facts
📊 **Output** - Ready for dashboards

I'll share:
- What I do and why
- Mistakes I make along the way
- Code and configuration you can reuse

Why? Because I learned data engineering the hard way-piecing together YouTube videos and documentation at 2am.

I want to create the resource I wish I had.

Dataset: Brazilian E-Commerce (100k orders, customers, products, reviews)

First post drops next week.

Follow along if you're interested. Corrections and suggestions welcome-I'm still learning too.

I also wrote about the Bronze layer mistake that taught me to never transform raw data. Link in comments.

**CTA:** Link to Article 2 + Teaser for Phase 2

---

## Medium Articles for Phase 1

### Article 1.1: "How I Taught Myself Data Engineering While Working Night Shifts"
- Expanded version of Post 1.1
- Include specific resources that helped
- Timeline of skill progression
- Advice for others in similar situations

### Article 1.2: "The Bronze Layer Mistake That Cost Me 6 Months of Data"
- Expanded version of Post 1.2
- Technical deep dive on medallion architecture
- Code examples of correct Bronze layer patterns
- Link to Odibi docs (soft intro)

---

## Posting Schedule

| Week | Day | Post | Medium Article |
|------|-----|------|----------------|
| 1 | Mon | 1.1 - Origin Story | Article 1.1 (later in week) |
| 1 | Wed | 1.2 - Biggest Mistake | - |
| 1 | Fri | 1.3 - What I Wish I Knew | Article 1.2 (later in week) |
| 2 | Mon | 2.1 - Working With Constraints | - |
| 2 | Wed | 2.2 - Solo Data Engineer | - |
| 2 | Fri | 2.3 - Building in Public Announcement | - |

---

## Notes

- **Do not mention Odibi** in Phase 1 - focus on credibility
- **Do not mention company name** - keep it generic
- **Respond to every comment** - algorithm rewards engagement
- **Post between 8-10am** your timezone for best reach
