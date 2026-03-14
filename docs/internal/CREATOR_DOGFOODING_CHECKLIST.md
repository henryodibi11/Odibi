# Creator's Dogfooding Checklist

**Time:** 30-45 minutes  
**Goal:** Experience what users experience. Find what's broken, confusing, or missing.  
**Philosophy:** "If I wouldn't use this, why would they?"

---

## 🎯 **The Test**

**Can you complete these tasks using ONLY the docs (no prior knowledge)?**

---

## ✅ **Task 1: Find How to Install (2 min)**

**Scenario:** You forgot how to install Odibi.

**Do This:**
1. Go to `docs/README.md` 
2. Find installation instructions
3. Time how long it takes

**Questions:**
- Was it in the first screen? Or did you have to scroll/search?
- Is the command copy-pasteable?
- Does it tell you how to verify it worked?

**Mark:** ⏱️ _____ seconds | ✅ Clear | ⚠️ Confusing | ❌ Couldn't find

---

## ✅ **Task 2: Run Your First Pipeline (5 min)**

**Scenario:** You're a junior engineer on day 1.

**Do This:**
1. Open `docs/golden_path.md`
2. Follow Steps 1-3 EXACTLY as written (no shortcuts)
3. Stop at first error or confusion

**Questions:**
- Did the commands work as written?
- Are file paths correct for Windows?
- Does `odibi init` match what's documented?
- Does the template name exist?

**Mark:** ✅ Worked perfectly | ⚠️ Minor fixes needed | ❌ Broken

**Errors Found:**
- 
- 

---

## ✅ **Task 3: Understand Incremental Loading (3 min)**

**Scenario:** Someone asks "Should I use stateful or rolling window?"

**Do This:**
1. Open `docs/visuals/incremental_decision_tree.md`
2. Use ONLY the decision tree to answer
3. Don't use your existing knowledge

**Questions:**
- Does the flowchart give you a clear answer?
- Are the examples helpful (SCADA, lab results)?
- Would you send this to a junior engineer?

**Mark:** ✅ Clear decision | ⚠️ Needs examples | ❌ Confusing

---

## ✅ **Task 4: Add Validation to a Pipeline (5 min)**

**Scenario:** You need to add a "not null" check.

**Do This:**
1. Open `docs/guides/faq.md`
2. Find the answer to "Contracts vs Validation Tests"
3. Copy the YAML example
4. Could you paste this into a real pipeline?

**Questions:**
- Did you find the answer quickly?
- Is the YAML snippet complete (not a fragment)?
- Does it explain WHEN to use contracts vs validation?

**Mark:** ✅ Found + understood | ⚠️ Found but unclear | ❌ Couldn't find

---

## ✅ **Task 5: Fix an SCD2 Issue (5 min)**

**Scenario:** Your SCD2 isn't creating versions.

**Do This:**
1. Go to `docs/visuals/scd2_timeline.md`
2. Look at "Common Pitfalls"
3. Would this help you debug?

**Questions:**
- Are the pitfalls realistic (ones you've actually hit)?
- Do the ❌ Wrong / ✅ Right examples show the fix clearly?
- Is there a pitfall you've hit that's NOT listed?

**Mark:** ✅ Would help debug | ⚠️ Missing pitfalls | ❌ Not useful

**Missing Pitfalls:**
- 
- 

---

## ✅ **Task 6: Verify a Pipeline Worked (3 min)**

**Scenario:** You want to check if Example 3 (SCD2) ran correctly.

**Do This:**
1. Open `examples/verify/verify_03_scd2_dimension.py`
2. Read the code
3. Does it check what you'd actually check?

**Questions:**
- Are the validations correct (is_current, valid_from, surrogate keys)?
- Would the error messages help you debug?
- Is there a check you'd add?

**Mark:** ✅ Comprehensive | ⚠️ Missing checks | ❌ Wrong checks

**Checks to Add:**
- 
- 

---

## ✅ **Task 7: Navigate to Related Content (3 min)**

**Scenario:** You're reading about the fact pattern and want to see an example.

**Do This:**
1. Open `docs/patterns/fact.md`
2. Look at the front-matter (top of file)
3. Click the `next:` links

**Questions:**
- Do the links work?
- Are the suggestions actually helpful?
- Would you have picked different "next" pages?

**Mark:** ✅ Links work + helpful | ⚠️ Links work but wrong suggestions | ❌ Broken links

**Better Suggestions:**
- 
- 

---

## ✅ **Task 8: Read a BA Journey Module (5 min)**

**Scenario:** Test if a non-technical person could follow this.

**Do This:**
1. Open `docs/journeys/business-analyst.md`
2. Read Module 2 (Reading a Data Story)
3. Imagine you're not a data engineer

**Questions:**
- Could a BA actually do the "Do" steps?
- Are the questions too technical?
- Is the verification realistic?
- What would confuse them?

**Mark:** ✅ BA could do this | ⚠️ Needs simplification | ❌ Too technical

**What Would Confuse Them:**
- 
- 

---

## ✅ **Task 9: Find a Specific Answer (2 min)**

**Scenario:** Quick lookups (how you'd actually use docs).

**Test these searches (Ctrl+F in browser or grep):**

1. "How do I reset incremental state?"
   - Found in: _______________
   - Time: _____ seconds
   
2. "SCD2 vs snapshots"
   - Found in: _______________
   - Time: _____ seconds

3. "Orphan handling"
   - Found in: _______________
   - Time: _____ seconds

**Mark:** ✅ All found < 30 sec | ⚠️ Some found | ❌ Couldn't find

---

## ✅ **Task 10: Test the Playbook (5 min)**

**Scenario:** "I need to track dimension history"

**Do This:**
1. Open `docs/playbook/README.md`
2. Find the answer
3. Does it point you to the right place?

**Questions:**
- Did the table give you the answer immediately?
- Did it link to the pattern AND an example?
- Is the decision tree helpful?

**Mark:** ✅ Instant answer | ⚠️ Found but slow | ❌ Didn't help

---

## 📋 **Summary: Issues Found**

### Critical (Breaks User Experience)
- [ ] 
- [ ] 
- [ ] 

### Important (Causes Confusion)
- [ ] 
- [ ] 
- [ ] 

### Nice to Have (Minor Improvements)
- [ ] 
- [ ] 
- [ ] 

---

## 🎯 **The Real Questions**

After this 30-minute test, answer honestly:

1. **Would you hand the BA Journey to an actual analyst right now?**
   - [ ] Yes, confidently
   - [ ] Yes, with caveats: _______________
   - [ ] No, needs fixes first

2. **Could a junior hire onboard using the Jr Journey without you?**
   - [ ] Yes, fully self-service
   - [ ] Mostly, with occasional Slack questions
   - [ ] No, they'd still need handholding

3. **Are the visuals better than text-only explanations?**
   - [ ] Yes, much clearer
   - [ ] Somewhat helpful
   - [ ] Not really needed

4. **What's the biggest gap you found?**
   - 

5. **What surprised you (good or bad)?**
   - 

---

## 💡 **Next Actions Based on Results**

### If 0-2 Critical Issues
**Ship it.** Deploy to GitHub Pages and get real user feedback.

### If 3-5 Critical Issues
**Fix first.** Spend 1-2 hours addressing, then deploy.

### If 6+ Critical Issues
**Redesign needed.** The approach isn't working. Back to drawing board.

---

## 🙏 **Why This Matters**

You built Odibi to **empower yourself** (solo DE getting time back).

The docs should do the same for **others** (make them independent).

**If the docs don't work for you (the expert), they definitely won't work for beginners.**

---

**Time to eat your own dog food.** 🍽️

Go through this checklist. Find what's broken. Then we fix it together.

**I'll wait.**
