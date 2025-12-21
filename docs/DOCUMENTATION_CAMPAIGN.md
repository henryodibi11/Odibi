# Odibi Documentation Campaign

## Goal

Make all documentation accurate, consistent, and professional - matching the quality of the codebase (958 tests passing).

## Context

**Current State:** 155+ markdown files across the project with significant gaps:
- Broken links and missing files in mkdocs.yml
- Duplicate files (yaml_schema.md in two locations)
- Archived work mixed with user-facing docs
- Root tracking files (GAPS.md, STABILITY_CAMPAIGN.md) outdated
- GitHub issues #7-31 document specific gaps

**Success Criteria:**
- [x] `mkdocs build` runs without errors
- [x] All nav links resolve to existing files
- [x] No duplicate or broken files
- [x] Root tracking files reflect current state
- [x] GitHub documentation issues addressed or closed

---

## Phase 1: Cleanup (Delete/Move)

### Files to Delete

| File | Reason |
|------|--------|
| `docs/yaml_schema.md` | Contains Python traceback error (3 lines) |

### Files to Move to `_archive/`

| Source | Destination |
|--------|-------------|
| `docs/archive/` (16 files) | `_archive/docs/` |
| `odibi/agents/docs/` (22 files) | `_archive/agents/docs/` |
| `odibi/agents/ROADMAP.md` | `_archive/agents/` |
| `odibi/agents/AUDIT_FIX_PLAN.md` | `_archive/agents/` |

### Duplicates to Resolve

| File 1 | File 2 | Action |
|--------|--------|--------|
| `docs/WSL_SETUP.md` | `docs/guides/wsl_setup.md` | Delete `docs/WSL_SETUP.md`, keep guides version |

---

## Phase 2: Fix mkdocs.yml

### Broken Nav Links

| Current (broken) | Fix |
|------------------|-----|
| `docs/patterns/hwm_pattern_guide.md` | Change to `docs/patterns/incremental_stateful.md` |
| `docs/guides/contributing.md` | Change to `CONTRIBUTING.md` (root file) |

### Missing Patterns (add to nav)

```yaml
# Add under Patterns section:
- SCD2 Pattern: docs/patterns/scd2.md
- Incremental Stateful: docs/patterns/incremental_stateful.md
- Smart Read: docs/patterns/smart_read.md
- Skip If Unchanged: docs/patterns/skip_if_unchanged.md
```

### Missing Features Section (add to nav)

```yaml
- Features:
    - Engines: docs/features/engines.md
    - Pipelines: docs/features/pipelines.md
    - Connections: docs/features/connections.md
    - CLI: docs/features/cli.md
    - Validation: docs/features/quality_gates.md
    - Quarantine: docs/features/quarantine.md
    - Alerting: docs/features/alerting.md
    - Stories: docs/features/stories.md
    - Catalog: docs/features/catalog.md
    - Lineage: docs/features/lineage.md
    - Transformers: docs/features/transformers.md
    - Patterns: docs/features/patterns.md
    - Schema Tracking: docs/features/schema_tracking.md
    - Diagnostics: docs/features/diagnostics.md
    - Orchestration: docs/features/orchestration.md
```

---

## Phase 3: Update Root Tracking Files

### GAPS.md

Verify each gap against current code:

| Gap | Status to Verify |
|-----|------------------|
| GAP-001: datetime.utcnow deprecation | Check if fixed |
| GAP-002: Pydantic .dict() | Check if fixed |
| GAP-003: Pandas FutureWarning | Check if fixed |
| GAP-004: Polars API deprecation | Check if fixed |
| GAP-005: GitHub Events dataset | Check if exists |
| GAP-006-010 | Verify current state |

Update checkboxes in "Roadmap Recommendations" section.

### STABILITY_CAMPAIGN.md

Update success criteria checkboxes:
- [x] All tests passing (958 on Windows)
- [x] BUGS.md addressed (24 fixed)
- [ ] Review remaining items

### README.md

Fix broken documentation links:

| Current (broken) | Fix |
|------------------|-----|
| `docs/guides/cli_master.md` | `docs/guides/cli_master_guide.md` |
| `docs/guides/writing-transformations.md` | `docs/guides/writing_transformations.md` |

---

## Phase 4: Audit Reference Docs

Verify against actual code implementation:

| File | Verify Against |
|------|----------------|
| `docs/reference/yaml_schema.md` | `odibi/config.py` Pydantic models |
| `docs/reference/cheatsheet.md` | CLI commands, YAML options |
| `docs/reference/configuration.md` | `odibi/config.py` |
| `docs/reference/PARITY_TABLE.md` | Actual engine implementations |

### yaml_schema.md Regeneration

Per AGENTS.md, regenerate from Pydantic models:
```bash
python odibi/introspect.py
```

---

## Phase 5: Audit Guides

For each guide, verify accuracy:

| Guide | Key Checks |
|-------|------------|
| `cli_master_guide.md` | All commands exist and work |
| `writing_transformations.md` | Registration process accurate |
| `production_deployment.md` | Polars mentioned? Engine options correct? |
| `performance_tuning.md` | Polars optimization section? |
| `secrets.md` | Implementation details accurate? |
| `MIGRATION_GUIDE.md` | V3 references still valid? |
| `best_practices.md` | Validation best practices included? |

---

## Phase 6: Audit Feature Docs

For each feature doc, verify:
1. Feature exists in code
2. Configuration examples work
3. No references to non-existent features

| Feature Doc | Priority | Issue # |
|-------------|----------|---------|
| `engines.md` | HIGH | #13 (Polars missing) |
| `quarantine.md` | HIGH | #23 (Integration unclear) |
| `quality_gates.md` | MEDIUM | #11 (Error handling) |
| `schema_tracking.md` | MEDIUM | #31 (Not documented) |
| `cli.md` | MEDIUM | #14 (Commands may not exist) |

---

## Phase 7: Address GitHub Issues

### Documentation Issues to Close/Update

| Issue | Title | Action |
|-------|-------|--------|
| #7 | Configuration.md Incomplete | Audit and update |
| #8 | Getting Started Missing Validation | Add example |
| #9 | README.md Commands Outdated | Verify commands |
| #10 | FK Validation Integration Unclear | Update docs/validation/fk.md |
| #11 | Validation Error Handling Not Documented | Add to feature doc |
| #13 | Polars Engine Missing from Docs | Add to engines.md |
| #14 | CLI Guide References Non-Existent Commands | Audit CLI |
| #15 | Version Mismatch Between Docs | Standardize versions |
| #16 | Secrets Management Missing Details | Update secrets.md |
| #17 | Patterns Doc References Non-Existent File | Fix reference |
| #18 | Engine Parity Table Incomplete | Update PARITY_TABLE.md |
| #19 | Architecture Doc May Be Outdated | Audit architecture.md |
| #20 | Date Dimension Config Not Documented | Add to pattern doc |
| #21 | Custom SQL Transformation Unclear | Update guide |
| #23 | Quarantine Not Integrated with Validation Guide | Cross-link docs |
| #24 | Performance Guide Missing Polars | Add Polars section |
| #25 | Writing Transformations Missing Registration | Add registration |
| #26 | Migration Guide V3 Outdated | Update or note |
| #27 | Best Practices Missing Validation | Add section |
| #28 | Production Deployment References Polars | Verify/add |
| #29 | CLI Commands May Not Match | Audit cheatsheet |
| #30 | Cross-Env Secrets Not Documented | Add to secrets.md |
| #31 | Schema Tracking Not Documented | Create/update doc |

---

## Phase 8: Polish

1. **Consistent formatting** across all docs
2. **Cross-links** between related docs
3. **Navigation** - ensure logical flow
4. **Build test** - `mkdocs build` succeeds
5. **Visual review** - `mkdocs serve` and check

---

## Files Summary

### Keep (User-Facing)

```
docs/
├── tutorials/          # 14 files - Getting started, dimensional modeling
├── guides/             # 15 files - How-to guides
├── reference/          # 9 files - API, schema, cheatsheet
├── patterns/           # 13 files - Pattern documentation
├── features/           # 15 files - Feature documentation
├── semantics/          # 4 files - Semantic layer
├── validation/         # 1 file - FK validation
├── explanation/        # 2 files - Architecture, case studies
└── README.md           # Docs index
```

### Delete

```
docs/yaml_schema.md     # Broken (contains traceback)
docs/WSL_SETUP.md       # Duplicate of guides/wsl_setup.md
```

### Move to _archive/

```
docs/archive/           # 16 historical gap analysis files
odibi/agents/docs/      # 22 archived agent docs
odibi/agents/*.md       # Agent planning docs
```

### Keep but Update

```
GAPS.md                 # Update status of each gap
STABILITY_CAMPAIGN.md   # Check off completed items
README.md               # Fix broken links
BUGS.md                 # Keep as-is (accurate)
CHANGELOG.md            # Keep as-is (accurate)
```

---

## Estimated Effort

| Phase | Effort | Files |
|-------|--------|-------|
| 1. Cleanup | 15 min | ~40 |
| 2. mkdocs.yml | 15 min | 1 |
| 3. Root files | 30 min | 3 |
| 4. Reference audit | 1 hr | 5 |
| 5. Guides audit | 1.5 hr | 15 |
| 6. Features audit | 1 hr | 15 |
| 7. GitHub issues | 2 hr | 25 issues |
| 8. Polish | 30 min | All |

**Total: ~7 hours**

---

## Success Metrics

| Metric | Before | After |
|--------|--------|-------|
| mkdocs build errors | Unknown | 0 |
| Broken nav links | 2 | 0 |
| Duplicate files | 2 | 0 |
| GitHub doc issues open | 25 | 0 |
| Pattern docs in nav | 8 | 13 |
| Feature docs in nav | 0 | 15 |
