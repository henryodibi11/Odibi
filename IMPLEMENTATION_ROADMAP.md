# Odibi Implementation Roadmap

**Last Updated:** 2025-11-10  
**Status:** Planning Complete - Ready for Execution

---

## Overview

This document provides the complete implementation roadmap from Phase 2.5 (Reorganization) through Phase 3 (Transparency & Documentation).

**Timeline:** ~13 weeks total
- Phase 2.5: 2 weeks (Reorganization)
- Phase 3: 11 weeks (Feature Development)

**Guiding Principles:**
1. **Test before moving forward** - Every feature thoroughly tested
2. **Document as you build** - Not after the fact
3. **No shortcuts** - Do it right, not fast
4. **Backward compatible** - No breaking changes
5. **Enforce excellence** - Quality is mandatory, not optional

---

## Phase Breakdown

### Phase 2.5: Reorganization & Foundation (2 weeks)

**Purpose:** Prepare codebase for Phase 3 growth

**Deliverables:**
- CLI module reorganization
- Phase 3 scaffolding (empty folders with documentation)
- Dependencies added (markdown2, Jinja2, SQL libraries)
- All 152+ tests passing
- No breaking changes

**Documentation:** [PHASE_2.5_PLAN.md](PHASE_2.5_PLAN.md)

**Acceptance Criteria:**
- ✅ CLI commands work (`odibi run`, `odibi validate`)
- ✅ Phase 3 modules importable (but empty)
- ✅ Dependencies installed
- ✅ PROJECT_STRUCTURE.md updated
- ✅ Release: v1.2.0-alpha.4-phase2.5

---

### Phase 3: Transparency & Documentation (11 weeks)

**Purpose:** Build self-documenting, transparent pipelines

**Sub-Phases:**

#### 3A: Foundation (4 weeks)
- Transformation registry
- Explanation system
- Quality enforcement (linting)
- Built-in operations (pivot, unpivot, join, sql)

**Deliverables:**
- @transformation decorator working
- @func.explain pattern implemented
- Explanation linter enforcing quality
- 4 core operations with explanations
- 200+ tests passing

**Documentation:** [PHASE_3A_PLAN.md](PHASE_3A_PLAN.md)

**Release:** v1.3.0-alpha.1-phase3a

---

#### 3B: Stories (3 weeks)
- Run story engine (auto-capture runtime)
- Doc story generator (pull from explanations)
- Theme system (branding, footer, colors)

**Deliverables:**
- Every pipeline run generates HTML story
- `odibi story generate` creates doc stories
- Customizable themes and branding
- Reference tables support

**Documentation:** [PHASE_3B_PLAN.md](PHASE_3B_PLAN.md) (to be created)

**Release:** v1.3.0-alpha.2-phase3b

---

#### 3C: CLI + Diffing (2 weeks)
- Enhanced CLI commands
- Story diffing (compare runs)
- Batch story generation

**Deliverables:**
- `odibi story generate config.yaml`
- `odibi story generate-all configs/*.yaml`
- `odibi story diff run1.html run2.html`
- `odibi validate --check-explanations`

**Documentation:** [PHASE_3C_PLAN.md](PHASE_3C_PLAN.md) (to be created)

**Release:** v1.3.0-alpha.3-phase3c

---

#### 3D: Azure SQL (1 week) - Can run in parallel
- Azure SQL connector implementation
- Read/write operations
- Examples and tests

**Deliverables:**
- connections/azure_sql.py implemented
- Read and write to Azure SQL
- 15+ tests passing

**Documentation:** [PHASE_3D_PLAN.md](PHASE_3D_PLAN.md) (to be created)

**Release:** v1.3.0-alpha.4-phase3d

---

#### 3E: Documentation (1 week)
- User guides
- API documentation
- Best practices
- Examples

**Deliverables:**
- Complete user documentation
- Story generation guide
- Explanation writing guide
- Best practices documented

**Documentation:** [PHASE_3E_PLAN.md](PHASE_3E_PLAN.md) (to be created)

**Release:** v1.3.0-rc.1

---

## Test Count Progression

| Phase | New Tests | Total Tests | Coverage |
|-------|-----------|-------------|----------|
| 2C (Current) | - | 137 | 78% |
| 2.5 | +15 | 152 | 78% |
| 3A | +48 | 200 | 80% |
| 3B | +30 | 230 | 82% |
| 3C | +15 | 245 | 82% |
| 3D | +15 | 260 | 83% |
| 3E | +10 | 270 | 83% |

**Target by end of Phase 3: 270+ tests, 83% coverage**

---

## Documentation Deliverables

### Phase 2.5
- [x] PROJECT_STRUCTURE.md updated
- [x] PHASE3_ROADMAP.md created
- [x] Module __init__.py files documented

### Phase 3A
- [ ] Transformation registry docs
- [ ] Explanation writing guide
- [ ] Template library docs
- [ ] Built-in operations reference

### Phase 3B
- [ ] Story generation guide
- [ ] Theme customization guide
- [ ] Reference tables guide

### Phase 3C
- [ ] CLI command reference
- [ ] Story diffing guide
- [ ] Troubleshooting guide

### Phase 3D
- [ ] Azure SQL setup guide
- [ ] Connection examples

### Phase 3E
- [ ] Complete user manual
- [ ] API reference (auto-generated)
- [ ] Best practices guide
- [ ] Migration guide (if needed)

---

## File Structure Evolution

### Current (Phase 2C)
```
odibi/
├── config.py
├── pipeline.py
├── engine/
├── connections/
└── utils/
```

### After Phase 2.5
```
odibi/
├── cli/               # NEW
├── operations/        # NEW (empty)
├── transformations/   # NEW (empty)
├── story/            # NEW (empty)
├── validation/       # NEW (empty)
├── testing/          # NEW (empty)
└── [existing modules]
```

### After Phase 3A
```
odibi/
├── transformations/   # IMPLEMENTED
│   ├── registry.py
│   ├── decorators.py
│   ├── context.py
│   ├── explanation.py
│   └── templates.py
├── operations/        # IMPLEMENTED
│   ├── pivot.py
│   ├── unpivot.py
│   ├── join.py
│   └── sql.py
├── validation/        # IMPLEMENTED
│   └── explanation_linter.py
└── [other modules]
```

### After Phase 3 Complete
```
odibi/
├── cli/              # Enhanced
├── transformations/  # Complete
├── operations/       # Complete
├── story/           # Complete
│   ├── engine.py
│   ├── run_tracker.py
│   ├── doc_generator.py
│   ├── renderer.py
│   ├── theme.py
│   └── diff.py
├── validation/       # Complete
├── testing/         # Complete
└── [existing modules]
```

---

## Dependencies Added

### Phase 2.5
```toml
markdown2 = "^2.4.0"     # Story markdown rendering
Jinja2 = "^3.1.0"        # HTML templating
pyodbc = "^5.0.0"        # Azure SQL (optional)
sqlalchemy = "^2.0.0"    # Azure SQL (optional)
```

### Phase 3
No additional dependencies - all features use Phase 2.5 additions.

---

## Git Workflow

### Branching Strategy
```
main (protected)
  ├── phase-2.5-reorganization
  ├── phase-3a-foundation
  ├── phase-3b-stories
  ├── phase-3c-cli-diffing
  ├── phase-3d-azure-sql (can branch from main in parallel)
  └── phase-3e-documentation
```

### Merge & Release Process
1. Complete phase in feature branch
2. All tests passing
3. Documentation updated
4. Create PR to main
5. Review & merge
6. Tag release
7. Publish GitHub release

---

## CI/CD Updates

### Phase 2.5
- Add SQL extras job

### Phase 3A
- Add explanation linting to CI
- Test transformation registry

### Phase 3B
- Test story generation
- Verify HTML output

### Phase 3C
- Test CLI commands
- Test story diffing

All phases: Test on Python 3.9, 3.10, 3.11, 3.12

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Tests break during reorganization | Test after each change, maintain backward compatibility |
| Complexity explosion | Keep modules focused, one responsibility per file |
| Performance issues | Profile early, optimize if needed (likely fine for Phase 3) |
| Documentation falls behind | Document during development, not after |
| Breaking changes | Maintain backward compatibility, no breaking changes |

---

## Success Criteria (Phase 3 Complete)

### Functionality
- [ ] Users can register transformations with `@transformation`
- [ ] Transformations define `explain()` methods
- [ ] Every pipeline run generates run story (auto)
- [ ] `odibi story generate` creates doc stories
- [ ] `odibi story diff` compares runs
- [ ] `odibi validate --check-explanations` enforces quality
- [ ] Azure SQL connector works

### Quality
- [ ] 270+ tests passing
- [ ] Coverage ≥ 83%
- [ ] CI/CD green on all Python versions
- [ ] No critical linter errors
- [ ] No breaking changes

### Documentation
- [ ] Complete user guide
- [ ] API documentation
- [ ] Best practices documented
- [ ] Examples working

### Usability
- [ ] Clear error messages
- [ ] Intuitive CLI
- [ ] Good documentation
- [ ] Working examples

---

## Post-Phase 3

**User Validation (1-2 weeks):**
- Deploy to test environment
- Gather feedback
- Fix bugs

**Stable Release:**
- v1.3.0 stable
- Announce release
- Update documentation site

**Phase 4 Planning:**
- Performance optimization
- Production hardening
- Reliability features

---

## Current Status

- [x] Phase 2C complete (v1.2.0-alpha.3)
- [ ] Phase 2.5 planned (ready to execute)
- [ ] Phase 3A planned (ready to execute)
- [ ] Phase 3B-E to be planned (detailed plans coming)

**Next Action:** Begin Phase 2.5 execution

---

## Detailed Plans

1. **[PHASE_2.5_PLAN.md](PHASE_2.5_PLAN.md)** - Reorganization (2 weeks)
2. **[PHASE_3A_PLAN.md](PHASE_3A_PLAN.md)** - Foundation (4 weeks)
3. **[PHASE_3_COMPLETE_PLAN.md](PHASE_3_COMPLETE_PLAN.md)** - Overview

**To be created:**
- PHASE_3B_PLAN.md (Stories - 3 weeks)
- PHASE_3C_PLAN.md (CLI + Diffing - 2 weeks)
- PHASE_3D_PLAN.md (Azure SQL - 1 week)
- PHASE_3E_PLAN.md (Documentation - 1 week)

---

**Questions? Review the detailed plans above or start with Phase 2.5.**

---

**Last Updated:** 2025-11-10  
**Maintainer:** Henry Odibi
