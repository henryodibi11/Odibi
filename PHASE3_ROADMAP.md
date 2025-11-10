# Phase 3 Roadmap

**Target:** Q1 2026  
**Status:** Planning (Phase 2.5 in progress)

---

## Overview

Phase 3 completes Odibi's transparency and documentation capabilities with:
- Automatic story generation (run + documentation)
- Transformation registry with enforced documentation
- Quality linting and validation
- Azure SQL connector
- Enhanced CLI tools

---

## Sub-Phases

### Phase 3A: Foundation (4 weeks)

**Deliverables:**
- Transformation registry
- Explanation system
- Quality enforcement (linting)
- Built-in operations (pivot, unpivot, join, sql)

**Key Features:**
- `@transformation` decorator working
- `@func.explain` pattern implemented
- Explanation linter enforcing quality
- 4 core operations with explanations
- 200+ tests passing

**Documentation:** [PHASE_3A_PLAN.md](PHASE_3A_PLAN.md)

**Release:** v1.3.0-alpha.1-phase3a

---

### Phase 3B: Stories (3 weeks)

**Deliverables:**
- Run story engine (auto-capture runtime)
- Doc story generator (pull from explanations)
- Theme system (branding, footer, colors)

**Key Features:**
- Every pipeline run generates HTML story
- `odibi story generate` creates doc stories
- Customizable themes and branding
- Reference tables support

**Documentation:** PHASE_3B_PLAN.md (to be created)

**Release:** v1.3.0-alpha.2-phase3b

---

### Phase 3C: CLI + Diffing (2 weeks)

**Deliverables:**
- Enhanced CLI commands
- Story diffing (compare runs)
- Batch story generation

**Key Features:**
- `odibi story generate config.yaml`
- `odibi story generate-all configs/*.yaml`
- `odibi story diff run1.html run2.html`
- `odibi validate --check-explanations`

**Documentation:** PHASE_3C_PLAN.md (to be created)

**Release:** v1.3.0-alpha.3-phase3c

---

### Phase 3D: Azure SQL (1 week)

**Deliverables:**
- Azure SQL connector implementation
- Read/write operations
- Examples and tests

**Key Features:**
- connections/azure_sql.py implemented
- Read and write to Azure SQL
- 15+ tests passing

**Documentation:** PHASE_3D_PLAN.md (to be created)

**Release:** v1.3.0-alpha.4-phase3d

**Note:** Can run in parallel with other sub-phases.

---

### Phase 3E: Documentation (1 week)

**Deliverables:**
- User guides
- API documentation
- Best practices
- Examples

**Key Features:**
- Complete user documentation
- Story generation guide
- Explanation writing guide
- Best practices documented

**Documentation:** PHASE_3E_PLAN.md (to be created)

**Release:** v1.3.0-rc.1

---

## Total Timeline

**11 weeks** to complete Phase 3

| Sub-Phase | Duration | Focus |
|-----------|----------|-------|
| 3A | 4 weeks | Foundation & Registry |
| 3B | 3 weeks | Story Generation |
| 3C | 2 weeks | CLI & Diffing |
| 3D | 1 week | Azure SQL (parallel) |
| 3E | 1 week | Documentation |

---

## Test Count Progression

| Phase | New Tests | Total Tests | Coverage |
|-------|-----------|-------------|----------|
| 2.5 (Current) | +87 | 224 | 79% |
| 3A | +48 | 272 | 82% |
| 3B | +30 | 302 | 85% |
| 3C | +15 | 317 | 87% |
| 3D | +15 | 332 | 88% |
| 3E | +10 | 342 | 90% |

**Target by end of Phase 3: 342+ tests, 90% coverage**

---

## Acceptance Criteria (Phase 3 Complete)

### Functionality
- [ ] Users can register transformations with `@transformation`
- [ ] Transformations define `explain()` methods
- [ ] Every pipeline run generates run story (auto)
- [ ] `odibi story generate` creates doc stories
- [ ] `odibi story diff` compares runs
- [ ] `odibi validate --check-explanations` enforces quality
- [ ] Azure SQL connector works

### Quality
- [ ] 342+ tests passing
- [ ] Coverage ≥ 90%
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

## Current Status (Phase 2.5)

- [x] Phase 2C complete (v1.2.0-alpha.3)
- [x] Phase 2.5 in progress (v1.2.0-alpha.4)
  - [x] CLI module created
  - [x] Phase 3 scaffolding in place
  - [x] Dependencies added
  - [x] 79% test coverage
  - [ ] Final validation pending
- [ ] Phase 3A planned (ready to execute)

**Next Action:** Complete Phase 2.5, then begin Phase 3A

---

## Detailed Plans

1. **[PHASE_2.5_PLAN.md](PHASE_2.5_PLAN.md)** - Reorganization (2 weeks) ← Current
2. **[PHASE_3A_PLAN.md](PHASE_3A_PLAN.md)** - Foundation (4 weeks) ← Next
3. **[PHASE_3_COMPLETE_PLAN.md](PHASE_3_COMPLETE_PLAN.md)** - Full overview

**To be created:**
- PHASE_3B_PLAN.md (Stories - 3 weeks)
- PHASE_3C_PLAN.md (CLI + Diffing - 2 weeks)
- PHASE_3D_PLAN.md (Azure SQL - 1 week)
- PHASE_3E_PLAN.md (Documentation - 1 week)

---

**Last Updated:** 2025-11-10  
**Maintainer:** Henry Odibi
