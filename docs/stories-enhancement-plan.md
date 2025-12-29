# Stories Enhancement Plan

> **Issue Reference:** [#45 - Pipeline DAG Visualization Improvements](https://github.com/henryodibi11/Odibi/issues/45)  
> **Created:** 2025-12-28  
> **Status:** ✅ Complete (All 5 Phases)

## Overview

Transform pipeline stories from static HTML reports into a powerful debugging and observability tool. The goal is to make a failing run diagnosable in 10-30 seconds and provide "what changed?" context without digging through Git or logs.

## Current State

- Stories are HTML reports generated after pipeline runs
- Uses **Mermaid** for static DAG visualization (limited interactivity)
- Tracks: node execution metadata, schemas, SQL logic, data samples, validation failures, Delta Lake info, retries
- Renders via Jinja2 templates to HTML/JSON/Markdown
- Rich metadata captured but **not fully surfaced** in the UI

## Technical Approach

### DAG Visualization
- Replace Mermaid with **Cytoscape.js** for interactive graph (CDN, no Python dependency)
- Optionally integrate **ELK.js** for better hierarchical layout
- Embed graph data as JSON in the HTML template

### Cross-Run Comparison
- Stories are saved as: `stories/{pipeline}/{YYYY-MM-DD}/run_{HH-MM-SS}.json`
- Multiple runs per day supported; retention keeps up to 100 files
- Compare by globbing `stories/{pipeline}/**/*.json`, sorting by path, finding last success

---

## Implementation Phases

### Phase 1: Core Triage (Must Have) ✅ COMPLETE
*Goal: Instant "what broke?" diagnosis*

| # | Feature | Effort | Status | Description |
|---|---------|--------|--------|-------------|
| 1.1 | Run Health Header | M | ✅ | Top-of-page card showing failed node names, counts, first failure |
| 1.2 | Anomaly Badges | M | ✅ | Flag nodes 3× slower or ±50% rows vs historical avg |
| 1.3 | Auto-expand Failed Nodes | S | ✅ | Collapsed by default, auto-open failed nodes + anomalies |
| 1.4 | Filter Bar | S | ✅ | "Show: All \| Failed \| Anomalous" toggle (pure JS) |
| 1.5 | Copy Buttons | S | ✅ | "Copy SQL", "Copy Error", "Copy Config YAML" buttons |

**Completed:** 2025-12-28

---

### Phase 2: Interactive DAG ✅ COMPLETE
*Goal: Turn the graph into a debugging tool, not decoration*

| # | Feature | Effort | Status | Description |
|---|---------|--------|--------|-------------|
| 2.1 | Graph JSON in metadata | M | ✅ | Compute nodes/edges from `DependencyGraph`, embed in story JSON |
| 2.2 | Cytoscape.js DAG | L | ✅ | Replace Mermaid with interactive graph; color nodes by status |
| 2.3 | Click node → scroll to card | S | ✅ | Click node in DAG → scroll to corresponding node card |
| 2.4 | Hover tooltips | M | ✅ | Show status, duration, rows, validation count on hover |
| 2.5 | Highlight failed lineage | M | ✅ | Shift+click node → highlight upstream/downstream; grey out others |

**Completed:** 2025-12-28

**Technical notes:**
- Uses Cytoscape.js via CDN with dagre layout for hierarchical display
- Graph data enriched with runtime metadata (status, duration, rows, anomalies)
- Graph data structure:
  ```json
  {
    "nodes": [{"id": "node_name", "status": "success", "duration": 1.5, "rows_out": 1000, "is_anomaly": false}],
    "edges": [{"source": "upstream", "target": "downstream"}]
  }
  ```

---

### Phase 3: Cross-Run Context ✅ COMPLETE
*Goal: Answer "what changed?" without Git digging*

| # | Feature | Effort | Status | Description |
|---|---------|--------|--------|-------------|
| 3.1 | Compare vs last success | L | ✅ | Load prior JSON, compute per-node diffs (SQL hash, schema, rows, duration) |
| 3.2 | Changes summary card | M | ✅ | Card showing: "5 nodes changed SQL, 2 changed schema, 1 newly failing" |
| 3.3 | Config diff viewer | M | ✅ | Side-by-side YAML diff with inline diff view toggle |
| 3.4 | Pipeline history index | M | ✅ | Generate `stories/{pipeline}/index.html` with table of recent runs |
| 3.5 | Git info card | S | ✅ | Show commit/branch in header: "main @ abc123" |

**Completed:** 2025-12-28

**Implementation notes:**
- Comparison finds last successful run by globbing `stories/{pipeline}/**/*.json`
- Detects changes in: SQL hash, schema, row counts (>20%), duration (2x slower)
- "Changed" filter button added alongside Failed/Anomalous filters
- Pipeline history index auto-generated after each run

---

### Phase 4: Polish & UX ✅ COMPLETE
*Goal: Quality-of-life improvements*

| # | Feature | Effort | Status | Description |
|---|---------|--------|--------|-------------|
| 4.1 | Dark mode toggle | S | ✅ | CSS variables + JS toggle; save preference to localStorage |
| 4.2 | Timestamp localizer | S | ✅ | Convert UTC timestamps to user's local timezone via JS |
| 4.3 | Duration sparkline | M | ✅ | Inline SVG sparkline showing duration trend across last 10 runs |
| 4.4 | Node search box | S | ✅ | Filter nodes by name; highlight in DAG and node list |
| 4.5 | View mode toggle | S | ✅ | "Developer / Stakeholder" switch; hide SQL/tracebacks for stakeholders |

**Completed:** 2025-12-28

**Features:**
- Dark mode with full CSS variable theming, persisted to localStorage
- Timestamps auto-convert to user's local timezone on page load
- Search box filters node cards AND highlights matching nodes in the DAG
- Stakeholder mode hides SQL tabs, Config tabs, and tracebacks

---

### Phase 5: Quality & Documentation ✅ COMPLETE
*Goal: Data quality visibility and self-documenting stories*

| # | Feature | Effort | Status | Description |
|---|---------|--------|--------|-------------|
| 5.1 | Data Quality Summary card | M | ✅ | Top-level: # validations failed, top columns by null%, total failed rows |
| 5.2 | Column statistics | M | ✅ | Min/max/mean/stddev per numeric column (sampled) - UI ready, data optional |
| 5.3 | Node descriptions from config | S | ✅ | Pull `description` field from NodeConfig, show in story cards |
| 5.4 | Runbook links | S | ✅ | Add `runbook_url` field; show "Troubleshooting guide →" link on failures |
| 5.5 | Freshness indicator | S | ✅ | Show max timestamp in date columns ("Data as of: 2025-12-27 23:59") |

**Completed:** 2025-12-28

**Implementation notes:**
- Data Quality Summary card aggregates validation warnings and failed rows across all nodes
- Top columns by null % displayed with color-coded badges (red >90%, orange >50%)
- Freshness indicator auto-detects date/timestamp columns from sample data
- Node descriptions shown below node name in card headers
- Runbook links appear as "Troubleshooting guide →" only on failed nodes
- Column statistics table supports min/max/mean/stddev (data collection is optional)

---

## Summary

| Phase | Features | Effort |
|-------|----------|--------|
| 1 - Core Triage | 5 | ~1 day |
| 2 - Interactive DAG | 5 | ~2 days |
| 3 - Cross-Run Context | 5 | ~2 days |
| 4 - Polish & UX | 5 | ~1 day |
| 5 - Quality & Docs | 5 | ~1 day |
| **Total** | **25** | **~7 days** |

---

## Files to Modify

| File | Changes |
|------|---------|
| `odibi/story/metadata.py` | Add `graph_data`, `change_summary`, `anomaly_flags` fields |
| `odibi/story/generator.py` | Add comparison logic, graph data generation, anomaly detection |
| `odibi/story/templates/run_story.html` | Major UI overhaul: header, DAG, filters, dark mode, etc. |
| `odibi/story/renderers.py` | Pass graph data to template |
| `odibi/graph.py` | Add `to_dict()` method for JSON export |
| `odibi/config.py` | Add `description`, `runbook_url` fields to NodeConfig |

---

## Future Considerations (Out of Scope)

- **Slack/Teams webhooks** - Post story summary on failure
- **PDF export** - One-click PDF for stakeholders
- **Multi-pipeline dashboard** - Global view of all pipelines
- **Column-level lineage** - Track column origins through transformations
- **Story metadata warehouse** - Query history via DuckDB/SQLite

---

## Success Criteria

1. Open a failing run story → know what broke in <30 seconds
2. See "what changed vs last success" without leaving the story
3. Click through the DAG to navigate/debug interactively
4. Works for 50+ node pipelines without becoming unreadable
5. Stakeholders can view a clean summary (stakeholder mode)
