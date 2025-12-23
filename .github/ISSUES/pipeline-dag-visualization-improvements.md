# Pipeline DAG Visualization Improvements

## Problem

When pipelines have many nodes (30+), the current DAG visualization in stories becomes difficult to read:

1. **Tangled edges** - Lines cross each other making it hard to trace flows
2. **No clear structure** - Nodes are placed wherever they fit, not by logical layer
3. **No interactivity** - Can't focus on a specific node's lineage
4. **Information overload** - All nodes shown at once with no way to simplify

### Current State
![Current messy DAG with crossing lines and no clear structure]

The current implementation uses Mermaid's default layout which doesn't optimize for hierarchical data pipelines.

---

## Proposed Solution

### 1. Strict Layered Column Layout

Force nodes into columns based on their layer (Bronze → Silver → Gold):

```
    BRONZE              SILVER                  GOLD
    ──────              ──────                  ────
    
┌──────────┐        ┌────────────────┐
│ bronze_a │───────►│ cleaned_a      │────┐
└──────────┘        └────────────────┘    │
                                          │   ┌──────────────────┐
┌──────────┐        ┌────────────────┐    ├──►│ combined_events  │
│ bronze_b │───────►│ cleaned_b      │────┤   └──────────────────┘
└──────────┘        └────────────────┘    │
                                          │
┌──────────┐        ┌────────────────┐    │
│ bronze_c │───────►│ cleaned_c      │────┘
└──────────┘        └────────────────┘
```

**Benefits:**
- Clear left-to-right flow
- Easy to see which layer a node belongs to
- Reduces edge crossings

### 2. Focus Mode (Click to Isolate Lineage)

Clicking a node shows only its upstream and downstream dependencies:

**Before click:** All 50 nodes visible, tangled  
**After clicking `cleaned_orders`:**

```
┌───────────────┐     ┌────────────────┐     ┌──────────────────┐
│ bronze.orders │────►│ cleaned_orders │────►│ combined_sales   │
└───────────────┘     └────────────────┘     └──────────────────┘
                             │
┌───────────────┐            │
│ cleaned_dim   │────────────┘
└───────────────┘

 [ All other nodes faded to 20% opacity or hidden ]
 [ Click background to reset ]
```

**Interactions:**
- Click node → Highlight lineage (upstream + downstream)
- Click background → Reset to full view
- Hover node → Tooltip with node details (rows, duration, status)

### 3. Orthogonal Edge Routing

Replace diagonal crossing lines with right-angle routing:

```
# Current (diagonal, messy)
A ──────────────────────────► X
     B ─────────╲─────────► Y
          C ─────╲────────► Z

# Proposed (orthogonal, clean)
A ───────────────────────────► X
          │
B ────────┼───────────────────► Y
          │
C ────────┴───────────────────► Z
```

### 4. Collapsible Layer Groups (Optional Enhancement)

For very large pipelines, allow collapsing entire layers:

```
┌─ Bronze (24 nodes) ──────────────────────┐
│  [−] Click to collapse                   │
│  ┌────────┐ ┌────────┐ ┌────────┐       │
│  │node_1  │ │node_2  │ │node_3  │ ...   │
│  └────────┘ └────────┘ └────────┘       │
└──────────────────────────────────────────┘
              │
              ▼
┌─ Silver (32 nodes) ──────────────────────┐
│  [+] Collapsed - click to expand         │
└──────────────────────────────────────────┘
              │
              ▼
┌─ Gold (6 nodes) ─────────────────────────┐
│  [−] Click to collapse                   │
│  ┌──────────────┐ ┌──────────────┐      │
│  │combined_prod │ │combined_down │ ...  │
│  └──────────────┘ └──────────────┘      │
└──────────────────────────────────────────┘
```

---

## Technical Approach

### Option A: Enhanced Mermaid (Limited)
- Mermaid has limited layout control
- Can use `%%{init: {'flowchart': {'rankDir': 'LR'}}}%%` for left-to-right
- Cannot do: focus mode, orthogonal routing, click interactions
- **Verdict:** Not sufficient for requirements

### Option B: Dagre + D3.js
- Dagre: Graph layout algorithm (hierarchical)
- D3.js: Rendering and interactions
- Pros: Full control, good hierarchical layout
- Cons: More code to maintain

### Option C: Cytoscape.js (Recommended)
- Purpose-built for graph visualization
- Built-in hierarchical layouts (`dagre`, `elk`)
- Easy click/hover interactions
- Edge bundling and orthogonal routing plugins
- Active community, well-documented
- **Verdict:** Best fit for requirements

### Option D: ELK.js (Eclipse Layout Kernel)
- Excellent for complex hierarchical graphs
- Best-in-class edge routing (orthogonal, spline)
- Can integrate with Cytoscape.js or use standalone
- **Verdict:** Use as layout engine with Cytoscape for rendering

---

## Recommended Implementation

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│   ELK.js          →    Cytoscape.js    →    HTML/CSS   │
│   (Layout)             (Render + Events)    (Story)    │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Phase 1: Basic Improvements
- [ ] Switch from Mermaid to Cytoscape.js
- [ ] Implement hierarchical layout with layer columns
- [ ] Add node status colors (success=green, failed=red, skipped=gray)
- [ ] Add hover tooltips (rows processed, duration)

### Phase 2: Focus Mode
- [ ] Click node to highlight lineage
- [ ] Fade non-related nodes to 20% opacity
- [ ] Click background to reset
- [ ] Keyboard shortcut: Escape to reset

### Phase 3: Advanced Layout
- [ ] Integrate ELK.js for orthogonal edge routing
- [ ] Reduce edge crossings
- [ ] Group nodes by layer with visual separators

### Phase 4: Collapsible Groups (Optional)
- [ ] Collapse/expand by layer
- [ ] Collapse/expand by domain (if tags defined)
- [ ] Remember collapse state in localStorage

---

## Acceptance Criteria

- [ ] DAG displays with clear left-to-right layer flow
- [ ] Clicking a node highlights only its upstream/downstream
- [ ] Edges have minimal crossings
- [ ] Works with 50+ node pipelines without becoming unreadable
- [ ] Node colors indicate status (success/failed/skipped)
- [ ] Hover shows node details

---

## References

- [Cytoscape.js](https://js.cytoscape.org/)
- [Cytoscape Dagre Layout](https://github.com/cytoscape/cytoscape.js-dagre)
- [ELK.js](https://github.com/kieler/elkjs)
- [Cytoscape ELK Layout](https://github.com/cytoscape/cytoscape.js-elk)

---

## Labels

`enhancement` `ux` `stories` `visualization`

## Priority

Medium - Improves usability but not blocking functionality
