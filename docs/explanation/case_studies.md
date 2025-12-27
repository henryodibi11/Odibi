# Case Studies (Reference Projects)

Odibi is battle-tested. To prove it, we built "The Gauntlet"—a series of reference implementations covering diverse industries and data challenges.

These projects live in the `examples/` directory and serve as blueprints for your own architectures.

---

## 1. OdibiFlix (High-Volume Clickstream)

**Scenario:** A streaming service processing millions of user events (play, pause, buffer).
**Challenge:**
*   **Sessionization:** Grouping raw events into distinct user sessions (30-minute timeout).
*   **State Management:** Calculating "buffer ratio" per session.

**Architecture:**
*   **Engine:** Spark (required for window functions).
*   **Pattern:** Medallion (Bronze JSON -> Silver Delta -> Gold Aggregates).
*   **Key Feature:** Uses `odibi.yaml` to orchestrate complex SQL window functions without writing Python code.

---

## 2. OdibiEats (Real-Time Delivery)

**Scenario:** A food delivery app with geospatial data.
**Challenge:**
*   **Geospatial Joins:** Mapping driver lat/long pings to neighborhood polygons.
*   **SCD Type 2:** Tracking menu price changes over time (historical accuracy).

**Architecture:**
*   **Engine:** Pandas (Geopandas extension).
*   **Pattern:** Star Schema (Fact Orders, Dim Restaurants, Dim Drivers).
*   **Key Feature:** Demonstrates how to handle "slowly changing dimensions" using Odibi's snapshotting capabilities.

---

## 3. OdibiHealth (Sensitive PII)

**Scenario:** A healthcare provider managing patient records.
**Challenge:**
*   **Compliance:** HIPAA requirements to mask PII (Patient Identity).
*   **Audit:** Strict lineage tracking of who accessed what data.

**Architecture:**
*   **Security:** Uses Odibi's `sensitive: true` flag to redact columns in logs/stories.
*   **Observability:** High-fidelity logging enabled.
*   **Key Feature:** Shows how to build secure pipelines that generate audit trails automatically via `odibi story`.

---

## 4. OdibiQuant (Financial Time-Series)

**Scenario:** High-frequency trading data analysis.
**Challenge:**
*   **Data Quality:** Ensuring no gaps in timestamps.
*   **Precision:** Handling 64-bit floats without rounding errors.

**Architecture:**
*   **Validation:** Heavy use of `expectations` (e.g., `row_count > 0`, `price > 0`).
*   **Engine:** DuckDB (via Pandas engine) for fast analytical queries.

---

## Why use these?

Don't start from scratch. If you are building a fintech app, copy `OdibiQuant`. If you are building an e-commerce site, look at `OdibiEats`.

Run them locally:
```bash
cd examples/odibi_flix
odibi run odibi.yaml
```
