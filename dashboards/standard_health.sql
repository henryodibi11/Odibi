-- ============================================================================
-- ODIBI 2.1 STANDARD DASHBOARD TEMPLATE
-- ============================================================================
-- Copy/Paste these queries into a Databricks SQL Dashboard or Power BI Source.
--
-- PRE-REQUISITE:
-- Ensure '_odibi_system' tables are registered in the Hive Metastore or Unity Catalog.
-- If using file paths, replace `odibi_system.meta_runs` with `delta.\`path/to/meta_runs\``
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. PIPELINE HEALTH OVERVIEW
-- Goal: What is the status of my latest runs?
-- ----------------------------------------------------------------------------
WITH latest_runs AS (
    SELECT
        pipeline_name,
        node_name,
        max(timestamp) as last_run_at
    FROM delta.`_odibi_system/meta_runs`
    GROUP BY 1, 2
)
SELECT
    r.pipeline_name,
    r.node_name,
    r.status,
    r.duration_ms / 1000.0 as duration_seconds,
    r.rows_processed,
    r.timestamp
FROM delta.`_odibi_system/meta_runs` r
JOIN latest_runs lr
    ON r.pipeline_name = lr.pipeline_name
    AND r.node_name = lr.node_name
    AND r.timestamp = lr.last_run_at
ORDER BY r.status DESC, r.timestamp DESC;

-- ----------------------------------------------------------------------------
-- 2. PERFORMANCE TRENDS (Last 30 Days)
-- Goal: Identify slow-degrading nodes.
-- ----------------------------------------------------------------------------
SELECT
    date_trunc('day', timestamp) as run_date,
    node_name,
    avg(duration_ms) / 1000.0 as avg_duration_sec,
    max(duration_ms) / 1000.0 as max_duration_sec
FROM delta.`_odibi_system/meta_runs`
WHERE timestamp >= current_date() - 30
GROUP BY 1, 2
ORDER BY 1 DESC;

-- ----------------------------------------------------------------------------
-- 3. DATA VOLUME ANOMALIES
-- Goal: Did we process significantly fewer rows than usual?
-- ----------------------------------------------------------------------------
WITH stats AS (
    SELECT
        node_name,
        avg(rows_processed) as avg_rows,
        stddev(rows_processed) as std_rows
    FROM delta.`_odibi_system/meta_runs`
    WHERE timestamp >= current_date() - 30
    GROUP BY 1
)
SELECT
    r.timestamp,
    r.node_name,
    r.rows_processed,
    s.avg_rows,
    (r.rows_processed - s.avg_rows) / nullif(s.std_rows, 0) as z_score
FROM delta.`_odibi_system/meta_runs` r
JOIN stats s ON r.node_name = s.node_name
WHERE r.timestamp >= current_date() - 1
  -- Show rows where volume is > 2 standard deviations from mean
  AND abs((r.rows_processed - s.avg_rows) / nullif(s.std_rows, 0)) > 2
ORDER BY z_score ASC;

-- ----------------------------------------------------------------------------
-- 4. PLATFORM INVENTORY
-- Goal: What assets do we have and are they compliant?
-- ----------------------------------------------------------------------------
SELECT
    t.project_name,
    t.table_name,
    t.pattern_type,
    t.format,
    t.updated_at as registered_at,
    p.compliance_score
FROM delta.`_odibi_system/meta_tables` t
LEFT JOIN delta.`_odibi_system/meta_patterns` p ON t.table_name = p.table_name;
