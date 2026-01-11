"""
Catalog Sync - Syncs system catalog data to secondary destinations.

Enables replication of Delta-based system tables to SQL Server (for dashboards/queries)
or another blob storage (for cross-region backup).
"""

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from odibi.config import SyncToConfig
from odibi.utils import get_logging_context

logger = logging.getLogger(__name__)


# Default high-priority tables to sync if not specified
DEFAULT_SYNC_TABLES = [
    "meta_runs",
    "meta_pipeline_runs",
    "meta_node_runs",
    "meta_tables",
    "meta_failures",
    "meta_sla_status",
]

# All available tables that can be synced
ALL_SYNC_TABLES = [
    "meta_tables",
    "meta_runs",
    "meta_patterns",
    "meta_metrics",
    "meta_state",
    "meta_pipelines",
    "meta_nodes",
    "meta_schemas",
    "meta_lineage",
    "meta_outputs",
    "meta_pipeline_runs",
    "meta_node_runs",
    "meta_failures",
    "meta_observability_errors",
    "meta_derived_applied_runs",
    "meta_daily_stats",
    "meta_pipeline_health",
    "meta_sla_status",
]

# SQL Server DDL templates for each table
# NOTE: These schemas MUST match the Delta table schemas in catalog.py
SQL_SERVER_DDL = {
    "meta_runs": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_runs' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_runs] (
                run_id NVARCHAR(100),
                project NVARCHAR(255),
                pipeline_name NVARCHAR(255),
                node_name NVARCHAR(255),
                status NVARCHAR(50),
                rows_processed BIGINT,
                duration_ms BIGINT,
                metrics_json NVARCHAR(MAX),
                environment NVARCHAR(50),
                timestamp DATETIME2,
                date DATE,
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
            CREATE INDEX IX_meta_runs_project_pipeline ON [{schema}].[meta_runs] (project, pipeline_name, date);
        END
    """,
    "meta_pipeline_runs": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_pipeline_runs' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_pipeline_runs] (
                run_id NVARCHAR(100) PRIMARY KEY,
                project NVARCHAR(255),
                pipeline_name NVARCHAR(255),
                owner NVARCHAR(255),
                layer NVARCHAR(50),
                run_start_at DATETIME2,
                run_end_at DATETIME2,
                duration_ms BIGINT,
                status NVARCHAR(50),
                nodes_total BIGINT,
                nodes_succeeded BIGINT,
                nodes_failed BIGINT,
                nodes_skipped BIGINT,
                rows_processed BIGINT,
                error_summary NVARCHAR(500),
                terminal_nodes NVARCHAR(MAX),
                environment NVARCHAR(50),
                databricks_cluster_id NVARCHAR(100),
                databricks_job_id NVARCHAR(100),
                databricks_workspace_id NVARCHAR(100),
                estimated_cost_usd FLOAT,
                actual_cost_usd FLOAT,
                cost_source NVARCHAR(50),
                created_at DATETIME2,
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
            CREATE INDEX IX_meta_pipeline_runs_project ON [{schema}].[meta_pipeline_runs] (project, pipeline_name, created_at);
        END
    """,
    "meta_node_runs": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_node_runs' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_node_runs] (
                run_id NVARCHAR(100),
                node_id NVARCHAR(100),
                project NVARCHAR(255),
                pipeline_name NVARCHAR(255),
                node_name NVARCHAR(255),
                status NVARCHAR(50),
                run_start_at DATETIME2,
                run_end_at DATETIME2,
                duration_ms BIGINT,
                rows_processed BIGINT,
                estimated_cost_usd FLOAT,
                metrics_json NVARCHAR(MAX),
                environment NVARCHAR(50),
                created_at DATETIME2,
                _synced_at DATETIME2 DEFAULT GETUTCDATE(),
                PRIMARY KEY (run_id, node_id)
            );
            CREATE INDEX IX_meta_node_runs_project ON [{schema}].[meta_node_runs] (project, pipeline_name, node_name);
        END
    """,
    "meta_tables": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_tables' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_tables] (
                project NVARCHAR(255),
                table_name NVARCHAR(500),
                path NVARCHAR(1000),
                format NVARCHAR(50),
                pattern_type NVARCHAR(50),
                schema_hash NVARCHAR(100),
                updated_at DATETIME2,
                environment NVARCHAR(50),
                _synced_at DATETIME2 DEFAULT GETUTCDATE(),
                PRIMARY KEY (project, table_name)
            );
            CREATE INDEX IX_meta_tables_project ON [{schema}].[meta_tables] (project);
        END
    """,
    "meta_failures": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_failures' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_failures] (
                failure_id NVARCHAR(100) PRIMARY KEY,
                run_id NVARCHAR(100),
                project NVARCHAR(255),
                pipeline_name NVARCHAR(255),
                node_name NVARCHAR(255),
                error_type NVARCHAR(100),
                error_message NVARCHAR(MAX),
                error_code NVARCHAR(50),
                stack_trace NVARCHAR(MAX),
                environment NVARCHAR(50),
                timestamp DATETIME2,
                date DATE,
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
            CREATE INDEX IX_meta_failures_project ON [{schema}].[meta_failures] (project, pipeline_name, date);
        END
    """,
    "meta_sla_status": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_sla_status' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_sla_status] (
                pipeline_name NVARCHAR(255) NOT NULL,
                owner NVARCHAR(255),
                freshness_sla NVARCHAR(50),
                freshness_anchor NVARCHAR(50),
                freshness_sla_minutes BIGINT,
                last_success_at DATETIME2,
                minutes_since_success BIGINT,
                sla_met BIGINT,
                hours_overdue FLOAT,
                updated_at DATETIME2,
                environment NVARCHAR(50),
                _synced_at DATETIME2 DEFAULT GETUTCDATE(),
                CONSTRAINT PK_meta_sla_status PRIMARY KEY (pipeline_name, environment)
            );
        END
    """,
}

# Dimension tables for executive dashboards (manually populated)
SQL_SERVER_DIM_TABLES = {
    "dim_pipeline_context": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_pipeline_context' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[dim_pipeline_context] (
                project NVARCHAR(255) NOT NULL,
                pipeline_name NVARCHAR(255) NOT NULL,
                environment NVARCHAR(50) NOT NULL,
                business_criticality NVARCHAR(10) NULL,
                business_owner NVARCHAR(255) NULL,
                business_process NVARCHAR(255) NULL,
                notes NVARCHAR(500) NULL,
                created_at DATETIME2 DEFAULT GETUTCDATE(),
                updated_at DATETIME2 DEFAULT GETUTCDATE(),
                CONSTRAINT PK_dim_pipeline_context PRIMARY KEY (project, pipeline_name, environment)
            );
        END
    """,
}

# Primary key columns for each table (used for MERGE upsert)
TABLE_PRIMARY_KEYS = {
    "meta_runs": ["run_id", "pipeline_name", "node_name"],  # No explicit PK, use composite
    "meta_pipeline_runs": ["run_id"],
    "meta_node_runs": ["run_id", "node_id"],
    "meta_tables": ["project", "table_name"],
    "meta_failures": ["failure_id"],
    "meta_sla_status": ["pipeline_name", "environment"],
}

# Power BI-ready views for dashboards
SQL_SERVER_VIEWS = {
    "vw_pipeline_summary": """
        CREATE OR ALTER VIEW [{schema}].[vw_pipeline_summary] AS
        SELECT
            project,
            pipeline_name,
            environment,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failure_count,
            CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate,
            AVG(duration_ms) as avg_duration_ms,
            SUM(rows_processed) as total_rows_processed,
            SUM(COALESCE(actual_cost_usd, estimated_cost_usd, 0)) as total_cost_usd,
            MAX(created_at) as last_run_at,
            MAX(CAST(created_at AS DATE)) as last_run_date
        FROM [{schema}].[meta_pipeline_runs]
        GROUP BY project, pipeline_name, environment
    """,
    "vw_daily_health": """
        CREATE OR ALTER VIEW [{schema}].[vw_daily_health] AS
        SELECT
            CAST(created_at AS DATE) as run_date,
            project,
            environment,
            COUNT(DISTINCT pipeline_name) as pipelines_run,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failure_count,
            CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate,
            SUM(rows_processed) as total_rows,
            AVG(duration_ms) as avg_duration_ms,
            SUM(COALESCE(actual_cost_usd, estimated_cost_usd, 0)) as total_cost_usd
        FROM [{schema}].[meta_pipeline_runs]
        GROUP BY CAST(created_at AS DATE), project, environment
    """,
    "vw_recent_failures": """
        CREATE OR ALTER VIEW [{schema}].[vw_recent_failures] AS
        SELECT TOP 100
            f.project,
            f.pipeline_name,
            f.node_name,
            f.error_type,
            f.error_message,
            f.timestamp as failure_time,
            f.date as failure_date,
            f.environment
        FROM [{schema}].[meta_failures] f
        ORDER BY f.timestamp DESC
    """,
    "vw_node_performance": """
        CREATE OR ALTER VIEW [{schema}].[vw_node_performance] AS
        SELECT
            project,
            pipeline_name,
            node_name,
            environment,
            COUNT(*) as execution_count,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
            CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate,
            AVG(duration_ms) as avg_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            SUM(rows_processed) as total_rows,
            SUM(COALESCE(estimated_cost_usd, 0)) as total_cost_usd
        FROM [{schema}].[meta_node_runs]
        GROUP BY project, pipeline_name, node_name, environment
    """,
    "vw_project_scorecard": """
        CREATE OR ALTER VIEW [{schema}].[vw_project_scorecard] AS
        SELECT
            project,
            environment,
            COUNT(DISTINCT pipeline_name) as pipeline_count,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failure_count,
            CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate,
            SUM(rows_processed) as total_rows_processed,
            SUM(COALESCE(actual_cost_usd, estimated_cost_usd, 0)) as total_cost_usd,
            MAX(created_at) as last_activity
        FROM [{schema}].[meta_pipeline_runs]
        GROUP BY project, environment
    """,
    "vw_pipeline_health_status": """
        CREATE OR ALTER VIEW [{schema}].[vw_pipeline_health_status] AS
        WITH runs_7d AS (
            SELECT
                project,
                pipeline_name,
                environment,
                status,
                created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY project, pipeline_name, environment
                    ORDER BY created_at DESC
                ) as rn
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -7, GETUTCDATE())
        ),
        agg AS (
            SELECT
                project,
                pipeline_name,
                environment,
                COUNT(*) as runs_7d,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_7d,
                SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failure_7d,
                CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate_7d,
                MAX(created_at) as last_run_at
            FROM runs_7d
            GROUP BY project, pipeline_name, environment
        ),
        latest AS (
            SELECT project, pipeline_name, environment, status as last_run_status
            FROM runs_7d
            WHERE rn = 1
        )
        SELECT
            a.project,
            a.pipeline_name,
            a.environment,
            a.runs_7d,
            a.success_7d,
            a.failure_7d,
            a.success_rate_7d,
            a.last_run_at,
            l.last_run_status,
            DATEDIFF(HOUR, a.last_run_at, GETUTCDATE()) as hours_since_last_run,
            CASE
                WHEN l.last_run_status = 'FAILURE' THEN 'RED'
                WHEN a.success_rate_7d < 0.90 THEN 'RED'
                WHEN a.runs_7d = 0 THEN 'RED'
                WHEN a.success_rate_7d < 1.0 THEN 'AMBER'
                WHEN DATEDIFF(HOUR, a.last_run_at, GETUTCDATE()) > 48 THEN 'AMBER'
                ELSE 'GREEN'
            END as health_status,
            CASE
                WHEN l.last_run_status = 'FAILURE' THEN 'Last run failed'
                WHEN a.success_rate_7d < 0.90 THEN 'Success rate below 90%'
                WHEN a.runs_7d = 0 THEN 'No runs in 7 days'
                WHEN a.success_rate_7d < 1.0 THEN CAST(a.failure_7d AS VARCHAR) + ' failure(s) in 7d'
                WHEN DATEDIFF(HOUR, a.last_run_at, GETUTCDATE()) > 48 THEN 'No run in 48+ hours'
                ELSE '100% success'
            END as health_reason
        FROM agg a
        LEFT JOIN latest l
          ON a.project = l.project
         AND a.pipeline_name = l.pipeline_name
         AND a.environment = l.environment
    """,
    "vw_exec_overview": """
        CREATE OR ALTER VIEW [{schema}].[vw_exec_overview] AS
        WITH runs_7d AS (
            SELECT project, environment, status, COALESCE(actual_cost_usd, estimated_cost_usd, 0) as cost
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -7, GETUTCDATE())
        ),
        runs_prev_7d AS (
            SELECT project, environment, status
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -14, GETUTCDATE())
              AND created_at < DATEADD(day, -7, GETUTCDATE())
        ),
        runs_30d AS (
            SELECT project, environment, status, COALESCE(actual_cost_usd, estimated_cost_usd, 0) as cost
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -30, GETUTCDATE())
        ),
        runs_90d AS (
            SELECT project, environment, status
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -90, GETUTCDATE())
        ),
        agg_7d AS (
            SELECT
                project, environment,
                COUNT(*) as runs_7d,
                CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate_7d,
                SUM(cost) as cost_7d
            FROM runs_7d
            GROUP BY project, environment
        ),
        agg_prev_7d AS (
            SELECT
                project, environment,
                CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate_prev_7d
            FROM runs_prev_7d
            GROUP BY project, environment
        ),
        agg_30d AS (
            SELECT
                project, environment,
                COUNT(*) as runs_30d,
                CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate_30d,
                SUM(cost) as cost_30d
            FROM runs_30d
            GROUP BY project, environment
        ),
        agg_90d AS (
            SELECT
                project, environment,
                COUNT(*) as runs_90d,
                CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as success_rate_90d
            FROM runs_90d
            GROUP BY project, environment
        )
        SELECT
            a7.project,
            a7.environment,
            a7.runs_7d,
            a7.success_rate_7d,
            a7.cost_7d,
            ap.success_rate_prev_7d,
            (a7.success_rate_7d - ISNULL(ap.success_rate_prev_7d, a7.success_rate_7d)) as trend_7d,
            a30.runs_30d,
            a30.success_rate_30d,
            a30.cost_30d,
            a90.runs_90d,
            a90.success_rate_90d,
            CAST(a7.success_rate_7d * 100 AS INT) as reliability_score
        FROM agg_7d a7
        LEFT JOIN agg_prev_7d ap ON a7.project = ap.project AND a7.environment = ap.environment
        LEFT JOIN agg_30d a30 ON a7.project = a30.project AND a7.environment = a30.environment
        LEFT JOIN agg_90d a90 ON a7.project = a90.project AND a7.environment = a90.environment
    """,
    "vw_table_freshness": """
        CREATE OR ALTER VIEW [{schema}].[vw_table_freshness] AS
        SELECT
            project,
            environment,
            table_name,
            updated_at,
            DATEDIFF(HOUR, updated_at, GETUTCDATE()) as hours_since_update,
            CASE
                WHEN updated_at IS NULL THEN 'Unknown'
                WHEN DATEDIFF(HOUR, updated_at, GETUTCDATE()) <= 6 THEN 'Fresh'
                WHEN DATEDIFF(HOUR, updated_at, GETUTCDATE()) <= 24 THEN 'Warning'
                ELSE 'Stale'
            END as freshness_status,
            CASE
                WHEN updated_at IS NULL THEN 'RED'
                WHEN DATEDIFF(HOUR, updated_at, GETUTCDATE()) <= 6 THEN 'GREEN'
                WHEN DATEDIFF(HOUR, updated_at, GETUTCDATE()) <= 24 THEN 'AMBER'
                ELSE 'RED'
            END as freshness_rag
        FROM [{schema}].[meta_tables]
    """,
    "vw_pipeline_sla_status": """
        CREATE OR ALTER VIEW [{schema}].[vw_pipeline_sla_status] AS
        WITH latest_run AS (
            SELECT
                project, pipeline_name, environment, owner, run_id, status,
                run_start_at, run_end_at, duration_ms, rows_processed,
                ROW_NUMBER() OVER (
                    PARTITION BY project, pipeline_name, environment
                    ORDER BY run_end_at DESC
                ) as rn
            FROM [{schema}].[meta_pipeline_runs]
        ),
        latest AS (
            SELECT * FROM latest_run WHERE rn = 1
        )
        SELECT
            l.project,
            l.pipeline_name,
            l.environment,
            l.owner,
            c.business_owner,
            c.business_process,
            c.business_criticality,
            l.status as last_run_status,
            l.run_end_at as last_run_end_at,
            l.duration_ms,
            l.rows_processed,
            s.freshness_sla,
            s.freshness_sla_minutes,
            s.last_success_at,
            s.minutes_since_success,
            s.sla_met,
            s.hours_overdue,
            CASE
                WHEN s.sla_met = 1 THEN 'GREEN'
                WHEN s.sla_met = 0 AND s.hours_overdue <= 1 THEN 'AMBER'
                WHEN s.sla_met = 0 THEN 'RED'
                WHEN l.status = 'FAILURE' THEN 'RED'
                WHEN c.business_criticality = 'High' AND l.status <> 'SUCCESS' THEN 'RED'
                WHEN l.status = 'SUCCESS' THEN 'GREEN'
                ELSE 'AMBER'
            END as sla_rag
        FROM latest l
        LEFT JOIN [{schema}].[meta_sla_status] s
          ON l.pipeline_name = s.pipeline_name
        LEFT JOIN [{schema}].[dim_pipeline_context] c
          ON l.project = c.project
         AND l.pipeline_name = c.pipeline_name
         AND l.environment = c.environment
    """,
    "vw_exec_current_issues": """
        CREATE OR ALTER VIEW [{schema}].[vw_exec_current_issues] AS
        WITH latest_run AS (
            SELECT
                project, pipeline_name, environment, status, run_end_at,
                ROW_NUMBER() OVER (
                    PARTITION BY project, pipeline_name, environment
                    ORDER BY run_end_at DESC
                ) as rn
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -7, GETUTCDATE())
        ),
        failed_pipelines AS (
            SELECT project, pipeline_name, environment, status as last_status, run_end_at
            FROM latest_run
            WHERE rn = 1 AND status = 'FAILURE'
        ),
        recent_failures AS (
            SELECT
                project, pipeline_name, node_name, error_type, error_message, timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY project, pipeline_name
                    ORDER BY timestamp DESC
                ) as rn
            FROM [{schema}].[meta_failures]
            WHERE timestamp >= DATEADD(day, -2, GETUTCDATE())
        ),
        context_info AS (
            SELECT project, pipeline_name, environment, business_criticality, business_owner, business_process
            FROM [{schema}].[dim_pipeline_context]
        )
        SELECT
            fp.project,
            fp.pipeline_name,
            fp.environment,
            c.business_criticality,
            c.business_owner,
            c.business_process,
            fp.last_status,
            fp.run_end_at as last_run_at,
            rf.node_name as failed_node,
            rf.error_type,
            rf.error_message,
            rf.timestamp as failure_time,
            CASE
                WHEN c.business_criticality = 'High' THEN 1
                WHEN c.business_criticality = 'Medium' THEN 2
                ELSE 3
            END as priority_order
        FROM failed_pipelines fp
        LEFT JOIN recent_failures rf
          ON fp.project = rf.project AND fp.pipeline_name = rf.pipeline_name AND rf.rn = 1
        LEFT JOIN context_info c
          ON fp.project = c.project AND fp.pipeline_name = c.pipeline_name AND fp.environment = c.environment
    """,
    "vw_pipeline_risk": """
        CREATE OR ALTER VIEW [{schema}].[vw_pipeline_risk] AS
        WITH runs_30d AS (
            SELECT
                project, pipeline_name, environment, owner, status, duration_ms,
                COALESCE(actual_cost_usd, estimated_cost_usd, 0) as cost
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -30, GETUTCDATE())
        ),
        agg AS (
            SELECT
                project, pipeline_name, environment,
                MAX(owner) as owner,
                COUNT(*) as runs_30d,
                SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failures_30d,
                CAST(SUM(CASE WHEN status = 'FAILURE' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) as failure_rate_30d,
                SUM(duration_ms) / 3600000.0 as runtime_hours_30d,
                SUM(cost) as cost_30d
            FROM runs_30d
            GROUP BY project, pipeline_name, environment
        ),
        ctx AS (
            SELECT project, pipeline_name, environment, business_criticality, business_owner, business_process
            FROM [{schema}].[dim_pipeline_context]
        )
        SELECT
            a.project,
            a.pipeline_name,
            a.environment,
            a.owner,
            c.business_owner,
            c.business_process,
            c.business_criticality,
            a.runs_30d,
            a.failures_30d,
            a.failure_rate_30d,
            a.runtime_hours_30d,
            a.cost_30d,
            (
                CASE c.business_criticality
                    WHEN 'High' THEN 3
                    WHEN 'Medium' THEN 2
                    ELSE 1
                END
            ) * (a.failure_rate_30d * 100 + LOG10(NULLIF(a.runtime_hours_30d, 0) + 1) * 5) as risk_score,
            CASE
                WHEN a.failure_rate_30d >= 0.10 AND c.business_criticality = 'High' THEN 'CRITICAL'
                WHEN a.failure_rate_30d >= 0.10 THEN 'HIGH'
                WHEN a.failure_rate_30d >= 0.05 THEN 'MEDIUM'
                ELSE 'LOW'
            END as risk_level
        FROM agg a
        LEFT JOIN ctx c
          ON a.project = c.project AND a.pipeline_name = c.pipeline_name AND a.environment = c.environment
    """,
    "vw_cost_summary": """
        CREATE OR ALTER VIEW [{schema}].[vw_cost_summary] AS
        WITH runs_7d AS (
            SELECT project, pipeline_name, environment, duration_ms,
                   COALESCE(actual_cost_usd, estimated_cost_usd, 0) as cost,
                   cost_source
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -7, GETUTCDATE())
        ),
        runs_30d AS (
            SELECT project, pipeline_name, environment, duration_ms,
                   COALESCE(actual_cost_usd, estimated_cost_usd, 0) as cost
            FROM [{schema}].[meta_pipeline_runs]
            WHERE created_at >= DATEADD(day, -30, GETUTCDATE())
        ),
        agg_7d AS (
            SELECT
                project, pipeline_name, environment,
                COUNT(*) as runs_7d,
                SUM(duration_ms) / 3600000.0 as runtime_hours_7d,
                SUM(cost) as cost_7d,
                MAX(cost_source) as cost_source
            FROM runs_7d
            GROUP BY project, pipeline_name, environment
        ),
        agg_30d AS (
            SELECT
                project, pipeline_name, environment,
                COUNT(*) as runs_30d,
                SUM(duration_ms) / 3600000.0 as runtime_hours_30d,
                SUM(cost) as cost_30d
            FROM runs_30d
            GROUP BY project, pipeline_name, environment
        )
        SELECT
            a7.project,
            a7.pipeline_name,
            a7.environment,
            a7.runs_7d,
            a7.runtime_hours_7d,
            a7.cost_7d,
            a7.cost_source,
            a30.runs_30d,
            a30.runtime_hours_30d,
            a30.cost_30d,
            CASE
                WHEN a7.cost_7d > 0 AND a30.cost_30d > 0
                THEN (a7.cost_7d * 4) / a30.cost_30d - 1
                ELSE 0
            END as cost_trend
        FROM agg_7d a7
        LEFT JOIN agg_30d a30
          ON a7.project = a30.project
         AND a7.pipeline_name = a30.pipeline_name
         AND a7.environment = a30.environment
    """,
}


class CatalogSyncer:
    """
    Syncs system catalog data from Delta tables to a secondary destination.

    Supports:
    - Delta → SQL Server (for dashboards/queries)
    - Delta → Delta (for cross-region replication)
    """

    def __init__(
        self,
        source_catalog: Any,  # CatalogManager
        sync_config: SyncToConfig,
        target_connection: Any,
        spark: Optional[Any] = None,
        environment: Optional[str] = None,
    ):
        """
        Initialize the catalog syncer.

        Args:
            source_catalog: CatalogManager instance (source of truth)
            sync_config: SyncToConfig with sync settings
            target_connection: Target connection object
            spark: SparkSession (optional, for Spark-based sync)
            environment: Environment tag
        """
        self.source = source_catalog
        self.config = sync_config
        self.target = target_connection
        self.spark = spark
        self.environment = environment
        self._ctx = get_logging_context()

        # Determine target type
        self.target_type = self._get_target_type()

    def _get_target_type(self) -> str:
        """Determine target connection type."""
        # Check various ways connections expose their type
        conn_type = None
        if hasattr(self.target, "connection_type"):
            conn_type = self.target.connection_type
        elif hasattr(self.target, "type"):
            conn_type = self.target.type
        else:
            # Check class name as fallback
            class_name = self.target.__class__.__name__.lower()
            if "sql" in class_name:
                conn_type = "sql_server"
            elif "adls" in class_name or "azure" in class_name or "blob" in class_name:
                conn_type = "azure_adls"
            elif "local" in class_name:
                conn_type = "local"

        if conn_type is None:
            conn_type = "unknown"

        # Normalize to sql_server or delta
        if conn_type in ("sql_server", "azure_sql", "AzureSQL"):
            return "sql_server"
        elif conn_type in ("azure_blob", "azure_adls", "adls", "local", "s3", "gcs"):
            return "delta"
        else:
            return "delta"  # Default to delta

    def get_tables_to_sync(self) -> List[str]:
        """Get list of tables to sync."""
        if self.config.tables:
            # Validate requested tables exist
            valid_tables = [t for t in self.config.tables if t in ALL_SYNC_TABLES]
            if len(valid_tables) != len(self.config.tables):
                invalid = set(self.config.tables) - set(valid_tables)
                self._ctx.warning(f"Unknown tables requested for sync: {invalid}")
            return valid_tables
        return DEFAULT_SYNC_TABLES

    def sync(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Sync catalog tables to target.

        Args:
            tables: Optional override of tables to sync

        Returns:
            Dict with sync results per table
        """
        tables_to_sync = tables or self.get_tables_to_sync()
        results = {}

        self._ctx.info(
            f"Starting catalog sync to {self.config.connection}",
            tables=tables_to_sync,
            mode=self.config.mode,
            target_type=self.target_type,
        )

        for table in tables_to_sync:
            try:
                if self.target_type == "sql_server":
                    result = self._sync_to_sql_server(table)
                else:
                    result = self._sync_to_delta(table)
                results[table] = result
            except Exception as e:
                self._ctx.warning(f"Failed to sync table {table}: {e}")
                results[table] = {"success": False, "error": str(e), "rows": 0}

        # Update last sync timestamp
        self._update_sync_state(results)

        # Create/update Power BI views for SQL Server targets
        if self.target_type == "sql_server":
            self._ensure_sql_views()

        success_count = sum(1 for r in results.values() if r.get("success"))
        total_rows = sum(r.get("rows", 0) for r in results.values())

        self._ctx.info(
            f"Catalog sync completed: {success_count}/{len(tables_to_sync)} tables",
            total_rows=total_rows,
        )

        return results

    def sync_async(self, tables: Optional[List[str]] = None) -> None:
        """Fire and forget sync - runs in background thread."""
        thread = threading.Thread(target=self.sync, args=(tables,), daemon=True)
        thread.start()

    def _sync_to_sql_server(self, table: str) -> Dict[str, Any]:
        """Sync a single table to SQL Server."""
        schema = self.config.schema_name or "odibi_system"

        # Ensure schema exists
        self._ensure_sql_schema(schema)

        # Ensure table exists
        self._ensure_sql_table(table, schema)

        # Read source data
        source_path = self.source.tables.get(table)
        if not source_path:
            return {"success": False, "error": f"Table {table} not found in source", "rows": 0}

        try:
            df = self._read_source_table(source_path)
            if df is None or (hasattr(df, "empty") and df.empty):
                return {"success": True, "rows": 0, "message": "No data to sync"}

            # Apply date filter for incremental mode
            if self.config.mode == "incremental":
                df = self._apply_incremental_filter(df, table)

            # Get row count before conversion
            row_count = len(df) if hasattr(df, "__len__") else df.count()
            if row_count == 0:
                return {"success": True, "rows": 0, "message": "No new data"}

            # Apply column mappings for schema alignment (Delta -> SQL Server)
            df = self._apply_column_mappings(df, table)

            # Inject environment if not present or NULL in source
            if self.environment and "environment" in df.columns:
                df["environment"] = df["environment"].fillna(self.environment)
            elif self.environment:
                df["environment"] = self.environment

            # Convert to records and insert
            if self.config.mode == "full":
                # Truncate and reload
                self.target.execute(f"TRUNCATE TABLE [{schema}].[{table}]")

            records = self._df_to_records(df)
            self._insert_to_sql_server(table, schema, records)

            return {"success": True, "rows": row_count}

        except Exception as e:
            logger.exception(f"Error syncing {table} to SQL Server")
            return {"success": False, "error": str(e), "rows": 0}

    def _sync_to_delta(self, table: str) -> Dict[str, Any]:
        """Sync a single table to another Delta location."""
        source_path = self.source.tables.get(table)

        # Build target path - ensure it's absolute
        sync_path = self.config.path or "_odibi_system"
        if hasattr(self.target, "get_path"):
            base_path = self.target.get_path(sync_path)
        elif hasattr(self.target, "uri"):
            base_path = self.target.uri(sync_path)
        else:
            # Fallback - this shouldn't happen for blob connections
            base_path = sync_path

        target_path = f"{base_path}/{table}"

        # Validate path is absolute (abfss://, s3://, etc.)
        if not target_path.startswith(("abfss://", "s3://", "gs://", "az://", "/")):
            return {
                "success": False,
                "error": f"Target path is not absolute: {target_path}. Check sync_to connection.",
                "rows": 0,
            }

        if not source_path:
            return {"success": False, "error": f"Table {table} not found in source", "rows": 0}

        try:
            if self.spark:
                # Spark-based Delta sync
                df = self.spark.read.format("delta").load(source_path)

                if self.config.mode == "incremental":
                    df = self._apply_incremental_filter_spark(df, table)

                row_count = df.count()
                if row_count == 0:
                    return {"success": True, "rows": 0, "message": "No new data"}

                write_mode = "overwrite" if self.config.mode == "full" else "append"
                df.write.format("delta").mode(write_mode).save(target_path)

                return {"success": True, "rows": row_count}
            else:
                # Engine-based sync (Pandas/Polars)
                df = self._read_source_table(source_path)
                if df is None or df.empty:
                    return {"success": True, "rows": 0, "message": "No data"}

                if self.config.mode == "incremental":
                    df = self._apply_incremental_filter(df, table)

                row_count = len(df)
                if row_count == 0:
                    return {"success": True, "rows": 0, "message": "No new data"}

                # Write to target
                self.source.engine.write(
                    df,
                    connection=self.target,
                    format="delta",
                    path=target_path,
                    mode="overwrite" if self.config.mode == "full" else "append",
                )

                return {"success": True, "rows": row_count}

        except Exception as e:
            logger.exception(f"Error syncing {table} to Delta")
            return {"success": False, "error": str(e), "rows": 0}

    def _read_source_table(self, path: str) -> Any:
        """Read a table from source catalog."""
        if self.spark:
            try:
                return self.spark.read.format("delta").load(path).toPandas()
            except Exception:
                pass

        if self.source.engine:
            return self.source._read_local_table(path)

        return None

    def _apply_incremental_filter(self, df: Any, table: str) -> Any:
        """Filter DataFrame to only include new records for incremental sync."""
        # Get last sync timestamp
        last_sync = self._get_last_sync_timestamp(table)

        if last_sync and "timestamp" in df.columns:
            df = df[df["timestamp"] > last_sync]
        elif self.config.sync_last_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.sync_last_days)
            if "timestamp" in df.columns:
                df = df[df["timestamp"] > cutoff]
            elif "date" in df.columns:
                df = df[df["date"] > cutoff.date()]

        return df

    def _apply_incremental_filter_spark(self, df: Any, table: str) -> Any:
        """Filter Spark DataFrame for incremental sync."""
        from pyspark.sql.functions import col

        last_sync = self._get_last_sync_timestamp(table)

        if last_sync and "timestamp" in df.columns:
            df = df.filter(col("timestamp") > last_sync)
        elif self.config.sync_last_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.sync_last_days)
            if "timestamp" in df.columns:
                df = df.filter(col("timestamp") > cutoff)
            elif "date" in df.columns:
                df = df.filter(col("date") > cutoff.date())

        return df

    def _get_last_sync_timestamp(self, table: str) -> Optional[datetime]:
        """Get last successful sync timestamp for a table."""
        try:
            key = f"sync_to:{self.config.connection}:{table}:last_timestamp"
            value = self.source.get_state(key)
            if value:
                return datetime.fromisoformat(value)
        except Exception:
            pass
        return None

    def _update_sync_state(self, results: Dict[str, Any]) -> None:
        """Update sync state with last sync timestamps."""
        now = datetime.now(timezone.utc).isoformat()
        for table, result in results.items():
            if result.get("success"):
                key = f"sync_to:{self.config.connection}:{table}:last_timestamp"
                try:
                    self.source.set_state(key, now)
                except Exception as e:
                    logger.debug(f"Failed to update sync state for {table}: {e}")

    def _ensure_sql_schema(self, schema: str) -> None:
        """Create SQL Server schema if it doesn't exist."""
        try:
            ddl = f"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
            BEGIN
                EXEC('CREATE SCHEMA [{schema}]')
            END
            """
            self.target.execute(ddl)
        except Exception as e:
            logger.debug(f"Schema creation note: {e}")

    def _ensure_sql_table(self, table: str, schema: str) -> None:
        """Create SQL Server table if it doesn't exist."""
        ddl_template = SQL_SERVER_DDL.get(table)
        if ddl_template:
            try:
                ddl = ddl_template.format(schema=schema)
                self.target.execute(ddl)
            except Exception as e:
                logger.debug(f"Table creation note for {table}: {e}")

    def _ensure_dim_tables(self) -> None:
        """Create dimension tables for executive dashboards (if not exist).

        These tables are manually populated but auto-created on first sync.
        """
        schema = self.config.schema_name or "odibi_system"
        tables_created = 0

        for table_name, ddl_template in SQL_SERVER_DIM_TABLES.items():
            try:
                ddl = ddl_template.format(schema=schema)
                self.target.execute(ddl)
                tables_created += 1
                logger.debug(f"Ensured dim table exists: {schema}.{table_name}")
            except Exception as e:
                logger.debug(f"Dim table creation note for {table_name}: {e}")

        if tables_created > 0:
            self._ctx.info(
                f"Ensured {tables_created} dimension table(s) exist",
                schema=schema,
                tables=list(SQL_SERVER_DIM_TABLES.keys()),
            )

    def _ensure_sql_views(self) -> None:
        """Create or update Power BI-ready views in SQL Server.

        Also ensures dimension tables exist before creating views that reference them.
        """
        schema = self.config.schema_name or "odibi_system"

        self._ensure_dim_tables()

        views_created = 0

        for view_name, view_ddl in SQL_SERVER_VIEWS.items():
            try:
                ddl = view_ddl.format(schema=schema)
                self.target.execute(ddl)
                views_created += 1
                logger.debug(f"Created/updated view: {schema}.{view_name}")
            except Exception as e:
                logger.debug(f"View creation note for {view_name}: {e}")

        if views_created > 0:
            self._ctx.info(
                f"Created/updated {views_created} Power BI views",
                schema=schema,
                views=list(SQL_SERVER_VIEWS.keys()),
            )

    def _apply_column_mappings(self, df: Any, table: str) -> Any:
        """Apply column mappings for schema alignment between Delta and SQL Server.

        Handles backward compatibility when Delta tables have old column names.
        """
        # Column mappings: old_name -> new_name
        mappings = {
            "meta_tables": {"project_name": "project"},
        }

        table_mappings = mappings.get(table, {})
        if not table_mappings:
            return df

        for old_col, new_col in table_mappings.items():
            if old_col in df.columns and new_col not in df.columns:
                df = df.rename(columns={old_col: new_col})

        return df

    def _df_to_records(self, df: Any) -> List[Dict[str, Any]]:
        """Convert DataFrame to list of records."""
        if hasattr(df, "to_dict"):
            return df.to_dict("records")
        elif hasattr(df, "to_dicts"):
            return df.to_dicts()
        return []

    def _insert_to_sql_server(self, table: str, schema: str, records: List[Dict[str, Any]]) -> None:
        """Upsert records to SQL Server table using MERGE."""
        if not records:
            return

        # Get column names from first record
        columns = list(records[0].keys())

        # Get primary key columns for this table
        pk_columns = TABLE_PRIMARY_KEYS.get(table, [])

        if pk_columns:
            # Build MERGE statement for upsert
            on_clause = " AND ".join([f"target.[{col}] = source.[{col}]" for col in pk_columns])
            update_cols = [col for col in columns if col not in pk_columns]
            update_set = ", ".join([f"target.[{col}] = source.[{col}]" for col in update_cols])
            source_select = ", ".join([f":{col} AS [{col}]" for col in columns])
            column_list = ", ".join([f"[{col}]" for col in columns])
            source_values = ", ".join([f"source.[{col}]" for col in columns])

            sql = f"""
            MERGE INTO [{schema}].[{table}] AS target
            USING (SELECT {source_select}) AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({source_values});
            """
        else:
            # Fallback to INSERT for tables without defined PKs
            placeholders = ", ".join([f":{col}" for col in columns])
            column_list = ", ".join([f"[{col}]" for col in columns])
            sql = f"INSERT INTO [{schema}].[{table}] ({column_list}) VALUES ({placeholders})"

        # Batch upsert
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            for record in batch:
                # Convert values to SQL-safe format
                safe_record = {}
                for k, v in record.items():
                    if isinstance(v, (dict, list)):
                        safe_record[k] = json.dumps(v, default=str)
                    elif isinstance(v, datetime):
                        safe_record[k] = v.isoformat()
                    elif isinstance(v, float) and (v != v):  # NaN check (NaN != NaN)
                        safe_record[k] = None
                    else:
                        safe_record[k] = v
                try:
                    self.target.execute(sql, safe_record)
                except Exception as e:
                    logger.debug(f"Upsert error for record: {e}")

    def purge_sql_tables(self, days: int = 90) -> Dict[str, Any]:
        """
        Purge old records from SQL Server sync tables.

        Args:
            days: Delete records older than this many days (default: 90)

        Returns:
            Dict with purge results per table
        """
        if self.target_type != "sql_server":
            return {"error": "Purge only supported for SQL Server targets"}

        schema = self.config.schema_name or "odibi_system"
        results = {}

        # Tables with date columns for purging
        purgeable_tables = {
            "meta_runs": "date",
            "meta_pipeline_runs": "date",
            "meta_node_runs": "date",
            "meta_failures": "date",
        }

        tables_to_purge = self.config.tables or list(purgeable_tables.keys())

        for table in tables_to_purge:
            date_col = purgeable_tables.get(table)
            if not date_col:
                results[table] = {"success": False, "error": "No date column for purge"}
                continue

            try:
                sql = f"""
                DELETE FROM [{schema}].[{table}]
                WHERE [{date_col}] < DATEADD(day, -{days}, GETDATE())
                """
                self.target.execute(sql)
                results[table] = {"success": True, "days_retained": days}
                self._ctx.info(f"Purged {table} (records older than {days} days)")
            except Exception as e:
                results[table] = {"success": False, "error": str(e)}
                self._ctx.warning(f"Failed to purge {table}: {e}")

        return results
