"""
Azure Data Factory Profiler
============================
Generates a full profile of any Azure Data Factory instance including:
- Factory metadata & global parameters
- Pipelines (activities, parameters, variables)
- Datasets (type, schema, linked service)
- Linked Services (connector type, connection info)
- Triggers (type, schedule, associated pipelines)
- Data Flows (sources, sinks, transformations)
- Integration Runtimes (type, status)
- Pipeline run history (last run, success rate, duration)
- Source-to-sink lineage per pipeline

Usage:
    from odibi.tools.adf_profiler import AdfProfiler

    profiler = AdfProfiler(
        subscription_id="...",
        resource_group="...",
        factory_name="..."
    )

    # Full profile as dict
    profile = profiler.full_profile(days_back=30)

    # Markdown report
    profiler.generate_report("adf_report.md")

    # Quick summary to console
    profiler.print_summary()
"""

from __future__ import annotations

import json
import statistics
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import requests
from azure.identity import DefaultAzureCredential


class AdfProfiler:
    """Profile any Azure Data Factory via the ARM REST API."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        factory_name: str,
        api_version: str = "2018-06-01",
        credential: Optional[Any] = None,
        max_workers: int = 8,
        on_progress: Optional[Callable[[str, int, int], None]] = None,
    ):
        self.sub_id = subscription_id
        self.rg = resource_group
        self.factory = factory_name
        self.api_version = api_version
        self.cred = credential or DefaultAzureCredential()
        self.max_workers = max_workers
        self._on_progress = on_progress or self._default_progress
        self._headers: dict[str, str] = {}

        self._cache: dict[str, Any] = {}

    @staticmethod
    def _default_progress(step: str, current: int, total: int):
        print(f"  [{current}/{total}] {step}")

    def _progress(self, step: str, current: int = 0, total: int = 0):
        self._on_progress(step, current, total)

    def clear_cache(self):
        """Clear all cached API responses."""
        self._cache.clear()

    def _cached(self, key: str, loader: Callable[[], Any]) -> Any:
        if key not in self._cache:
            self._cache[key] = loader()
        return self._cache[key]

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _refresh_token(self):
        token = self.cred.get_token("https://management.azure.com/.default")
        self._headers = {"Authorization": f"Bearer {token.token}"}

    def _base_url(self):
        return (
            f"https://management.azure.com/subscriptions/{self.sub_id}"
            f"/resourceGroups/{self.rg}/providers/Microsoft.DataFactory"
            f"/factories/{self.factory}"
        )

    def _url(self, suffix: str) -> str:
        return f"{self._base_url()}/{suffix}?api-version={self.api_version}"

    def _get(self, suffix: str) -> dict:
        self._refresh_token()
        resp = requests.get(self._url(suffix), headers=self._headers)
        resp.raise_for_status()
        return resp.json()

    def _get_paged(self, suffix: str) -> list[dict]:
        self._refresh_token()
        url = self._url(suffix)
        results: list[dict] = []
        while url:
            resp = requests.get(url, headers=self._headers)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("nextLink")
        return results

    def _post(self, suffix: str, body: dict) -> dict:
        self._refresh_token()
        resp = requests.post(self._url(suffix), headers=self._headers, json=body)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    #  Factory metadata                                                    #
    # ------------------------------------------------------------------ #

    def get_factory_info(self) -> dict:
        self._refresh_token()
        url = f"{self._base_url()}?api-version={self.api_version}"
        resp = requests.get(url, headers=self._headers)
        resp.raise_for_status()
        raw = resp.json()
        props = raw.get("properties", {})
        return {
            "name": raw.get("name"),
            "location": raw.get("location"),
            "tags": raw.get("tags", {}),
            "provisioning_state": props.get("provisioningState"),
            "create_time": props.get("createTime"),
            "version": props.get("version"),
            "repo_configuration": props.get("repoConfiguration"),
            "global_parameters": {
                k: {"type": v.get("type"), "value": v.get("value")}
                for k, v in props.get("globalParameters", {}).items()
            },
            "public_network_access": props.get("publicNetworkAccess"),
            "encryption": props.get("encryption"),
        }

    # ------------------------------------------------------------------ #
    #  Pipelines                                                           #
    # ------------------------------------------------------------------ #

    def list_pipelines(self) -> list[dict]:
        return self._cached("pipelines", self._fetch_pipelines)

    def _fetch_pipelines(self) -> list[dict]:
        raw = self._get_paged("pipelines")
        results = []
        for p in raw:
            props = p.get("properties", {})
            activities = props.get("activities", [])
            results.append(
                {
                    "name": p["name"],
                    "folder": props.get("folder", {}).get("name"),
                    "description": props.get("description"),
                    "concurrency": props.get("concurrency"),
                    "parameters": {
                        k: {
                            "type": v.get("type", "String"),
                            "default": v.get("defaultValue"),
                        }
                        for k, v in props.get("parameters", {}).items()
                    },
                    "variables": {
                        k: {
                            "type": v.get("type", "String"),
                            "default": v.get("defaultValue"),
                        }
                        for k, v in props.get("variables", {}).items()
                    },
                    "activity_count": len(activities),
                    "activities": [self._parse_activity(a) for a in activities],
                    "annotations": props.get("annotations", []),
                }
            )
        return results

    def _parse_activity(self, activity: dict) -> dict:
        a_type = activity.get("type", "")
        result: dict[str, Any] = {
            "name": activity.get("name"),
            "type": a_type,
            "depends_on": [
                {"activity": d.get("activity"), "conditions": d.get("dependencyConditions")}
                for d in activity.get("dependsOn", [])
            ],
        }
        type_props = activity.get("typeProperties", {})
        inputs = activity.get("inputs", [])
        outputs = activity.get("outputs", [])

        if inputs:
            result["inputs"] = [
                {"dataset": i.get("referenceName"), "parameters": i.get("parameters", {})}
                for i in inputs
            ]
        if outputs:
            result["outputs"] = [
                {"dataset": o.get("referenceName"), "parameters": o.get("parameters", {})}
                for o in outputs
            ]

        if a_type == "Copy":
            source = type_props.get("source", {})
            sink = type_props.get("sink", {})
            result["source_type"] = source.get("type")
            result["sink_type"] = sink.get("type")
            result["enable_staging"] = type_props.get("enableStaging", False)

        if a_type == "ExecutePipeline":
            ref = type_props.get("pipeline", {})
            result["called_pipeline"] = ref.get("referenceName")

        if a_type in ("IfCondition", "Switch", "ForEach", "Until"):
            result["sub_activity_count"] = self._count_nested_activities(type_props)

        if a_type == "WebActivity":
            result["method"] = type_props.get("method")
            result["url"] = type_props.get("url")

        if a_type in ("Lookup", "GetMetadata"):
            result["source_type"] = type_props.get("source", {}).get("type")

        if a_type == "ExecuteDataFlow":
            df_ref = type_props.get("dataFlow", {})
            result["data_flow"] = df_ref.get("referenceName")

        return result

    def _count_nested_activities(self, type_props: dict) -> int:
        count = 0
        for key in (
            "ifTrueActivities",
            "ifFalseActivities",
            "activities",
            "cases",
            "defaultActivities",
        ):
            val = type_props.get(key, [])
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and "activities" in item:
                        count += len(item["activities"])
                    elif isinstance(item, dict) and "type" in item:
                        count += 1
        return count

    # ------------------------------------------------------------------ #
    #  Datasets                                                            #
    # ------------------------------------------------------------------ #

    def list_datasets(self) -> list[dict]:
        return self._cached("datasets", self._fetch_datasets)

    def _fetch_datasets(self) -> list[dict]:
        raw = self._get_paged("datasets")
        results = []
        for d in raw:
            props = d.get("properties", {})
            type_props = props.get("typeProperties", {})
            ls_ref = props.get("linkedServiceName", {})
            results.append(
                {
                    "name": d["name"],
                    "type": props.get("type"),
                    "folder": props.get("folder", {}).get("name"),
                    "linked_service": ls_ref.get("referenceName"),
                    "description": props.get("description"),
                    "parameters": {
                        k: {"type": v.get("type", "String"), "default": v.get("defaultValue")}
                        for k, v in props.get("parameters", {}).items()
                    },
                    "schema": props.get("schema", []),
                    "structure": props.get("structure", []),
                    "annotations": props.get("annotations", []),
                    "location": self._extract_location(type_props),
                }
            )
        return results

    def _extract_location(self, type_props: dict) -> dict:
        loc = type_props.get("location", {})
        if loc:
            return {
                "type": loc.get("type"),
                "container": loc.get("container") or loc.get("fileSystem"),
                "folder_path": loc.get("folderPath"),
                "file_name": loc.get("fileName"),
            }
        result = {}
        for key in (
            "tableName",
            "table",
            "schema",
            "fileName",
            "folderPath",
            "relativeUrl",
            "container",
            "fileSystem",
        ):
            if key in type_props:
                result[key] = type_props[key]
        return result

    # ------------------------------------------------------------------ #
    #  Linked Services                                                     #
    # ------------------------------------------------------------------ #

    def list_linked_services(self) -> list[dict]:
        return self._cached("linked_services", self._fetch_linked_services)

    def _fetch_linked_services(self) -> list[dict]:
        raw = self._get_paged("linkedservices")
        results = []
        for ls in raw:
            props = ls.get("properties", {})
            type_props = props.get("typeProperties", {})
            results.append(
                {
                    "name": ls["name"],
                    "type": props.get("type"),
                    "description": props.get("description"),
                    "annotations": props.get("annotations", []),
                    "connect_via": props.get("connectVia", {}).get("referenceName"),
                    "connection_details": self._safe_connection_info(type_props),
                }
            )
        return results

    def _safe_connection_info(self, type_props: dict) -> dict:
        safe = {}
        safe_keys = [
            "url",
            "baseUrl",
            "connectionString",
            "accountName",
            "server",
            "database",
            "servicePrincipalId",
            "tenant",
            "functionAppUrl",
            "encryptedCredential",
            "authenticationType",
            "userName",
            "host",
            "port",
            "databaseName",
        ]
        for key in safe_keys:
            val = type_props.get(key)
            if val is not None:
                if isinstance(val, dict) and "value" in val:
                    safe[key] = val["value"]
                elif isinstance(val, str) and (
                    "secret" in key.lower() or "password" in key.lower()
                ):
                    safe[key] = "***REDACTED***"
                else:
                    safe[key] = val
        return safe

    # ------------------------------------------------------------------ #
    #  Triggers                                                            #
    # ------------------------------------------------------------------ #

    def list_triggers(self) -> list[dict]:
        return self._cached("triggers", self._fetch_triggers)

    def _fetch_triggers(self) -> list[dict]:
        raw = self._get_paged("triggers")
        results = []
        for t in raw:
            props = t.get("properties", {})
            type_props = props.get("typeProperties", {})
            result: dict[str, Any] = {
                "name": t["name"],
                "type": props.get("type"),
                "runtime_state": props.get("runtimeState"),
                "description": props.get("description"),
                "annotations": props.get("annotations", []),
            }

            if props.get("type") == "ScheduleTrigger":
                recurrence = type_props.get("recurrence", {})
                result["schedule"] = {
                    "frequency": recurrence.get("frequency"),
                    "interval": recurrence.get("interval"),
                    "start_time": recurrence.get("startTime"),
                    "end_time": recurrence.get("endTime"),
                    "time_zone": recurrence.get("timeZone"),
                    "schedule": recurrence.get("schedule"),
                }

            if props.get("type") == "TumblingWindowTrigger":
                result["tumbling_window"] = {
                    "frequency": type_props.get("frequency"),
                    "interval": type_props.get("interval"),
                    "start_time": type_props.get("startTime"),
                    "end_time": type_props.get("endTime"),
                    "delay": type_props.get("delay"),
                    "max_concurrency": type_props.get("maxConcurrency"),
                    "retry_policy": type_props.get("retryPolicy"),
                }

            if props.get("type") == "BlobEventsTrigger":
                result["blob_events"] = {
                    "blob_path_begins_with": type_props.get("blobPathBeginsWith"),
                    "blob_path_ends_with": type_props.get("blobPathEndsWith"),
                    "events": type_props.get("events"),
                    "scope": type_props.get("scope"),
                }

            pipelines = props.get("pipelines") or type_props.get("pipeline")
            if pipelines:
                if isinstance(pipelines, list):
                    result["associated_pipelines"] = [
                        {
                            "pipeline": p.get("pipelineReference", {}).get("referenceName"),
                            "parameters": p.get("parameters", {}),
                        }
                        for p in pipelines
                    ]
                elif isinstance(pipelines, dict):
                    ref = pipelines.get("pipelineReference", pipelines)
                    result["associated_pipelines"] = [
                        {
                            "pipeline": ref.get("referenceName"),
                            "parameters": pipelines.get("parameters", {}),
                        }
                    ]

            results.append(result)
        return results

    # ------------------------------------------------------------------ #
    #  Data Flows                                                          #
    # ------------------------------------------------------------------ #

    def list_data_flows(self) -> list[dict]:
        return self._cached("data_flows", self._fetch_data_flows)

    def _fetch_data_flows(self) -> list[dict]:
        raw = self._get_paged("dataflows")
        results = []
        for df in raw:
            props = df.get("properties", {})
            type_props = props.get("typeProperties", {})
            results.append(
                {
                    "name": df["name"],
                    "type": props.get("type"),
                    "folder": props.get("folder", {}).get("name"),
                    "description": props.get("description"),
                    "sources": [
                        {
                            "name": s.get("name"),
                            "dataset": s.get("dataset", {}).get("referenceName"),
                            "linked_service": s.get("linkedService", {}).get("referenceName"),
                        }
                        for s in type_props.get("sources", [])
                    ],
                    "sinks": [
                        {
                            "name": s.get("name"),
                            "dataset": s.get("dataset", {}).get("referenceName"),
                            "linked_service": s.get("linkedService", {}).get("referenceName"),
                        }
                        for s in type_props.get("sinks", [])
                    ],
                    "transformations": [
                        {"name": t.get("name"), "description": t.get("description")}
                        for t in type_props.get("transformations", [])
                    ],
                    "script": type_props.get("script"),
                    "script_lines": type_props.get("scriptLines"),
                }
            )
        return results

    # ------------------------------------------------------------------ #
    #  Integration Runtimes                                                #
    # ------------------------------------------------------------------ #

    def list_integration_runtimes(self) -> list[dict]:
        return self._cached("integration_runtimes", self._fetch_integration_runtimes)

    def _fetch_integration_runtimes(self) -> list[dict]:
        raw = self._get_paged("integrationRuntimes")
        results = []
        for ir in raw:
            props = ir.get("properties", {})
            type_props = props.get("typeProperties", {})
            result: dict[str, Any] = {
                "name": ir["name"],
                "type": props.get("type"),
                "description": props.get("description"),
            }
            if props.get("type") == "Managed":
                compute = type_props.get("computeProperties", {})
                result["compute"] = {
                    "location": compute.get("location"),
                    "core_count": compute.get("dataFlowProperties", {}).get("coreCount"),
                    "compute_type": compute.get("dataFlowProperties", {}).get("computeType"),
                    "time_to_live": compute.get("dataFlowProperties", {}).get("timeToLive"),
                }
            if props.get("type") == "SelfHosted":
                result["self_hosted"] = True

            # Get runtime status
            try:
                status = self._get_ir_status(ir["name"])
                result["state"] = status.get("properties", {}).get("state")
            except Exception:
                result["state"] = "Unknown"

            results.append(result)
        return results

    def _get_ir_status(self, ir_name: str) -> dict:
        self._refresh_token()
        url = (
            f"{self._base_url()}/integrationRuntimes/{ir_name}"
            f"/getStatus?api-version={self.api_version}"
        )
        resp = requests.post(url, headers=self._headers)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    #  Pipeline Runs & Activity Runs                                       #
    # ------------------------------------------------------------------ #

    def get_pipeline_runs(
        self,
        days_back: int = 30,
        pipeline_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        now = datetime.now(timezone.utc)
        body: dict[str, Any] = {
            "lastUpdatedAfter": (now - timedelta(days=days_back)).isoformat(),
            "lastUpdatedBefore": now.isoformat(),
        }
        filters = []
        if pipeline_name:
            filters.append(
                {
                    "operand": "PipelineName",
                    "operator": "Equals",
                    "values": [pipeline_name],
                }
            )
        if status:
            filters.append(
                {
                    "operand": "Status",
                    "operator": "Equals",
                    "values": [status],
                }
            )
        if filters:
            body["filters"] = filters

        body["orderBy"] = [{"orderBy": "RunEnd", "order": "DESC"}]

        data = self._post("queryPipelineRuns", body)
        runs = []
        for r in data.get("value", []):
            runs.append(
                {
                    "run_id": r.get("runId"),
                    "pipeline_name": r.get("pipelineName"),
                    "status": r.get("status"),
                    "run_start": r.get("runStart"),
                    "run_end": r.get("runEnd"),
                    "duration_ms": r.get("durationInMs"),
                    "triggered_by": r.get("invokedBy", {}).get("name"),
                    "trigger_type": r.get("invokedBy", {}).get("invokedByType"),
                    "parameters": r.get("parameters", {}),
                    "message": r.get("message"),
                    "run_group_id": r.get("runGroupId"),
                }
            )
        return runs

    def get_activity_runs(self, run_id: str, pipeline_name: str) -> list[dict]:
        now = datetime.now(timezone.utc)
        body = {
            "lastUpdatedAfter": (now - timedelta(days=90)).isoformat(),
            "lastUpdatedBefore": now.isoformat(),
        }
        data = self._post(f"pipelineRuns/{run_id}/queryActivityruns", body)
        runs = []
        for a in data.get("value", []):
            runs.append(
                {
                    "activity_name": a.get("activityName"),
                    "activity_type": a.get("activityType"),
                    "status": a.get("status"),
                    "start": a.get("activityRunStart"),
                    "end": a.get("activityRunEnd"),
                    "duration_ms": a.get("durationInMs"),
                    "error": a.get("error"),
                    "input": a.get("input"),
                    "output": a.get("output"),
                }
            )
        return runs

    def get_last_run(self, pipeline_name: str) -> Optional[dict]:
        runs = self.get_pipeline_runs(days_back=1095, pipeline_name=pipeline_name)
        return runs[0] if runs else None

    def get_pipeline_run_stats(self, pipeline_name: str, days_back: int = 30) -> dict:
        runs = self.get_pipeline_runs(days_back=days_back, pipeline_name=pipeline_name)
        if not runs:
            return {"total_runs": 0}
        status_counts = Counter(r["status"] for r in runs)
        durations = [r["duration_ms"] for r in runs if r.get("duration_ms") is not None]
        return {
            "total_runs": len(runs),
            "status_breakdown": dict(status_counts),
            "success_rate": round(status_counts.get("Succeeded", 0) / len(runs) * 100, 1),
            "avg_duration_sec": round(statistics.mean(durations) / 1000, 1) if durations else None,
            "min_duration_sec": round(min(durations) / 1000, 1) if durations else None,
            "max_duration_sec": round(max(durations) / 1000, 1) if durations else None,
            "last_run": runs[0] if runs else None,
        }

    # ------------------------------------------------------------------ #
    #  Lineage                                                             #
    # ------------------------------------------------------------------ #

    def get_pipeline_lineage(self, pipeline_name: str) -> dict:
        pipelines = {p["name"]: p for p in self.list_pipelines()}
        datasets = {d["name"]: d for d in self.list_datasets()}
        linked_services = {ls["name"]: ls for ls in self.list_linked_services()}

        pipe = pipelines.get(pipeline_name)
        if not pipe:
            return {"error": f"Pipeline '{pipeline_name}' not found"}

        lineage_entries = []
        for act in pipe.get("activities", []):
            entry: dict[str, Any] = {
                "activity": act["name"],
                "activity_type": act["type"],
                "sources": [],
                "sinks": [],
            }
            for inp in act.get("inputs", []):
                ds_name = inp.get("dataset")
                ds = datasets.get(ds_name, {})
                ls_name = ds.get("linked_service")
                ls = linked_services.get(ls_name, {})
                entry["sources"].append(
                    {
                        "dataset": ds_name,
                        "dataset_type": ds.get("type"),
                        "linked_service": ls_name,
                        "connector_type": ls.get("type"),
                        "location": ds.get("location", {}),
                    }
                )
            for out in act.get("outputs", []):
                ds_name = out.get("dataset")
                ds = datasets.get(ds_name, {})
                ls_name = ds.get("linked_service")
                ls = linked_services.get(ls_name, {})
                entry["sinks"].append(
                    {
                        "dataset": ds_name,
                        "dataset_type": ds.get("type"),
                        "linked_service": ls_name,
                        "connector_type": ls.get("type"),
                        "location": ds.get("location", {}),
                    }
                )
            if entry["sources"] or entry["sinks"]:
                lineage_entries.append(entry)

        return {
            "pipeline": pipeline_name,
            "lineage": lineage_entries,
        }

    # ------------------------------------------------------------------ #
    #  Full Profile                                                        #
    # ------------------------------------------------------------------ #

    def full_profile(self, include_runs: bool = True, days_back: int = 30) -> dict:
        start = time.time()
        steps = [
            "Factory info",
            "Pipelines",
            "Datasets",
            "Linked Services",
            "Triggers",
            "Data Flows",
            "Integration Runtimes",
        ]
        total_steps = len(steps) + (1 if include_runs else 0)

        self._progress("Factory info", 1, total_steps)
        factory = self.get_factory_info()

        self._progress("Pipelines", 2, total_steps)
        pipelines = self.list_pipelines()

        self._progress("Datasets", 3, total_steps)
        datasets = self.list_datasets()

        self._progress("Linked Services", 4, total_steps)
        linked_services = self.list_linked_services()

        self._progress("Triggers", 5, total_steps)
        triggers = self.list_triggers()

        self._progress("Data Flows", 6, total_steps)
        data_flows = self.list_data_flows()

        self._progress("Integration Runtimes", 7, total_steps)
        integration_runtimes = self.list_integration_runtimes()

        profile: dict[str, Any] = {
            "profiled_at": datetime.now(timezone.utc).isoformat(),
            "factory": factory,
            "summary": {
                "pipeline_count": len(pipelines),
                "dataset_count": len(datasets),
                "linked_service_count": len(linked_services),
                "trigger_count": len(triggers),
                "data_flow_count": len(data_flows),
                "integration_runtime_count": len(integration_runtimes),
                "connector_types": list({ls["type"] for ls in linked_services if ls.get("type")}),
                "dataset_types": list({ds["type"] for ds in datasets if ds.get("type")}),
                "activity_types": list(
                    {a["type"] for p in pipelines for a in p.get("activities", []) if a.get("type")}
                ),
            },
            "pipelines": pipelines,
            "datasets": datasets,
            "linked_services": linked_services,
            "triggers": triggers,
            "data_flows": data_flows,
            "integration_runtimes": integration_runtimes,
        }

        if include_runs:
            self._progress(
                f"Run statistics ({len(pipelines)} pipelines, {self.max_workers} threads)",
                8,
                total_steps,
            )
            profile["run_statistics"] = self._parallel_run_stats(pipelines, days_back)

        elapsed = round(time.time() - start, 1)
        self._progress(f"Done in {elapsed}s", total_steps, total_steps)
        profile["profile_duration_sec"] = elapsed
        return profile

    def _parallel_run_stats(self, pipelines: list[dict], days_back: int) -> dict[str, dict]:
        pipe_names = [p["name"] for p in pipelines]
        run_stats: dict[str, dict] = {}
        total = len(pipe_names)
        completed = 0

        def fetch_stats(name: str) -> tuple[str, dict]:
            try:
                return name, self.get_pipeline_run_stats(name, days_back=days_back)
            except Exception as e:
                return name, {"error": str(e)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(fetch_stats, n): n for n in pipe_names}
            for future in as_completed(futures):
                name, stats = future.result()
                run_stats[name] = stats
                completed += 1
                if completed % 10 == 0 or completed == total:
                    self._progress(
                        f"  Pipeline runs: {completed}/{total}",
                        completed,
                        total,
                    )

        return run_stats

    # ------------------------------------------------------------------ #
    #  Report Generation                                                   #
    # ------------------------------------------------------------------ #

    def generate_report(
        self,
        output_path: Optional[str] = None,
        include_runs: bool = True,
        days_back: int = 30,
    ) -> str:
        profile = self.full_profile(include_runs=include_runs, days_back=days_back)
        lines = self._build_report_lines(profile)
        report = "\n".join(lines)

        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(report)

        return report

    def _build_report_lines(self, profile: dict) -> list[str]:
        lines: list[str] = []
        factory = profile["factory"]
        summary = profile["summary"]

        # Header
        lines.append(f"# Azure Data Factory Profile: {factory['name']}")
        lines.append(f"*Generated: {profile['profiled_at']}*\n")

        # Factory Info
        lines.append("## Factory Overview")
        lines.append("| Property | Value |")
        lines.append("|----------|-------|")
        lines.append(f"| Location | {factory.get('location')} |")
        lines.append(f"| Provisioning State | {factory.get('provisioning_state')} |")
        lines.append(f"| Created | {factory.get('create_time')} |")
        lines.append(f"| Version | {factory.get('version')} |")
        repo = factory.get("repo_configuration")
        if repo:
            lines.append(f"| Repo Type | {repo.get('type')} |")
            lines.append(f"| Repo Name | {repo.get('repositoryName')} |")
            lines.append(f"| Branch | {repo.get('collaborationBranch')} |")
        lines.append("")

        if factory.get("global_parameters"):
            lines.append("### Global Parameters")
            lines.append("| Name | Type | Value |")
            lines.append("|------|------|-------|")
            for k, v in factory["global_parameters"].items():
                lines.append(f"| {k} | {v.get('type')} | {v.get('value')} |")
            lines.append("")

        # Summary
        lines.append("## Resource Summary")
        lines.append("| Resource | Count |")
        lines.append("|----------|-------|")
        lines.append(f"| Pipelines | {summary['pipeline_count']} |")
        lines.append(f"| Datasets | {summary['dataset_count']} |")
        lines.append(f"| Linked Services | {summary['linked_service_count']} |")
        lines.append(f"| Triggers | {summary['trigger_count']} |")
        lines.append(f"| Data Flows | {summary['data_flow_count']} |")
        lines.append(f"| Integration Runtimes | {summary['integration_runtime_count']} |")
        lines.append("")

        lines.append(
            f"**Connector Types:** {', '.join(sorted(summary.get('connector_types', [])))}"
        )
        lines.append(f"**Dataset Types:** {', '.join(sorted(summary.get('dataset_types', [])))}")
        lines.append(f"**Activity Types:** {', '.join(sorted(summary.get('activity_types', [])))}")
        lines.append("")

        # Linked Services
        lines.append("## Linked Services")
        lines.append("| Name | Type | Integration Runtime | Auth Type |")
        lines.append("|------|------|---------------------|-----------|")
        for ls in profile["linked_services"]:
            auth = ls.get("connection_details", {}).get("authenticationType", "")
            lines.append(
                f"| {ls['name']} | {ls.get('type')} "
                f"| {ls.get('connect_via', 'AutoResolve')} | {auth} |"
            )
        lines.append("")

        # Datasets
        lines.append("## Datasets")
        lines.append("| Name | Type | Linked Service | Folder |")
        lines.append("|------|------|----------------|--------|")
        for ds in profile["datasets"]:
            lines.append(
                f"| {ds['name']} | {ds.get('type')} "
                f"| {ds.get('linked_service')} | {ds.get('folder', '')} |"
            )
        lines.append("")

        # Pipelines
        lines.append("## Pipelines")
        for p in profile["pipelines"]:
            lines.append(f"### {p['name']}")
            if p.get("folder"):
                lines.append(f"*Folder: {p['folder']}*")
            if p.get("description"):
                lines.append(f"> {p['description']}")
            lines.append("")

            if p.get("parameters"):
                lines.append("**Parameters:**")
                lines.append("| Name | Type | Default |")
                lines.append("|------|------|---------|")
                for pk, pv in p["parameters"].items():
                    lines.append(f"| {pk} | {pv.get('type')} | {pv.get('default', '')} |")
                lines.append("")

            lines.append("**Activities:**")
            lines.append("| Name | Type | Depends On |")
            lines.append("|------|------|------------|")
            for a in p.get("activities", []):
                deps = ", ".join(d.get("activity", "") for d in a.get("depends_on", []))
                lines.append(f"| {a['name']} | {a['type']} | {deps} |")
            lines.append("")

        # Triggers
        lines.append("## Triggers")
        lines.append("| Name | Type | State | Associated Pipelines |")
        lines.append("|------|------|-------|---------------------|")
        for t in profile["triggers"]:
            pipes = ", ".join(p.get("pipeline", "") for p in t.get("associated_pipelines", []))
            lines.append(f"| {t['name']} | {t.get('type')} | {t.get('runtime_state')} | {pipes} |")
        lines.append("")

        # Data Flows
        if profile["data_flows"]:
            lines.append("## Data Flows")
            for df in profile["data_flows"]:
                lines.append(f"### {df['name']}")
                if df.get("sources"):
                    lines.append(
                        "**Sources:** "
                        + ", ".join(
                            f"{s['name']} ({s.get('dataset', s.get('linked_service', 'inline'))})"
                            for s in df["sources"]
                        )
                    )
                if df.get("sinks"):
                    lines.append(
                        "**Sinks:** "
                        + ", ".join(
                            f"{s['name']} ({s.get('dataset', s.get('linked_service', 'inline'))})"
                            for s in df["sinks"]
                        )
                    )
                if df.get("transformations"):
                    lines.append(
                        "**Transformations:** "
                        + ", ".join(t["name"] for t in df["transformations"])
                    )
                lines.append("")

        # Integration Runtimes
        lines.append("## Integration Runtimes")
        lines.append("| Name | Type | State |")
        lines.append("|------|------|-------|")
        for ir in profile["integration_runtimes"]:
            lines.append(f"| {ir['name']} | {ir.get('type')} | {ir.get('state')} |")
        lines.append("")

        # Run Statistics
        if "run_statistics" in profile:
            lines.append("## Pipeline Run Statistics (Recent)")
            lines.append(
                "| Pipeline | Runs | Success Rate | Avg Duration | Last Status | Last Run |"
            )
            lines.append("|----------|------|-------------|-------------|-------------|----------|")
            for pipe_name, stats in profile["run_statistics"].items():
                if "error" in stats:
                    lines.append(f"| {pipe_name} | Error | - | - | - | {stats['error']} |")
                    continue
                if stats.get("total_runs", 0) == 0:
                    lines.append(f"| {pipe_name} | 0 | - | - | - | Never |")
                    continue
                last = stats.get("last_run", {})
                avg_dur = stats.get("avg_duration_sec")
                avg_str = f"{avg_dur}s" if avg_dur else "-"
                lines.append(
                    f"| {pipe_name} | {stats['total_runs']} "
                    f"| {stats['success_rate']}% | {avg_str} "
                    f"| {last.get('status', '')} | {last.get('run_end', '')} |"
                )
            lines.append("")

        return lines

    def print_summary(self):
        profile = self.full_profile(include_runs=False)
        summary = profile["summary"]
        factory = profile["factory"]
        print(f"\n{'=' * 60}")
        print(f"  ADF Profile: {factory['name']}")
        print(f"  Location: {factory.get('location')}")
        print(f"{'=' * 60}")
        print(f"  Pipelines:           {summary['pipeline_count']}")
        print(f"  Datasets:            {summary['dataset_count']}")
        print(f"  Linked Services:     {summary['linked_service_count']}")
        print(f"  Triggers:            {summary['trigger_count']}")
        print(f"  Data Flows:          {summary['data_flow_count']}")
        print(f"  Integration Runtimes:{summary['integration_runtime_count']}")
        print(f"{'=' * 60}")
        print(f"  Connectors: {', '.join(sorted(summary.get('connector_types', [])))}")
        print(f"  Dataset Types: {', '.join(sorted(summary.get('dataset_types', [])))}")
        print(f"{'=' * 60}\n")

    def to_json(
        self,
        output_path: Optional[str] = None,
        include_runs: bool = True,
        days_back: int = 30,
    ) -> str:
        profile = self.full_profile(include_runs=include_runs, days_back=days_back)
        result = json.dumps(profile, indent=2, default=str)
        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(result)
        return result

    # ------------------------------------------------------------------ #
    #  Excel Report                                                        #
    # ------------------------------------------------------------------ #

    def to_excel(
        self,
        output_path: str = "adf_profile.xlsx",
        include_runs: bool = True,
        days_back: int = 30,
    ) -> str:
        import pandas as pd

        profile = self.full_profile(include_runs=include_runs, days_back=days_back)
        factory = profile["factory"]

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # -- Sheet 1: Summary --
            summary_rows = [
                {"Property": "Factory Name", "Value": factory.get("name")},
                {"Property": "Location", "Value": factory.get("location")},
                {"Property": "Provisioning State", "Value": factory.get("provisioning_state")},
                {"Property": "Created", "Value": factory.get("create_time")},
                {"Property": "Version", "Value": factory.get("version")},
                {"Property": "Profiled At", "Value": profile.get("profiled_at")},
                {"Property": "Duration (sec)", "Value": profile.get("profile_duration_sec")},
                {"Property": "", "Value": ""},
                {"Property": "Pipelines", "Value": profile["summary"]["pipeline_count"]},
                {"Property": "Datasets", "Value": profile["summary"]["dataset_count"]},
                {
                    "Property": "Linked Services",
                    "Value": profile["summary"]["linked_service_count"],
                },
                {"Property": "Triggers", "Value": profile["summary"]["trigger_count"]},
                {"Property": "Data Flows", "Value": profile["summary"]["data_flow_count"]},
                {
                    "Property": "Integration Runtimes",
                    "Value": profile["summary"]["integration_runtime_count"],
                },
                {"Property": "", "Value": ""},
                {
                    "Property": "Connector Types",
                    "Value": ", ".join(sorted(profile["summary"].get("connector_types", []))),
                },
                {
                    "Property": "Dataset Types",
                    "Value": ", ".join(sorted(profile["summary"].get("dataset_types", []))),
                },
                {
                    "Property": "Activity Types",
                    "Value": ", ".join(sorted(profile["summary"].get("activity_types", []))),
                },
            ]
            repo = factory.get("repo_configuration")
            if repo:
                summary_rows.extend(
                    [
                        {"Property": "", "Value": ""},
                        {"Property": "Repo Type", "Value": repo.get("type")},
                        {"Property": "Repo Name", "Value": repo.get("repositoryName")},
                        {"Property": "Branch", "Value": repo.get("collaborationBranch")},
                    ]
                )
            for k, v in factory.get("global_parameters", {}).items():
                summary_rows.append(
                    {
                        "Property": f"Global Param: {k}",
                        "Value": f"{v.get('value')} ({v.get('type')})",
                    }
                )
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

            # -- Sheet 2: Pipelines --
            pipe_rows = []
            for p in profile["pipelines"]:
                params = ", ".join(
                    f"{k} ({v.get('type', '')})" for k, v in p.get("parameters", {}).items()
                )
                variables = ", ".join(
                    f"{k} ({v.get('type', '')})" for k, v in p.get("variables", {}).items()
                )
                pipe_rows.append(
                    {
                        "Pipeline": p["name"],
                        "Folder": p.get("folder", ""),
                        "Description": p.get("description", ""),
                        "Activity Count": p.get("activity_count", 0),
                        "Concurrency": p.get("concurrency", ""),
                        "Parameters": params,
                        "Variables": variables,
                    }
                )
            pd.DataFrame(pipe_rows).to_excel(writer, sheet_name="Pipelines", index=False)

            # -- Sheet 3: Activities --
            act_rows = []
            for p in profile["pipelines"]:
                for a in p.get("activities", []):
                    deps = ", ".join(d.get("activity", "") for d in a.get("depends_on", []))
                    inputs = ", ".join(i.get("dataset", "") for i in a.get("inputs", []))
                    outputs = ", ".join(o.get("dataset", "") for o in a.get("outputs", []))
                    act_rows.append(
                        {
                            "Pipeline": p["name"],
                            "Activity": a["name"],
                            "Type": a["type"],
                            "Depends On": deps,
                            "Input Datasets": inputs,
                            "Output Datasets": outputs,
                            "Source Type": a.get("source_type", ""),
                            "Sink Type": a.get("sink_type", ""),
                            "Called Pipeline": a.get("called_pipeline", ""),
                            "Data Flow": a.get("data_flow", ""),
                        }
                    )
            pd.DataFrame(act_rows).to_excel(writer, sheet_name="Activities", index=False)

            # -- Sheet 4: Datasets --
            ds_rows = []
            for ds in profile["datasets"]:
                loc = ds.get("location", {})
                loc_str = ""
                if loc:
                    parts = [f"{k}={v}" for k, v in loc.items() if v]
                    loc_str = ", ".join(parts)
                params = ", ".join(
                    f"{k} ({v.get('type', '')})" for k, v in ds.get("parameters", {}).items()
                )
                ds_rows.append(
                    {
                        "Dataset": ds["name"],
                        "Type": ds.get("type", ""),
                        "Linked Service": ds.get("linked_service", ""),
                        "Folder": ds.get("folder", ""),
                        "Location": loc_str,
                        "Parameters": params,
                        "Description": ds.get("description", ""),
                    }
                )
            pd.DataFrame(ds_rows).to_excel(writer, sheet_name="Datasets", index=False)

            # -- Sheet 5: Linked Services --
            ls_rows = []
            for ls in profile["linked_services"]:
                conn = ls.get("connection_details", {})
                conn_str = ", ".join(f"{k}={v}" for k, v in conn.items() if v)
                ls_rows.append(
                    {
                        "Linked Service": ls["name"],
                        "Type": ls.get("type", ""),
                        "Integration Runtime": ls.get("connect_via", "AutoResolve"),
                        "Auth Type": conn.get("authenticationType", ""),
                        "Connection Details": conn_str,
                        "Description": ls.get("description", ""),
                    }
                )
            pd.DataFrame(ls_rows).to_excel(writer, sheet_name="Linked Services", index=False)

            # -- Sheet 6: Triggers --
            trig_rows = []
            for t in profile["triggers"]:
                pipes = ", ".join(p.get("pipeline", "") for p in t.get("associated_pipelines", []))
                schedule_str = ""
                if t.get("schedule"):
                    s = t["schedule"]
                    schedule_str = f"Every {s.get('interval', '')} {s.get('frequency', '')}"
                    if s.get("time_zone"):
                        schedule_str += f" ({s['time_zone']})"
                elif t.get("tumbling_window"):
                    tw = t["tumbling_window"]
                    schedule_str = f"Every {tw.get('interval', '')} {tw.get('frequency', '')}"
                elif t.get("blob_events"):
                    be = t["blob_events"]
                    schedule_str = f"Events: {', '.join(be.get('events', []))}"
                trig_rows.append(
                    {
                        "Trigger": t["name"],
                        "Type": t.get("type", ""),
                        "State": t.get("runtime_state", ""),
                        "Schedule": schedule_str,
                        "Associated Pipelines": pipes,
                        "Description": t.get("description", ""),
                    }
                )
            pd.DataFrame(trig_rows).to_excel(writer, sheet_name="Triggers", index=False)

            # -- Sheet 7: Data Flows --
            if profile["data_flows"]:
                df_rows = []
                for df in profile["data_flows"]:
                    sources = ", ".join(
                        f"{s['name']} ({s.get('dataset') or s.get('linked_service') or 'inline'})"
                        for s in df.get("sources", [])
                    )
                    sinks = ", ".join(
                        f"{s['name']} ({s.get('dataset') or s.get('linked_service') or 'inline'})"
                        for s in df.get("sinks", [])
                    )
                    transforms = ", ".join(t["name"] for t in df.get("transformations", []))
                    df_rows.append(
                        {
                            "Data Flow": df["name"],
                            "Type": df.get("type", ""),
                            "Folder": df.get("folder", ""),
                            "Sources": sources,
                            "Sinks": sinks,
                            "Transformations": transforms,
                            "Description": df.get("description", ""),
                        }
                    )
                pd.DataFrame(df_rows).to_excel(writer, sheet_name="Data Flows", index=False)

            # -- Sheet 8: Integration Runtimes --
            ir_rows = []
            for ir in profile["integration_runtimes"]:
                compute = ir.get("compute", {})
                ir_rows.append(
                    {
                        "Integration Runtime": ir["name"],
                        "Type": ir.get("type", ""),
                        "State": ir.get("state", ""),
                        "Location": compute.get("location", ""),
                        "Core Count": compute.get("core_count", ""),
                        "Compute Type": compute.get("compute_type", ""),
                        "Self Hosted": ir.get("self_hosted", False),
                        "Description": ir.get("description", ""),
                    }
                )
            pd.DataFrame(ir_rows).to_excel(writer, sheet_name="Integration Runtimes", index=False)

            # -- Sheet 9: Run Statistics --
            if "run_statistics" in profile:
                run_rows = []
                for pipe_name, stats in profile["run_statistics"].items():
                    if "error" in stats:
                        run_rows.append(
                            {
                                "Pipeline": pipe_name,
                                "Total Runs": "Error",
                                "Success Rate (%)": "",
                                "Avg Duration (sec)": "",
                                "Min Duration (sec)": "",
                                "Max Duration (sec)": "",
                                "Last Status": "",
                                "Last Run Start": "",
                                "Last Run End": "",
                                "Last Triggered By": "",
                                "Error": stats["error"],
                            }
                        )
                        continue
                    last = stats.get("last_run", {})
                    run_rows.append(
                        {
                            "Pipeline": pipe_name,
                            "Total Runs": stats.get("total_runs", 0),
                            "Success Rate (%)": stats.get("success_rate"),
                            "Avg Duration (sec)": stats.get("avg_duration_sec"),
                            "Min Duration (sec)": stats.get("min_duration_sec"),
                            "Max Duration (sec)": stats.get("max_duration_sec"),
                            "Last Status": last.get("status", ""),
                            "Last Run Start": last.get("run_start", ""),
                            "Last Run End": last.get("run_end", ""),
                            "Last Triggered By": last.get("triggered_by", ""),
                            "Error": "",
                        }
                    )
                pd.DataFrame(run_rows).to_excel(writer, sheet_name="Run Statistics", index=False)

            # -- Sheet 10: Lineage --
            lineage_rows = []
            datasets_map = {d["name"]: d for d in profile["datasets"]}
            ls_map = {ls["name"]: ls for ls in profile["linked_services"]}
            for p in profile["pipelines"]:
                for a in p.get("activities", []):
                    for inp in a.get("inputs", []):
                        ds_name = inp.get("dataset", "")
                        ds = datasets_map.get(ds_name, {})
                        ls_name = ds.get("linked_service", "")
                        ls = ls_map.get(ls_name, {})
                        lineage_rows.append(
                            {
                                "Pipeline": p["name"],
                                "Activity": a["name"],
                                "Direction": "Source",
                                "Dataset": ds_name,
                                "Dataset Type": ds.get("type", ""),
                                "Linked Service": ls_name,
                                "Connector Type": ls.get("type", ""),
                            }
                        )
                    for out in a.get("outputs", []):
                        ds_name = out.get("dataset", "")
                        ds = datasets_map.get(ds_name, {})
                        ls_name = ds.get("linked_service", "")
                        ls = ls_map.get(ls_name, {})
                        lineage_rows.append(
                            {
                                "Pipeline": p["name"],
                                "Activity": a["name"],
                                "Direction": "Sink",
                                "Dataset": ds_name,
                                "Dataset Type": ds.get("type", ""),
                                "Linked Service": ls_name,
                                "Connector Type": ls.get("type", ""),
                            }
                        )
            if lineage_rows:
                pd.DataFrame(lineage_rows).to_excel(writer, sheet_name="Lineage", index=False)

            self._auto_fit_columns(writer)

        self._progress(f"Excel report saved to {output_path}", 0, 0)
        return output_path

    @staticmethod
    def _auto_fit_columns(writer):
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = 0
                col_letter = col[0].column_letter
                for cell in col:
                    val = str(cell.value) if cell.value is not None else ""
                    max_len = max(max_len, len(val))
                ws.column_dimensions[col_letter].width = min(max_len + 3, 60)
