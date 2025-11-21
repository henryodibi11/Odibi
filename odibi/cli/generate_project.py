"""Project generation command for automated stress testing."""

import os
import json
import csv
from pathlib import Path
from typing import Dict, List, Any
import yaml
from odibi.utils.logging import logger


def add_generate_project_parser(subparsers):
    """Add subparser for generate-project command."""
    parser = subparsers.add_parser(
        "generate-project",
        help="Generate a new Odibi project from a dataset (Stress Testing)",
    )
    parser.add_argument(
        "--input", "-i", required=True, help="Input directory containing raw data (CSV/JSON)"
    )
    parser.add_argument(
        "--output", "-o", required=True, help="Output directory for the new project"
    )
    parser.add_argument("--name", "-n", help="Project name (defaults to output directory name)")


def analyze_csv(file_path: Path) -> Dict[str, Any]:
    """Analyze a CSV file to infer schema."""
    schema = {}
    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            if not headers:
                return {}

            # Read a sample of rows (e.g., up to 100)
            sample_rows = []
            for _ in range(100):
                try:
                    row = next(reader)
                    sample_rows.append(row)
                except StopIteration:
                    break

            # Infer types
            for i, header in enumerate(headers):
                col_values = [row[i] for row in sample_rows if i < len(row) and row[i].strip()]
                schema[header] = infer_type(col_values)

    except Exception as e:
        logger.warning(f"Failed to analyze CSV {file_path}: {e}")

    return schema


def analyze_json(file_path: Path) -> Dict[str, Any]:
    """Analyze a JSON file to infer schema."""
    schema = {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            # Try reading as line-delimited JSON first
            lines = []
            for _ in range(100):
                line = f.readline()
                if not line:
                    break
                try:
                    data = json.loads(line)
                    lines.append(data)
                except json.JSONDecodeError:
                    # Not line-delimited, try reading whole file
                    f.seek(0)
                    try:
                        full_data = json.load(f)
                        if isinstance(full_data, list):
                            lines = full_data[:100]
                        else:
                            # Single object? Treat as one row?
                            pass
                    except Exception:
                        pass
                    break

            if not lines:
                return {}

            # Infer types from first non-empty value in each field
            all_keys = set()
            for row in lines:
                if isinstance(row, dict):
                    all_keys.update(row.keys())

            for key in all_keys:
                values = [str(row.get(key)) for row in lines if row.get(key) is not None]
                schema[key] = infer_type(values)

    except Exception as e:
        logger.warning(f"Failed to analyze JSON {file_path}: {e}")

    return schema


def infer_type(values: List[str]) -> str:
    """Infer data type from a list of string values."""
    if not values:
        return "string"

    is_int = True
    is_float = True

    for v in values:
        # Check int
        if is_int:
            try:
                int(v)
            except ValueError:
                is_int = False

        # Check float
        if is_float:
            try:
                float(v)
            except ValueError:
                is_float = False

    if is_int:
        return "integer"
    if is_float:
        return "float"

    return "string"


def analyze_parquet(file_path: Path) -> Dict[str, Any]:
    """Analyze a Parquet file to infer schema."""
    schema = {}
    try:
        import pandas as pd

        # Read metadata only if possible, or small sample
        # Fastparquet or PyArrow needed
        # Just read head with pandas
        df = pd.read_parquet(file_path)

        # Map pandas types to Odibi types
        for col, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            if "int" in dtype_str:
                schema[col] = "integer"
            elif "float" in dtype_str:
                schema[col] = "float"
            else:
                schema[col] = "string"

    except Exception as e:
        logger.warning(f"Failed to analyze Parquet {file_path}: {e}")

    return schema


def analyze_excel(file_path: Path) -> Dict[str, Any]:
    """Analyze an Excel file to infer schema."""
    schema = {}
    try:
        import pandas as pd

        # Read head
        df = pd.read_excel(file_path, nrows=100)

        for col, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            if "int" in dtype_str:
                schema[col] = "integer"
            elif "float" in dtype_str:
                schema[col] = "float"
            else:
                schema[col] = "string"

    except Exception as e:
        logger.warning(f"Failed to analyze Excel {file_path}: {e}")

    return schema


def generate_project_structure(input_dir: Path, output_dir: Path, project_name: str) -> None:
    """Generate the project structure."""
    if not output_dir.exists():
        os.makedirs(output_dir)

    # Create standard directories
    (output_dir / "data").mkdir(exist_ok=True)
    (output_dir / "scripts").mkdir(exist_ok=True)

    files_found = []
    schemas = {}

    # Scan input directory
    for file_path in input_dir.glob("*"):
        file_schema = {}
        file_type = ""

        if file_path.suffix.lower() == ".csv":
            logger.info(f"Analyzing {file_path.name}...")
            file_schema = analyze_csv(file_path)
            file_type = "csv"
        elif file_path.suffix.lower() == ".json":
            logger.info(f"Analyzing {file_path.name}...")
            file_schema = analyze_json(file_path)
            file_type = "json"
        elif file_path.suffix.lower() == ".parquet":
            logger.info(f"Analyzing {file_path.name}...")
            file_schema = analyze_parquet(file_path)
            file_type = "parquet"
        elif file_path.suffix.lower() in [".xlsx", ".xls"]:
            logger.info(f"Analyzing {file_path.name}...")
            file_schema = analyze_excel(file_path)
            file_type = "excel"

        if file_schema:
            # Copy file to output data directory
            import shutil

            dest_path = output_dir / "data" / file_path.name
            shutil.copy2(file_path, dest_path)

            schemas[file_path.stem] = {
                "type": file_type,
                "path": str(dest_path),
                "schema": file_schema,
            }
            files_found.append(file_path)

    # Generate odibi.yaml
    pipeline_config = {
        "project": project_name,
        "engine": "pandas",
        "retry": {"enabled": True, "max_attempts": 3, "backoff": "exponential"},
        "story": {
            "connection": "local",
            "path": ".odibi/stories",
            "auto_generate": True,
            "max_sample_rows": 10,
            "retention_days": 30,
            "retention_count": 100,
        },
        "connections": {
            "local": {"type": "local", "base_path": "./data", "validation_mode": "lazy"}
        },
        "pipelines": [{"pipeline": "main", "nodes": []}],
    }

    nodes = []
    for name, info in schemas.items():
        file_name = Path(info["path"]).name
        # Sanitize name for SQL compatibility (replace spaces/dots/hyphens)
        safe_name = "".join(c if c.isalnum() else "_" for c in name)

        # Bronze Node (Loader)
        bronze_node = {
            "name": f"load_{safe_name}",
            "read": {"connection": "local", "format": info["type"], "path": file_name},
        }
        nodes.append(bronze_node)

        # Silver Node (Validation & Transformation)
        silver_node = {
            "name": f"clean_{safe_name}",
            "depends_on": [f"load_{safe_name}"],
            "transform": {"steps": []},
            "validation": {},
        }

        # Heuristics
        validation_rules = {}
        schema = info["schema"]

        # 1. Validation: Check for lat/lon
        if "lat" in schema and "lon" in schema:
            validation_rules["ranges"] = {
                "lat": {"min": -90, "max": 90},
                "lon": {"min": -180, "max": 180},
            }
        elif "latitude" in schema and "longitude" in schema:
            validation_rules["ranges"] = {
                "latitude": {"min": -90, "max": 90},
                "longitude": {"min": -180, "max": 180},
            }

        # 2. Transform: Cast types if needed (Pandas mostly infers, but we can be explicit in SQL)
        # For now, let's just select all
        silver_node["transform"]["steps"].append(f"SELECT * FROM load_{safe_name}")

        if validation_rules:
            silver_node["validation"] = validation_rules

        nodes.append(silver_node)

    pipeline_config["pipelines"][0]["nodes"] = nodes

    # Write odibi.yaml
    with open(output_dir / "odibi.yaml", "w") as f:
        yaml.dump(pipeline_config, f, sort_keys=False)

    logger.info(f"Generated project '{project_name}' in {output_dir}")
    logger.info(f"Found {len(files_found)} datasets.")


def generate_project_command(args):
    """Entry point for generate-project command."""
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    project_name = args.name or output_dir.name

    if not input_dir.exists():
        logger.error(f"Input directory {input_dir} does not exist.")
        return 1

    try:
        generate_project_structure(input_dir, output_dir, project_name)
        return 0
    except Exception as e:
        logger.error(f"Failed to generate project: {e}")
        import traceback

        traceback.print_exc()
        return 1
