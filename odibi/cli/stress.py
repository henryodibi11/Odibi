"""Stress testing command for Odibi."""

import os
import sys
import shutil
import random
import string
import time
import csv
import json
from pathlib import Path
from typing import List, Optional
import subprocess
from odibi.utils.logging import logger


def add_stress_parser(subparsers):
    """Add subparser for stress command."""
    parser = subparsers.add_parser(
        "stress",
        help="Run stress tests (Infinite Gauntlet) against generated or provided datasets",
    )
    parser.add_argument(
        "--count", "-c", type=int, default=5, help="Number of synthetic datasets to generate"
    )
    parser.add_argument(
        "--output", "-o", default="stress_test_runs", help="Directory to store test runs"
    )
    parser.add_argument("--keep", action="store_true", help="Keep generated projects after run")
    parser.add_argument(
        "--source", choices=["synthetic", "kaggle"], default="synthetic", help="Data source"
    )
    parser.add_argument(
        "--kaggle-key", help="Path to kaggle.json (optional, defaults to standard locations)"
    )
    parser.add_argument("--keyword", help="Keyword to search Kaggle for (e.g., 'finance')")
    parser.add_argument(
        "--max-size", type=int, default=50, help="Maximum dataset size in MB (default: 50)"
    )
    parser.add_argument(
        "--file-type",
        default="csv",
        choices=["csv", "json", "sqlite", "parquet", "bigQuery"],
        help="File type to search for on Kaggle",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: CPU count - 1)",
    )


def setup_kaggle_auth(key_path: Optional[str] = None) -> bool:
    """Setup Kaggle authentication env vars."""
    if key_path:
        path = Path(key_path)
    else:
        # Try default location
        path = Path.home() / "Downloads" / "kaggle.json"
        if not path.exists():
            path = Path.home() / ".kaggle" / "kaggle.json"

    if path.exists():
        try:
            with open(path) as f:
                creds = json.load(f)
                os.environ["KAGGLE_USERNAME"] = creds["username"]
                os.environ["KAGGLE_KEY"] = creds["key"]
                logger.info(f"Loaded Kaggle credentials from {path}")
                return True
        except Exception as e:
            logger.warning(f"Failed to load kaggle.json: {e}")

    # Check if env vars already set
    if "KAGGLE_USERNAME" in os.environ and "KAGGLE_KEY" in os.environ:
        return True

    logger.error("Kaggle credentials not found. Please provide --kaggle-key or set env vars.")
    return False


def download_kaggle_dataset(dataset: str, output_dir: Path) -> bool:
    """Download and unzip a Kaggle dataset."""
    try:
        import kaggle

        logger.info(f"Downloading {dataset}...")
        kaggle.api.dataset_download_files(dataset, path=str(output_dir), unzip=True)
        return True
    except ImportError:
        logger.error("Kaggle library not installed. Run 'pip install kaggle'")
        return False
    except Exception as e:
        logger.error(f"Failed to download {dataset}: {e}")
        return False


def get_kaggle_datasets(
    keyword: Optional[str] = None, max_size_mb: int = 50, file_type: str = "csv"
) -> List[str]:
    """Dynamically fetch datasets from Kaggle."""
    try:
        import kaggle

        # If no keyword, pick a random topic/sort
        search_term = keyword or random.choice(
            [
                "sales",
                "finance",
                "healthcare",
                "retail",
                "education",
                "weather",
                "traffic",
                "inventory",
                "marketing",
            ]
        )

        if file_type not in ["all", "csv", "sqlite", "json", "bigQuery"]:
            # Fallback for types like 'parquet', 'excel'
            api_file_type = "all"
            # Append file type to search query to find relevant datasets
            search_term = f"{search_term} {file_type}"
        else:
            api_file_type = file_type

        logger.info(
            f"Searching Kaggle for '{search_term}' datasets (<{max_size_mb}MB, type={api_file_type})..."
        )

        # Fetch more than we need to filter
        # Kaggle API returns 20 by default. We need to iterate pages.
        datasets = []
        page = 1
        while len(datasets) < 100:  # Hard limit to prevent infinite loops
            batch = kaggle.api.dataset_list(
                search=search_term,
                sort_by="votes",
                file_type=api_file_type,
                max_size=max_size_mb * 1024 * 1024,
                page=page,
            )
            if not batch:
                break

            datasets.extend(batch)
            page += 1

            # If we got less than 20, we probably hit the end
            if len(batch) < 20:
                break

        # Return ref names "user/slug"
        return [d.ref for d in datasets]

    except ImportError:
        logger.error("Kaggle library not installed.")
        return []
    except Exception as e:
        logger.error(f"Failed to list Kaggle datasets: {e}")
        return []


def generate_random_string(length=10):
    return "".join(random.choices(string.ascii_letters, k=length))


def generate_messy_csv(path: Path, rows=100):
    """Generate a CSV with potential issues (nulls, weird chars)."""
    headers = ["id", "name", "score", "created_at", "notes"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(rows):
            # Inject chaos
            if random.random() < 0.05:
                # Skip row
                continue

            row = [
                i,
                generate_random_string(random.randint(5, 20)),
                random.uniform(0, 100) if random.random() > 0.1 else "",  # Occasional null
                "2023-01-01" if random.random() > 0.1 else "invalid-date",
                generate_random_string(50) if random.random() > 0.1 else None,
            ]
            writer.writerow(row)


def _process_dataset(item, i, args, output_dir):
    """Process a single dataset (worker function)."""
    
    if args.source == "kaggle":
        run_id = item.replace("/", "_")
        dataset_name = item
    else:
        run_id = f"run_{i+1}"
        dataset_name = None

    run_dir = output_dir / run_id
    data_dir = run_dir / "raw_data"
    project_dir = run_dir / "project"

    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        # If race condition or error
        logger.error(f"[{run_id}] Failed to create dir: {e}")
        return {"id": run_id, "status": "setup_error", "error": str(e)}

    # 1. Generate/Download Data
    if args.source == "kaggle":
        logger.info(f"[{run_id}] Downloading {dataset_name}...")
        if not download_kaggle_dataset(dataset_name, data_dir):
            return {"id": run_id, "status": "download_failed", "error": "API Error"}
    else:
        logger.info(f"[{run_id}] Generating data...")
        generate_messy_csv(data_dir / "users.csv", rows=random.randint(50, 500))
        if random.choice([True, False]):
            generate_messy_csv(data_dir / "orders.csv", rows=random.randint(50, 500))

    # 2. Run odibi generate-project
    logger.info(f"[{run_id}] Generating project...")
    cmd_gen = [
        sys.executable,
        "-m",
        "odibi",
        "generate-project",
        "--input",
        str(data_dir),
        "--output",
        str(project_dir),
        "--name",
        f"stress_{run_id}",
    ]

    try:
        # Capture output to log file
        log_path = run_dir / "generation.log"
        with open(log_path, "w", encoding="utf-8") as f:
            proc = subprocess.run(cmd_gen, check=False, capture_output=True)
            f.write("STDOUT:\n")
            f.write(proc.stdout.decode(errors="replace"))
            f.write("\nSTDERR:\n")
            f.write(proc.stderr.decode(errors="replace"))

        if proc.returncode != 0:
            error_msg = proc.stderr.decode(errors="replace")
            logger.error(f"[{run_id}] Project generation failed. See {log_path}")
            return {"id": run_id, "status": "gen_failed", "error": error_msg, "log": str(log_path)}

    except Exception as e:
        logger.error(f"[{run_id}] Generation exception: {e}")
        return {"id": run_id, "status": "gen_failed", "error": str(e)}

    # 3. Run odibi run
    logger.info(f"[{run_id}] Running pipeline...")
    cmd_run = [sys.executable, "-m", "odibi", "run", "odibi.yaml"]

    start_time = time.time()
    try:
        log_path = run_dir / "execution.log"
        with open(log_path, "w", encoding="utf-8") as f:
            proc = subprocess.run(cmd_run, cwd=str(project_dir), capture_output=True)
            f.write("STDOUT:\n")
            f.write(proc.stdout.decode(errors="replace"))
            f.write("\nSTDERR:\n")
            f.write(proc.stderr.decode(errors="replace"))

        duration = time.time() - start_time

        if proc.returncode == 0:
            logger.info(f"[{run_id}] Success ({duration:.2f}s)")
            return {"id": run_id, "status": "success", "duration": duration}
        else:
            error_msg = (
                proc.stderr.decode(errors="replace") + "\n" + proc.stdout.decode(errors="replace")
            )
            logger.error(f"[{run_id}] Failed. See {log_path}")
            return {"id": run_id, "status": "run_failed", "error": error_msg, "log": str(log_path)}

    except Exception as e:
        logger.error(f"[{run_id}] Execution error: {e}")
        return {"id": run_id, "status": "exec_error", "error": str(e)}


def run_stress_test(args):
    """Execute stress test."""
    output_dir = Path(args.output)
    if output_dir.exists():
        try:
            shutil.rmtree(output_dir)
        except PermissionError:
            logger.warning(f"Could not clean up {output_dir}. Using a new directory.")
            # Generate a unique suffix
            import uuid

            output_dir = Path(f"{args.output}_{uuid.uuid4().hex[:6]}")

    output_dir.mkdir(exist_ok=True)

    results = []

    if args.source == "kaggle":
        if not setup_kaggle_auth(args.kaggle_key):
            return 1

        # Dynamically fetch datasets
        available_datasets = get_kaggle_datasets(args.keyword, args.max_size, args.file_type)

        if not available_datasets:
            logger.error("No datasets found on Kaggle matching criteria.")
            return 1

        # Randomly select requested count
        if len(available_datasets) > args.count:
            datasets = random.sample(available_datasets, args.count)
        else:
            datasets = available_datasets

        logger.info(f"Selected {len(datasets)} datasets: {', '.join(datasets)}")
    else:
        logger.info(f"Starting stress test with {args.count} synthetic datasets...")
        datasets = range(args.count)

    import concurrent.futures
    import multiprocessing

    max_workers = args.workers
    if max_workers is None:
        # Default to CPU count - 1, minimum 1
        try:
            max_workers = max(1, multiprocessing.cpu_count() - 1)
        except NotImplementedError:
            max_workers = 4

    logger.info(f"Running with {max_workers} workers")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i, item in enumerate(datasets):
            futures.append(executor.submit(_process_dataset, item, i, args, output_dir))

        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                logger.error(f"Worker exception: {e}")

    # Save structured report
    report_path = output_dir / "stress_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Detailed report saved to {report_path}")

    # Report
    print("\n--- Stress Test Results ---")
    success_count = sum(1 for r in results if r["status"] == "success")
    print(f"Total Runs: {len(datasets)}")
    print(f"Success: {success_count}")
    print(f"Failed: {len(datasets) - success_count}")

    for r in results:
        if r["status"] != "success":
            print(f"- {r['id']}: {r['status']}")

    if not args.keep:
        shutil.rmtree(output_dir)

    return 0 if success_count == len(datasets) else 1
