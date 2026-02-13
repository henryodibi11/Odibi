import logging
import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from odibi.state import StateManager

logger = logging.getLogger(__name__)

app = FastAPI(title="Odibi UI")

# Resolve paths
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # Load config to get state backend
    config_path = os.getenv("ODIBI_CONFIG")
    if not config_path:
        # Fallback to defaults
        for p in ["odibi.yaml", "odibi.yml", "project.yaml"]:
            if os.path.exists(p):
                config_path = p
                break

    if config_path:
        from odibi.config import load_config_from_file
        from odibi.state import create_state_backend

        try:
            config = load_config_from_file(config_path)
            # Create backend connected to System Catalog
            backend = create_state_backend(config, project_root=os.path.dirname(config_path))
            state_mgr = StateManager(backend=backend)
            state = state_mgr.backend.load_state()
        except Exception as e:
            logger.debug(f"[UI state backend load]: {type(e).__name__}: {e}")
            state = {}
    else:
        state = {}

    pipelines = []
    if state and "pipelines" in state:
        for name, p_data in state["pipelines"].items():
            nodes = p_data.get("nodes", {})
            total = len(nodes)
            # Determine pipeline status based on nodes
            # This is a heuristic as we don't store pipeline-level success explicitly in simple backend
            # usually pipeline_data has it?
            # LocalFileStateBackend saves: {"last_run": ..., "nodes": ...}
            # We don't check individual nodes for pipeline level status in backend save.
            # But we can infer.

            failed_nodes = [n for n in nodes.values() if not n.get("success")]
            status = "FAILED" if failed_nodes else "SUCCESS"
            if total == 0:
                status = "UNKNOWN"

            pipelines.append(
                {
                    "name": name,
                    "last_run": p_data.get("last_run"),
                    "status": status,
                    "nodes_total": total,
                    "nodes_success": total - len(failed_nodes),
                }
            )

    return templates.TemplateResponse(
        "index.html", {"request": request, "pipelines": pipelines, "project": "Odibi Project"}
    )


@app.get("/stories", response_class=HTMLResponse)
async def stories(request: Request):
    # Determine stories root from config if available
    stories_root = Path("stories")
    config_path = os.getenv("ODIBI_CONFIG")
    if config_path:
        from odibi.config import load_config_from_file

        try:
            config = load_config_from_file(config_path)
            # Resolve story path
            # Connection: system -> base_path + config.story.path
            conn_name = config.story.connection
            conn_config = config.connections.get(conn_name)
            if conn_config and conn_config.type == "local":
                base = conn_config.base_path
                if not os.path.isabs(base):
                    base = os.path.join(os.path.dirname(config_path), base)
                stories_root = Path(base) / config.story.path
        except Exception:
            pass

    runs = []

    if stories_root.exists():
        # Traverse: pipeline/date/run.html
        for p_dir in stories_root.iterdir():
            if p_dir.is_dir():
                for d_dir in p_dir.iterdir():
                    if d_dir.is_dir():
                        for f in d_dir.glob("*.html"):
                            runs.append(
                                {
                                    "pipeline": p_dir.name,
                                    "date": d_dir.name,
                                    "name": f.name,
                                    "path": f"/stories_static/{p_dir.name}/{d_dir.name}/{f.name}",
                                }
                            )

    # Sort
    runs.sort(key=lambda x: (x["date"], x["name"]), reverse=True)

    return templates.TemplateResponse("stories.html", {"request": request, "runs": runs})


@app.get("/config", response_class=HTMLResponse)
async def config_view(request: Request):
    config_path = os.getenv("ODIBI_CONFIG")
    content = ""
    error = None

    if config_path:
        try:
            with open(config_path, "r") as f:
                content = f.read()
        except Exception as e:
            error = str(e)
    else:
        # Try default locations
        for p in ["odibi.yaml", "odibi.yml", "project.yaml"]:
            if os.path.exists(p):
                config_path = p
                with open(p, "r") as f:
                    content = f.read()
                break
        if not content:
            error = "No configuration file found. Run with 'odibi ui config.yaml'"

    return templates.TemplateResponse(
        "config.html",
        {"request": request, "config_path": config_path, "content": content, "error": error},
    )


# Mount static files for stories
# We try to mount the configured stories path if possible, otherwise default
# This is tricky because mounting happens at startup, but config might change per request?
# Actually config is set via env var before startup in CLI.
config_path_env = os.getenv("ODIBI_CONFIG")
static_stories_dir = Path("stories")

if config_path_env and os.path.exists(config_path_env):
    # Resolve absolute path to avoid ambiguity
    abs_config_path = Path(config_path_env).resolve()

    from odibi.config import load_config_from_file

    try:
        # Use the official loader to get Pydantic defaults/validation
        config = load_config_from_file(str(abs_config_path))

        s_conn = config.story.connection
        s_path = config.story.path

        if s_conn in config.connections:
            c_conf = config.connections[s_conn]
            if c_conf.type == "local":
                base = c_conf.base_path
                if not os.path.isabs(base):
                    base = os.path.join(abs_config_path.parent, base)
                static_stories_dir = Path(base) / s_path
    except Exception as e:
        logger.debug(f"[UI story path resolution]: {type(e).__name__}: {e}")

if static_stories_dir.exists():
    app.mount(
        "/stories_static", StaticFiles(directory=str(static_stories_dir)), name="stories_static"
    )
