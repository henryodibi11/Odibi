import os

from odibi.utils.logging import logger


def ui_command(args):
    # Set env var for config path so app can find it
    os.environ["ODIBI_CONFIG"] = args.config

    try:
        import uvicorn

        from odibi.ui.app import app
    except ImportError as e:
        logger.error(f"UI dependencies not installed: {e}. Run 'pip install fastapi uvicorn'.")
        return 1

    port = args.port
    host = args.host

    print(f"Starting Odibi UI on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


def add_ui_parser(subparsers):
    parser = subparsers.add_parser("ui", help="Launch observability UI")
    parser.add_argument("config", help="Path to YAML config file")
    parser.add_argument("--port", type=int, default=8000, help="Port to run on")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    return parser
