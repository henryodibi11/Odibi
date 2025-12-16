#!/bin/bash
# run_task.sh - Execution boundary for Odibi AI Agent Suite
#
# All agent execution goes through this single script.
# This ensures:
# - All execution happens in WSL (Ubuntu)
# - Spark runs in local mode
# - Python uses virtual environments
# - No Windows-native commands
# - No global software installation
#
# Usage: run_task.sh [--workdir DIR] [--timeout SECONDS] TASK_TYPE [ARGS...]
#
# Task Types:
#   env-check    - Validate environment setup
#   pipeline     - Run Odibi pipeline
#   validate     - Validate configuration
#   pytest       - Run tests
#   ruff         - Run linter
#   black        - Run formatter
#   mypy         - Run type checker
#   odibi        - Run Odibi CLI commands
#   spark-submit - Submit Spark job
#   python       - Run Python script

set -e

# Configuration
ODIBI_ROOT="${ODIBI_ROOT:-$(dirname "$(dirname "$(realpath "$0")")")}"
VENV_PATH="${ODIBI_ROOT}/.venv"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"

# Parse options
WORKDIR=""
TIMEOUT=300

while [[ $# -gt 0 ]]; do
    case $1 in
        --workdir)
            WORKDIR="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

TASK_TYPE="${1:-}"
shift || true

# Validate task type
if [[ -z "$TASK_TYPE" ]]; then
    echo "Error: No task type specified"
    echo "Usage: run_task.sh [--workdir DIR] [--timeout SECONDS] TASK_TYPE [ARGS...]"
    exit 1
fi

# Change to working directory if specified
if [[ -n "$WORKDIR" ]]; then
    cd "$WORKDIR"
else
    cd "$ODIBI_ROOT"
fi

# Get the correct Python command
get_python() {
    if command -v python &> /dev/null; then
        echo "python"
    elif command -v python3 &> /dev/null; then
        echo "python3"
    else
        echo "python"  # Will fail with clear error message
    fi
}

# Activate virtual environment
activate_venv() {
    if [[ -f "${VENV_PATH}/bin/activate" ]]; then
        source "${VENV_PATH}/bin/activate"
    elif [[ -f "${ODIBI_ROOT}/venv/bin/activate" ]]; then
        source "${ODIBI_ROOT}/venv/bin/activate"
    else
        echo "Warning: No virtual environment found, using system Python"
    fi
}

# Environment check
env_check() {
    echo "=== Environment Check ==="
    echo ""

    # WSL check
    echo "WSL:"
    if [[ -f /proc/version ]] && grep -qi microsoft /proc/version; then
        echo "  Status: OK (WSL detected)"
    else
        echo "  Status: OK (Native Linux)"
    fi
    echo ""

    # Java check
    echo "Java:"
    if command -v java &> /dev/null; then
        java_version=$(java -version 2>&1 | head -n 1)
        echo "  Status: OK"
        echo "  Version: $java_version"
    else
        echo "  Status: ISSUE - Java not found"
        echo "  Fix: sudo apt install openjdk-11-jdk"
    fi
    echo ""

    # Spark check
    echo "Spark:"
    if [[ -d "$SPARK_HOME" ]] && [[ -f "$SPARK_HOME/bin/spark-submit" ]]; then
        spark_version=$("$SPARK_HOME/bin/spark-submit" --version 2>&1 | grep -i "version" | head -n 1)
        echo "  Status: OK"
        echo "  Home: $SPARK_HOME"
        echo "  Version: $spark_version"
    else
        echo "  Status: ISSUE - Spark not found"
        echo "  Fix: Install Spark to $SPARK_HOME"
    fi
    echo ""

    # Python venv check
    echo "Python venv:"
    activate_venv
    if [[ -n "$VIRTUAL_ENV" ]]; then
        echo "  Status: OK"
        echo "  Path: $VIRTUAL_ENV"
        echo "  Python: $($(get_python) --version)"
    else
        echo "  Status: ISSUE - No virtual environment active"
        echo "  Fix: python -m venv ${VENV_PATH}"
    fi
    echo ""

    # Odibi check
    echo "Odibi:"
    if $(get_python) -c "import odibi" 2>/dev/null; then
        odibi_version=$($(get_python) -c "import odibi; print(odibi.__version__)" 2>/dev/null || echo "unknown")
        echo "  Status: OK"
        echo "  Version: $odibi_version"
    else
        echo "  Status: ISSUE - Odibi not importable"
        echo "  Fix: pip install -e ${ODIBI_ROOT}"
    fi
    echo ""

    # Resources check
    echo "Resources:"
    echo "  Disk: $(df -h . | awk 'NR==2 {print $4}') available"
    echo "  Memory: $(free -h | awk '/^Mem:/ {print $7}') available"
    echo ""

    echo "=== Check Complete ==="
}

# Run Odibi pipeline
run_pipeline() {
    activate_venv

    # Force unbuffered output for streaming
    export PYTHONUNBUFFERED=1

    local action="${1:-run}"
    shift || true

    # Use stdbuf -oL to force line-buffered output at OS level
    # This ensures output reaches Windows pipes immediately when crossing WSL boundary
    case "$action" in
        run)
            stdbuf -oL $(get_python) -u -m odibi.cli run "$@"
            ;;
        validate)
            stdbuf -oL $(get_python) -u -m odibi.cli validate "$@"
            ;;
        *)
            echo "Error: Unknown pipeline action: $action"
            exit 1
            ;;
    esac
}

# Validate configuration
validate_config() {
    activate_venv
    $(get_python) -m odibi.cli validate "$@"
}

# Run pytest
run_pytest() {
    activate_venv
    $(get_python) -m pytest "$@"
}

# Run ruff linter
run_ruff() {
    activate_venv
    $(get_python) -m ruff "$@"
}

# Run black formatter
run_black() {
    activate_venv
    $(get_python) -m black "$@"
}

# Run mypy type checker
run_mypy() {
    activate_venv
    $(get_python) -m mypy "$@"
}

# Run Odibi CLI
run_odibi() {
    activate_venv
    $(get_python) -m odibi.cli "$@"
}

# Run Spark submit
run_spark_submit() {
    activate_venv
    export PYSPARK_PYTHON="${VIRTUAL_ENV}/bin/python"
    "$SPARK_HOME/bin/spark-submit" \
        --master local[*] \
        --conf spark.driver.memory=2g \
        "$@"
}

# Run Python script
run_python() {
    activate_venv
    $(get_python) "$@"
}

# Execute task with timeout
# Note: We don't use 'timeout' for bash functions as it can't execute them directly.
# Instead, we rely on the Python-level timeout in ExecutionGateway.run_task().
execute_task() {
    "$@"
}

# Main dispatch
case "$TASK_TYPE" in
    env-check)
        env_check
        ;;
    pipeline)
        execute_task run_pipeline "$@"
        ;;
    validate)
        execute_task validate_config "$@"
        ;;
    pytest)
        execute_task run_pytest "$@"
        ;;
    ruff)
        execute_task run_ruff "$@"
        ;;
    black)
        execute_task run_black "$@"
        ;;
    mypy)
        execute_task run_mypy "$@"
        ;;
    odibi)
        execute_task run_odibi "$@"
        ;;
    spark-submit)
        execute_task run_spark_submit "$@"
        ;;
    python)
        execute_task run_python "$@"
        ;;
    *)
        echo "Error: Unknown task type: $TASK_TYPE"
        echo ""
        echo "Valid task types:"
        echo "  env-check    - Validate environment setup"
        echo "  pipeline     - Run Odibi pipeline"
        echo "  validate     - Validate configuration"
        echo "  pytest       - Run tests"
        echo "  ruff         - Run linter"
        echo "  black        - Run formatter"
        echo "  mypy         - Run type checker"
        echo "  odibi        - Run Odibi CLI commands"
        echo "  spark-submit - Submit Spark job"
        echo "  python       - Run Python script"
        exit 1
        ;;
esac
