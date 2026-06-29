#!/usr/bin/env python3
"""
Verify that odibi-mcp installation is complete and working.

Run this after installing odibi-mcp to verify all dependencies are available:
    pip install odibi-mcp
    python verify_install.py
"""

import sys
from typing import List, Tuple


def test_imports() -> List[Tuple[str, bool, str]]:
    """Test critical imports and return results."""
    results = []
    
    # Test MCP core
    try:
        from odibi_mcp.dispatcher import OdibiDispatcher
        results.append(("odibi_mcp.dispatcher", True, "OdibiDispatcher available"))
    except ImportError as e:
        results.append(("odibi_mcp.dispatcher", False, str(e)))
    
    # Test workflow engine
    try:
        from odibi_mcp.tools.workflows import WorkflowEngine
        results.append(("odibi_mcp.tools.workflows", True, "WorkflowEngine available"))
    except ImportError as e:
        results.append(("odibi_mcp.tools.workflows", False, str(e)))
    
    # Test Odibi core dependencies
    try:
        from odibi.config import PipelineConfig
        results.append(("odibi.config", True, "PipelineConfig available"))
    except ImportError as e:
        results.append(("odibi.config", False, str(e)))
    
    try:
        from odibi.registry import FunctionRegistry
        results.append(("odibi.registry", True, "FunctionRegistry available"))
    except ImportError as e:
        results.append(("odibi.registry", False, str(e)))
    
    # Test critical transitive dependencies
    try:
        import pint
        results.append(("pint", True, f"version {pint.__version__}"))
    except ImportError as e:
        results.append(("pint", False, "Required by odibi.transformers.units"))
    
    try:
        import pydantic
        results.append(("pydantic", True, f"version {pydantic.__version__}"))
    except ImportError as e:
        results.append(("pydantic", False, "Required by odibi.config"))
    
    try:
        import pandas
        results.append(("pandas", True, f"version {pandas.__version__}"))
    except ImportError as e:
        results.append(("pandas", False, "Required by odibi core"))
    
    return results


def main():
    """Run verification and report results."""
    print("🔍 Verifying odibi-mcp installation...\n")
    
    results = test_imports()
    
    # Print results
    passed = 0
    failed = 0
    
    for module, success, message in results:
        if success:
            print(f"✅ {module:35s} {message}")
            passed += 1
        else:
            print(f"❌ {module:35s} FAILED: {message}")
            failed += 1
    
    # Summary
    print(f"\n{'='*70}")
    print(f"Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("\n✅ Installation verified! All dependencies are available.")
        print("\n📦 Dependency chain:")
        print("   odibi-mcp → odibi>=3.11.0 → pint + pandas + pydantic + ...")
        print("   All Odibi dependencies installed transitively from PyPI")
        return 0
    else:
        print("\n❌ Installation incomplete. Missing dependencies.")
        print("\nTo fix, run:")
        print("   pip install odibi-mcp")
        print("\nOr if installing from source:")
        print("   cd /path/to/Odibi/odibi_mcp")
        print("   pip install -e .")
        return 1


if __name__ == "__main__":
    sys.exit(main())
