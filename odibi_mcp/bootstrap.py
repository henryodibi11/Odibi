"""Bootstrap helper for Odibi MCP — notebook ergonomics (Databricks Free Edition).

This is the path that works WITHOUT Databricks Apps: import it in a notebook and
drive Odibi through the same dispatcher the MCP server uses.

Usage:
    import sys
    # Point at the repo ROOT (the folder that CONTAINS the odibi_mcp package),
    # so `from odibi_mcp...` imports resolve:
    sys.path.insert(0, "/Workspace/Users/<you>/Odibi")
    from odibi_mcp.bootstrap import init
    odibi, odibi_help = init()

    # Now use like:
    odibi_help()                               # Full catalog
    odibi_help(category="Discovery")           # Filtered
    odibi_help(action="profile_source")        # Action details
    odibi("onboard")                           # Orient
    odibi("search_docs", query="simulation")   # Discover capabilities
    odibi("profile_source", connection=None, path="/path/to/data.csv")
"""
from __future__ import annotations

from typing import Any

from odibi_mcp.dispatcher import OdibiDispatcher


def init() -> tuple[callable, callable]:
    """Initialize Odibi MCP dispatcher and return helper functions.
    
    Returns:
        (odibi, odibi_help) tuple:
        - odibi(action, **kwargs): Execute an action
        - odibi_help(category=None, action=None): Get help
    """
    dispatcher = OdibiDispatcher()
    
    def odibi(action: str, **kwargs) -> dict[str, Any]:
        """Execute an Odibi action.
        
        Args:
            action: Action name (e.g., 'profile_source', 'list_workflows')
            **kwargs: Action arguments
            
        Returns:
            Action result as dictionary
            
        Examples:
            odibi("list_workflows")
            odibi("profile_source", source_path="/data/file.csv")
            odibi("run_workflow", workflow_name="ingestion", params={"date": "2024-01-01"})
        """
        return dispatcher.dispatch(action, **kwargs)
    
    def odibi_help(category: str | None = None, action: str | None = None) -> dict[str, Any]:
        """Get help on Odibi actions.
        
        Args:
            category: Optional category filter ("Workflows", "Discovery", etc.)
            action: Optional action name for detailed help
            
        Returns:
            Help documentation as dictionary
            
        Examples:
            odibi_help()  # Full catalog
            odibi_help(category="Discovery")  # Category filter
            odibi_help(action="profile_source")  # Action details
        """
        return dispatcher.help(category=category, action=action)
    
    return odibi, odibi_help