"""Bootstrap helper for Odibi MCP — notebook ergonomics.

Usage:
    import sys
    sys.path.insert(0, "/Workspace/Users/henryodibi@outlook.com/Odibi/odibi_mcp")
    from odibi_mcp.bootstrap import init
    odibi, odibi_help = init()
    
    # Now use like:
    result = odibi_help()  # Full catalog
    result = odibi_help(category="Discovery")  # Filtered
    result = odibi_help(action="profile_source")  # Action details
    result = odibi("list_workflows")  # Execute action
    result = odibi("profile_source", source_path="/path/to/data.csv")  # With kwargs
"""
from __future__ import annotations

import json
from typing import Any

from dispatcher import OdibiDispatcher


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