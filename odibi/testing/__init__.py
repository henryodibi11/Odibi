"""
Testing Utilities and Fixtures
===============================

This module provides utilities to help users test their pipelines.

Planned features (Phase 3):
- Test fixtures: Sample data generators, temp directories
- Assertions: DataFrame equality checks (engine-agnostic)
- Mock objects: Spark sessions, connections
- Test helpers: Common testing patterns

Example (future):
    from odibi.testing import fixtures, assertions

    def test_my_pipeline():
        with fixtures.temp_workspace() as ws:
            df = fixtures.sample_data(rows=100)
            result = my_transform(df)
            assertions.assert_df_equal(result, expected)
"""

__all__ = []
__version__ = "0.0.0"
