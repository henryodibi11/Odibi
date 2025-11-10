"""
Transformation Registry and Decorators
=======================================

This module provides the infrastructure for users to define and register
custom transformations.

Planned features (Phase 3):
- @transformation decorator: Register user-defined transformations
- TransformationRegistry: Global registry of all transformations
- Context passing: Enable transformations to receive pipeline metadata

Example (future):
    from odibi import transformation
    
    @transformation("my_custom_calc")
    def my_custom_calc(df, threshold):
        '''Filter records above threshold.'''
        return df[df.value > threshold]
    
    @my_custom_calc.explain
    def explain(threshold, **context):
        plant = context.get('plant')
        return f"Filter {plant} records above {threshold}"
"""

__all__ = []
__version__ = "0.0.0"
