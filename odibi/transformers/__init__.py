from odibi.registry import FunctionRegistry

# Import all transform modules
from odibi.transformers import advanced, relational, sql_core, scd

# List of all standard library modules
_MODULES = [sql_core, relational, advanced, scd]


def register_standard_library():
    """
    Registers all standard transformations into the global registry.
    This is called automatically when the framework initializes.
    """
    # Helper to register functions from a module
    # We look for functions that match the transform signature or are explicitly exported
    # For now, we manually register the known list to be safe and explicit.

    registry = FunctionRegistry

    # SQL Core
    registry.register(sql_core.filter_rows, "filter_rows", sql_core.FilterRowsParams)
    registry.register(sql_core.derive_columns, "derive_columns", sql_core.DeriveColumnsParams)
    registry.register(sql_core.cast_columns, "cast_columns", sql_core.CastColumnsParams)
    registry.register(sql_core.clean_text, "clean_text", sql_core.CleanTextParams)
    registry.register(sql_core.extract_date_parts, "extract_date_parts", sql_core.ExtractDateParams)
    registry.register(sql_core.normalize_schema, "normalize_schema", sql_core.NormalizeSchemaParams)
    registry.register(sql_core.sort, "sort", sql_core.SortParams)
    registry.register(sql_core.limit, "limit", sql_core.LimitParams)
    registry.register(sql_core.sample, "sample", sql_core.SampleParams)
    registry.register(sql_core.distinct, "distinct", sql_core.DistinctParams)
    registry.register(sql_core.fill_nulls, "fill_nulls", sql_core.FillNullsParams)
    registry.register(sql_core.split_part, "split_part", sql_core.SplitPartParams)
    registry.register(sql_core.date_add, "date_add", sql_core.DateAddParams)
    registry.register(sql_core.date_trunc, "date_trunc", sql_core.DateTruncParams)
    registry.register(sql_core.date_diff, "date_diff", sql_core.DateDiffParams)
    registry.register(sql_core.case_when, "case_when", sql_core.CaseWhenParams)
    registry.register(sql_core.convert_timezone, "convert_timezone", sql_core.ConvertTimezoneParams)
    registry.register(sql_core.concat_columns, "concat_columns", sql_core.ConcatColumnsParams)

    # Relational
    registry.register(relational.join, "join", relational.JoinParams)
    registry.register(relational.union, "union", relational.UnionParams)
    registry.register(relational.pivot, "pivot", relational.PivotParams)
    registry.register(relational.unpivot, "unpivot", relational.UnpivotParams)
    registry.register(relational.aggregate, "aggregate", relational.AggregateParams)

    # Advanced
    registry.register(advanced.deduplicate, "deduplicate", advanced.DeduplicateParams)
    registry.register(advanced.explode_list_column, "explode_list_column", advanced.ExplodeParams)
    registry.register(advanced.dict_based_mapping, "dict_based_mapping", advanced.DictMappingParams)
    registry.register(advanced.regex_replace, "regex_replace", advanced.RegexReplaceParams)
    registry.register(advanced.unpack_struct, "unpack_struct", advanced.UnpackStructParams)
    registry.register(advanced.hash_columns, "hash_columns", advanced.HashParams)
    registry.register(advanced.parse_json, "parse_json", advanced.ParseJsonParams)
    registry.register(
        advanced.generate_surrogate_key, "generate_surrogate_key", advanced.SurrogateKeyParams
    )
    registry.register(
        advanced.validate_and_flag, "validate_and_flag", advanced.ValidateAndFlagParams
    )
    registry.register(
        advanced.window_calculation, "window_calculation", advanced.WindowCalculationParams
    )

    # SCD (Registered as params only for now as we don't have implementation)
    # The user plan just says "New Transformer: SCD Type 2" with "SCD2Params".
    # It seems like this is primarily for the schema.
    # Since I don't have an actual function implementation for SCD2, I won't register a function.
    # BUT, the plan says "New Transformer: SCD Type 2", implying I should probably expose it somehow.
    # The prompt says "Implement the new fields... (e.g. SCD2Params transformer)".
    # Usually transformers are high-level like 'merge'.
    # If it's a transformer like 'merge', it's not in standard library but in `odibi.transformers`.
    # I should check if I need to add it to introspect.py.
    # The user just asked to "Implement the new fields... SCD2Params transformer".
    # It doesn't explicitly ask for the implementation logic, just the schema/params.

    # Wait, looking at the code:
    # registry.register(func, name, params_model)
    # If I don't have a function, I can't register it here.
    # However, 'merge' is mentioned in NodeConfig comments as a 'transformer'.
    # The user plan lists "SCD2Params" under "New Transformer".
    # The goal is likely to have it show up in docs.

    # Let's stick to just creating the file and adding it to __init__.py imports so introspect can find it.
    # I will modify __init__.py to import scd so introspect.py sees it.


# Auto-register on import
register_standard_library()
