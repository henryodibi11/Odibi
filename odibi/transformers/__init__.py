from odibi.registry import FunctionRegistry

# Import all transform modules
from odibi.transformers import (
    advanced,
    delete_detection,
    manufacturing,
    merge_transformer,
    relational,
    scd,
    sql_core,
    validation,
)

# List of all standard library modules
_MODULES = [sql_core, relational, advanced, scd, validation, delete_detection]


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
    registry.register(sql_core.select_columns, "select_columns", sql_core.SelectColumnsParams)
    registry.register(sql_core.drop_columns, "drop_columns", sql_core.DropColumnsParams)
    registry.register(sql_core.rename_columns, "rename_columns", sql_core.RenameColumnsParams)
    registry.register(sql_core.add_prefix, "add_prefix", sql_core.AddPrefixParams)
    registry.register(sql_core.add_suffix, "add_suffix", sql_core.AddSuffixParams)
    registry.register(
        sql_core.normalize_column_names,
        "normalize_column_names",
        sql_core.NormalizeColumnNamesParams,
    )
    registry.register(sql_core.coalesce_columns, "coalesce_columns", sql_core.CoalesceColumnsParams)
    registry.register(sql_core.replace_values, "replace_values", sql_core.ReplaceValuesParams)
    registry.register(sql_core.trim_whitespace, "trim_whitespace", sql_core.TrimWhitespaceParams)

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
        advanced.generate_numeric_key, "generate_numeric_key", advanced.NumericKeyParams
    )
    registry.register(
        advanced.validate_and_flag, "validate_and_flag", advanced.ValidateAndFlagParams
    )
    registry.register(
        advanced.window_calculation, "window_calculation", advanced.WindowCalculationParams
    )
    registry.register(advanced.normalize_json, "normalize_json", advanced.NormalizeJsonParams)
    registry.register(advanced.sessionize, "sessionize", advanced.SessionizeParams)
    registry.register(advanced.geocode, "geocode", advanced.GeocodeParams)
    registry.register(
        advanced.split_events_by_period,
        "split_events_by_period",
        advanced.SplitEventsByPeriodParams,
    )

    # SCD
    registry.register(scd.scd2, "scd2", scd.SCD2Params)

    # Merge
    registry.register(merge_transformer.merge, "merge", merge_transformer.MergeParams)

    # Validation
    registry.register(validation.cross_check, "cross_check", validation.CrossCheckParams)

    # Delete Detection
    from odibi.config import DeleteDetectionConfig

    registry.register(delete_detection.detect_deletes, "detect_deletes", DeleteDetectionConfig)

    # Manufacturing
    registry.register(
        manufacturing.detect_sequential_phases,
        "detect_sequential_phases",
        manufacturing.DetectSequentialPhasesParams,
    )


# Auto-register on import
# register_standard_library() # Removed to allow explicit registration
