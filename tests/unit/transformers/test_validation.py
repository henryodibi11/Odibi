import pytest
from unittest.mock import MagicMock, patch

from pydantic import ValidationError as PydanticValidationError

from odibi.exceptions import ValidationError
from odibi.transformers.validation import cross_check, CrossCheckParams


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_context(datasets: dict, row_counts: dict = None, schemas: dict = None):
    """Build a mock EngineContext with the given datasets, row counts, and schemas."""
    ctx_mock = MagicMock()
    ctx_mock.context.get.side_effect = lambda name: datasets.get(name)
    ctx_mock.context._data = MagicMock()
    ctx_mock.context._data.keys.return_value = list(datasets.keys())

    if row_counts is not None:
        ctx_mock.engine.count_rows.side_effect = lambda df: row_counts[df]

    if schemas is not None:
        ctx_mock.engine.get_schema.side_effect = lambda df: schemas[df]

    return ctx_mock


@pytest.fixture
def df_a():
    return MagicMock(name="df_a")


@pytest.fixture
def df_b():
    return MagicMock(name="df_b")


@pytest.fixture
def df_c():
    return MagicMock(name="df_c")


PATCH_LOG = patch("odibi.transformers.validation.get_logging_context")


# ---------------------------------------------------------------------------
# CrossCheckParams validation
# ---------------------------------------------------------------------------


class TestCrossCheckParams:
    def test_valid_params(self):
        params = CrossCheckParams(
            type="row_count_diff",
            inputs=["node_a", "node_b"],
            threshold=0.05,
        )
        assert params.type == "row_count_diff"
        assert params.inputs == ["node_a", "node_b"]
        assert params.threshold == 0.05

    def test_threshold_defaults_to_zero(self):
        params = CrossCheckParams(type="schema_match", inputs=["a", "b"])
        assert params.threshold == 0.0

    def test_missing_type_raises(self):
        with pytest.raises(PydanticValidationError):
            CrossCheckParams(inputs=["a", "b"])

    def test_missing_inputs_raises(self):
        with pytest.raises(PydanticValidationError):
            CrossCheckParams(type="row_count_diff")


# ---------------------------------------------------------------------------
# row_count_diff
# ---------------------------------------------------------------------------


class TestRowCountDiff:
    @PATCH_LOG
    def test_matching_counts_pass(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            row_counts={df_a: 100, df_b: 100},
        )
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.0)
        result = cross_check(ctx, params)
        assert result is None

    @PATCH_LOG
    def test_within_threshold_passes(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            row_counts={df_a: 100, df_b: 104},
        )
        params = CrossCheckParams(
            type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.05
        )
        result = cross_check(ctx, params)
        assert result is None

    @PATCH_LOG
    def test_exceeds_threshold_raises(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            row_counts={df_a: 100, df_b: 120},
        )
        params = CrossCheckParams(
            type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.05
        )
        with pytest.raises(ValidationError) as exc_info:
            cross_check(ctx, params)
        assert "node_b" in str(exc_info.value)
        assert "node_a" in str(exc_info.value)

    @PATCH_LOG
    def test_exact_threshold_boundary_passes(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            row_counts={df_a: 100, df_b: 105},
        )
        params = CrossCheckParams(
            type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.05
        )
        result = cross_check(ctx, params)
        assert result is None

    @PATCH_LOG
    def test_base_zero_with_nonzero_comparison(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            row_counts={df_a: 0, df_b: 10},
        )
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.5)
        with pytest.raises(ValidationError):
            cross_check(ctx, params)

    @PATCH_LOG
    def test_both_zero_passes(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            row_counts={df_a: 0, df_b: 0},
        )
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.0)
        result = cross_check(ctx, params)
        assert result is None

    @PATCH_LOG
    def test_three_inputs_all_pass(self, _mock_log, df_a, df_b, df_c):
        ctx = _make_context(
            {"a": df_a, "b": df_b, "c": df_c},
            row_counts={df_a: 100, df_b: 100, df_c: 100},
        )
        params = CrossCheckParams(type="row_count_diff", inputs=["a", "b", "c"], threshold=0.0)
        result = cross_check(ctx, params)
        assert result is None

    @PATCH_LOG
    def test_three_inputs_one_fails(self, _mock_log, df_a, df_b, df_c):
        ctx = _make_context(
            {"a": df_a, "b": df_b, "c": df_c},
            row_counts={df_a: 100, df_b: 100, df_c: 200},
        )
        params = CrossCheckParams(type="row_count_diff", inputs=["a", "b", "c"], threshold=0.05)
        with pytest.raises(ValidationError) as exc_info:
            cross_check(ctx, params)
        assert "c" in str(exc_info.value)


# ---------------------------------------------------------------------------
# schema_match
# ---------------------------------------------------------------------------


class TestSchemaMatch:
    @PATCH_LOG
    def test_matching_schemas_pass(self, _mock_log, df_a, df_b):
        schema = {"id": "int64", "name": "string"}
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            schemas={df_a: schema, df_b: schema},
        )
        params = CrossCheckParams(type="schema_match", inputs=["node_a", "node_b"])
        result = cross_check(ctx, params)
        assert result is None

    @PATCH_LOG
    def test_mismatched_schemas_raises(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"node_a": df_a, "node_b": df_b},
            schemas={
                df_a: {"id": "int64", "name": "string"},
                df_b: {"id": "int64", "value": "float64"},
            },
        )
        params = CrossCheckParams(type="schema_match", inputs=["node_a", "node_b"])
        with pytest.raises(ValidationError) as exc_info:
            cross_check(ctx, params)
        err_msg = str(exc_info.value)
        assert "node_b" in err_msg
        assert "Missing" in err_msg or "Extra" in err_msg

    @PATCH_LOG
    def test_extra_column_raises(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"a": df_a, "b": df_b},
            schemas={
                df_a: {"id": "int64"},
                df_b: {"id": "int64", "extra": "string"},
            },
        )
        params = CrossCheckParams(type="schema_match", inputs=["a", "b"])
        with pytest.raises(ValidationError) as exc_info:
            cross_check(ctx, params)
        assert "Extra" in str(exc_info.value)

    @PATCH_LOG
    def test_missing_column_raises(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"a": df_a, "b": df_b},
            schemas={
                df_a: {"id": "int64", "name": "string"},
                df_b: {"id": "int64"},
            },
        )
        params = CrossCheckParams(type="schema_match", inputs=["a", "b"])
        with pytest.raises(ValidationError) as exc_info:
            cross_check(ctx, params)
        assert "Missing" in str(exc_info.value)

    @PATCH_LOG
    def test_three_inputs_schema_match(self, _mock_log, df_a, df_b, df_c):
        schema = {"id": "int64"}
        ctx = _make_context(
            {"a": df_a, "b": df_b, "c": df_c},
            schemas={df_a: schema, df_b: schema, df_c: schema},
        )
        params = CrossCheckParams(type="schema_match", inputs=["a", "b", "c"])
        result = cross_check(ctx, params)
        assert result is None

    @PATCH_LOG
    def test_type_change_raises(self, _mock_log, df_a, df_b):
        ctx = _make_context(
            {"a": df_a, "b": df_b},
            schemas={
                df_a: {"id": "int64"},
                df_b: {"id": "string"},
            },
        )
        params = CrossCheckParams(type="schema_match", inputs=["a", "b"])
        with pytest.raises(ValidationError):
            cross_check(ctx, params)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @PATCH_LOG
    def test_fewer_than_two_inputs_raises(self, _mock_log):
        ctx = _make_context({})
        params = CrossCheckParams(type="row_count_diff", inputs=["only_one"], threshold=0.0)
        with pytest.raises(ValueError, match="at least 2 inputs"):
            cross_check(ctx, params)

    @PATCH_LOG
    def test_empty_inputs_raises(self, _mock_log):
        ctx = _make_context({})
        params = CrossCheckParams(type="row_count_diff", inputs=[], threshold=0.0)
        with pytest.raises(ValueError, match="at least 2 inputs"):
            cross_check(ctx, params)

    @PATCH_LOG
    def test_input_not_found_raises(self, _mock_log, df_a):
        ctx = _make_context({"node_a": df_a})
        params = CrossCheckParams(
            type="row_count_diff", inputs=["node_a", "missing_node"], threshold=0.0
        )
        with pytest.raises(ValueError, match="not found in context"):
            cross_check(ctx, params)

    @PATCH_LOG
    def test_unknown_check_type_returns_none(self, _mock_log, df_a, df_b):
        ctx = _make_context({"a": df_a, "b": df_b})
        params = CrossCheckParams(type="unknown_type", inputs=["a", "b"], threshold=0.0)
        result = cross_check(ctx, params)
        assert result is None
