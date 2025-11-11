"""Tests for built-in operations."""

import pandas as pd
import pytest

from odibi.transformations import get_registry


class TestPivotOperation:
    """Tests for pivot operation."""

    @classmethod
    def setup_class(cls):
        """Import operations once per class."""
        import odibi.operations.pivot  # noqa: F401

    def test_pivot_basic(self):
        """Should pivot DataFrame from long to wide format."""
        df = pd.DataFrame(
            {
                "ID": ["A", "A", "B", "B"],
                "Category": ["Sales", "Revenue", "Sales", "Revenue"],
                "Value": [100, 200, 150, 250],
            }
        )

        registry = get_registry()
        pivot_func = registry.get("pivot")
        result = pivot_func(df, group_by="ID", pivot_column="Category", value_column="Value")

        assert "Sales" in result.columns
        assert "Revenue" in result.columns
        assert len(result) == 2  # Two IDs

    def test_pivot_explain(self):
        """Should generate explanation for pivot."""
        registry = get_registry()
        pivot_func = registry.get("pivot")

        explanation = pivot_func.get_explanation(
            group_by=["ID"],
            pivot_column="Category",
            value_column="Value",
        )

        assert "**Purpose:**" in explanation
        assert "wide-format" in explanation
        assert "Category" in explanation


class TestUnpivotOperation:
    """Tests for unpivot operation."""

    @classmethod
    def setup_class(cls):
        """Import operations once per class."""
        import odibi.operations.unpivot  # noqa: F401

    def test_unpivot_basic(self):
        """Should unpivot DataFrame from wide to long format."""
        df = pd.DataFrame({"ID": ["A", "B"], "Sales": [100, 150], "Revenue": [200, 250]})

        registry = get_registry()
        unpivot_func = registry.get("unpivot")
        result = unpivot_func(df, id_vars="ID")

        assert "variable" in result.columns
        assert "value" in result.columns
        assert len(result) == 4  # 2 IDs × 2 metrics

    def test_unpivot_explain(self):
        """Should generate explanation for unpivot."""
        registry = get_registry()
        unpivot_func = registry.get("unpivot")

        explanation = unpivot_func.get_explanation(
            id_vars=["ID"], var_name="metric", value_name="value"
        )

        assert "**Purpose:**" in explanation
        assert "long-format" in explanation
        assert "ID" in explanation


class TestJoinOperation:
    """Tests for join operation."""

    @classmethod
    def setup_class(cls):
        """Import operations once per class."""
        import odibi.operations.join  # noqa: F401

    def test_join_inner(self):
        """Should perform inner join."""
        left_df = pd.DataFrame({"ID": ["A", "B"], "Value": [100, 150]})
        right_df = pd.DataFrame({"ID": ["A", "C"], "Target": [120, 110]})

        registry = get_registry()
        join_func = registry.get("join")
        result = join_func(left_df, right_df=right_df, on="ID", how="inner")

        assert len(result) == 1  # Only A matches
        assert "Value" in result.columns
        assert "Target" in result.columns

    def test_join_explain(self):
        """Should generate explanation for join."""
        registry = get_registry()
        join_func = registry.get("join")

        explanation = join_func.get_explanation(on="ID", how="inner")

        assert "**Purpose:**" in explanation
        assert "inner" in explanation
        assert "ID" in explanation


class TestSqlOperation:
    """Tests for SQL operation."""

    @classmethod
    def setup_class(cls):
        """Import operations once per class."""
        import odibi.operations.sql  # noqa: F401

    def test_sql_select(self):
        """Should execute SELECT query."""
        df = pd.DataFrame({"ID": ["A", "B", "C"], "Value": [100, 80, 90]})

        registry = get_registry()
        sql_func = registry.get("sql")

        try:
            result = sql_func(df, query="SELECT * FROM df WHERE Value > 85")
            assert len(result) == 2  # A and C
            assert all(result["Value"] > 85)
        except ImportError:
            pytest.skip("DuckDB not installed")

    def test_sql_explain(self):
        """Should generate explanation for SQL."""
        registry = get_registry()
        sql_func = registry.get("sql")

        explanation = sql_func.get_explanation(
            query="SELECT * FROM df WHERE Value > 100",
            table_name="df",
        )

        assert "**Purpose:**" in explanation
        assert "SQL" in explanation or "query" in explanation.lower()


class TestOperationMetadata:
    """Tests for operation registration metadata."""

    @classmethod
    def setup_class(cls):
        """Import all operations once per class."""
        import odibi.operations.pivot  # noqa: F401
        import odibi.operations.unpivot  # noqa: F401
        import odibi.operations.join  # noqa: F401
        import odibi.operations.sql  # noqa: F401

    def test_all_operations_registered(self):
        """Should register all four operations."""
        registry = get_registry()
        operations = registry.list_all()

        assert "pivot" in operations
        assert "unpivot" in operations
        assert "join" in operations
        assert "sql" in operations

    def test_operations_have_metadata(self):
        """Should store metadata for all operations."""
        registry = get_registry()

        for op_name in ["pivot", "unpivot", "join", "sql"]:
            metadata = registry.get_metadata(op_name)
            assert metadata is not None
            assert "category" in metadata
            assert "tags" in metadata

    def test_operations_have_explanations(self):
        """Should have explanation methods for all operations."""
        registry = get_registry()

        for op_name in ["pivot", "unpivot", "join", "sql"]:
            func = registry.get(op_name)
            assert hasattr(func, "get_explanation")
