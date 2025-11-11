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

    def test_pivot_with_string_group_by(self):
        """Should accept string for group_by parameter."""
        df = pd.DataFrame(
            {
                "ID": ["A", "A", "B"],
                "Category": ["X", "Y", "X"],
                "Value": [10, 20, 30],
            }
        )

        registry = get_registry()
        pivot_func = registry.get("pivot")
        result = pivot_func(df, group_by="ID", pivot_column="Category", value_column="Value")

        assert len(result) == 2
        assert "X" in result.columns

    def test_pivot_with_list_group_by(self):
        """Should accept list for group_by parameter."""
        df = pd.DataFrame(
            {
                "Plant": ["NKC", "NKC", "SFB"],
                "Asset": ["Dryer1", "Dryer1", "Dryer1"],
                "Metric": ["Temp", "Pressure", "Temp"],
                "Value": [100, 200, 110],
            }
        )

        registry = get_registry()
        pivot_func = registry.get("pivot")
        result = pivot_func(
            df, group_by=["Plant", "Asset"], pivot_column="Metric", value_column="Value"
        )

        assert "Temp" in result.columns
        assert "Plant" in result.columns
        assert "Asset" in result.columns

    def test_pivot_with_aggregation(self):
        """Should support different aggregation functions."""
        df = pd.DataFrame(
            {
                "ID": ["A", "A", "A"],
                "Category": ["X", "X", "Y"],
                "Value": [10, 20, 30],
            }
        )

        registry = get_registry()
        pivot_func = registry.get("pivot")

        # Sum aggregation
        result_sum = pivot_func(
            df, group_by="ID", pivot_column="Category", value_column="Value", agg_func="sum"
        )
        assert result_sum["X"].iloc[0] == 30  # 10 + 20

        # Mean aggregation
        result_mean = pivot_func(
            df, group_by="ID", pivot_column="Category", value_column="Value", agg_func="mean"
        )
        assert result_mean["X"].iloc[0] == 15  # (10 + 20) / 2

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

    def test_pivot_explain_with_context(self):
        """Should generate context-aware explanation."""
        registry = get_registry()
        pivot_func = registry.get("pivot")

        explanation = pivot_func.get_explanation(
            group_by=["Plant", "Asset"],
            pivot_column="Metric",
            value_column="Reading",
            agg_func="mean",
            plant="NKC",
            asset="Germ Dryer 1",
        )

        assert "**Purpose:**" in explanation
        assert "Metric" in explanation
        assert "mean" in explanation

    def test_pivot_metadata(self):
        """Should have correct metadata."""
        registry = get_registry()
        metadata = registry.get_metadata("pivot")

        assert metadata["category"] == "reshaping"
        assert "restructure" in metadata["tags"]
        assert "wide" in metadata["tags"]


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

    def test_unpivot_with_string_id_vars(self):
        """Should accept string for id_vars parameter."""
        df = pd.DataFrame({"ID": ["A"], "X": [10], "Y": [20]})

        registry = get_registry()
        unpivot_func = registry.get("unpivot")
        result = unpivot_func(df, id_vars="ID")

        assert len(result) == 2
        assert result["variable"].tolist() == ["X", "Y"]

    def test_unpivot_with_custom_names(self):
        """Should accept custom column names."""
        df = pd.DataFrame({"Plant": ["NKC"], "Temp": [100], "Pressure": [200]})

        registry = get_registry()
        unpivot_func = registry.get("unpivot")
        result = unpivot_func(df, id_vars="Plant", var_name="metric", value_name="reading")

        assert "metric" in result.columns
        assert "reading" in result.columns
        assert result["metric"].tolist() == ["Temp", "Pressure"]

    def test_unpivot_with_value_vars(self):
        """Should unpivot only specified value columns."""
        df = pd.DataFrame({"ID": ["A"], "Sales": [100], "Revenue": [200], "Cost": [50]})

        registry = get_registry()
        unpivot_func = registry.get("unpivot")
        result = unpivot_func(df, id_vars="ID", value_vars=["Sales", "Revenue"])

        assert len(result) == 2  # Only Sales and Revenue
        assert "Cost" not in result["variable"].values

    def test_unpivot_multiple_id_vars(self):
        """Should support multiple identifier columns."""
        df = pd.DataFrame(
            {
                "Plant": ["NKC", "SFB"],
                "Asset": ["Dryer1", "Dryer2"],
                "Temp": [100, 110],
                "Pressure": [200, 210],
            }
        )

        registry = get_registry()
        unpivot_func = registry.get("unpivot")
        result = unpivot_func(df, id_vars=["Plant", "Asset"])

        assert "Plant" in result.columns
        assert "Asset" in result.columns
        assert len(result) == 4  # 2 rows × 2 metrics

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

    def test_unpivot_explain_with_value_vars(self):
        """Should explain with specific value columns."""
        registry = get_registry()
        unpivot_func = registry.get("unpivot")

        explanation = unpivot_func.get_explanation(
            id_vars=["Plant"],
            value_vars=["Temp", "Pressure"],
            var_name="metric",
            value_name="reading",
        )

        assert "**Purpose:**" in explanation
        assert "metric" in explanation
        assert "Temp" in explanation
        assert "Pressure" in explanation

    def test_unpivot_metadata(self):
        """Should have correct metadata."""
        registry = get_registry()
        metadata = registry.get_metadata("unpivot")

        assert metadata["category"] == "reshaping"
        assert "restructure" in metadata["tags"]
        assert "long" in metadata["tags"]


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

    def test_join_left(self):
        """Should perform left join keeping all left records."""
        left_df = pd.DataFrame({"ID": ["A", "B"], "Value": [100, 150]})
        right_df = pd.DataFrame({"ID": ["A"], "Target": [120]})

        registry = get_registry()
        join_func = registry.get("join")
        result = join_func(left_df, right_df=right_df, on="ID", how="left")

        assert len(result) == 2  # Both A and B
        assert pd.isna(result.loc[result["ID"] == "B", "Target"].iloc[0])

    def test_join_right(self):
        """Should perform right join keeping all right records."""
        left_df = pd.DataFrame({"ID": ["A"], "Value": [100]})
        right_df = pd.DataFrame({"ID": ["A", "C"], "Target": [120, 110]})

        registry = get_registry()
        join_func = registry.get("join")
        result = join_func(left_df, right_df=right_df, on="ID", how="right")

        assert len(result) == 2  # Both A and C
        assert pd.isna(result.loc[result["ID"] == "C", "Value"].iloc[0])

    def test_join_outer(self):
        """Should perform outer join keeping all records."""
        left_df = pd.DataFrame({"ID": ["A", "B"], "Value": [100, 150]})
        right_df = pd.DataFrame({"ID": ["A", "C"], "Target": [120, 110]})

        registry = get_registry()
        join_func = registry.get("join")
        result = join_func(left_df, right_df=right_df, on="ID", how="outer")

        assert len(result) == 3  # A, B, C

    def test_join_different_column_names(self):
        """Should join on different column names."""
        left_df = pd.DataFrame({"LeftID": ["A", "B"], "Value": [100, 150]})
        right_df = pd.DataFrame({"RightID": ["A", "C"], "Target": [120, 110]})

        registry = get_registry()
        join_func = registry.get("join")
        result = join_func(
            left_df, right_df=right_df, left_on="LeftID", right_on="RightID", how="inner"
        )

        assert len(result) == 1  # Only A matches

    def test_join_multiple_keys(self):
        """Should join on multiple columns."""
        left_df = pd.DataFrame(
            {"Plant": ["NKC", "SFB"], "Asset": ["Dryer1", "Dryer1"], "Value": [100, 150]}
        )
        right_df = pd.DataFrame(
            {"Plant": ["NKC", "SFB"], "Asset": ["Dryer1", "Dryer2"], "Target": [120, 110]}
        )

        registry = get_registry()
        join_func = registry.get("join")
        result = join_func(left_df, right_df=right_df, on=["Plant", "Asset"], how="inner")

        assert len(result) == 1  # Only NKC Dryer1 matches

    def test_join_explain(self):
        """Should generate explanation for join."""
        registry = get_registry()
        join_func = registry.get("join")

        explanation = join_func.get_explanation(on="ID", how="inner")

        assert "**Purpose:**" in explanation
        assert "inner" in explanation
        assert "ID" in explanation

    def test_join_explain_left_join(self):
        """Should explain left join correctly."""
        registry = get_registry()
        join_func = registry.get("join")

        explanation = join_func.get_explanation(on="ID", how="left")

        assert "**Purpose:**" in explanation
        assert "left" in explanation
        assert "Keep all records from left dataset" in explanation

    def test_join_explain_different_keys(self):
        """Should explain join with different column names."""
        registry = get_registry()
        join_func = registry.get("join")

        explanation = join_func.get_explanation(left_on="LeftID", right_on="RightID", how="inner")

        assert "**Purpose:**" in explanation
        assert "LeftID" in explanation
        assert "RightID" in explanation

    def test_join_explain_multiple_keys(self):
        """Should explain join on multiple columns."""
        registry = get_registry()
        join_func = registry.get("join")

        explanation = join_func.get_explanation(on=["Plant", "Asset"], how="outer")

        assert "**Purpose:**" in explanation
        assert "Plant" in explanation
        assert "Asset" in explanation

    def test_join_metadata(self):
        """Should have correct metadata."""
        registry = get_registry()
        metadata = registry.get_metadata("join")

        assert metadata["category"] == "combining"
        assert "merge" in metadata["tags"]
        assert "combine" in metadata["tags"]


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

    def test_sql_aggregation(self):
        """Should execute GROUP BY query."""
        df = pd.DataFrame(
            {
                "Category": ["A", "A", "B", "B"],
                "Value": [100, 150, 80, 90],
            }
        )

        registry = get_registry()
        sql_func = registry.get("sql")

        try:
            result = sql_func(
                df, query="SELECT Category, SUM(Value) as Total FROM df GROUP BY Category"
            )
            assert len(result) == 2
            assert "Total" in result.columns
            assert result.loc[result["Category"] == "A", "Total"].iloc[0] == 250
        except ImportError:
            pytest.skip("DuckDB not installed")

    def test_sql_filtering(self):
        """Should filter data with WHERE clause."""
        df = pd.DataFrame(
            {
                "Plant": ["NKC", "SFB", "NKC"],
                "Value": [100, 80, 120],
            }
        )

        registry = get_registry()
        sql_func = registry.get("sql")

        try:
            result = sql_func(df, query="SELECT * FROM df WHERE Plant = 'NKC'")
            assert len(result) == 2
            assert all(result["Plant"] == "NKC")
        except ImportError:
            pytest.skip("DuckDB not installed")

    def test_sql_computed_columns(self):
        """Should create computed columns."""
        df = pd.DataFrame(
            {
                "Fuel": [100, 150],
                "Output": [200, 250],
            }
        )

        registry = get_registry()
        sql_func = registry.get("sql")

        try:
            result = sql_func(df, query="SELECT *, Output / Fuel as Efficiency FROM df")
            assert "Efficiency" in result.columns
            assert result["Efficiency"].iloc[0] == 2.0
        except ImportError:
            pytest.skip("DuckDB not installed")

    def test_sql_ordering(self):
        """Should order results."""
        df = pd.DataFrame(
            {
                "ID": ["C", "A", "B"],
                "Value": [90, 100, 80],
            }
        )

        registry = get_registry()
        sql_func = registry.get("sql")

        try:
            result = sql_func(df, query="SELECT * FROM df ORDER BY Value DESC")
            assert result["ID"].tolist() == ["A", "C", "B"]
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

    def test_sql_explain_aggregation(self):
        """Should explain aggregation queries."""
        registry = get_registry()
        sql_func = registry.get("sql")

        explanation = sql_func.get_explanation(
            query="SELECT Category, COUNT(*) FROM df GROUP BY Category",
        )

        assert "**Purpose:**" in explanation
        assert "GROUP BY" in explanation or "aggregate" in explanation.lower()

    def test_sql_explain_long_query(self):
        """Should truncate long queries in explanation."""
        registry = get_registry()
        sql_func = registry.get("sql")

        long_query = "SELECT " + ", ".join([f"col{i}" for i in range(50)]) + " FROM df"
        explanation = sql_func.get_explanation(query=long_query)

        assert "**Purpose:**" in explanation
        assert "..." in explanation  # Truncated

    def test_sql_metadata(self):
        """Should have correct metadata."""
        registry = get_registry()
        metadata = registry.get_metadata("sql")

        assert metadata["category"] == "querying"
        assert "query" in metadata["tags"]
        assert "filter" in metadata["tags"]


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
