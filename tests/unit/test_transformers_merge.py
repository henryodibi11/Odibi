import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.context import PandasContext, SparkContext
from odibi.transformers.merge_transformer import merge


class TestMergeTransformerPandas:
    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def temp_dir(self, tmp_path):
        return str(tmp_path)

    def test_pandas_merge_upsert(self, context, temp_dir):
        # Setup Target
        target_path = os.path.join(temp_dir, "target.parquet")
        target_df = pd.DataFrame(
            {"id": [1, 2], "value": ["old1", "old2"], "untouched": ["keep1", "keep2"]}
        )
        target_df.to_parquet(target_path)

        # Source (Update 1, Insert 3)
        source_df = pd.DataFrame(
            {
                "id": [2, 3],
                "value": ["new2", "new3"],
                "untouched": [
                    "ignored2",
                    "ignored3",
                ],  # Should be ignored in update depending on implementation?
                # Wait, pandas update updates all common columns.
            }
        )

        # Execute Merge
        params = {"target": target_path, "keys": ["id"], "strategy": "upsert"}

        merge(context, source_df, **params)

        # Verify
        result = pd.read_parquet(target_path)

        # Sort by id
        result = result.sort_values("id").reset_index(drop=True)

        # Row 1: Untouched
        assert result.loc[0, "id"] == 1
        assert result.loc[0, "value"] == "old1"

        # Row 2: Updated
        assert result.loc[1, "id"] == 2
        assert result.loc[1, "value"] == "new2"

        # Row 3: Inserted
        assert result.loc[2, "id"] == 3
        assert result.loc[2, "value"] == "new3"

    def test_pandas_merge_append_only(self, context, temp_dir):
        # Setup Target
        target_path = os.path.join(temp_dir, "target_append.parquet")
        target_df = pd.DataFrame({"id": [1, 2], "value": ["old1", "old2"]})
        target_df.to_parquet(target_path)

        # Source (Try Update 2, Insert 3)
        source_df = pd.DataFrame({"id": [2, 3], "value": ["new2", "new3"]})

        # Execute Merge
        params = {"target": target_path, "keys": ["id"], "strategy": "append_only"}

        merge(context, source_df, **params)

        # Verify
        result = pd.read_parquet(target_path)
        result = result.sort_values("id").reset_index(drop=True)

        # Row 2: Should NOT be updated
        assert result.loc[1, "id"] == 2
        assert result.loc[1, "value"] == "old2"

        # Row 3: Inserted
        assert result.loc[2, "id"] == 3
        assert result.loc[2, "value"] == "new3"

    def test_pandas_merge_delete_match(self, context, temp_dir):
        # Setup Target
        target_path = os.path.join(temp_dir, "target_delete.parquet")
        target_df = pd.DataFrame({"id": [1, 2, 3], "value": ["keep", "delete", "keep"]})
        target_df.to_parquet(target_path)

        # Source (Match 2)
        source_df = pd.DataFrame({"id": [2], "value": ["any"]})

        # Execute Merge
        params = {"target": target_path, "keys": ["id"], "strategy": "delete_match"}

        merge(context, source_df, **params)

        # Verify
        result = pd.read_parquet(target_path)
        result = result.sort_values("id").reset_index(drop=True)

        assert len(result) == 2
        assert result.loc[0, "id"] == 1
        assert result.loc[1, "id"] == 3

    def test_pandas_audit_cols(self, context, temp_dir):
        target_path = os.path.join(temp_dir, "audit.parquet")

        # Initial Write (Target empty)
        source_df = pd.DataFrame({"id": [1], "val": ["a"]})

        params = {
            "target": target_path,
            "keys": ["id"],
            "strategy": "upsert",
            "audit_cols": {"created_col": "created_at", "updated_col": "updated_at"},
        }

        merge(context, source_df, **params)

        result = pd.read_parquet(target_path)
        assert "created_at" in result.columns
        assert "updated_at" in result.columns
        assert pd.notnull(result.loc[0, "created_at"])
        created_ts = result.loc[0, "created_at"]

        # Second Write (Update)
        # Sleep a bit to ensure timestamp diff?
        # Pandas timestamp precision is high, should differ slightly.
        import time

        time.sleep(0.1)

        source_df_2 = pd.DataFrame({"id": [1], "val": ["b"]})

        merge(context, source_df_2, **params)

        result_2 = pd.read_parquet(target_path)
        assert result_2.loc[0, "val"] == "b"
        assert result_2.loc[0, "created_at"] == created_ts  # Should not change
        assert result_2.loc[0, "updated_at"] > created_ts  # Should update


class TestMergeTransformerSpark:
    @pytest.fixture
    def mock_spark_context(self):
        spark = MagicMock()
        context = SparkContext(spark)
        return context

    @patch("odibi.transformers.merge_transformer.DeltaTable")
    def test_spark_merge_upsert(self, MockDeltaTable, mock_spark_context):
        # Setup
        mock_delta_table = MagicMock()
        MockDeltaTable.forName.return_value = mock_delta_table
        MockDeltaTable.isDeltaTable.return_value = True

        # Mock Merger
        mock_merger = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merger
        mock_merger.whenMatchedUpdate.return_value = mock_merger
        mock_merger.whenNotMatchedInsertAll.return_value = mock_merger

        source_df = MagicMock()
        source_df.columns = ["id", "val"]
        source_df.isStreaming = False

        # Execute
        params = {"target": "silver.table", "keys": ["id"], "strategy": "upsert"}

        merge(mock_spark_context, source_df, **params)

        # Verify
        MockDeltaTable.forName.assert_called_with(mock_spark_context.spark, "silver.table")
        mock_delta_table.alias.assert_called_with("target")
        # Check merge condition
        # Cannot easily check arguments of chained calls without complex assert,
        # but verify execute() called
        mock_merger.execute.assert_called_once()
        mock_merger.whenMatchedUpdate.assert_called_once()
        mock_merger.whenNotMatchedInsertAll.assert_called_once()

    @patch("odibi.transformers.merge_transformer.DeltaTable")
    def test_spark_merge_append_only(self, MockDeltaTable, mock_spark_context):
        # Setup
        mock_delta_table = MagicMock()
        MockDeltaTable.forName.return_value = mock_delta_table
        MockDeltaTable.isDeltaTable.return_value = True

        mock_merger = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merger
        mock_merger.whenNotMatchedInsertAll.return_value = mock_merger

        source_df = MagicMock()
        source_df.isStreaming = False

        # Execute
        params = {"target": "silver.table", "keys": ["id"], "strategy": "append_only"}

        merge(mock_spark_context, source_df, **params)

        # Verify
        mock_merger.execute.assert_called_once()
        mock_merger.whenMatchedUpdate.assert_not_called()  # Should not update
        mock_merger.whenNotMatchedInsertAll.assert_called_once()

    @patch("odibi.transformers.merge_transformer.DeltaTable")
    def test_spark_merge_delete_match(self, MockDeltaTable, mock_spark_context):
        # Setup
        mock_delta_table = MagicMock()
        MockDeltaTable.forName.return_value = mock_delta_table
        MockDeltaTable.isDeltaTable.return_value = True

        mock_merger = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merger
        mock_merger.whenMatchedDelete.return_value = mock_merger

        source_df = MagicMock()
        source_df.isStreaming = False

        # Execute
        params = {"target": "silver.table", "keys": ["id"], "strategy": "delete_match"}

        merge(mock_spark_context, source_df, **params)

        # Verify
        mock_merger.execute.assert_called_once()
        mock_merger.whenMatchedDelete.assert_called_once()
