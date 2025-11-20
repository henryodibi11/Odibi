import pytest
import pandas as pd
from odibi.engine.pandas_engine import PandasEngine


class TestIdempotencyPandas:
    @pytest.fixture
    def engine(self):
        return PandasEngine()

    @pytest.fixture
    def connection(self, tmp_path):
        class MockConn:
            def get_path(self, p):
                return str(tmp_path / p)

            def pandas_storage_options(self):
                return {}

        return MockConn()

    @pytest.fixture
    def data(self):
        return pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"], "ts": [100, 100, 100]})

    def test_delta_upsert(self, engine, connection, data):
        """Test Delta Lake upsert (merge)."""
        pytest.importorskip("deltalake")

        # 1. Initial Write
        engine.write(data, connection, "delta", path="delta_table", mode="overwrite")

        # 2. Update (update id=2, insert id=4)
        update_data = pd.DataFrame({"id": [2, 4], "val": ["b_updated", "d"], "ts": [200, 200]})

        # 3. Upsert
        engine.write(
            update_data,
            connection,
            "delta",
            path="delta_table",
            mode="upsert",
            options={"keys": ["id"]},
        )

        # 4. Verify
        result = engine.read(connection, "delta", path="delta_table")
        result = result.sort_values("id").reset_index(drop=True)

        assert len(result) == 4  # 1, 2, 3, 4
        assert result.loc[result["id"] == 2, "val"].iloc[0] == "b_updated"
        assert result.loc[result["id"] == 4, "val"].iloc[0] == "d"
        assert result.loc[result["id"] == 1, "val"].iloc[0] == "a"

    def test_delta_append_once(self, engine, connection, data):
        """Test Delta Lake append_once (dedup insert)."""
        pytest.importorskip("deltalake")

        # 1. Initial Write
        engine.write(data, connection, "delta", path="delta_dedup", mode="overwrite")

        # 2. New Data (overlap id=2, new id=4)
        # Note: id=2 should be IGNORED even if val is different
        new_data = pd.DataFrame({"id": [2, 4], "val": ["b_ignored", "d"], "ts": [200, 200]})

        # 3. Append Once
        engine.write(
            new_data,
            connection,
            "delta",
            path="delta_dedup",
            mode="append_once",
            options={"keys": ["id"]},
        )

        # 4. Verify
        result = engine.read(connection, "delta", path="delta_dedup")
        result = result.sort_values("id").reset_index(drop=True)

        assert len(result) == 4
        # id=2 should match ORIGINAL value 'b'
        assert result.loc[result["id"] == 2, "val"].iloc[0] == "b"
        # id=4 should be inserted
        assert result.loc[result["id"] == 4, "val"].iloc[0] == "d"

    def test_parquet_append_once(self, engine, connection, data):
        """Test Parquet append_once (simulated)."""
        # 1. Initial Write
        engine.write(data, connection, "parquet", path="parquet_dedup.parquet", mode="overwrite")

        # 2. New Data (overlap id=2, new id=4)
        new_data = pd.DataFrame({"id": [2, 4], "val": ["b_ignored", "d"], "ts": [200, 200]})

        # 3. Append Once
        engine.write(
            new_data,
            connection,
            "parquet",
            path="parquet_dedup.parquet",
            mode="append_once",
            options={"keys": ["id"]},
        )

        # 4. Verify
        result = engine.read(connection, "parquet", path="parquet_dedup.parquet")
        result = result.sort_values("id").reset_index(drop=True)

        assert len(result) == 4
        # id=2 should match ORIGINAL value 'b'
        assert result.loc[result["id"] == 2, "val"].iloc[0] == "b"
        assert result.loc[result["id"] == 4, "val"].iloc[0] == "d"

    def test_parquet_upsert(self, engine, connection, data):
        """Test Parquet upsert (simulated - full rewrite)."""
        # 1. Initial Write
        engine.write(data, connection, "parquet", path="parquet_upsert.parquet", mode="overwrite")

        # 2. Update Data (update id=2, new id=4)
        update_data = pd.DataFrame({"id": [2, 4], "val": ["b_updated", "d"], "ts": [200, 200]})

        # 3. Upsert
        engine.write(
            update_data,
            connection,
            "parquet",
            path="parquet_upsert.parquet",
            mode="upsert",
            options={"keys": ["id"]},
        )

        # 4. Verify
        result = engine.read(connection, "parquet", path="parquet_upsert.parquet")
        result = result.sort_values("id").reset_index(drop=True)

        assert len(result) == 4
        assert result.loc[result["id"] == 2, "val"].iloc[0] == "b_updated"
        assert result.loc[result["id"] == 4, "val"].iloc[0] == "d"
        assert result.loc[result["id"] == 1, "val"].iloc[0] == "a"
