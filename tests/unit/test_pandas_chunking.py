import pandas as pd
from odibi.engine.pandas_engine import PandasEngine


class MockConnection:
    def get_path(self, path):
        return path

    def pandas_storage_options(self):
        return {}


def test_csv_chunking(tmp_path):
    """Test reading and writing CSV in chunks."""
    # 1. Setup
    engine = PandasEngine()
    connection = MockConnection()

    # Create source CSV
    source_file = tmp_path / "source.csv"
    df_source = pd.DataFrame({"a": range(100), "b": range(100, 200)})
    df_source.to_csv(source_file, index=False)

    # 2. Read with chunksize
    chunksize = 10
    result = engine.read(
        connection=connection, format="csv", path=str(source_file), options={"chunksize": chunksize}
    )

    # 3. Verify it is an iterator
    # TextFileReader is an iterator
    assert hasattr(result, "__next__")

    # 4. Iterate and verify
    chunks = list(result)
    assert len(chunks) == 10
    # Compare values only (ignore dtypes since Arrow backend changes them)
    pd.testing.assert_frame_equal(
        chunks[0], df_source.iloc[0:10].reset_index(drop=True), check_dtype=False
    )

    # 5. Write iterator
    # Create a generator again because previous one is exhausted
    result_iter = engine.read(
        connection=connection, format="csv", path=str(source_file), options={"chunksize": chunksize}
    )

    dest_file = tmp_path / "dest.csv"
    engine.write(
        df=result_iter, connection=connection, format="csv", path=str(dest_file), mode="overwrite"
    )

    # 6. Verify output
    assert dest_file.exists()
    df_dest = pd.read_csv(dest_file)
    # Compare values only
    pd.testing.assert_frame_equal(df_source, df_dest, check_dtype=False)


def test_json_chunking(tmp_path):
    """Test reading and writing JSON (lines) in chunks."""
    # 1. Setup
    engine = PandasEngine()
    connection = MockConnection()

    # Create source JSON (lines=True)
    source_file = tmp_path / "source.json"
    df_source = pd.DataFrame({"a": range(50), "b": range(50, 100)})
    df_source.to_json(source_file, orient="records", lines=True)

    # 2. Read with chunksize
    chunksize = 10
    result = engine.read(
        connection=connection,
        format="json",
        path=str(source_file),
        options={"chunksize": chunksize, "lines": True},
    )

    # 3. Verify it is an iterator
    assert hasattr(result, "__next__")

    # 4. Write iterator
    result_iter = engine.read(
        connection=connection,
        format="json",
        path=str(source_file),
        options={"chunksize": chunksize, "lines": True},
    )

    dest_file = tmp_path / "dest.json"
    engine.write(
        df=result_iter,
        connection=connection,
        format="json",
        path=str(dest_file),
        mode="overwrite",
        options={"lines": True},  # write needs lines=True for chunked writing typically?
        # Actually pandas to_json with orient='records' and lines=True appends nicely.
        # But default to_json writes a list which is invalid json if appended.
        # User must provide lines=True or we handle it?
        # PandasEngine.write for json:
        # df.to_json(full_path, orient="records", **merged_options)
        # It defaults orient="records".
        # If we append, we end up with [record, record][record, record] which is invalid JSON unless lines=True is used.
        # So for this test I must pass lines=True.
    )

    # 5. Verify output
    # Note: to_json(orient='records') produces [{}, {}]. Appending produces [{}, {}][{}, {}].
    # to_json(orient='records', lines=True) produces {}\n{}\n. Appending produces {}\n{}\n{}\n{}\n.

    # So we must use lines=True for chunked writing to JSON.

    # Read back
    df_dest = pd.read_json(dest_file, lines=True)
    # Compare values only
    pd.testing.assert_frame_equal(df_source, df_dest, check_dtype=False)
