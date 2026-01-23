from odibi_mcp.contracts.schema import ColumnSpec, SchemaResponse, ColumnChange, SchemaChange
from datetime import datetime


def test_column_spec():
    col = ColumnSpec(
        name="id", dtype="int", nullable=False, description="ID column", semantic_type="key"
    )
    dump = col.model_dump()
    assert dump["name"] == "id"
    assert dump["dtype"] == "int"
    assert dump["nullable"] is False
    assert dump["semantic_type"] == "key"


def test_schema_response():
    col = ColumnSpec(name="amount", dtype="float")
    resp = SchemaResponse(columns=[col], row_count=10, partition_columns=["date"])
    dump = resp.model_dump()
    assert dump["row_count"] == 10
    assert dump["partition_columns"] == ["date"]
    assert dump["columns"][0]["name"] == "amount"


def test_column_change_and_schema_change():
    ch = ColumnChange(name="foo", old_type="int", new_type="float")
    sch = SchemaChange(
        run_id="abc", timestamp=datetime.now(), added=[], removed=[], type_changed=[ch]
    )
    dump = sch.model_dump()
    assert dump["run_id"] == "abc"
    assert isinstance(dump["type_changed"], list)
