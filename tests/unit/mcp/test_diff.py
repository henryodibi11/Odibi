from odibi_mcp.contracts.diff import DiffSummary
from odibi_mcp.contracts.schema import SchemaChange


def test_diff_summary():
    sc = SchemaChange(
        run_id="aaa", timestamp="2023-01-01T00:00:00", added=[], removed=[], type_changed=[]
    )
    diff = DiffSummary(
        schema_diff=sc,
        row_count_diff=3,
        null_count_changes={"foo": 1},
        distinct_count_changes={"foo": 2},
    )
    dump = diff.model_dump()
    assert dump["row_count_diff"] == 3
    assert dump["schema_diff"]["run_id"] == "aaa"
    assert dump["null_count_changes"]["foo"] == 1
