from odibi_mcp.contracts.enums import TruncatedReason


def test_truncated_reason_enum():
    assert TruncatedReason.ROW_LIMIT.value == "row_limit"
    assert TruncatedReason.COLUMN_LIMIT.value == "column_limit"
    assert TruncatedReason.BYTE_LIMIT.value == "byte_limit"
    assert TruncatedReason.CELL_LIMIT.value == "cell_limit"
    assert TruncatedReason.POLICY_MASKING.value == "policy_masking"
    assert TruncatedReason.SAMPLING_ONLY.value == "sampling_only"
    assert TruncatedReason.PAGINATION.value == "pagination"
