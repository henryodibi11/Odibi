from odibi_mcp.contracts.selectors import RunById


def test_run_selector_by_id():
    selector = RunById(run_id="abc123")
    dump = selector.model_dump()
    assert dump["run_id"] == "abc123"
