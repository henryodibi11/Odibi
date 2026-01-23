from odibi_mcp.contracts.discovery import FileInfo, ListFilesResponse
from datetime import datetime


def test_file_info():
    info = FileInfo(
        logical_name="a.csv", size_bytes=123, modified=datetime(2023, 1, 2), is_directory=False
    )
    dump = info.model_dump()
    assert dump["logical_name"] == "a.csv"
    assert dump["size_bytes"] == 123
    assert dump["is_directory"] is False


def test_list_files_response():
    fi = FileInfo(logical_name="b.csv", size_bytes=555, modified=datetime.now())
    resp = ListFilesResponse(
        connection="main", path="/", files=[fi], next_token=None, truncated=False, total_count=1
    )
    dump = resp.model_dump()
    assert dump["connection"] == "main"
    assert dump["files"][0]["logical_name"] == "b.csv"
    assert dump["total_count"] == 1
