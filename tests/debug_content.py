import inspect

import odibi.engine.pandas_engine


def test_file_content():
    path = inspect.getfile(odibi.engine.pandas_engine)
    print(f"DEBUG PATH: {path}")
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        # Check line 923 approx
        for i, line in enumerate(lines):
            if "LazyDataset" in line:
                print(f"LINE {i + 1}: {line.rstrip()}")
    assert False
