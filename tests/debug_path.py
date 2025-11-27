import inspect

import odibi.engine.pandas_engine


def test_path():
    print("DEBUG PATH:", inspect.getfile(odibi.engine.pandas_engine))
    assert False
