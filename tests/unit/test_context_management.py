from odibi.context import Context


class DummyContext(Context):
    def __init__(self):
        self.data = {}

    def clear(self):
        self.data.clear()

    def get(self, key):
        return self.data.get(key)

    def get_metadata(self, key):
        return {}

    def has(self, key):
        return key in self.data

    def list_names(self):
        return list(self.data.keys())

    def register(self, key, value):
        self.data[key] = value


def test_register_and_get():
    ctx = DummyContext()
    ctx.register("foo", "bar")
    assert ctx.get("foo") == "bar"


def test_get_non_existent_key():
    ctx = DummyContext()
    assert ctx.get("nonexistent") is None
