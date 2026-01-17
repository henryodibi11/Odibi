import pytest
from odibi.state import StateManager, StateBackend


# Fake backend to simulate StateBackend behavior for testing.
class FakeBackend(StateBackend):
    def __init__(self, state=None):
        self.state = state or {"pipelines": {}, "hwm": {}}
        self.saved_pipeline_run_data = None
        self.last_hwm_value = {}
        self.last_hwm_batch = []
        self.get_last_run_info_return = None
        self.get_last_run_status_return = None

    def load_state(self) -> dict:
        return self.state

    def save_pipeline_run(self, pipeline_name: str, pipeline_data: dict) -> None:
        self.saved_pipeline_run_data = (pipeline_name, pipeline_data)
        self.state.setdefault("pipelines", {})[pipeline_name] = pipeline_data

    def get_last_run_info(self, pipeline_name: str, node_name: str) -> dict:
        return self.get_last_run_info_return

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> bool:
        return self.get_last_run_status_return

    def get_hwm(self, key: str):
        return self.state.get("hwm", {}).get(key)

    def set_hwm(self, key: str, value: any) -> None:
        self.state.setdefault("hwm", {})[key] = value
        self.last_hwm_value[key] = value

    def set_hwm_batch(self, updates: list) -> None:
        self.state.setdefault("hwm", {})
        for update in updates:
            key = update["key"]
            value = update["value"]
            self.state["hwm"][key] = value
        self.last_hwm_batch = updates


# Dummy classes to simulate result objects with optional to_dict and node_results attributes.
class DummyNodeResult:
    def __init__(self, success, metadata):
        self.success = success
        self.metadata = metadata


class DummyResultWithToDict:
    def __init__(self, data, node_results=None):
        self._data = data
        self.node_results = node_results

    def to_dict(self):
        return self._data


# Pytest fixtures for setting up a fake backend and StateManager instance.
@pytest.fixture
def fake_backend():
    return FakeBackend()


@pytest.fixture
def state_manager(fake_backend):
    return StateManager(backend=fake_backend)


def test_init_no_backend():
    with pytest.raises(ValueError):
        StateManager()


def test_save_pipeline_run_with_to_dict(state_manager, fake_backend):
    # Create a dummy result object with a to_dict method and node_results attribute.
    dummy_node_result = DummyNodeResult(
        success=True, metadata={"timestamp": "2023-01-01T12:30:00", "info": "node info"}
    )
    dummy_result = DummyResultWithToDict(
        data={"end_time": "2023-01-01T12:00:00"}, node_results={"nodeA": dummy_node_result}
    )
    state_manager.save_pipeline_run("pipeline1", dummy_result)
    expected_pipeline_data = {
        "last_run": "2023-01-01T12:00:00",
        "nodes": {
            "nodeA": {
                "success": True,
                "timestamp": "2023-01-01T12:30:00",
                "metadata": {"timestamp": "2023-01-01T12:30:00", "info": "node info"},
            }
        },
    }
    assert fake_backend.saved_pipeline_run_data is not None
    pipeline_name, pipeline_data = fake_backend.saved_pipeline_run_data
    assert pipeline_name == "pipeline1"
    assert pipeline_data == expected_pipeline_data
    # The StateManager state should be refreshed from the backend.
    assert state_manager.state == fake_backend.state


def test_save_pipeline_run_without_to_dict(state_manager, fake_backend):
    # Pass a plain dictionary as result (without to_dict or node_results).
    result_dict = {"end_time": "2023-02-01T10:00:00"}
    state_manager.save_pipeline_run("pipeline2", result_dict)
    expected_pipeline_data = {"last_run": "2023-02-01T10:00:00", "nodes": {}}
    assert fake_backend.saved_pipeline_run_data is not None
    pipeline_name, pipeline_data = fake_backend.saved_pipeline_run_data
    assert pipeline_name == "pipeline2"
    assert pipeline_data == expected_pipeline_data


def test_get_last_run_info(state_manager, fake_backend):
    dummy_info = {"success": True, "metadata": {"data": "info"}}
    fake_backend.get_last_run_info_return = dummy_info
    info = state_manager.get_last_run_info("pipelineX", "nodeX")
    assert info == dummy_info


def test_get_last_run_status(state_manager, fake_backend):
    fake_backend.get_last_run_status_return = False
    status = state_manager.get_last_run_status("pipelineX", "nodeX")
    assert status is False


def test_get_hwm(state_manager, fake_backend):
    fake_backend.state.setdefault("hwm", {})["key1"] = 123
    value = state_manager.get_hwm("key1")
    assert value == 123


def test_set_hwm(state_manager, fake_backend):
    state_manager.set_hwm("key2", 456)
    assert fake_backend.last_hwm_value.get("key2") == 456
    assert fake_backend.state["hwm"]["key2"] == 456


def test_set_hwm_batch(state_manager, fake_backend):
    updates = [{"key": "batch1", "value": "val1"}, {"key": "batch2", "value": "val2"}]
    state_manager.set_hwm_batch(updates)
    assert fake_backend.last_hwm_batch == updates
    assert fake_backend.state["hwm"]["batch1"] == "val1"
    assert fake_backend.state["hwm"]["batch2"] == "val2"
