import json
from pathlib import Path
from odibi.state import StateManager

def test_state_manager_save(tmp_path):
    """Test state manager saves correctly (with or without lock)."""
    # Use tmp_path for isolation
    root = tmp_path / "project"
    root.mkdir()
    
    manager = StateManager(project_root=str(root))
    
    # Simulate pipeline result
    results = {
        "end_time": "2025-01-01T12:00:00",
        "node_results": {}
    }
    
    manager.save_pipeline_run("test_pipeline", results)
    
    state_file = root / ".odibi" / "state.json"
    assert state_file.exists()
    
    with open(state_file) as f:
        data = json.load(f)
        
    assert "test_pipeline" in data["pipelines"]
    assert data["pipelines"]["test_pipeline"]["last_run"] == "2025-01-01T12:00:00"

def test_state_manager_reload(tmp_path):
    """Test state manager reloads existing state."""
    root = tmp_path / "project"
    root.mkdir()
    state_dir = root / ".odibi"
    state_dir.mkdir()
    
    # Pre-populate state
    initial_state = {
        "pipelines": {
            "existing": {"last_run": "old"}
        }
    }
    with open(state_dir / "state.json", "w") as f:
        json.dump(initial_state, f)
        
    manager = StateManager(project_root=str(root))
    
    # Save new pipeline
    results = {"end_time": "new", "node_results": {}}
    manager.save_pipeline_run("new_pipeline", results)
    
    with open(state_dir / "state.json") as f:
        data = json.load(f)
        
    # Check both exist (no overwrite)
    assert "existing" in data["pipelines"]
    assert "new_pipeline" in data["pipelines"]
