import pytest
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from odibi.story.generator import StoryGenerator

class TestStoryRetention:
    
    @pytest.fixture
    def generator(self, tmp_path):
        """Create a generator pointing to tmp path."""
        return StoryGenerator(
            pipeline_name="test_pipeline",
            output_path=str(tmp_path),
            retention_days=7,
            retention_count=5
        )
        
    def test_cleanup_by_count(self, generator, tmp_path):
        """Test retention by count (keep latest N)."""
        # Create 10 dummy stories
        for i in range(10):
            p = tmp_path / f"test_pipeline_run{i}.md"
            p.touch()
            # Ensure different mtimes (newest last)
            # On some FS resolution is low, so sleep tiny bit or mock stats
            # Mocking stat is safer but let's just force mtime via os.utime
            t = time.time() + i * 10 
            import os
            os.utime(p, (t, t))
            
        # Run cleanup (limit is 5)
        generator.cleanup()
        
        files = list(tmp_path.glob("test_pipeline_*.md"))
        assert len(files) == 5
        
        # Ensure we kept the newest ones (run5 to run9)
        names = [p.name for p in files]
        assert "test_pipeline_run9.md" in names
        assert "test_pipeline_run0.md" not in names

    def test_cleanup_by_time(self, generator, tmp_path):
        """Test retention by time (keep newer than N days)."""
        # Config says 7 days
        
        # 1. Create recent file (should keep)
        recent = tmp_path / "test_pipeline_recent.md"
        recent.touch()
        
        # 2. Create old file (should delete)
        old = tmp_path / "test_pipeline_old.md"
        old.touch()
        
        # Modify mtime to be 8 days ago
        import os
        past = time.time() - (8 * 24 * 3600)
        os.utime(old, (past, past))
        
        generator.cleanup()
        
        assert recent.exists()
        assert not old.exists()

    def test_cleanup_integration(self, generator, tmp_path):
        """Test cleanup runs after generation."""
        # Pre-populate with many files
        for i in range(10):
            (tmp_path / f"test_pipeline_{i}.md").touch()
            
        # Run generation
        generator.generate({}, [], [], [], 0.0, "", "")
        
        # Should have cleaned up to retention_count (5) + 1 new one? 
        # The logic counts all files including new one.
        files = list(tmp_path.glob("test_pipeline_*.md"))
        assert len(files) <= 5
