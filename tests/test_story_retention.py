import pytest
import time
from odibi.story.generator import StoryGenerator


class TestStoryRetention:
    @pytest.fixture
    def generator(self, tmp_path):
        """Create a generator pointing to tmp path."""
        return StoryGenerator(
            pipeline_name="test_pipeline",
            output_path=str(tmp_path),
            retention_days=7,
            retention_count=5,
        )

    def test_cleanup_by_count(self, generator, tmp_path):
        """Test retention by count (keep latest N)."""
        # Create 10 dummy stories (mix of html and json)
        for i in range(10):
            p_html = tmp_path / f"test_pipeline_run{i}.html"
            p_html.touch()

            p_json = tmp_path / f"test_pipeline_run{i}.json"
            p_json.touch()

            # Ensure different mtimes (newest last)
            t = time.time() + i * 10
            import os

            os.utime(p_html, (t, t))
            os.utime(p_json, (t, t))

        # Run cleanup (limit is 5)
        generator.cleanup()

        files_html = list(tmp_path.glob("test_pipeline_*.html"))
        files_json = list(tmp_path.glob("test_pipeline_*.json"))

        assert len(files_html) == 5
        assert len(files_json) == 5

        # Ensure we kept the newest ones (run5 to run9)
        names = [p.name for p in files_html]
        assert "test_pipeline_run9.html" in names
        assert "test_pipeline_run0.html" not in names

    def test_cleanup_by_time(self, generator, tmp_path):
        """Test retention by time (keep newer than N days)."""
        # Config says 7 days

        # 1. Create recent file (should keep)
        recent_html = tmp_path / "test_pipeline_recent.html"
        recent_html.touch()

        # 2. Create old file (should delete)
        old_html = tmp_path / "test_pipeline_old.html"
        old_html.touch()

        # Modify mtime to be 8 days ago
        import os

        past = time.time() - (8 * 24 * 3600)
        os.utime(old_html, (past, past))

        generator.cleanup()

        assert recent_html.exists()
        assert not old_html.exists()

    def test_cleanup_integration(self, generator, tmp_path):
        """Test cleanup runs after generation."""
        # Pre-populate with many files
        for i in range(10):
            (tmp_path / f"test_pipeline_{i}.html").touch()

        # Run generation
        generator.generate({}, [], [], [], 0.0, "", "")

        # Should have cleaned up to retention_count (5) + 1 new one?
        # The logic counts all files including new one.
        files = list(tmp_path.glob("test_pipeline_*.html"))
        assert len(files) <= 5 + 1  # +1 for the newly generated file
