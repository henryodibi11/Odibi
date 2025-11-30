import pytest

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
        # We update the test to use the new directory structure:
        # {pipeline}/{date}/run_{time}.html

        import datetime

        today = datetime.date.today().isoformat()

        pipeline_dir = tmp_path / "test_pipeline" / today
        pipeline_dir.mkdir(parents=True, exist_ok=True)

        for i in range(10):
            # Format: run_HH-MM-SS.html
            # We fake the time to ensure sorting
            time_str = f"10-00-{i:02d}"

            p_html = pipeline_dir / f"run_{time_str}.html"
            p_html.touch()

            p_json = pipeline_dir / f"run_{time_str}.json"
            p_json.touch()

        # Run cleanup (limit is 5)
        generator.cleanup()

        files_html = list(pipeline_dir.glob("*.html"))
        files_json = list(pipeline_dir.glob("*.json"))

        assert len(files_html) == 5
        assert len(files_json) == 5

        # Ensure we kept the newest ones (run 5 to 9)
        names = [p.name for p in files_html]
        assert "run_10-00-09.html" in names
        assert "run_10-00-00.html" not in names

    def test_cleanup_by_time(self, generator, tmp_path):
        """Test retention by time (keep newer than N days)."""
        # Config says 7 days

        import datetime

        today = datetime.date.today().isoformat()

        # 1. Create recent file (should keep)
        recent_dir = tmp_path / "test_pipeline" / today
        recent_dir.mkdir(parents=True, exist_ok=True)

        recent_html = recent_dir / "run_recent.html"
        recent_html.touch()

        # 2. Create old file (should delete)
        # We rely on directory name for date parsing primarily now
        # So we create an old directory

        eight_days_ago = (datetime.date.today() - datetime.timedelta(days=8)).isoformat()
        old_dir = tmp_path / "test_pipeline" / eight_days_ago
        old_dir.mkdir(parents=True, exist_ok=True)

        old_html = old_dir / "run_old.html"
        old_html.touch()

        generator.cleanup()

        assert recent_html.exists()
        assert not old_html.exists()

    def test_cleanup_integration(self, generator, tmp_path):
        """Test cleanup runs after generation."""
        # Pre-populate with many files in new structure
        import datetime

        today = datetime.date.today().isoformat()
        pipeline_dir = tmp_path / "test_pipeline" / today
        pipeline_dir.mkdir(parents=True, exist_ok=True)

        for i in range(10):
            (pipeline_dir / f"run_10-00-{i:02d}.html").touch()
            (pipeline_dir / f"run_10-00-{i:02d}.json").touch()

        # Run generation
        generator.generate({}, [], [], [], 0.0, "", "")

        # Check files
        # Since generate creates a new file, cleanup runs AFTER generation
        # Total HTML files should be retention_count (5)

        # Note: generator.generate() creates the file AND then runs cleanup()
        # So we expect exactly retention_count files

        all_files = list(tmp_path.glob("**/*.html"))
        assert len(all_files) == 5


class TestStoryGeneratorAlertSummary:
    """Tests for StoryGenerator alert summary features."""

    def test_get_alert_summary_empty(self, tmp_path):
        """Should return empty dict if no story generated."""
        generator = StoryGenerator(
            pipeline_name="test_pipeline",
            output_path=str(tmp_path),
        )

        summary = generator.get_alert_summary()
        assert summary == {}

    def test_get_alert_summary_after_generation(self, tmp_path):
        """Should return summary with story path after generation."""
        from odibi.node import NodeResult

        generator = StoryGenerator(
            pipeline_name="test_pipeline",
            output_path=str(tmp_path),
        )

        # Create mock node results
        results = {
            "load": NodeResult(
                node_name="load",
                success=True,
                duration=1.0,
                rows_processed=1000,
            ),
            "filter": NodeResult(
                node_name="filter",
                success=True,
                duration=0.5,
                rows_processed=900,
            ),
        }

        # Generate story
        generator.generate(
            node_results=results,
            completed=["load", "filter"],
            failed=[],
            skipped=[],
            duration=1.5,
            start_time="2025-11-30T10:00:00",
            end_time="2025-11-30T10:00:01",
        )

        summary = generator.get_alert_summary()

        assert "story_path" in summary
        assert summary["story_path"] is not None
        assert "test_pipeline" in summary["story_path"]
        assert summary["completed_nodes"] == 2
        assert summary["failed_nodes"] == 0
