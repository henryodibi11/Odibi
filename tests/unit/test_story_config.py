"""Tests for story configuration."""

import pytest

from odibi.config import StoryConfig


class TestStoryConfig:
    """Tests for StoryConfig class."""

    def test_default_failure_sample_settings(self):
        """Should have sensible defaults for failure sample settings."""
        config = StoryConfig(
            connection="local_data",
            path="stories/",
        )

        assert config.failure_sample_size == 100
        assert config.max_failure_samples == 500
        assert config.max_sampled_validations == 5

    def test_custom_failure_sample_settings(self):
        """Should allow custom failure sample settings."""
        config = StoryConfig(
            connection="local_data",
            path="stories/",
            failure_sample_size=50,
            max_failure_samples=200,
            max_sampled_validations=3,
        )

        assert config.failure_sample_size == 50
        assert config.max_failure_samples == 200
        assert config.max_sampled_validations == 3

    def test_failure_sample_size_bounds(self):
        """Should validate failure_sample_size bounds."""
        # Lower bound
        config = StoryConfig(
            connection="local_data",
            path="stories/",
            failure_sample_size=0,
        )
        assert config.failure_sample_size == 0

        # Upper bound
        config = StoryConfig(
            connection="local_data",
            path="stories/",
            failure_sample_size=1000,
        )
        assert config.failure_sample_size == 1000

        # Above upper bound should fail
        with pytest.raises(ValueError):
            StoryConfig(
                connection="local_data",
                path="stories/",
                failure_sample_size=1001,
            )

    def test_max_failure_samples_bounds(self):
        """Should validate max_failure_samples bounds."""
        config = StoryConfig(
            connection="local_data",
            path="stories/",
            max_failure_samples=5000,
        )
        assert config.max_failure_samples == 5000

        with pytest.raises(ValueError):
            StoryConfig(
                connection="local_data",
                path="stories/",
                max_failure_samples=5001,
            )

    def test_max_sampled_validations_bounds(self):
        """Should validate max_sampled_validations bounds."""
        config = StoryConfig(
            connection="local_data",
            path="stories/",
            max_sampled_validations=1,
        )
        assert config.max_sampled_validations == 1

        config = StoryConfig(
            connection="local_data",
            path="stories/",
            max_sampled_validations=20,
        )
        assert config.max_sampled_validations == 20

        with pytest.raises(ValueError):
            StoryConfig(
                connection="local_data",
                path="stories/",
                max_sampled_validations=21,
            )

    def test_retention_policy_required(self):
        """Should require at least one retention policy."""
        with pytest.raises(ValueError):
            StoryConfig(
                connection="local_data",
                path="stories/",
                retention_days=None,
                retention_count=None,
            )

    def test_basic_config_fields(self):
        """Should validate basic config fields."""
        config = StoryConfig(
            connection="adls_bronze",
            path="stories/pipeline/",
            max_sample_rows=20,
            retention_days=60,
            retention_count=50,
        )

        assert config.connection == "adls_bronze"
        assert config.path == "stories/pipeline/"
        assert config.max_sample_rows == 20
        assert config.retention_days == 60
        assert config.retention_count == 50
        assert config.auto_generate is True
