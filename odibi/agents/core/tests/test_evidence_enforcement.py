"""Tests for evidence-based execution enforcement (Phase 6).

These tests verify that:
1. UserAgent requires ExecutionGateway and returns ExecutionEvidence
2. Fabricated execution reports are not produced
3. NO_EVIDENCE propagates correctly to Observer
4. Observer handles NO_EVIDENCE explicitly
"""

import os
import tempfile
from unittest.mock import MagicMock

import pytest

from odibi.agents.core.agent_base import AgentContext, OdibiAgent
from odibi.agents.core.evidence import EvidenceStatus, ExecutionEvidence
from odibi.agents.core.execution import ExecutionResult


class TestUserAgentEvidenceEnforcement:
    """Tests that UserAgent enforces evidence-based execution."""

    @pytest.fixture
    def mock_azure_config(self):
        """Create a mock Azure config."""
        config = MagicMock()
        config.openai_endpoint = "https://test.openai.azure.com"
        config.chat_deployment = "gpt-4"
        config.openai_api_key = "test-key"
        return config

    @pytest.fixture
    def user_agent(self, mock_azure_config):
        """Create a UserAgent with mocked LLM."""
        from odibi.agents.prompts.cycle_agents import UserAgent

        OdibiAgent.set_global_llm_client(MagicMock())
        agent = UserAgent(mock_azure_config)
        return agent

    def test_user_agent_returns_no_evidence_without_gateway(self, user_agent):
        """UserAgent must return NO_EVIDENCE when no gateway available."""
        context = AgentContext(
            query="Execute pipelines",
            odibi_root="d:/odibi",
            metadata={},
        )

        response = user_agent.process(context)

        assert "NO_EVIDENCE" in response.content
        assert response.metadata.get("mode") == "no_evidence"
        assert response.metadata.get("pipelines_executed") == 0

        evidence = response.metadata.get("execution_evidence")
        assert evidence is not None
        assert evidence.status == EvidenceStatus.NO_EVIDENCE

    def test_user_agent_returns_no_evidence_when_no_configs_found(self, user_agent):
        """UserAgent must return NO_EVIDENCE when no pipeline configs exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mock_gateway = MagicMock()

            context = AgentContext(
                query="Execute pipelines",
                odibi_root=tmpdir,
                metadata={"execution_gateway": mock_gateway},
            )

            response = user_agent.process(context)

            assert "NO_EVIDENCE" in response.content
            assert "No pipeline configs found" in response.content
            assert response.metadata.get("mode") == "no_evidence"

    def test_user_agent_uses_gateway_when_configs_exist(self, user_agent):
        """UserAgent must use ExecutionGateway when pipeline configs exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "odibi.yaml")
            with open(config_path, "w") as f:
                f.write("pipeline: test\n")

            mock_result = ExecutionResult(
                stdout="Pipeline completed",
                stderr="",
                exit_code=0,
                command="wsl pipeline run",
                success=True,
            )

            mock_gateway = MagicMock()
            mock_gateway.run_task_streaming.return_value = mock_result
            mock_gateway._to_wsl_path.side_effect = (
                lambda p: f"/mnt/{p.replace(':', '').replace(chr(92), '/')}"
            )

            context = AgentContext(
                query="Execute pipelines",
                odibi_root=tmpdir,
                metadata={"execution_gateway": mock_gateway},
            )

            response = user_agent.process(context)

            mock_gateway.run_task_streaming.assert_called()
            assert response.metadata.get("mode") == "evidence_based_execution"
            assert response.metadata.get("pipelines_executed") >= 1

            evidence = response.metadata.get("execution_evidence")
            assert evidence is not None
            assert isinstance(evidence, ExecutionEvidence)

    def test_user_agent_does_not_fabricate_results(self, user_agent):
        """UserAgent must not contain fabricated data sources or pipelines."""
        context = AgentContext(
            query="Execute pipelines",
            odibi_root="d:/nonexistent",
            metadata={},
        )

        response = user_agent.process(context)

        fabricated_patterns = [
            "hive://",
            "s3://",
            "postgres://",
            "redshift://",
            "bigquery://",
            "mlflow://",
            "rows processed",
            "1,250,000",
            "user_activity_etl",
            "daily_sales_aggregation",
            "customer_churn",
        ]

        for pattern in fabricated_patterns:
            assert (
                pattern.lower() not in response.content.lower()
            ), f"Fabricated pattern '{pattern}' found in response"

    def test_user_agent_includes_evidence_hash(self, user_agent):
        """UserAgent must include evidence hash in response when executing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "odibi.yaml")
            with open(config_path, "w") as f:
                f.write("pipeline: test\n")

            mock_result = ExecutionResult(
                stdout="OK",
                stderr="",
                exit_code=0,
                command="test",
                success=True,
            )

            mock_gateway = MagicMock()
            mock_gateway.run_task_streaming.return_value = mock_result
            mock_gateway._to_wsl_path.side_effect = (
                lambda p: f"/mnt/{p.replace(':', '').replace(chr(92), '/')}"
            )

            context = AgentContext(
                query="Execute pipelines",
                odibi_root=tmpdir,
                metadata={"execution_gateway": mock_gateway},
            )

            response = user_agent.process(context)

            assert "Evidence Hash:" in response.content


class TestObserverNoEvidenceHandling:
    """Tests that Observer handles NO_EVIDENCE correctly."""

    @pytest.fixture
    def mock_azure_config(self):
        """Create a mock Azure config."""
        config = MagicMock()
        config.openai_endpoint = "https://test.openai.azure.com"
        config.chat_deployment = "gpt-4"
        config.openai_api_key = "test-key"
        return config

    @pytest.fixture
    def observer_agent(self, mock_azure_config):
        """Create an ObserverAgent with mocked LLM."""
        from odibi.agents.prompts.cycle_agents import ObserverAgent

        OdibiAgent.set_global_llm_client(MagicMock())
        agent = ObserverAgent(mock_azure_config)
        return agent

    def test_observer_handles_no_evidence_explicitly(self, observer_agent):
        """Observer must create EXECUTION_ERROR for NO_EVIDENCE."""
        no_evidence = {
            "status": "NO_EVIDENCE",
            "stderr": "No execution gateway available",
            "raw_command": "",
            "exit_code": -1,
        }

        context = AgentContext(
            query="Analyze execution results",
            odibi_root="d:/odibi",
            metadata={"execution_evidence": no_evidence},
        )

        response = observer_agent.process(context)

        assert response.metadata.get("no_evidence_handled") is True
        assert response.metadata.get("observations_count") == 1

        observer_output = response.metadata.get("observer_output", {})
        issues = observer_output.get("issues", [])
        assert len(issues) == 1
        assert issues[0]["type"] == "EXECUTION_ERROR"
        assert issues[0]["severity"] == "HIGH"
        assert "NO_EVIDENCE" in issues[0]["evidence"]

    def test_observer_does_not_invent_issues_for_no_evidence(self, observer_agent):
        """Observer must not invent additional issues when NO_EVIDENCE."""
        no_evidence = {
            "status": "NO_EVIDENCE",
            "stderr": "No gateway",
            "raw_command": "",
            "exit_code": -1,
        }

        context = AgentContext(
            query="Analyze execution results",
            odibi_root="d:/odibi",
            metadata={"execution_evidence": no_evidence},
        )

        response = observer_agent.process(context)

        observer_output = response.metadata.get("observer_output", {})
        issues = observer_output.get("issues", [])

        assert len(issues) == 1, "Should only have 1 issue for NO_EVIDENCE"

    def test_observer_uses_evidence_when_available(self, observer_agent):
        """Observer should include execution evidence in its analysis context."""
        evidence = {
            "status": "FAILURE",
            "stderr": "Error: authentication failed",
            "stdout": "",
            "raw_command": "wsl pipeline run test.yaml",
            "exit_code": 1,
            "evidence_hash": "abc123",
        }

        mock_response = """```json
{
  "issues": [
    {
      "type": "AUTH_ERROR",
      "location": "test.yaml",
      "description": "Authentication failed",
      "severity": "HIGH",
      "evidence": "Error: authentication failed"
    }
  ],
  "observation_summary": "1 issue found"
}
```"""
        observer_agent._llm_client.chat_with_tools = MagicMock(
            return_value=MagicMock(content=mock_response)
        )

        context = AgentContext(
            query="Analyze execution results",
            odibi_root="d:/odibi",
            metadata={"execution_evidence": evidence, "previous_logs": []},
        )

        response = observer_agent.process(context)

        assert response.metadata.get("evidence_based") is True


class TestEnvironmentAgentEvidence:
    """Tests that EnvironmentAgent returns ExecutionEvidence."""

    @pytest.fixture
    def mock_azure_config(self):
        """Create a mock Azure config."""
        config = MagicMock()
        config.openai_endpoint = "https://test.openai.azure.com"
        config.chat_deployment = "gpt-4"
        config.openai_api_key = "test-key"
        return config

    @pytest.fixture
    def env_agent(self, mock_azure_config):
        """Create an EnvironmentAgent with mocked LLM."""
        from odibi.agents.prompts.cycle_agents import EnvironmentAgent

        OdibiAgent.set_global_llm_client(MagicMock())
        agent = EnvironmentAgent(mock_azure_config)
        return agent

    def test_env_agent_returns_no_evidence_without_gateway(self, env_agent):
        """EnvironmentAgent must return NO_EVIDENCE when no gateway."""
        context = AgentContext(
            query="Validate environment",
            odibi_root="d:/odibi",
            metadata={},
        )

        response = env_agent.process(context)

        assert response.metadata.get("environment_ready") is False
        evidence = response.metadata.get("execution_evidence")
        assert evidence is not None
        assert evidence.status == EvidenceStatus.NO_EVIDENCE

    def test_env_agent_returns_evidence_with_gateway(self, env_agent):
        """EnvironmentAgent must return real ExecutionEvidence with gateway."""
        env_check_output = """
WSL:
  Status: OK
  WSL detected

Java:
  Status: OK

Spark:
  Status: OK

Python:
  Status: OK

Odibi:
  Status: OK
"""
        mock_result = ExecutionResult(
            stdout=env_check_output,
            stderr="",
            exit_code=0,
            command="wsl env-check",
            success=True,
        )

        mock_gateway = MagicMock()
        mock_gateway.run_task_streaming.return_value = mock_result

        context = AgentContext(
            query="Validate environment",
            odibi_root="d:/odibi",
            metadata={"execution_gateway": mock_gateway},
        )

        response = env_agent.process(context)

        mock_gateway.run_task_streaming.assert_called_once()
        assert response.metadata.get("environment_ready") is True

        evidence = response.metadata.get("execution_evidence")
        assert evidence is not None
        assert isinstance(evidence, ExecutionEvidence)
        assert evidence.status == EvidenceStatus.SUCCESS


class TestNoFabrication:
    """Tests that agents do not fabricate execution results."""

    def test_user_agent_prompt_forbids_fabrication(self):
        """UserAgent prompt must explicitly forbid fabrication."""
        from odibi.agents.prompts.cycle_agents import USER_AGENT_PROMPT

        assert "FORBIDDEN" in USER_AGENT_PROMPT
        assert "fabricat" in USER_AGENT_PROMPT.lower()
        assert "hive://" in USER_AGENT_PROMPT
        assert "NO_EVIDENCE" in USER_AGENT_PROMPT

    def test_user_agent_prompt_requires_gateway(self):
        """UserAgent prompt must require ExecutionGateway."""
        from odibi.agents.prompts.cycle_agents import USER_AGENT_PROMPT

        assert "ExecutionGateway" in USER_AGENT_PROMPT
        assert "REQUIRED" in USER_AGENT_PROMPT

    def test_observer_prompt_handles_no_evidence(self):
        """Observer prompt must explain NO_EVIDENCE handling."""
        from odibi.agents.prompts.cycle_agents import OBSERVER_AGENT_PROMPT

        assert "NO_EVIDENCE" in OBSERVER_AGENT_PROMPT
        assert "EXECUTION_ERROR" in OBSERVER_AGENT_PROMPT


class TestNullEvidenceHandling:
    """Tests for None stdout/stderr handling (regression test).

    These tests verify that agents do not crash when execution
    evidence contains None values for stdout/stderr fields.
    """

    @pytest.fixture
    def mock_azure_config(self):
        """Create a mock Azure config."""
        config = MagicMock()
        config.openai_endpoint = "https://test.openai.azure.com"
        config.chat_deployment = "gpt-4"
        config.openai_api_key = "test-key"
        return config

    @pytest.fixture
    def observer_agent(self, mock_azure_config):
        """Create an ObserverAgent with mocked LLM."""
        from odibi.agents.prompts.cycle_agents import ObserverAgent

        OdibiAgent.set_global_llm_client(MagicMock())
        agent = ObserverAgent(mock_azure_config)
        return agent

    def test_observer_handles_none_stdout_stderr(self, observer_agent):
        """Observer must not crash when stdout/stderr are None.

        Regression test: Previously, slicing None values caused:
        TypeError: 'NoneType' object is not subscriptable
        """
        evidence_with_nulls = {
            "status": "SUCCESS",
            "stdout": None,  # None, not empty string
            "stderr": None,  # None, not empty string
            "raw_command": "echo test",
            "exit_code": 0,
            "evidence_hash": "abc123",
        }

        mock_response = """```json
{
  "issues": [],
  "observation_summary": "No issues found"
}
```"""
        observer_agent._llm_client.chat_with_tools = MagicMock(
            return_value=MagicMock(content=mock_response)
        )

        context = AgentContext(
            query="Analyze execution results",
            odibi_root="d:/odibi",
            metadata={"execution_evidence": evidence_with_nulls},
        )

        # This must not raise TypeError
        response = observer_agent.process(context)

        # Verify response is valid
        assert response is not None
        assert response.agent_role is not None

        # Evidence-based flag should be set
        assert response.metadata.get("evidence_based") is True

    def test_observer_handles_mixed_none_values(self, observer_agent):
        """Observer handles evidence where only some fields are None."""
        evidence_mixed = {
            "status": "FAILURE",
            "stdout": "Some output",  # Has value
            "stderr": None,  # None
            "raw_command": "failing command",
            "exit_code": 1,
            "evidence_hash": "def456",
        }

        mock_response = """```json
{
  "issues": [
    {
      "type": "EXECUTION_FAILURE",
      "location": "command",
      "description": "Command failed",
      "severity": "HIGH",
      "evidence": "exit code 1"
    }
  ],
  "observation_summary": "1 issue found"
}
```"""
        observer_agent._llm_client.chat_with_tools = MagicMock(
            return_value=MagicMock(content=mock_response)
        )

        context = AgentContext(
            query="Analyze execution results",
            odibi_root="d:/odibi",
            metadata={"execution_evidence": evidence_mixed},
        )

        # Must not crash
        response = observer_agent.process(context)

        # Should still emit issues
        observer_output = response.metadata.get("observer_output", {})
        issues = observer_output.get("issues", [])
        assert len(issues) >= 1

    def test_safe_truncate_helper(self):
        """Test the _safe_truncate helper function directly."""
        from odibi.agents.prompts.cycle_agents import _safe_truncate

        # None returns empty string
        assert _safe_truncate(None) == ""

        # Empty string returns empty string
        assert _safe_truncate("") == ""

        # Short string returned as-is
        assert _safe_truncate("hello") == "hello"

        # Long string is truncated
        long_string = "x" * 5000
        result = _safe_truncate(long_string)
        assert len(result) == 2000

        # Custom limit works
        assert len(_safe_truncate(long_string, 500)) == 500

        # None with custom limit
        assert _safe_truncate(None, 500) == ""
