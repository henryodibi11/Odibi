import logging

from odibi.utils.logging import StructuredLogger


def test_secret_redaction(caplog):
    """Test that registered secrets are redacted from logs."""
    caplog.set_level(logging.INFO)
    logger = StructuredLogger(structured=False)

    secret = "super_secret_password"
    logger.register_secret(secret)

    # Test direct message redaction
    logger.info(f"Connecting with password: {secret}")

    # Check log records
    assert len(caplog.records) > 0
    assert "password: [REDACTED]" in caplog.text
    assert secret not in caplog.text


def test_kwargs_redaction(caplog):
    """Test that kwargs values are redacted."""
    caplog.clear()
    caplog.set_level(logging.INFO)
    logger = StructuredLogger(structured=False)

    key = "my_api_key"
    logger.register_secret(key)

    logger.info("Authenticating", api_key=key)

    assert len(caplog.records) > 0
    assert "api_key=[REDACTED]" in caplog.text
    assert key not in caplog.text


def test_json_redaction(capsys):
    """Test redaction in JSON structured logs."""
    logger = StructuredLogger(structured=True)

    secret = "hidden_token"
    logger.register_secret(secret)

    logger.info(f"Token is {secret}", token=secret)

    captured = capsys.readouterr()
    assert '"message": "Token is [REDACTED]"' in captured.out
    assert '"token": "[REDACTED]"' in captured.out
    assert secret not in captured.out
