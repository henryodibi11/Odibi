# Security Policy

## Supported Versions

We release security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 1.1.x   | :white_check_mark: |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please report it responsibly.

### How to Report

**Please DO NOT create a public GitHub issue for security vulnerabilities.**

Instead, report vulnerabilities via one of these channels:

1. **GitHub Security Advisories** (preferred):
   - Go to https://github.com/henryodibi11/Odibi/security/advisories
   - Click "Report a vulnerability"
   - Provide detailed information about the vulnerability

2. **Email**:
   - Send to: henryodibi@outlook.com
   - Subject: "[SECURITY] ODIBI Vulnerability Report"
   - Include detailed description and reproduction steps

### What to Include

Please provide:
- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Potential impact
- Suggested fix (if available)

### Response Timeline

- **Acknowledgment**: Within 72 hours
- **Initial Assessment**: Within 7 days
- **Status Updates**: Every 2 weeks until resolved
- **Fix Timeline**: Depends on severity (critical issues prioritized)

### Disclosure Policy

- We will work with you to understand and resolve the issue
- We ask that you do not publicly disclose until a fix is available
- We will credit you in the security advisory (unless you prefer anonymity)

## Security Best Practices

When using ODIBI:

1. **Never commit credentials** to version control
2. **Use environment variables** for sensitive configuration
3. **Rotate secrets regularly** (SAS tokens, passwords)
4. **Use managed identities** when running on Azure
5. **Keep dependencies updated** (`pip install --upgrade odibi`)
6. **Review YAML configs** before running pipelines from untrusted sources

## Known Security Considerations

- **YAML Loading**: ODIBI uses safe YAML loading (no code execution)
- **SQL Injection**: Use parameterized queries (provided by SQLAlchemy)
- **Path Traversal**: Connection paths are validated and sandboxed
- **Secrets in Logs**: ODIBI redacts common secret patterns from logs

## Security Updates

Security updates are released as:
- **Patch versions** for minor fixes (e.g., 1.0.1 → 1.0.2)
- **Minor versions** for significant changes (e.g., 1.0.x → 1.1.x)

Subscribe to releases on GitHub to stay informed.
