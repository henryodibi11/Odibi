# Odibi MCP Server

Install the MCP interface from the root distribution:

```bash
pip install "odibi[mcp]"
```

`odibi_help(action=...)` discovers the current registered inventory,
signatures/parameters, and effect classes; `odibi_execute(action, args_json=None)`
dispatches an action. Transport-route policy is documented in the MCP gateway guide,
not returned authoritatively by help.

Odibi MCP is not universally read-only or universally single-project. Actions have one
of five effects: `public_read`, `sensitive_read`, `execution`, `file_write`, or
`session_mutation`. Non-public effects require an authorized application identity. Only
current managed remote runtime-data actions additionally receive exact managed-project
preparation. `test_pipeline` remains authorization class `execution` even though its
current implementation creates an immutable plan.

Run stdio with `python -m odibi_mcp`; without HTTP identity, only public reads are
available. Run HTTP with `python -m odibi_mcp.databricks_app`; HTTP uses exact Bearer
identity, effect authorization, and managed preparation where applicable. Explicit
in-process `trusted-local` behavior is not evidence of remote behavior. The default bind
is `0.0.0.0:8000`; this does not prove TLS, proxy identity, Internet isolation,
reachability, or safety.

See the [MCP gateway guide](../docs/guides/mcp_guide.md), immutable
[planning authority](../docs/features/planning.md), and [security policy](../SECURITY.md).

```bash
python -m pip check
python -c "import odibi, odibi_mcp.mcp_server, odibi_mcp.databricks_app"
```
