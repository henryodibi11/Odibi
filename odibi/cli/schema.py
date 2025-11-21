import json
from odibi.config import ProjectConfig


def schema_command(args):
    """Generate JSON schema for Odibi configuration."""
    schema = ProjectConfig.model_json_schema()
    print(json.dumps(schema, indent=2))
    return 0
