import pandas as pd

from odibi.connections.base import BaseConnection
from odibi.plugins import register_connection_factory


class MockPostgresConnection(BaseConnection):
    def __init__(self, host, database):
        self.host = host
        self.database = database
        print(f"Initialized MockPostgresConnection to {host}:{database}")

    def get_path(self, path):
        return f"mock://{self.host}/{self.database}/{path}"

    def validate(self):
        print("Validating MockPostgresConnection...")
        pass

    def pandas_storage_options(self):
        return {}

    # Mock read/write to simulate behavior
    def read_table(self, table_name, schema="public"):
        print(f"Reading table {schema}.{table_name} from MockPostgres")
        return pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})


def create_mock_postgres(name, config):
    return MockPostgresConnection(
        host=config.get("host", "localhost"), database=config.get("database", "mydb")
    )


# Register immediately when imported
print("Registering mock_postgres plugin...")
register_connection_factory("mock_postgres", create_mock_postgres)
