"""Example Postgres Connection Plugin."""

from odibi.connections import BaseConnection


class PostgresConnection(BaseConnection):
    """Postgres connection implementation."""

    def __init__(self, host: str, database: str, user: str, password: str):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        print(f"Initialized PostgresConnection to {user}@{host}/{database}")

    def resolve_path(self, path: str) -> str:
        """Resolve table name."""
        return path

    def validate(self) -> bool:
        """Validate connection."""
        return True


def create_connection(name: str, config: dict) -> PostgresConnection:
    """Factory for Postgres connection."""
    return PostgresConnection(
        host=config["host"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
    )
