from setuptools import setup, find_packages

setup(
    name="odibi-connector-postgres",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "odibi>=1.0.0",
        "psycopg2-binary>=2.9.0",
        "sqlalchemy>=1.4.0",
    ],
    entry_points={
        "odibi.connections": [
            "postgres = odibi_connector_postgres:create_connection",
        ],
    },
)
