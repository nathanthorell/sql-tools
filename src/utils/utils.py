import os
import re
from dataclasses import dataclass
from typing import Optional

import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class Connection:
    connection_string: str
    driver: Optional[str] = None
    encrypt: Optional[str] = None

    def __post_init__(self) -> None:
        """Set default values for driver and encrypt if not provided."""
        if self.driver is None:
            self.driver = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
        if self.encrypt is None:
            self.encrypt = os.getenv("DB_ENCRYPT", "yes")

    @property
    def server(self) -> str:
        """Extract server name from connection string."""
        server_match = re.search(r"Server\s*=\s*([^;]+)", self.connection_string, re.IGNORECASE)
        if server_match:
            # Extract the full server portion (might include port)
            server_port = server_match.group(1).strip()
            # Split by comma and take the first part as the server name
            server_name = server_port.split(",")[0].strip()
            return server_name
        return ""

    @property
    def database(self) -> str:
        """Extract database name from connection string."""
        db_match = re.search(r"Database=([^;]+)", self.connection_string)
        return db_match.group(1) if db_match else ""

    @property
    def full_connection_string(self) -> str:
        """Build the complete connection string."""
        return f"{self.connection_string};Driver={self.driver};Encrypt={self.encrypt}"

    def connect(self) -> pyodbc.Connection:
        """Create and return a database connection."""
        return pyodbc.connect(self.full_connection_string)

    def get_sqlalchemy_engine(self) -> Engine:
        """
        Get a SQLAlchemy engine for this connection.

        This creates a SQLAlchemy engine that can be used with pandas
        and other libraries that work with SQLAlchemy.

        Returns:
            SQLAlchemy engine instance
        """
        # Create SQLAlchemy engine using the pyodbc driver
        # The connection string needs to be in SQLAlchemy format
        odbc_connect = self.full_connection_string
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_connect}")
        return engine

    def __str__(self) -> str:
        return f"Server: [{self.server}] Database: [{self.database}]"


def get_connection(env_var_name: str) -> Connection:
    """Helper function to get a connection from an environment variable."""
    conn_str = os.getenv(env_var_name)
    if not conn_str:
        raise ValueError(f"Environment variable '{env_var_name}' not found or empty")
    return Connection(connection_string=conn_str)


def modify_connection_for_database(connection: Connection, database_name: str) -> Connection:
    """
    Creates a new Connection object with the specified database name.
    """
    # Create a copy of the connection string with the new database name
    connection_string = re.sub(
        r"Database=[^;]+", f"Database={database_name}", connection.connection_string
    )

    # Return a new Connection object with the modified connection string
    return Connection(
        connection_string=connection_string, driver=connection.driver, encrypt=connection.encrypt
    )
