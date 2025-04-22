import contextlib
import os
import re
from dataclasses import dataclass
from typing import Generator, Optional, Union

import psycopg2
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

ConnectionType = Union[pyodbc.Connection, "psycopg2.extensions.connection"]


@dataclass
class Connection:
    connection_string: str
    db_type: Optional[str] = None  # "mssql" or "postgres"
    driver: Optional[str] = None
    encrypt: Optional[str] = None

    def __post_init__(self) -> None:
        """Set default values for if not provided."""
        if self.db_type == "mssql":
            if self.driver is None:
                self.driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
            if self.encrypt is None:
                self.encrypt = os.getenv("DB_ENCRYPT", "yes")

    @contextlib.contextmanager
    def get_connection(
        self,
    ) -> Generator[Union[pyodbc.Connection, "psycopg2.extensions.connection"], None, None]:
        """Context manager for database connections to ensure they're always closed."""
        conn = None
        try:
            conn = self.connect()
            yield conn
        finally:
            if conn:
                conn.close()

    @property
    def server(self) -> str:
        """Extract server name from connection string."""
        if self.db_type == "mssql":
            server_match = re.search(r"Server\s*=\s*([^;]+)", self.connection_string, re.IGNORECASE)
            if server_match:
                # Extract the full server portion (might include port)
                server_port = server_match.group(1).strip()
                # Split by comma and take the first part as the server name
                server_name = server_port.split(",")[0].strip()
                return server_name
        elif self.db_type == "postgres":
            host_match = re.search(r"host\s*=\s*([^\s]+)", self.connection_string, re.IGNORECASE)
            if host_match:
                return host_match.group(1).strip()
        return ""

    @property
    def database(self) -> str:
        """Extract database name from connection string."""
        if self.db_type == "mssql":
            db_match = re.search(r"Database\s*=\s*([^;]+)", self.connection_string, re.IGNORECASE)
            return db_match.group(1) if db_match else ""
        elif self.db_type == "postgres":
            db_match = re.search(r"dbname\s*=\s*([^\s]+)", self.connection_string, re.IGNORECASE)
            return db_match.group(1) if db_match else ""
        return ""

    @property
    def full_connection_string(self) -> str:
        """Build the complete connection string."""
        if self.db_type == "mssql":
            return f"{self.connection_string};Driver={self.driver};Encrypt={self.encrypt}"
        return self.connection_string

    def connect(self) -> Union[pyodbc.Connection, "psycopg2.extensions.connection"]:
        """Create and return a database connection."""
        if self.db_type == "mssql":
            return pyodbc.connect(self.full_connection_string)
        elif self.db_type == "postgres" or self.db_type == "pg":
            return psycopg2.connect(self.connection_string)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def get_sqlalchemy_engine(self) -> Engine:
        """
        Get a SQLAlchemy engine for this connection.

        This creates a SQLAlchemy engine that can be used with pandas
        and other libraries that work with SQLAlchemy.

        Returns:
            SQLAlchemy engine instance
        """
        if self.db_type == "mssql":
            # Create SQLAlchemy engine using the pyodbc driver
            odbc_connect = self.full_connection_string
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_connect}")
            return engine
        elif self.db_type == "postgres" or self.db_type == "pg":
            if self.connection_string.startswith("postgresql://"):
                engine = create_engine(self.connection_string)
            else:
                # For connection strings in key=value format
                engine = create_engine(f"postgresql+psycopg2://{self.connection_string}")
            return engine
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def __str__(self) -> str:
        return f"Server: [{self.server}] Database: [{self.database}] Type: [{self.db_type}]"


def get_connection(env_var_name: str, db_type: Optional[str] = None) -> Connection:
    """Helper function to get a connection from an environment variable."""
    conn_str = os.getenv(env_var_name)
    if not conn_str:
        raise ValueError(f"Environment variable '{env_var_name}' not found or empty")

    # Determine db_type if not provided
    if db_type is None:
        # Try to guess based on the connection string
        if "postgresql" in conn_str.lower() or "host=" in conn_str.lower():
            db_type = "postgres"
        else:
            db_type = "mssql"

    return Connection(connection_string=conn_str, db_type=db_type)


def modify_connection_for_database(connection: Connection, database_name: str) -> Connection:
    """
    Creates a new Connection object with the specified database name.
    """
    if connection.db_type == "mssql":
        # Create a copy of the connection string with the new database name
        connection_string = re.sub(
            r"Database\s*=\s*[^;]+",
            f"Database={database_name}",
            connection.connection_string,
            flags=re.IGNORECASE,
        )
    elif connection.db_type in ["postgres", "pg"]:
        # For postgres, update the dbname parameter
        if "dbname=" in connection.connection_string:
            connection_string = re.sub(
                r"dbname\s*=\s*[^\s;]+",
                f"dbname={database_name}",
                connection.connection_string,
                flags=re.IGNORECASE,
            )
        else:
            # If dbname is not in the connection string, add it
            connection_string = f"{connection.connection_string} dbname={database_name}"
    else:
        raise ValueError(f"Unsupported database type: {connection.db_type}")

    # Return a new Connection object with the modified connection string
    return Connection(
        connection_string=connection_string,
        db_type=connection.db_type,
        driver=connection.driver,
        encrypt=connection.encrypt,
    )
