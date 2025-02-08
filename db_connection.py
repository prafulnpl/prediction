"""PostgreSQL database connection management module."""
import logging
from contextlib import contextmanager
from typing import Generator, Optional, Tuple
import os
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import connection, cursor
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables once at module load
load_dotenv()

class DatabaseConfigError(Exception):
    """Custom exception for configuration errors"""

class DatabaseConnectionError(Exception):
    """Custom exception for connection failures"""

def validate_db_config() -> None:
    """Validate required database environment variables."""
    required_vars = {
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_NAME': os.getenv('DB_NAME'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PORT': os.getenv('DB_PORT')
    }
    missing = [var for var, val in required_vars.items() if not val]
    if missing:
        raise DatabaseConfigError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

@contextmanager
def database_connection(auto_close: bool = False) -> Generator[Tuple[connection, cursor], None, None]:
    """
    Context manager for handling database connections.

    Parameters:
        auto_close (bool): If True (default), the connection and cursor are closed
                           when the context exits. For long-running tasks (e.g., CDC replication),
                           set this to False so that the connection remains open.
                           
    NOTE: In this version, if auto_close is False the connection is never closed.
    """
    conn: Optional[connection] = None
    cur: Optional[cursor] = None

    try:
        validate_db_config()

        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()
        logger.info(f"Connected to database: {os.getenv('DB_NAME')}")
        yield conn, cur
        conn.commit()

    except OperationalError as e:
        logger.error(f"Connection failed: {str(e)}")
        raise DatabaseConnectionError("Database connection error") from e

    finally:
        if auto_close:
            # In this version, we intentionally do not close the connection.
            logger.info("auto_close=True but persistent connection is configured never to close.")
        else:
            logger.info("Persistent connection remains open indefinitely.")

def get_connection_params() -> dict:
    """
    Retrieve a dictionary of connection parameters from environment variables.
    This function also validates the required environment variables.
    """
    validate_db_config()
    return {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT"),
    }

def get_connection() -> connection:
    """
    Returns a persistent database connection that is NEVER closed.
    This is useful for long-running processes (e.g., CDC replication) where
    you do not want the connection to be closed automatically.
    """
    validate_db_config()
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        logger.info(f"Connected to database: {os.getenv('DB_NAME')}")
        # Intentionally do not close this connection ever.
        return conn
    except OperationalError as e:
        logger.error(f"Connection failed: {str(e)}")
        raise DatabaseConnectionError("Database connection error") from e

# Example usage:
if __name__ == "__main__":
    # Example using the context manager with auto_close disabled (persistent connection)
    try:
        with database_connection(auto_close=False) as (conn, cur):
            cur.execute("SELECT version()")
            db_version = cur.fetchone()
            logger.info(f"Database version: {db_version[0]}")
        logger.info("Persistent connection remains open for further use.")
        # You can continue using 'conn' here; it will never be closed automatically.
    except DatabaseConnectionError as e:
        logger.error("Failed to establish database connection")
        raise SystemExit(1) from e


