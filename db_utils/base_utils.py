import psycopg2
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection(db_prefix):
    """
    Establishes a connection to the specified database using environment variables.
    
    Args:
        db_prefix (str): The prefix for the database environment variables.
    
    Returns:
        tuple: A tuple containing the database connection and cursor.
    """
    db_host = os.getenv(f'{db_prefix}HOST')
    db_port = os.getenv(f'{db_prefix}PORT')
    db_name = os.getenv(f'{db_prefix}NAME')
    db_user = os.getenv(f'{db_prefix}USER')
    db_password = os.getenv(f'{db_prefix}PASSWORD')

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        logger.error(f"Failed to connect to the {db_prefix[:-1]} database: {e}")
        return None, None

def connect_db(db_prefix: str, suppress_log: bool = False):
    """
    Establishes a connection to the PostgreSQL database using environment variables.

    Parameters:
        db_prefix (str): The prefix for the database environment variables.
        suppress_log (bool): If True, suppresses logger.info output. Defaults to False.

    Returns:
        connection: A psycopg2 connection object.
    """
    db_host = os.getenv(f'{db_prefix}HOST')
    db_port = os.getenv(f'{db_prefix}PORT')
    db_name = os.getenv(f'{db_prefix}NAME')
    db_user = os.getenv(f'{db_prefix}USER')
    db_password = os.getenv(f'{db_prefix}PASSWORD')

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        if not suppress_log:
            logger.info("Database connection established.")
        return conn
    except psycopg2.Error as db_err:
        logger.error(f"Failed to connect to the database: {db_err}")
        raise


def disconnect_db(conn, suppress_log: bool = False):
    """
    Closes the connection to the PostgreSQL database.

    Parameters:
        conn: A psycopg2 connection object.
        suppress_log (bool): If True, suppresses logger.info output. Defaults to False.
    """
    try:
        if conn:
            conn.close()
            if not suppress_log:
                logger.info("Database connection closed.")
    except psycopg2.Error as db_err:
        logger.error(f"Failed to close the database connection: {db_err}")

# Add other shared utilities as needed