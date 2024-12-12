# db_utils/base_utils.py
import psycopg2
import os

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
        print(f"Failed to connect to the {db_prefix[:-1]} database:", e)
        return None, None

# Add other shared utilities as needed