from datetime import datetime
import logging
import os
from psycopg2.extras import execute_values
from .base_utils import get_db_connection
from utils import get_request

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_store_the_odds_events(date, enable_logging=False):
    """
    Fetch NHL events from the_odds API and store them in the PostgreSQL database.

    Args:
        date (str): The date to fetch events for in ISO8601 format (e.g. '2024-12-31')
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        list: A list of event dictionaries retrieved from the API.
    """
    if enable_logging:
        logging.info(f"Fetching and storing NHL events for date: {date}")

    # Construct API URL for NHL events
    api_key = '2d61bfcd50b060d771fec317f16bf249'
    url = f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/events?apiKey={api_key}"
    
    # Get the events data from the API
    events_data = get_request(url)

    if not events_data:
        if enable_logging:
            logging.warning("No data retrieved from API.")
        return

    # Establish a connection using the helper function
    conn, cursor = get_db_connection('THE_ODDS_DB_')

    if not conn or not cursor:
        if enable_logging:
            logging.error("Failed to establish a database connection.")
        return

    try:
        # Define the insert query
        insert_query = """
            INSERT INTO game_info (
                id,
                sport_key,
                sport_title,
                home_team,
                away_team,
                commence_time
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE
            SET 
                sport_key = EXCLUDED.sport_key,
                sport_title = EXCLUDED.sport_title,
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                commence_time = EXCLUDED.commence_time,
                last_updated = CURRENT_TIMESTAMP;
        """

        # Prepare data for insertion
        records_to_insert = [
            (
                event['id'],
                'icehockey_nhl',  # Hardcoded since we're only dealing with NHL
                'NHL',            # Hardcoded sport title
                event['home_team'],
                event['away_team'],
                event['commence_time']
            )
            for event in events_data
        ]

        # Use execute_values for efficient bulk insertion
        execute_values(cursor, insert_query, records_to_insert)

        # Commit the transaction
        conn.commit()
        if enable_logging:
            logging.info(f"Inserted/Updated {len(records_to_insert)} records into game_info table.")

    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while inserting data into the database: %s", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    if enable_logging:
        logging.info("Completed fetching and storing events.")
    return events_data 