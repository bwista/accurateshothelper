# insert_player.py
import psycopg2
import requests

from requests.adapters import HTTPAdapter, Retry    
from pbp_utils import retrieve_schedule

API_URL = 'https://api-web.nhle.com/v1'

def insert_player(player_id: int, db_config: dict) -> None:
    """
    Retrieves player information from the NHL API and inserts it into the players table.

    Parameters:
        player_id (int): The unique identifier for the player.
        db_config (dict): Database configuration with keys: dbname, user, password, host, port.

    Example:
        db_config = {
            'dbname': 'nhl_players_db',
            'user': 'postgres',
            'password': 'your_password',
            'host': 'localhost',
            'port': '5432'
        }
        insert_player(player_id=8478236, db_config=db_config)
    """
    # Define the API endpoint
    url = f'{API_URL}/player/{player_id}/landing'

    try:
        # Fetch player data from the NHL API
        response = requests.get(url)
        response.raise_for_status()
        player_json = response.json()

        # Debug: Print the entire JSON response (optional, can be commented out)
        # print(json.dumps(player_json, indent=4))

        # Extract relevant fields from the JSON response
        player_data = {
            'player_id': extract_player_id(player_json),
            'first_name': extract_name(player_json.get('firstName')),
            'last_name': extract_name(player_json.get('lastName')),
            'full_name': extract_full_name(player_json),
            'position': extract_position(player_json.get('position')),
            'jersey_number': player_json.get('sweaterNumber'),
            'date_of_birth': player_json.get('birthDate'),
            'nationality': player_json.get('birthCountry'),
            'height': player_json.get('heightInInches'),
            'weight': player_json.get('weightInPounds'),
            'shoots': player_json.get('shootsCatches'),
            'current_team_id': player_json.get('currentTeamId'),
            'current_team_name': extract_name(player_json.get('fullTeamName')),
            'current_team_abbreviation': player_json.get('currentTeamAbbrev'),
            'is_active': player_json.get('isActive')
        }

        # Debug: Print the types and values of each field in player_data
        print("Player Data Types and Values:")
        for key, value in player_data.items():
            print(f"{key}: {type(value).__name__} - {value}")

        # Validate that player_id is not None
        if player_data['player_id'] is None:
            print("Error: 'player_id' is None. Cannot proceed with database insertion.")
            return

        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Define the SQL INSERT statement with upsert capability
        insert_query = """
        INSERT INTO players (
            player_id,
            first_name,
            last_name,
            full_name,
            position,
            jersey_number,
            date_of_birth,
            nationality,
            height,
            weight,
            shoots,
            current_team_id,
            current_team_name,
            current_team_abbreviation,
            is_active
        ) VALUES (
            %(player_id)s,
            %(first_name)s,
            %(last_name)s,
            %(full_name)s,
            %(position)s,
            %(jersey_number)s,
            %(date_of_birth)s,
            %(nationality)s,
            %(height)s,
            %(weight)s,
            %(shoots)s,
            %(current_team_id)s,
            %(current_team_name)s,
            %(current_team_abbreviation)s,
            %(is_active)s
        )
        ON CONFLICT (player_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            full_name = EXCLUDED.full_name,
            position = EXCLUDED.position,
            jersey_number = EXCLUDED.jersey_number,
            date_of_birth = EXCLUDED.date_of_birth,
            nationality = EXCLUDED.nationality,
            height = EXCLUDED.height,
            weight = EXCLUDED.weight,
            shoots = EXCLUDED.shoots,
            current_team_id = EXCLUDED.current_team_id,
            current_team_name = EXCLUDED.current_team_name,
            current_team_abbreviation = EXCLUDED.current_team_abbreviation,
            is_active = EXCLUDED.is_active,
            last_updated = CURRENT_TIMESTAMP;
        """

        # Execute the INSERT statement
        cursor.execute(insert_query, player_data)

        # Commit the transaction
        conn.commit()

        print(f"Player {player_data['full_name']} inserted/updated successfully.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching player data: {http_err}")
    except psycopg2.Error as db_err:
        print(f"Database error occurred: {db_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def extract_player_id(player_json: dict) -> int:
    """
    Extracts the player ID from the JSON response.

    Parameters:
        player_json (dict): The entire JSON response from the API.

    Returns:
        int: The player ID or None if not available.
    """
    try:
        return player_json.get('playerId')
    except AttributeError:
        return None

def extract_name(name_dict: dict) -> str:
    """
    Extracts the default name from a name dictionary.

    Parameters:
        name_dict (dict): Dictionary containing name translations.

    Returns:
        str: The default name or None if not available.
    """
    if isinstance(name_dict, dict):
        return name_dict.get('default')
    return name_dict  # If it's already a string

def extract_full_name(player_json: dict) -> str:
    """
    Extracts the full name from the JSON response.

    Parameters:
        player_json (dict): The entire JSON response from the API.

    Returns:
        str: The full name or constructs it from first and last names.
    """
    full_name = player_json.get('fullName')
    if not full_name:
        first_name = extract_name(player_json.get('firstName'))
        last_name = extract_name(player_json.get('lastName'))
        if first_name and last_name:
            return f"{first_name} {last_name}"
    return full_name

def extract_position(position: dict) -> str:
    """
    Extracts the position name from the position dictionary.

    Parameters:
        position (dict): The position dictionary from the API response.

    Returns:
        str: The name of the position or None if not available.
    """
    if isinstance(position, dict):
        return position.get('name')
    return position

def extract_current_team_id(current_team: dict) -> int:
    """
    Extracts the current team ID from the currentTeam dictionary.

    Parameters:
        current_team (dict): The currentTeam dictionary from the API response.

    Returns:
        int: The ID of the current team or None if not available.
    """
    if isinstance(current_team, dict):
        return current_team.get('id')
    return current_team

def extract_current_team_name(current_team: dict) -> str:
    """
    Extracts the current team name from the currentTeam dictionary.

    Parameters:
        current_team (dict): The currentTeam dictionary from the API response.

    Returns:
        str: The name of the current team or None if not available.
    """
    if isinstance(current_team, dict):
        return current_team.get('default')
    return current_team

def extract_current_team_abbreviation(current_team: dict) -> str:
    """
    Extracts the current team abbreviation from the currentTeam dictionary.

    Parameters:
        current_team (dict): The currentTeam dictionary from the API response.

    Returns:
        str: The abbreviation of the current team or None if not available.
    """
    if isinstance(current_team, dict):
        return current_team.get('abbreviation')
    return current_team

# Example usage
# if __name__ == "__main__":
#     # Database configuration
#     db_config = {
#         'dbname': 'nhl_players_db',
#         'user': 'postgres',
#         'password': 'willIam21278$',  # Replace with your actual password or use environment variables
#         'host': 'localhost',
#         'port': '5432'
#     }

#     player_id = 8475193  # Example player ID

#     insert_player(player_id, db_config)

def append_player_ids(player_list, db_config):
    """
    Loops through the player_list (input), matches each player's name with the full_name in the database,
    retrieves the player_id, and appends it to the Player objects.

    Args:
        player_list (list of Player): The list of Player objects to update with player_id.
        db_config (dict): Database configuration with keys: dbname, user, password, host, port.
    """
    # Removed loading environment variables since db_config is provided as an argument

    # Extract unique player names to minimize database queries
    player_names = list(set(player.name for player in player_list))
    
    try:
        # Establish a connection to the database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Prepare and execute the SQL query to fetch player_ids
        query = """
        SELECT full_name, player_id
        FROM players
        WHERE full_name = ANY(%s)
        """
        cursor.execute(query, (player_names,))
        results = cursor.fetchall()
        
        # Create a mapping from full_name to player_id
        name_to_id = {name: pid for name, pid in results}
        
        # Iterate through the player_list and assign player_id
        for player in player_list:
            if player.name in name_to_id:
                player.player_id = name_to_id[player.name]
                print(f"Assigned player_id {player.player_id} to {player.name}.")
            else:
                print(f"Warning: No player_id found for {player.name}.")
                
    except psycopg2.Error as e:
        print(f"Database error occurred: {e}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def get_boxscores(start_date_str, end_date_str):
    """
    Retrieves boxscore information for all games within a specified date range.

    Parameters:
        start_date_str (str): The start date in 'YYYY-MM-DD' format.
        end_date_str (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        list: A list of boxscore JSON objects for each game.
    """
    # Retrieve the schedule to get all game IDs within the date range
    schedule = retrieve_schedule(start_date_str, end_date_str)
    game_ids = schedule['game_ids']

    boxscores = []

    # Set up a session with retry strategy
    session = requests.Session()
    retry = Retry(
        total=5,  # Total number of retries
        backoff_factor=1,  # Exponential backoff factor
        status_forcelist=[500, 502, 503, 504, 522, 524],  # HTTP status codes to retry
        allowed_methods=["GET"]  # Methods to retry
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    for game_id in game_ids:
        try:
            response = session.get(
                f"{API_URL}/gamecenter/{game_id}/boxscore",
                headers={"Content-Type": "application/json"},
                timeout=10  # Timeout after 10 seconds
            )
            response.raise_for_status()  # Raise HTTPError for bad responses
            boxscore_data = response.json()
            boxscores.append(boxscore_data)
            print(f"Successfully fetched boxscore for game {game_id}")
        
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch boxscore for game {game_id}: {e}")
            # Optionally, log the error or store it for later analysis

    session.close()
    return boxscores

def extract_unique_player_ids(start_date_str: str, end_date_str: str) -> set:
    """
    Extracts all unique player IDs from boxscore data within the specified date range.

    Parameters:
        start_date_str (str): The start date in 'YYYY-MM-DD' format.
        end_date_str (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        set: A set of unique player IDs.
    """
    # Retrieve boxscore data for the given date range
    boxscores = get_boxscores(start_date_str, end_date_str)
    unique_player_ids = set()
    
    print(f"Retrieved {len(boxscores)} boxscores.")

    if not boxscores:
        print("No boxscores found for the given date range.")
        return unique_player_ids

    # Iterate through each boxscore
    for idx, boxscore in enumerate(boxscores):
        print(f"\nProcessing boxscore {idx + 1}/{len(boxscores)} (Game ID: {boxscore.get('id')})")
        
        # Access the playerByGameStats section
        player_stats = boxscore.get('playerByGameStats', {})
        if not player_stats:
            print("  No 'playerByGameStats' found in this boxscore.")
            continue

        # Iterate through both homeTeam and awayTeam
        for team_key in ['homeTeam', 'awayTeam']:
            team_stats = player_stats.get(team_key, {})
            if not team_stats:
                print(f"  No '{team_key}' data found.")
                continue

            print(f"  Processing team: {team_key}")

            # Define the player positions to extract
            # Updated 'defensemen' to 'defense'
            position_groups = ['forwards', 'defense', 'goalies']
            for position_group in position_groups:
                players = team_stats.get(position_group, [])
                if not players:
                    print(f"    No players found in position group '{position_group}'.")
                    continue

                print(f"    Processing position group: {position_group} ({len(players)} players)")
                
                # Extract player IDs from each position group
                for player in players:
                    player_id = player.get('playerId')
                    if player_id:
                        unique_player_ids.add(player_id)
                    else:
                        print("      Player without 'playerId' encountered.")

    print(f"\nTotal unique player IDs extracted: {len(unique_player_ids)}")
    return unique_player_ids

def update_player_db(start_date_str: str, end_date_str: str, db_config: dict, skip_existing: bool = False) -> None:
    """
    Updates the players database by extracting unique player IDs within the specified date range
    and inserting/updating their information.

    Parameters:
        start_date_str (str): The start date in 'YYYY-MM-DD' format.
        end_date_str (str): The end date in 'YYYY-MM-DD' format.
        db_config (dict): Database configuration with keys: dbname, user, password, host, port.
        skip_existing (bool): If True, skips players that already exist in the database. Defaults to False.
    """
    # Extract unique player IDs within the date range
    unique_player_ids = extract_unique_player_ids(start_date_str, end_date_str)

    if not unique_player_ids:
        print("No unique player IDs found for the given date range.")
        return

    print(f"Found {len(unique_player_ids)} unique player IDs.")

    # If skip_existing is True, get existing player IDs from the database
    existing_player_ids = set()
    if skip_existing:
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT player_id FROM players")
            existing_player_ids = {row[0] for row in cursor.fetchall()}
            print(f"Found {len(existing_player_ids)} existing players in database.")
        except psycopg2.Error as e:
            print(f"Database error while fetching existing players: {e}")
            return
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()

        # Filter out existing player IDs
        player_ids_to_update = unique_player_ids - existing_player_ids
        print(f"Will process {len(player_ids_to_update)} new players.")
    else:
        player_ids_to_update = unique_player_ids
        print(f"Will process all {len(player_ids_to_update)} players.")

    # Iterate through the filtered player IDs and insert/update each player in the database
    for player_id in player_ids_to_update:
        insert_player(player_id, db_config)

    print("Player database update completed.")

def check_last_update(db_config: dict) -> str:
    """
    Checks the last time the players database was updated by retrieving the most recent
    `last_updated` timestamp from the players table.

    Parameters:
        db_config (dict): Database configuration with keys: dbname, user, password, host, port.

    Returns:
        str: The most recent `last_updated` timestamp as a string in ISO format.
             Returns a message if the table is empty or an error occurs.
    
    Example:
        db_config = {
            'dbname': 'nhl_players_db',
            'user': 'postgres',
            'password': 'your_password',
            'host': 'localhost',
            'port': '5432'
        }
        last_update = check_last_update(db_config)
        print(f"Last database update was on: {last_update}")
    """
    import psycopg2

    query = """
    SELECT MAX(last_updated) AS last_update
    FROM players;
    """

    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Execute the query to get the latest last_updated timestamp
        cursor.execute(query)
        result = cursor.fetchone()

        if result and result[0]:
            last_update_date = result[0].date().isoformat()
            print(f"Last database update was on: {last_update_date}")
            return last_update_date
        else:
            message = "The players table is empty or 'last_updated' column is missing."
            print(message)
            return message

    except psycopg2.Error as db_err:
        error_message = f"Database error occurred: {db_err}"
        print(error_message)
        return error_message
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        return error_message
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()