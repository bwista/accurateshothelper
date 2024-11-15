# insert_player.py
import psycopg2
import requests

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
    url = f'https://api-web.nhle.com/v1/player/{player_id}/landing'

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