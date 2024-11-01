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
            'primary_position': extract_primary_position(player_json.get('primaryPosition')),
            'jersey_number': player_json.get('primaryNumber'),
            'date_of_birth': player_json.get('birthDate'),
            'nationality': player_json.get('nationality'),
            'height_cm': convert_height_to_cm(player_json.get('height', '')),
            'weight_kg': player_json.get('weight'),
            'shoots': player_json.get('shootsCatches'),
            'current_team_id': extract_current_team_id(player_json.get('currentTeam')),
            'current_team_name': extract_current_team_name(player_json.get('currentTeam')),
            'current_team_abbreviation': extract_current_team_abbreviation(player_json.get('currentTeam')),
            'is_active': player_json.get('active')
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
            primary_position,
            jersey_number,
            date_of_birth,
            nationality,
            height_cm,
            weight_kg,
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
            %(primary_position)s,
            %(jersey_number)s,
            %(date_of_birth)s,
            %(nationality)s,
            %(height_cm)s,
            %(weight_kg)s,
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
            primary_position = EXCLUDED.primary_position,
            jersey_number = EXCLUDED.jersey_number,
            date_of_birth = EXCLUDED.date_of_birth,
            nationality = EXCLUDED.nationality,
            height_cm = EXCLUDED.height_cm,
            weight_kg = EXCLUDED.weight_kg,
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

def extract_primary_position(primary_position: dict) -> str:
    """
    Extracts the primary position name from the primaryPosition dictionary.

    Parameters:
        primary_position (dict): The primaryPosition dictionary from the API response.

    Returns:
        str: The name of the primary position or None if not available.
    """
    if isinstance(primary_position, dict):
        return primary_position.get('name')
    return primary_position

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
        return current_team.get('name')
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

def convert_height_to_cm(height_str: str) -> float:
    """
    Converts height from a string format (e.g., "6' 1\"") to centimeters.

    Parameters:
        height_str (str): Height string in the format feet'inches".

    Returns:
        float: Height in centimeters or None if conversion fails.
    """
    try:
        feet, inches = height_str.split("'")
        inches = inches.replace('"', '').strip()
        total_inches = int(feet) * 12 + int(inches)
        cm = round(total_inches * 2.54, 2)
        return cm
    except (ValueError, AttributeError):
        return None  # Return None if height is not available or parsing fails

# Example usage
if __name__ == "__main__":
    # Database configuration
    db_config = {
        'dbname': 'nhl_players_db',
        'user': 'postgres',
        'password': 'willIam21278$',  # Replace with your actual password or use environment variables
        'host': 'localhost',
        'port': '5432'
    }

    player_id = 8475193  # Example player ID

    insert_player(player_id, db_config)