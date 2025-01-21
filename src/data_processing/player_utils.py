import logging
import requests
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = 'https://api-web.nhle.com/'

def fetch_player_data(player_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetches and processes player data from the NHL API.

    Args:
        player_id (int): The ID of the player to fetch.

    Returns:
        Optional[Dict[str, Any]]: Processed player data dictionary or None if fetch fails.
    """
    url = f"{API_URL}/v1/player/{player_id}/landing"
    
    try:
        # Fetch player data from the NHL API
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to fetch data for player_id {player_id}")
            return None
        
        player_json = response.json()

        # Process and return the player information
        return {
            "player_id": player_id,
            "first_name": extract_name(player_json.get('firstName')),
            "last_name": extract_name(player_json.get('lastName')),
            "full_name": extract_full_name(player_json),
            "position": extract_position(player_json.get('position')),
            "jersey_number": player_json.get('sweaterNumber'),
            "date_of_birth": player_json.get('birthDate'),
            "nationality": player_json.get('birthCountry'),
            "height": player_json.get('heightInInches'),
            "weight": player_json.get('weightInPounds'),
            "shoots": player_json.get('shootsCatches'),
            "current_team_id": player_json.get('currentTeamId'),
            "current_team_name": extract_name(player_json.get('fullTeamName')),
            "current_team_abbreviation": player_json.get('currentTeamAbbrev'),
            "is_active": player_json.get('isActive')
        }

    except Exception as e:
        logger.error(f"An error occurred while fetching player {player_id}: {e}")
        return None

def extract_name(name_dict: dict) -> str:
    """
    Extracts the default name from a name dictionary.

    Args:
        name_dict (dict): Dictionary containing name translations.

    Returns:
        str: The default name or None if not available.
    """
    if isinstance(name_dict, dict):
        return name_dict.get('default')
    return name_dict

def extract_full_name(player_json: dict) -> str:
    """
    Extracts the full name from the JSON response.

    Args:
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

    Args:
        position (dict): The position dictionary from the API response.

    Returns:
        str: The name of the position or None if not available.
    """
    if isinstance(position, dict):
        return position.get('name')
    return position 