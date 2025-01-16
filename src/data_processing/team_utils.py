import requests
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

API_URL = 'https://api-web.nhle.com/'

def get_team_info():
    """
    Retrieves NHL team information from the NHL API.

    Makes a request to the NHL API to get team data and creates a dictionary mapping 
    team IDs to team information including full name and triCode (abbreviation).

    Returns:
        dict: A dictionary where keys are team IDs and values are dictionaries containing:
            - fullName (str): The full name of the team
            - triCode (str): The three letter abbreviation code for the team
    """
    nhl_teams = {}
    # https://api.nhle.com/stats/rest/en/team
    response = requests.get(f"{API_URL}/stats/rest/en/team", params={"Content-Type": "application/json"})
    data = response.json()

    for team in data["data"]:
        team_id = team['id']
        team_info = {
            "fullName": team["fullName"],
            "triCode": team["triCode"]
        }
        nhl_teams[team_id] = team_info
    return nhl_teams

def save_team_info_to_file(team_info, file_path):
    """
    Saves the team_info dictionary to a JSON file.

    Parameters:
        team_info (dict): The dictionary containing team information.
        file_path (str): The path to the file where the data will be saved.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Write the team_info dictionary to a JSON file with indentation for readability
    with open(file_path, 'w') as f:
        json.dump(team_info, f, indent=4)

# Specify the desired file path
# file_path = 'data/team_info.json'

# Call the function to save team_info
# save_team_info_to_file(team_info, file_path)

# print(f"team_info has been saved to {file_path}")

# Initialize a cache for team information
_team_info_cache: Optional[Dict[str, Dict[str, str]]] = None

def _load_team_info() -> Dict[str, Dict[str, str]]:
    global _team_info_cache
    if _team_info_cache is None:
        file_path = '../data/team_info.json'
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        with open(file_path, 'r') as f:
            _team_info_cache = json.load(f)
    return _team_info_cache

def get_tricode_by_fullname(full_name: str) -> Optional[str]:
    """
    Retrieves the triCode (abbreviation) for a given NHL team's full name.

    Parameters:
        full_name (str): The full name of the NHL team.

    Returns:
        Optional[str]: The triCode of the team if found, otherwise None.
    """
    team_info = _load_team_info()

    for info in team_info.values():
        if info.get('fullName').lower() == full_name.lower():
            return info.get('triCode')
    return None

def get_fullname_by_tricode(tri_code: str) -> Optional[str]:
    """
    Retrieves the fullName for a given NHL team's triCode (abbreviation).

    Parameters:
        tri_code (str): The triCode (abbreviation) of the NHL team.

    Returns:
        Optional[str]: The fullName of the team if found, otherwise None.
    """
    team_info = _load_team_info()

    for info in team_info.values():
        if info.get('triCode').lower() == tri_code.lower():
            return info.get('fullName')
    return None

def nst_to_nhl_tricode(nst_abbreviation: str) -> Optional[str]:
    """
    Converts a Natural Stat Trick (NST) abbreviation to the standardized NHL tri-code abbreviation.

    Parameters:
        nst_abbreviation (str): The NST abbreviation to convert.

    Returns:
        Optional[str]: The corresponding NHL tri-code abbreviation if found, otherwise None.
    """
    nst_to_nhl_map = {
        'MIN': 'MIN', 'NYR': 'NYR', 'ANA': 'ANA', 'BUF': 'BUF', 'NYI': 'NYI',
        'N.J': 'NJD', 'L.A': 'LAK', 'DET': 'DET', 'FLA': 'FLA', 'EDM': 'EDM',
        'COL': 'COL', 'NSH': 'NSH', 'SEA': 'SEA', 'CHI': 'CHI', 'CAR': 'CAR',
        'OTT': 'OTT', 'STL': 'STL', 'T.B': 'TBL', 'BOS': 'BOS', 'TOR': 'TOR',
        'WPG': 'WPG', 'PIT': 'PIT', 'VAN': 'VAN', 'S.J': 'SJS', 'CBJ': 'CBJ',
        'CGY': 'CGY', 'MTL': 'MTL', 'VGK': 'VGK', 'UTA': 'UTA', 'PHI': 'PHI',
        'DAL': 'DAL', 'WSH': 'WSH'
    }

    return nst_to_nhl_map.get(nst_abbreviation)

# Example usage:
# nhl_tricode = nst_to_nhl_tricode('N.J')
# print(nhl_tricode)  # Output: NJD

def get_team_roster(team_code: str, season: int) -> dict:
    """
    Retrieves the roster for a specific NHL team and season.

    Parameters:
        team_code (str): Three-letter team code (e.g., 'TOR').
        season (int): Season in YYYYYYYY format (e.g., 20222023).

    Returns:
        dict: JSON response containing the team roster.
    """
    roster_url = f"{API_URL}v1/roster/{team_code}/{season}"
    response = requests.get(roster_url)
    response.raise_for_status()
    return response.json()

def get_week_schedule(team: str, date: str) -> dict:
    """
    Retrieves the weekly schedule for a specific NHL team and date.

    Parameters:
        team (str): Three-letter team code (e.g., 'TOR').
        date (str): Date in YYYY-MM-DD format.

    Returns:
        dict: JSON response containing the weekly schedule.
    """
    schedule_url = f"{API_URL}v1/club-schedule/{team}/week/{date}"
    response = requests.get(schedule_url)
    response.raise_for_status()
    return response.json()

def get_most_recent_game_id(team: str, date: str) -> Optional[int]:
    """
    Finds the most recent past game_id based on the game_date from the weekly schedule.

    Args:
        team (str): Three-letter team code (e.g., 'TOR').
        date (str): The date to retrieve the weekly schedule for in 'YYYY-MM-DD' format.

    Returns:
        Optional[int]: The game_id of the most recent past game. Returns None if no past games are found.
    """
    try:
        # Parse the original date string to a date object
        ref_date = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("date must be in 'YYYY-MM-DD' format.")

    # Add a 7-day offset to the original date
    date_plus_offset = ref_date - timedelta(days=5)
    date_plus_offset_str = date_plus_offset.strftime('%Y-%m-%d')

    # Call get_week_schedule with the offset date
    schedule = get_week_schedule(team, date_plus_offset_str)

    # Filter games that have a gameDate before the reference_date
    past_games = [
        game for game in schedule.get('games', [])
        if datetime.strptime(game.get('gameDate'), '%Y-%m-%d').date() < ref_date
    ]

    if not past_games:
        return None

    # Find the game with the latest gameDate
    most_recent_game = max(
        past_games,
        key=lambda x: datetime.strptime(x.get('gameDate'), '%Y-%m-%d').date()
    )

    return most_recent_game.get('id')