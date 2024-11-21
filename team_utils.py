import requests
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

API_URL = 'https://api-web.nhle.com/'

# Creating a dictionary to store the NHL team information
def get_team_info():
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
    date_plus_offset = ref_date - timedelta(days=7)
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