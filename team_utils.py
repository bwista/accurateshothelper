import requests
import json
import os

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
