import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta
import pandas as pd

API_URL = 'https://api-web.nhle.com/v1'

def get_game_boxscore(game_id: int, clean: bool = False) -> dict:
    """
    Retrieves boxscore information for a specific NHL game.

    Parameters:
        game_id (int): The ID of the game to retrieve boxscore data for.
        clean (bool): Whether to return a cleaned version of the data with only
                      gameDate, awayTeam abbrev, and homeTeam abbrev. Defaults to True.

    Returns:
        dict: JSON response containing the game's boxscore information or a cleaned version.
    
    Raises:
        requests.exceptions.RequestException: If the API request fails.
    """
    boxscore_url = f"{API_URL}/gamecenter/{game_id}/boxscore"
    response = requests.get(boxscore_url)
    response.raise_for_status()
    data = response.json()
    
    if clean:
        return {
            'away_team': data.get('awayTeam', {}).get('abbrev'),
            'home_team': data.get('homeTeam', {}).get('abbrev')
        }
    
    return data

def display_boxscore(game_data):
    """
    Processes the game data dictionary and returns four DataFrames:
    1. Away Team Skaters
    2. Away Team Goalies
    3. Home Team Skaters
    4. Home Team Goalies

    Parameters:
    - game_data (Union[dict, int]): Either a game data dictionary or game ID integer.

    Returns:
    - away_skaters_df (pd.DataFrame)
    - away_goalies_df (pd.DataFrame)
    - home_skaters_df (pd.DataFrame)
    - home_goalies_df (pd.DataFrame)
    """
    # If game_data is an int, fetch the boxscore data
    if isinstance(game_data, int):
        game_data = get_game_boxscore(game_data)

    # Helper function to process skater data
    def process_skaters(team_data, team_type):
        forwards = team_data.get('forwards', [])
        defense = team_data.get('defense', [])
        skaters = forwards + defense
        for skater in skaters:
            skater['team'] = team_type
            # Extract default name value
            skater['name'] = skater['name']['default']
        return skaters

    # Helper function to process goalie data
    def process_goalies(goalies, team_type):
        for goalie in goalies:
            goalie['team'] = team_type
            # Extract default name value
            goalie['name'] = goalie['name']['default']
        return goalies

    # Extract away and home team data
    player_stats = game_data.get('playerByGameStats', {})
    away_team_data = player_stats.get('awayTeam', {})
    home_team_data = player_stats.get('homeTeam', {})

    # Process skaters
    away_skaters = process_skaters(away_team_data, 'Away')
    home_skaters = process_skaters(home_team_data, 'Home')

    # Process goalies
    away_goalies = process_goalies(away_team_data.get('goalies', []), 'Away')
    home_goalies = process_goalies(home_team_data.get('goalies', []), 'Home')

    # Create DataFrames
    away_skaters_df = pd.DataFrame(away_skaters)
    home_skaters_df = pd.DataFrame(home_skaters)
    away_goalies_df = pd.DataFrame(away_goalies)
    home_goalies_df = pd.DataFrame(home_goalies)

    return away_skaters_df, away_goalies_df, home_skaters_df, home_goalies_df

# Example usage:
# away_skaters, away_goalies, home_skaters, home_goalies = display_boxscore(game_data)
# Display the DataFrames
# print("Away Team Skaters:")
# print(away_skaters)
# print("\nAway Team Goalies:")
# print(away_goalies)
# print("\nHome Team Skaters:")
# print(home_skaters)
# print("\nHome Team Goalies:")
# print(home_goalies)
