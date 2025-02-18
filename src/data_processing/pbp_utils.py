import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta
import pandas as pd

API_URL = 'https://api-web.nhle.com/v1'

def get_matchup_games(start_date, end_date):
    """
    Retrieves NHL game matchups for a given date range from the NHL API.

    Parameters:
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        dict: A dictionary containing:
            - next_start_date (str): The next available date after the requested range
            - game_ids (dict): Dictionary with three lists:
                - id: List of game IDs
                - date: List of corresponding game dates
                - game_start_time: List of game start times

    Note:
        The function fetches one week of games starting from start_date and filters
        games up to end_date. For multi-week ranges, use retrieve_schedule().
        Games with gameScheduleState 'PPD' (postponed) are filtered out.
    """
    r = requests.get(url=API_URL + '/schedule/' + str(start_date))
    data = r.json()

    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    matchup_games = {'next_start_date': '', 'game_ids': {'id': [], 'date': [], 'game_start_time': []}}

    matchup_games['next_start_date'] = data['nextStartDate']

    for day in data['gameWeek']:
        for game in day['games']:
            # Skip postponed games
            if game.get('gameScheduleState') == 'PPD':
                continue
            if game.get('gameType') != 2:
                continue
            
            game_start_time = game['startTimeUTC']  # Read the game's start time
            # game_date = datetime.strptime(game_date_timestamp, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
            game_date = day['date']
            # Strip the time and retain only the date this causes problems for the sweden games

            if datetime.strptime(game_date, '%Y-%m-%d').date() <= end_date_dt.date():
                matchup_games['game_ids']['id'].append(game['id'])
                matchup_games['game_ids']['date'].append(game_date)
                matchup_games['game_ids']['game_start_time'].append(game_start_time)

    return matchup_games

def retrieve_schedule(start_date_str, end_date_str):
    all_game_ids = {'game_ids': [], 'game_dates': []}
    temp_week = get_matchup_games(start_date_str, end_date_str)

    all_game_ids['game_ids'].extend(temp_week['game_ids']['id'])
    all_game_ids['game_dates'].extend(temp_week['game_ids']['date'])

    end_date_dt = datetime.strptime(end_date_str, '%Y-%m-%d')

    while True:
        temp_next_start = datetime.strptime(temp_week['next_start_date'], '%Y-%m-%d')

        if temp_next_start <= end_date_dt:
            temp_start = temp_week['next_start_date']
            temp_week = get_matchup_games(temp_start, end_date_str)

            game_ids = temp_week['game_ids']['id']
            game_dates = temp_week['game_ids']['date']

            for game_id, game_date in zip(game_ids, game_dates):
                game_date_dt = datetime.strptime(game_date, '%Y-%m-%d').date()
                if game_date_dt <= end_date_dt.date():
                    all_game_ids['game_ids'].append(game_id)
                    all_game_ids['game_dates'].append(game_date)
                else:
                    # Instead of breaking here, move to the next week
                    break
        else:
            break

    return all_game_ids

def get_livedata_from_game(game_list):
    """
    Fetches live play-by-play data for a list of games with retry mechanism.

    Parameters:
        game_list (dict): A dictionary containing game IDs and dates.

    Returns:
        list: A list of play-by-play records.
    """
    all_plays = []
    
    # Set up a session with retry strategy
    session = requests.Session()
    retry = Retry(
        total=5,  # Total number of retries
        backoff_factor=1,  # Exponential backoff factor (e.g., 1, 2, 4, 8, 16 seconds)
        status_forcelist=[500, 502, 503, 504, 522, 524],  # HTTP status codes to retry
        allowed_methods=["GET"]  # Methods to retry
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    for game in game_list['game_ids']:
        try:
            response = session.get(
                f"{API_URL}/gamecenter/{game}/play-by-play",
                headers={"Content-Type": "application/json"},
                timeout=10  # Timeout after 10 seconds
            )
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            temp_game_plays = data.get('plays', [])

            for play in temp_game_plays:
                play_record = {
                    'gid': str(game),
                    'eventId': play.get('eventId'),
                    'sortOrder': play.get('sortOrder'),
                    'period_number': play.get('periodDescriptor', {}).get('number'),
                    'period_type': play.get('periodDescriptor', {}).get('periodType'),
                    'maxRegulationPeriods': play.get('periodDescriptor', {}).get('maxRegulationPeriods'),
                    'timeInPeriod': play.get('timeInPeriod'),
                    'timeRemaining': play.get('timeRemaining'),
                    'situationCode': play.get('situationCode'),
                    'homeTeamDefendingSide': play.get('homeTeamDefendingSide'),
                    'typeCode': play.get('typeCode'),
                    'typeDescKey': play.get('typeDescKey')
                }

                details = play.get('details', {})
                for key, value in details.items():
                    play_record[f'details_{key}'] = value

                all_plays.append(play_record)
        
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for game {game}: {e}")
            # Optionally, log the error or store it for later analysis

    session.close()
    return all_plays

def scrape_month_playbyplay(year: int, month: int) -> pd.DataFrame:
    """
    Scrapes all play-by-play data for the specified month.

    Parameters:
        year (int): The year of the month to scrape.
        month (int): The month to scrape (1-12).

    Returns:
        pd.DataFrame: A DataFrame containing all play-by-play data for the month.
    """
    # Define the start and end dates for the month
    start_date = datetime(year, month, 1)
    # Handle month wrap-around for December
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"Scraping play-by-play data from {start_date_str} to {end_date_str}")
    
    # Retrieve the schedule for the specified date range
    schedule = retrieve_schedule(start_date_str, end_date_str)
    
    # Fetch play-by-play data for all games in the schedule
    playbyplay_data = get_livedata_from_game(schedule)
    
    # Convert the play-by-play data to a DataFrame
    df_pbp = pd.DataFrame(playbyplay_data)
    
    return df_pbp