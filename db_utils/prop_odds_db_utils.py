# db_utils/prop_odds_db_utils.py
from .base_utils import get_db_connection
from datetime import datetime
import urllib.parse
from utils import get_request  # Import from the new utils module
from psycopg2.extras import execute_values  # Import execute_values
import logging
from team_utils import get_tricode_by_fullname

DB_PREFIX = 'PROP_ODDS_DB_'

BASE_URL = 'https://api.prop-odds.com'
API_KEY = '5DQv4UzUztm6itoSLRaFdXDi5Dt4zGNFT1DvEFh0D0M'
# API_KEY = 'EEfdxYtFG2BgeCA2xDfM8KMgHvZVbYEaDGS5lCd38U'
# API_KEY = '9WnS02ST9fLkXsMZgFQKBJdzGwdtIt5yx0s7YFrHfc'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_prop_odds_db_connection():
    return get_db_connection(DB_PREFIX)

# Add Prop Odds specific functions...
def fetch_and_store_nhl_games(query_date=None, enable_logging=False):
    """
    Fetch Prop Odds games for a given date and store them in the PostgreSQL prop_odds database.

    Args:
        query_date (str, optional): The date to query in 'YYYY-MM-DD' format. Defaults to today.
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        list: A list of game dictionaries retrieved from the API.
    """
    if query_date is None:
        query_date = datetime.now()
    else:
        query_date = datetime.strptime(query_date, '%Y-%m-%d')

    query_params = {
        'date': query_date.strftime('%Y-%m-%d'),
        'tz': 'America/New_York',
        'api_key': API_KEY,
    }
    params = urllib.parse.urlencode(query_params)
    url = f"{BASE_URL}/beta/games/nhl?{params}"  # Adjust the endpoint as needed
    games_data = get_request(url)

    if not games_data:
        if enable_logging:
            logging.warning("No data retrieved from API.")
        return

    games = games_data.get('games', [])

    # Establish a connection using the helper function
    conn, cursor = get_db_connection('PROP_ODDS_DB_')

    if not conn or not cursor:
        if enable_logging:
            logging.error("Failed to establish a database connection.")
        return

    try:
        # Define the insert query
        insert_query = """
            INSERT INTO game_info (
                id, 
                game_id, 
                away_team, 
                home_team, 
                start_timestamp
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE
            SET 
                game_id = EXCLUDED.game_id,
                away_team = EXCLUDED.away_team,
                home_team = EXCLUDED.home_team,
                start_timestamp = EXCLUDED.start_timestamp,
                last_updated = CURRENT_TIMESTAMP;
        """

        # Prepare data for insertion
        records_to_insert = [
            (
                game['id'],
                game['game_id'],
                game['away_team'],
                game['home_team'],
                game['start_timestamp']
            )
            for game in games
        ]

        # Use execute_values for efficient bulk insertion
        execute_values(cursor, insert_query, records_to_insert)

        # Commit the transaction
        conn.commit()
        if enable_logging:
            logging.info(f"Inserted/Updated {len(records_to_insert)} records into prop_odds.game_info table.")

    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while inserting data into the Prop Odds database: %s", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return games

def get_nhl_games_from_db(query_date=None, enable_logging=False):
    """
    Retrieve NHL games from the PostgreSQL prop_odds database for a given date.

    Args:
        query_date (str, optional): The date to query in 'YYYY-MM-DD' format. Defaults to today.
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        list: A list of game dictionaries retrieved from the database.
    """
    if query_date is None:
        query_date = datetime.now()
    else:
        query_date = datetime.strptime(query_date, '%Y-%m-%d')
    
    try:
        conn, cursor = get_db_connection('PROP_ODDS_DB_')
        if not conn or not cursor:
            if enable_logging:
                logging.error("Failed to establish a database connection.")
            return []
        
        query = """
            SELECT id, game_id, away_team, home_team, start_timestamp
            FROM game_info
            WHERE start_timestamp::date = %s;
        """
        cursor.execute(query, (query_date.strftime('%Y-%m-%d'),))
        rows = cursor.fetchall()
        
        games = []
        for row in rows:
            game = {
                'id': row[0],
                'game_id': row[1],
                'away_team': row[2],
                'home_team': row[3],
                'start_timestamp': row[4]
            }
            games.append(game)
        
        if enable_logging:
            logging.info(f"Retrieved {len(games)} games from the database for date {query_date.strftime('%Y-%m-%d')}.")

        return games
    
    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while reading data from the database: %s", e)
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# get_nhl_games_from_db('2024-12-11')

def fetch_game_markets(game_id, market_name=None):
    """
    Fetch game markets for a given game ID and optionally a specific market name, then store them in the database.

    Args:
        game_id (str): The unique identifier for the game.
        market_name (str, optional): The specific market to fetch. Defaults to None, which fetches all markets.

    Returns:
        dict: The market data retrieved from the API.
    """
    if market_name is not None:
        url = f"{BASE_URL}/beta/odds/{game_id}/{market_name}?api_key={API_KEY}"
    else:
        url = f"{BASE_URL}/beta/markets/{game_id}?api_key={API_KEY}"

    # Use the get_request function from utils.py
    market_data = get_request(url)

    if market_data is not None:
        return market_data
    else:
        print("Failed to retrieve market data.")
        return None
# fetch_game_markets('c1fa7d3f30fdb408b78917509d1633c3')

def format_player_name(name):
    """
    Extracts the player's name and the bet type from the outcome name.

    Args:
        name (str): The name string from the outcome (e.g., "Artturi Lehkonen Over 0.5").

    Returns:
        tuple: A tuple containing the player's name and the bet type ('Over' or 'Under').
    """
    parts = name.split()

    # Ensure the name has at least three parts: Player Name, Bet Type, Handicap
    if len(parts) < 3:
        return None, None

    # Extract bet type ('Over' or 'Under')
    bet_type = parts[-2]
    if bet_type not in ['Over', 'Under']:
        return None, None

    # Extract player name by joining all parts except the last two
    player_name = ' '.join(parts[:-2])
    return player_name, bet_type

def insert_outcome_into_db(outcome, enable_logging=False):
    """
    Inserts a single outcome record into the Outcomes table.
    If the record already exists, it does nothing.
    
    Parameters:
        outcome (dict): The outcome data to insert.
        enable_logging (bool): If True, enables logging. Defaults to False.
    """
    try:
        if enable_logging:
            logging.info("Attempting to insert outcome into the database: %s", outcome)

        # Establish a connection using the helper function
        conn, cursor = get_db_connection('PROP_ODDS_DB_')

        if not conn or not cursor:
            if enable_logging:
                logging.error("Failed to establish a database connection.")
            return
        
        insert_query = """
        INSERT INTO player_shots_ou (
            game_id,
            sportsbook,
            player,
            ou,
            handicap,
            odds,
            timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id, sportsbook, player, ou, handicap, odds, timestamp) DO NOTHING
        """
        
        data_tuple = (
            outcome['game_id'],
            outcome['sportsbook'],
            outcome['player'],
            outcome['ou'],
            outcome['handicap'],
            outcome['odds'],
            outcome['timestamp']
        )
        
        cursor.execute(insert_query, data_tuple)
        conn.commit()

        if enable_logging:
            logging.info("Successfully inserted outcome into the database.")
        
    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while inserting data into the database: %s", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_game_markets(query_date, team_abbr, market_name='player_shots_over_under', enable_logging=False):
    """
    Processes game markets data for a specific date and team, and inserts outcomes into the database.
    
    Parameters:
        query_date (str): The date to query in 'YYYY-MM-DD' format.
        team_abbr (str): The team abbreviation to filter games.
        market_name (str): The market name to fetch. Defaults to 'player_shots_over_under'.
        enable_logging (bool): If True, enables logging. Defaults to False.
    """
    # Retrieve games from the database for the given date
    games = get_nhl_games_from_db(query_date, enable_logging=enable_logging)
    
    # Filter games to find the one involving the specified team
    game_id = None
    for game in games:
        away_team_abbr = get_tricode_by_fullname(game['away_team'])
        home_team_abbr = get_tricode_by_fullname(game['home_team'])
        
        if team_abbr in (away_team_abbr, home_team_abbr):
            game_id = game['game_id']
            break
    
    # If no game is found, log and return
    if not game_id:
        if enable_logging:
            logging.warning(f"No games found for team {team_abbr} on {query_date}.")
        return
    
    # Fetch game markets for the found game_id
    data = fetch_game_markets(game_id, market_name)
    
    if not data:
        if enable_logging:
            logging.warning(f"No market data found for game {game_id} and market {market_name}.")
        return
    
    # Process the market data
    supported_bookies = ['fanduel', 'pinnacle', 'draftkings', 'betmgm','barstool',
                        'betrivers','pointsbet', 'fliff', 'hardrock', 'betonline', 'fanatics']
    
    for sportsbook in data['sportsbooks']:
        bookie_key = sportsbook['bookie_key']
        market = sportsbook['market']
        market_key = market['market_key']
        
        if bookie_key not in supported_bookies:
            if enable_logging:
                logging.warning(f"Unsupported bookmaker: {bookie_key}")
            continue  # Skip unsupported bookmakers
        
        for outcome in market['outcomes']:
            timestamp = outcome['timestamp']
            handicap = outcome.get('handicap')
            odds = outcome.get('odds')
            participant = outcome.get('participant')
            participant_name = outcome.get('participant_name')
            name = outcome.get('name')
            description = outcome.get('description')
            
            player_name, over_under = format_player_name(name)
            
            if not player_name or not over_under:
                if enable_logging:
                    logging.warning(f"Invalid outcome name format: {name}")
                continue  # Skip if the name format is incorrect
            
            # Create a dictionary for the outcome
            outcome_data = {
                'game_id': game_id,
                'sportsbook': bookie_key,
                'player': player_name,
                'ou': over_under,
                'handicap': handicap,
                'odds': odds,
                'timestamp': timestamp,
            }
            
            # Pass the dictionary to the insert function
            insert_outcome_into_db(outcome_data, enable_logging=enable_logging)
# game_id = "c1fa7d3f30fdb408b78917509d1633c3"
# data = fetch_game_markets(game_id, 'player_shots_over_under' )
# process_game_markets(game_id, data)