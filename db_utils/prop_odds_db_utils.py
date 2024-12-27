# db_utils/prop_odds_db_utils.py
from .base_utils import get_db_connection
from datetime import datetime
import urllib.parse
from utils import get_request  # Import from the new utils module
from psycopg2.extras import execute_values  # Import execute_values
import logging
from team_utils import get_tricode_by_fullname
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2.pool
from functools import partial
import os
import time
import threading
from queue import Queue

DB_PREFIX = 'PROP_ODDS_DB_'

BASE_URL = 'https://api.prop-odds.com'
API_KEY = '5DQv4UzUztm6itoSLRaFdXDi5Dt4zGNFT1DvEFh0D0M'
# API_KEY = 'EEfdxYtFG2BgeCA2xDfM8KMgHvZVbYEaDGS5lCd38U'
# API_KEY = '9WnS02ST9fLkXsMZgFQKBJdzGwdtIt5yx0s7YFrHfc'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a connection pool
connection_pool = None

# Add rate limiting configuration
API_RATE_LIMIT = 2  # requests per second
api_request_times = Queue()
api_lock = threading.Lock()

def init_connection_pool(min_conn=2, max_conn=10):
    """Initialize the connection pool for database operations."""
    global connection_pool
    if connection_pool is None:
        logging.info(f"Initializing connection pool with min_conn={min_conn}, max_conn={max_conn}")
        try:
            connection_pool = psycopg2.pool.SimpleConnectionPool(
                min_conn,
                max_conn,
                dbname=os.getenv('PROP_ODDS_DB_NAME'),
                user=os.getenv('PROP_ODDS_DB_USER'),
                password=os.getenv('PROP_ODDS_DB_PASSWORD'),
                host=os.getenv('PROP_ODDS_DB_HOST'),
                port=os.getenv('PROP_ODDS_DB_PORT')
            )
            logging.info("Successfully initialized connection pool")
        except Exception as e:
            logging.error(f"Failed to initialize connection pool: {e}")
            logging.error(f"Error type: {type(e)}")
            return None
    return connection_pool

def get_pooled_connection():
    """Get a connection from the pool."""
    global connection_pool
    if connection_pool is None:
        logging.info("Connection pool not initialized, initializing now")
        init_connection_pool()
    try:
        conn = connection_pool.getconn()
        logging.info("Successfully got connection from pool")
        return conn
    except Exception as e:
        logging.error(f"Failed to get connection from pool: {e}")
        logging.error(f"Error type: {type(e)}")
        return None

def return_connection(conn):
    """Return a connection to the pool."""
    global connection_pool
    if connection_pool is not None:
        connection_pool.putconn(conn)

def get_prop_odds_db_connection():
    logging.info("Establishing Prop Odds DB connection.")
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
    if enable_logging:
        logging.info(f"Fetching and storing NHL games for date: {query_date}")
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
    games_data = get_request(url, enable_logging=enable_logging)

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

    if enable_logging:
        logging.info("Completed fetching and storing NHL games.")
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
    if enable_logging:
        logging.info(f"Retrieving NHL games from DB for date: {query_date}")
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

def rate_limited_api_request(url, enable_logging=False):
    """Make an API request with rate limiting."""
    with api_lock:
        current_time = time.time()
        
        # Remove old request timestamps
        while not api_request_times.empty():
            old_time = api_request_times.get()
            if current_time - old_time < 1:  # Keep requests within last second
                api_request_times.put(old_time)
                break
        
        # Check if we've hit the rate limit
        if api_request_times.qsize() >= API_RATE_LIMIT:
            sleep_time = 1.0 - (current_time - api_request_times.queue[0])
            if sleep_time > 0:
                if enable_logging:
                    logging.info(f"Rate limit reached, waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        # Make the request
        response = get_request(url, enable_logging=enable_logging)
        api_request_times.put(time.time())
        return response

def fetch_game_markets(game_id, market_name=None, enable_logging=False):
    """Fetch game markets with rate limiting."""
    if market_name is not None:
        url = f"{BASE_URL}/beta/odds/{game_id}/{market_name}?api_key={API_KEY}"
    else:
        url = f"{BASE_URL}/beta/markets/{game_id}?api_key={API_KEY}"
    
    return rate_limited_api_request(url, enable_logging=enable_logging)

def format_player_name(name):
    """
    Extracts the player's name and the bet type from the outcome name.

    Args:
        name (str): The name string from the outcome (e.g., "Artturi Lehkonen Over 0.5").

    Returns:
        tuple: A tuple containing the player's name and the bet type ('Over' or 'Under').
    """
    logging.info(f"Formatting player name from: {name}")
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
    logging.info(f"Extracted player name: {player_name}, bet type: {bet_type}")
    return player_name, bet_type

def insert_outcome_into_db(outcome, enable_logging=False):
    """
    Inserts a single outcome record into the Outcomes table.
    If the record already exists, it does nothing.
    
    Parameters:
        outcome (dict): The outcome data to insert.
        enable_logging (bool): If True, enables logging. Defaults to False.
    """
    if enable_logging:
        logging.info(f"Inserting outcome into DB: {outcome}")
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
    if enable_logging:
        logging.info(f"Processing game markets for date: {query_date}, team: {team_abbr}, market: {market_name}")
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
            
    if enable_logging:
        logging.info("Completed processing game markets.")

def process_team_markets_optimized(date, team_abbr, game_id, market_name='player_shots_over_under', enable_logging=False):
    """Optimized version of process_game_markets that uses a pooled connection."""
    if enable_logging:
        logging.info(f"Processing markets for team {team_abbr} on {date}")
    
    conn = None
    cursor = None
    try:
        if enable_logging:
            logging.info("Attempting to get database connection from pool")
        conn = get_pooled_connection()
        if not conn:
            if enable_logging:
                logging.error("Failed to get connection from pool")
            return
            
        if enable_logging:
            logging.info("Successfully got database connection from pool")
        
        # Verify table exists
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_catalog.pg_tables 
                WHERE schemaname = 'public'
                AND tablename = 'player_shots_ou'
            );
        """)
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            if enable_logging:
                logging.error("Table player_shots_ou does not exist in public schema")
                # List all tables in public schema
                cursor.execute("""
                    SELECT tablename 
                    FROM pg_catalog.pg_tables 
                    WHERE schemaname = 'public';
                """)
                tables = cursor.fetchall()
                logging.error(f"Available tables in public schema: {[t[0] for t in tables]}")
            return
        
        # Fetch game markets for the game_id with timeout and retries
        max_retries = 3
        retry_count = 0
        data = None
        
        while retry_count < max_retries and not data:
            try:
                if enable_logging:
                    logging.info(f"Fetching markets for game {game_id} (attempt {retry_count + 1})")
                data = fetch_game_markets(game_id, market_name, enable_logging=enable_logging)
                if not data and retry_count < max_retries - 1:
                    retry_count += 1
                    wait_time = 2 ** retry_count  # Exponential backoff
                    if enable_logging:
                        logging.warning(f"Retry {retry_count} for game {game_id}, waiting {wait_time} seconds")
                    time.sleep(wait_time)
            except Exception as e:
                if enable_logging:
                    logging.error(f"Error fetching markets (attempt {retry_count + 1}): {e}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
        
        if not data:
            if enable_logging:
                logging.error(f"Failed to fetch market data for game {game_id} after {max_retries} attempts")
            return
        
        if enable_logging:
            logging.info(f"Successfully fetched market data for game {game_id}")
        
        # Process the market data
        supported_bookies = ['fanduel', 'pinnacle', 'draftkings', 'betmgm', 'barstool',
                           'betrivers', 'pointsbet', 'fliff', 'hardrock', 'betonline', 'fanatics']
        
        # Prepare batch insert data
        batch_data = []
        for sportsbook in data.get('sportsbooks', []):
            bookie_key = sportsbook.get('bookie_key')
            if not bookie_key or bookie_key not in supported_bookies:
                continue
                
            market = sportsbook.get('market', {})
            for outcome in market.get('outcomes', []):
                name = outcome.get('name')
                if not name:
                    continue
                    
                player_name, over_under = format_player_name(name)
                if not player_name or not over_under:
                    if enable_logging:
                        logging.warning(f"Invalid outcome name format: {name}")
                    continue
                
                handicap = outcome.get('handicap')
                odds = outcome.get('odds')
                timestamp = outcome.get('timestamp')
                
                if None in (handicap, odds, timestamp):
                    if enable_logging:
                        logging.warning(f"Missing required data for outcome: {outcome}")
                    continue
                
                try:
                    # Convert timestamp string to datetime if it's not already
                    if isinstance(timestamp, str):
                        timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
                    
                    batch_data.append((
                        game_id,
                        bookie_key,
                        player_name,
                        over_under,
                        float(handicap),  # Ensure handicap is float
                        int(odds),        # Ensure odds is integer
                        timestamp
                    ))
                except (ValueError, TypeError) as e:
                    if enable_logging:
                        logging.error(f"Data conversion error for outcome {outcome}: {e}")
                    continue
        
        if enable_logging:
            logging.info(f"Prepared {len(batch_data)} records for insertion")
        
        # Batch insert using execute_values
        if batch_data:
            try:
                if enable_logging:
                    logging.info("Creating database cursor")
                cursor = conn.cursor()
                
                insert_query = """
                    INSERT INTO player_shots_ou (
                        game_id, sportsbook, player, ou, handicap, odds, timestamp
                    ) VALUES %s
                    ON CONFLICT (game_id, sportsbook, player, ou, handicap, odds, timestamp) 
                    DO NOTHING
                """
                
                if enable_logging:
                    logging.info(f"Executing batch insert of {len(batch_data)} records")
                    if batch_data:
                        logging.info(f"Sample record: {batch_data[0]}")
                
                execute_values(cursor, insert_query, batch_data, page_size=100)
                
                if enable_logging:
                    logging.info("Committing transaction")
                conn.commit()
                
                if enable_logging:
                    logging.info(f"Successfully inserted {len(batch_data)} records for game {game_id}")
            except Exception as e:
                if enable_logging:
                    logging.error(f"Error during database insertion: {str(e)}")
                    logging.error(f"Error type: {type(e)}")
                if conn and not conn.closed:
                    conn.rollback()
                raise
            finally:
                if cursor and not cursor.closed:
                    cursor.close()
    except Exception as e:
        if enable_logging:
            logging.error(f"Error processing team markets: {str(e)}")
            logging.error(f"Error type: {type(e)}")
        if conn and not conn.closed:
            conn.rollback()
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if conn:
            if enable_logging:
                logging.info("Returning connection to pool")
            return_connection(conn)

def process_nhl_games_for_date_optimized(date, market='player_shots_over_under', enable_logging=False, max_workers=2):
    """
    Optimized version of process_nhl_games_for_date using parallel processing and connection pooling.
    
    Parameters:
        date (str): The date to query in 'YYYY-MM-DD' format.
        market (str): The market name to fetch. Defaults to 'player_shots_over_under'.
        enable_logging (bool): If True, enables logging. Defaults to False.
        max_workers (int): Maximum number of parallel workers. Defaults to 2.
    """
    if enable_logging:
        logging.info(f"Processing NHL games for date: {date}, market: {market}")
    
    # Initialize connection pool
    init_connection_pool()
    
    try:
        # Get games using a pooled connection
        conn = get_pooled_connection()
        if not conn:
            if enable_logging:
                logging.error("Failed to get connection from pool")
            return
            
        # Verify table exists
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_catalog.pg_tables 
                WHERE schemaname = 'public'
                AND tablename = 'player_shots_ou'
            );
        """)
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            if enable_logging:
                logging.error("Table player_shots_ou does not exist in public schema")
                # List all tables in public schema
                cursor.execute("""
                    SELECT tablename 
                    FROM pg_catalog.pg_tables 
                    WHERE schemaname = 'public';
                """)
                tables = cursor.fetchall()
                logging.error(f"Available tables in public schema: {[t[0] for t in tables]}")
            return
        
        games = get_nhl_games_from_db(date, enable_logging=enable_logging)
        
        if not games:
            if enable_logging:
                logging.info(f"No games found in the database for date {date}. Fetching from API.")
            fetch_and_store_nhl_games(date, enable_logging=enable_logging)
            games = get_nhl_games_from_db(date, enable_logging=enable_logging)
        
        if not games:
            if enable_logging:
                logging.warning(f"No games found for date {date}")
            return
        
        # Prepare tasks for parallel processing
        tasks = []
        for game in games:
            game_id = game['game_id']
            away_team_abbr = get_tricode_by_fullname(game['away_team'])
            home_team_abbr = get_tricode_by_fullname(game['home_team'])
            
            # Add tasks for both teams
            for team_abbr in (away_team_abbr, home_team_abbr):
                tasks.append((date, team_abbr, game_id))
        
        if enable_logging:
            logging.info(f"Processing {len(tasks)} tasks with {max_workers} workers")
        
        # Process tasks in parallel with progress tracking
        completed_tasks = 0
        failed_tasks = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            process_func = partial(
                process_team_markets_optimized,
                market_name=market,
                enable_logging=enable_logging
            )
            
            futures = [executor.submit(process_func, *task) for task in tasks]
            
            for future in as_completed(futures):
                try:
                    future.result(timeout=60)  # 60 second timeout for each task
                    completed_tasks += 1
                    if enable_logging:
                        logging.info(f"Completed {completed_tasks}/{len(tasks)} tasks")
                except Exception as e:
                    failed_tasks += 1
                    if enable_logging:
                        logging.error(f"Task failed: {e}")
                        logging.error(f"Error type: {type(e)}")
        
        if enable_logging:
            logging.info(f"Completed {completed_tasks}/{len(tasks)} tasks, {failed_tasks} failed")
        
    except Exception as e:
        if enable_logging:
            logging.error(f"Error in process_nhl_games_for_date_optimized: {e}")
            logging.error(f"Error type: {type(e)}")
        if conn and not conn.closed:
            conn.rollback()
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if conn:
            if enable_logging:
                logging.info("Returning connection to pool")
            return_connection(conn)
        # Clean up connection pool
        if connection_pool is not None:
            connection_pool.closeall()
    
    if enable_logging:
        logging.info("Completed processing NHL games for date.")

def american_to_decimal(odds):
    """
    Convert American odds to decimal odds.
    
    Args:
        odds (int): The American odds to convert.
        
    Returns:
        float: The decimal odds.
    """
    if odds > 0:
        return 1 + (odds / 100)
    else:
        return 1 - (100 / odds)

def filter_odds_closest_to_100(odds_dict):
    """
    Filters odds to find those closest to +100 in decimal format.
    For each sportsbook, finds the most recent handicap when Over/Under don't match,
    and returns both Over and Under for that handicap.

    Args:
        odds_dict (dict): A dictionary of odds categorized by sportsbook and over/under.

    Returns:
        list: A list of dictionaries containing the filtered odds.
    """
    # First, get the most recent lines for each unique combination
    most_recent_odds = {}
    for key, odds_list in odds_dict.items():
        for odd in odds_list:
            # Create a unique key for each combination
            unique_key = (odd['game_id'], odd['sportsbook'], odd['player'], odd['ou'], odd['handicap'])
            
            # Convert timestamp string to datetime for comparison
            timestamp = datetime.strptime(str(odd['timestamp']), '%Y-%m-%d %H:%M:%S%z')
            
            # Update if this is the first occurrence or if it's more recent
            if unique_key not in most_recent_odds or timestamp > datetime.strptime(str(most_recent_odds[unique_key]['timestamp']), '%Y-%m-%d %H:%M:%S%z'):
                most_recent_odds[unique_key] = odd

    # Organize by sportsbook and handicap
    by_sportsbook = {}
    for odd in most_recent_odds.values():
        sportsbook = odd['sportsbook']
        if sportsbook not in by_sportsbook:
            by_sportsbook[sportsbook] = {'over': {}, 'under': {}}
        
        side = odd['ou'].lower()
        handicap = odd['handicap']
        timestamp = datetime.strptime(str(odd['timestamp']), '%Y-%m-%d %H:%M:%S%z')
        
        by_sportsbook[sportsbook][side][handicap] = {
            'odd': odd,
            'timestamp': timestamp
        }

    # Find matching handicaps or most recent ones
    filtered_odds = []
    for sportsbook, sides in by_sportsbook.items():
        over_handicaps = set(sides['over'].keys())
        under_handicaps = set(sides['under'].keys())
        
        # Find matching handicaps
        matching_handicaps = over_handicaps.intersection(under_handicaps)
        
        if matching_handicaps:
            # Use the first matching handicap
            handicap = next(iter(matching_handicaps))
            filtered_odds.append(sides['over'][handicap]['odd'])
            filtered_odds.append(sides['under'][handicap]['odd'])
        else:
            # Find the most recent handicap
            all_entries = []
            for handicap, data in sides['over'].items():
                all_entries.append((handicap, data['timestamp'], 'over'))
            for handicap, data in sides['under'].items():
                all_entries.append((handicap, data['timestamp'], 'under'))
            
            if all_entries:
                # Sort by timestamp and get the most recent
                most_recent = max(all_entries, key=lambda x: x[1])
                handicap = most_recent[0]
                
                # Add both sides for this handicap if they exist
                if handicap in sides['over']:
                    filtered_odds.append(sides['over'][handicap]['odd'])
                if handicap in sides['under']:
                    filtered_odds.append(sides['under'][handicap]['odd'])

    return filtered_odds

def get_player_shots_ou_odds(player_name=None, query_date=None, sportsbook=None, team_name=None, line=False):
    """
    Retrieve player shot over/under odds from the PostgreSQL prop_odds database
    based on a specific player name, sportsbook, and/or team name.

    Args:
        player_name (str, optional): The full name of the player to filter odds by.
        query_date (str, optional): The date to query in 'YYYY-MM-DD' format. Used to retrieve games. Defaults to today.
        sportsbook (str, optional): The name of the sportsbook to filter odds by.
        team_name (str, optional): The full name of the team to filter games by.
        line (bool, optional): If True, filters odds to find those closest to +100. Defaults to False.

    Returns:
        list: A list of dictionaries containing player shot OU odds.
    """
    logging.info(f"Retrieving player shots OU odds for player: {player_name}, date: {query_date}, sportsbook: {sportsbook}, team: {team_name}, line: {line}")

    # Validate that at least one filter is provided
    if not player_name and not sportsbook and not team_name:
        print("At least one of player_name, sportsbook, or team_name must be provided.")
        return []

    # Retrieve games from the database for the given date
    games = get_nhl_games_from_db(query_date, enable_logging=True)

    if not games:
        print(f"No games found in the game_info table for date {query_date}.")
        return []

    # Filter games to find the game_id involving the specified team
    game_ids = []
    for game in games:
        if team_name in (game['away_team'], game['home_team']):
            game_ids.append(game['game_id'])

    if not game_ids:
        print(f"No games found for team {team_name} on {query_date}.")
        return []

    try:
        # Establish a database connection
        conn, cursor = get_db_connection('PROP_ODDS_DB_')
        if not conn or not cursor:
            print("Database connection failed.")
            return []

        # Initialize the base query
        base_query = """
            SELECT 
                pso.game_id,
                pso.sportsbook,
                pso.player,
                pso.ou,
                pso.handicap,
                pso.odds,
                pso.timestamp
            FROM player_shots_ou pso
            WHERE pso.game_id = ANY(%s)
        """

        # Initialize parameters list with the game_ids
        params = [game_ids]

        # Add filters based on provided arguments
        if player_name:
            base_query += " AND pso.player ILIKE %s"
            params.append(player_name)
        
        if sportsbook:
            base_query += " AND pso.sportsbook ILIKE %s"
            params.append(sportsbook)

        # Execute the query with parameters
        cursor.execute(base_query, tuple(params))
        rows = cursor.fetchall()

        # Process the retrieved rows
        odds_dict = {}
        for row in rows:
            odds = {
                'game_id': row[0],
                'sportsbook': row[1],
                'player': row[2],
                'ou': row[3],
                'handicap': row[4],
                'odds': row[5],
                'timestamp': row[6]
            }
            key = (odds['sportsbook'], odds['ou'])
            if key not in odds_dict:
                odds_dict[key] = []
            odds_dict[key].append(odds)

        if not odds_dict:
            filters = []
            if player_name:
                filters.append(f"player '{player_name}'")
            if sportsbook:
                filters.append(f"sportsbook '{sportsbook}'")
            if team_name:
                filters.append(f"team '{team_name}'")
            filter_str = " and ".join(filters)
            print(f"No odds found in player_shots_ou table for game_ids {game_ids} with filters: {filter_str}")
            return []

        # Filter for odds closest to +100 if the line flag is set
        result = filter_odds_closest_to_100(odds_dict) if line else [odds for odds_list in odds_dict.values() for odds in odds_list]
        logging.info("Completed retrieving player shots OU odds.")
        return result

    except Exception as e:
        print("An error occurred:", e)
        return []
    finally:
        # Ensure the cursor and connection are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_mismatched_game_ids_with_details(enable_logging=False):
    """
    Compare distinct game_ids in game_info and player_shots_ou tables
    and return game_ids that are not present in both tables, along with
    additional details for missing games in game_info.

    Args:
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        dict: A dictionary with keys 'only_in_game_info' and 'only_in_player_shots_ou'
              containing lists of game_ids not present in both tables. 'only_in_game_info'
              includes additional details like away_team, home_team, and start_timestamp.
    """
    if enable_logging:
        logging.info("Comparing distinct game_ids in game_info and player_shots_ou tables.")
    try:
        # Establish a database connection
        conn, cursor = get_db_connection('PROP_ODDS_DB_')
        if not conn or not cursor:
            if enable_logging:
                logging.error("Failed to establish a database connection.")
            return {'only_in_game_info': [], 'only_in_player_shots_ou': []}

        # Query distinct game_ids from both tables
        cursor.execute("SELECT DISTINCT game_id FROM game_info")
        game_info_ids = {row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT DISTINCT game_id FROM player_shots_ou")
        player_shots_ou_ids = {row[0] for row in cursor.fetchall()}

        # Find game_ids not present in both tables
        only_in_game_info_ids = game_info_ids - player_shots_ou_ids
        only_in_player_shots_ou = player_shots_ou_ids - game_info_ids

        # Fetch additional details for game_ids only in game_info
        only_in_game_info = []
        if only_in_game_info_ids:
            cursor.execute("""
                SELECT game_id, away_team, home_team, start_timestamp
                FROM game_info
                WHERE game_id = ANY(%s)
            """, (list(only_in_game_info_ids),))
            only_in_game_info = cursor.fetchall()

        if enable_logging:
            logging.info(f"Game IDs only in game_info: {only_in_game_info}")
            logging.info(f"Game IDs only in player_shots_ou: {only_in_player_shots_ou}")

        if enable_logging:
            logging.info("Completed comparison of game_ids.")
            
        return {
            'only_in_game_info': [
                {
                    'game_id': row[0],
                    'away_team': row[1],
                    'home_team': row[2],
                    'start_timestamp': row[3].strftime('%Y-%m-%d')
                }
                for row in only_in_game_info
            ],
            'only_in_player_shots_ou': list(only_in_player_shots_ou)
        }

    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while comparing game_ids: %s", e)
        return {'only_in_game_info': [], 'only_in_player_shots_ou': []}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_last_game_game_info(enable_logging=False):
    """
    Get the most recent game's start timestamp from the game_info table.

    Args:
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        str: The most recent game's start timestamp in format 'YYYY-MM-DD HH:MM:SS', or None if no games found.
    """
    if enable_logging:
        logging.info("Retrieving most recent game start timestamp from game_info table.")
    try:
        # Establish a database connection
        conn, cursor = get_db_connection('PROP_ODDS_DB_')
        if not conn or not cursor:
            if enable_logging:
                logging.error("Failed to establish a database connection.")
            return None

        # Query for the most recent start timestamp
        cursor.execute("""
            SELECT start_timestamp 
            FROM game_info 
            ORDER BY start_timestamp DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()

        if result:
            # Format the timestamp as a readable string
            formatted_timestamp = result[0].strftime('%Y-%m-%d %H:%M:%S')
            if enable_logging:
                logging.info(f"Found most recent game timestamp: {formatted_timestamp}")
            return formatted_timestamp
        else:
            if enable_logging:
                logging.warning("No games found in game_info table.")
            return None

    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while retrieving last game timestamp: %s", e)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_last_game_player_shots_db(enable_logging=False):
    """
    Get the most recent game's information from games that have player shot data.

    Args:
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        dict: A dictionary containing game_id, away_team, home_team, and start_timestamp (formatted),
              or None if no games found.
    """
    if enable_logging:
        logging.info("Retrieving most recent game info for games with player shot data.")
    try:
        # Establish a database connection
        conn, cursor = get_db_connection('PROP_ODDS_DB_')
        if not conn or not cursor:
            if enable_logging:
                logging.error("Failed to establish a database connection.")
            return None

        # Query for the most recent game info that has player shot data
        cursor.execute("""
            SELECT DISTINCT gi.game_id, gi.away_team, gi.home_team, gi.start_timestamp
            FROM game_info gi
            INNER JOIN (
                SELECT DISTINCT game_id 
                FROM player_shots_ou
            ) pso ON gi.game_id = pso.game_id
            ORDER BY gi.start_timestamp DESC
            LIMIT 1
        """)
        result = cursor.fetchone()

        if result:
            # Format the data into a dictionary
            game_info = {
                'game_id': result[0],
                'away_team': result[1],
                'home_team': result[2],
                'start_timestamp': result[3].strftime('%Y-%m-%d %H:%M:%S')
            }
            if enable_logging:
                logging.info(f"Found most recent game info: {game_info}")
            return game_info
        else:
            if enable_logging:
                logging.warning("No games found with player shot data.")
            return None

    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while retrieving last game info: %s", e)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()