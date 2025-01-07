from datetime import datetime, timezone
import logging
import os
import urllib.parse
from psycopg2.extras import execute_values
from .base_utils import get_db_connection
from utils import get_request
from fuzzywuzzy import fuzz
from datetime import timedelta
from zoneinfo import ZoneInfo

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API Configuration
API_KEY = '9a5c793d56cca6f031c468dc649e054d'
API_BASE_URL = 'https://api.the-odds-api.com/v4/sports/icehockey_nhl'
API_HISTORICAL_URL = 'https://api.the-odds-api.com/v4/historical/sports/icehockey_nhl'

def fetch_and_store_nhl_games(date=None, enable_logging=False):
    """
    Fetch NHL events from the_odds API and store them in the PostgreSQL database.
    Uses historical API if date is provided, otherwise uses current events API.

    Args:
        date (str, optional): The date to fetch events for in 'YYYY-MM-DD' format in CST timezone. If not provided, uses current events.
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        list: A list of event dictionaries retrieved from the API.
    """
    if enable_logging:
        logging.info(f"Fetching and storing NHL events for date: {date}")

    # Get current time in CST
    cst = ZoneInfo("America/Chicago")
    today_cst = datetime.now(cst).strftime('%Y-%m-%d')

    # Determine if we should use historical API
    use_historical = date is not None and date != today_cst

    # Convert input date to datetime objects in CST
    if date:
        # Convert CST date to UTC for API
        start_time = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=cst)
        end_time = start_time + timedelta(days=1) - timedelta(seconds=1)
        
        # Convert to UTC for API
        start_time_utc = start_time.astimezone(ZoneInfo("UTC"))
        end_time_utc = end_time.astimezone(ZoneInfo("UTC"))

    # Construct API URL for NHL events
    if use_historical:
        query_params = {
            'apiKey': API_KEY,
            'date': end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        query_string = urllib.parse.urlencode(query_params)
        url = f"{API_HISTORICAL_URL}/events?{query_string}"
        if enable_logging:
            logging.info(f"Using historical API for date: {date}")
    else:
        query_params = {
            'apiKey': API_KEY,
            'commenceTimeFrom': start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commenceTimeTo': end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        query_string = urllib.parse.urlencode(query_params)
        url = f"{API_BASE_URL}/events?{query_string}"
        if enable_logging:
            logging.info("Using current events API")
    
    # Get the events data from the API
    response_data = get_request(url)
    
    # For historical API, the events are nested in the 'data' field
    events_data = response_data.get('data', response_data) if use_historical else response_data

    if not events_data:
        if enable_logging:
            logging.warning("No data retrieved from API.")
        return

    # Establish a connection using the helper function
    conn, cursor = get_db_connection('THE_ODDS_DB_')

    if not conn or not cursor:
        if enable_logging:
            logging.error("Failed to establish a database connection.")
        return

    try:
        # Define the insert query
        insert_query = """
            INSERT INTO game_info (
                id,
                sport_key,
                sport_title,
                home_team,
                away_team,
                commence_time
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE
            SET 
                sport_key = EXCLUDED.sport_key,
                sport_title = EXCLUDED.sport_title,
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                commence_time = EXCLUDED.commence_time,
                last_updated = CURRENT_TIMESTAMP;
        """

        # Prepare data for insertion
        records_to_insert = [
            (
                event['id'],
                'icehockey_nhl',  # Hardcoded since we're only dealing with NHL
                'NHL',            # Hardcoded sport title
                event['home_team'],
                event['away_team'],
                event['commence_time']
            )
            for event in events_data
        ]

        # Use execute_values for efficient bulk insertion
        execute_values(cursor, insert_query, records_to_insert)

        # Commit the transaction
        conn.commit()
        if enable_logging:
            logging.info(f"Inserted/Updated {len(records_to_insert)} records into game_info table.")

    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while inserting data into the database: %s", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    if enable_logging:
        logging.info("Completed fetching and storing events.")
    return events_data

def get_nhl_events_from_db(query_date=None, enable_logging=False):
    """
    Retrieve NHL events from the PostgreSQL the_odds database for a given date.

    Args:
        query_date (str, optional): The date to query in 'YYYY-MM-DD' format. Defaults to today.
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        list: A list of event dictionaries retrieved from the database.
    """
    if enable_logging:
        logging.info(f"Retrieving NHL events from DB for date: {query_date}")
    if query_date is None:
        query_date = datetime.now()
    else:
        query_date = datetime.strptime(query_date, '%Y-%m-%d')
    
    try:
        conn, cursor = get_db_connection('THE_ODDS_DB_')
        if not conn or not cursor:
            if enable_logging:
                logging.error("Failed to establish a database connection.")
            return []
        
        # Query using date range in local time (-06:00 for Central Time)
        query = """
            SELECT id, sport_key, sport_title, home_team, away_team, commence_time
            FROM game_info
            WHERE DATE(commence_time AT TIME ZONE 'America/Chicago') = %s::date;
        """
        cursor.execute(query, (query_date.strftime('%Y-%m-%d'),))
        rows = cursor.fetchall()
        
        events = []
        for row in rows:
            event = {
                'id': row[0],
                'sport_key': row[1],
                'sport_title': row[2],
                'home_team': row[3],
                'away_team': row[4],
                'commence_time': row[5]
            }
            events.append(event)
        
        if enable_logging:
            logging.info(f"Retrieved {len(events)} events from the database for date {query_date.strftime('%Y-%m-%d')}.")

        return events
    
    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while reading data from the database: %s", e)
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_player_sog_odds(player_name=None, query_date=None, sportsbook=None, team_name=None, line=False, fuzzy_threshold=85):
    """
    Retrieve player shots on goal odds from the PostgreSQL the_odds database with fuzzy matching for player names.

    Args:
        player_name (str, optional): The full name of the player to filter odds by.
        query_date (str, optional): The date to query in 'YYYY-MM-DD' format. Used to retrieve games.
        sportsbook (str, optional): The name of the sportsbook to filter odds by.
        team_name (str, optional): The full name of the team to filter games by.
        line (bool, optional): If True, filters odds to find those closest to +100. Defaults to False.
        fuzzy_threshold (int, optional): Minimum similarity score for fuzzy matching (0-100). Defaults to 85.

    Returns:
        list: A list of dictionaries containing player shots on goal odds.
    """
    logging.info(f"Retrieving player SOG odds for player: {player_name}, date: {query_date}, sportsbook: {sportsbook}, team: {team_name}, line: {line}")

    # Validate that at least one filter is provided
    if not player_name and not sportsbook and not team_name:
        print("At least one of player_name, sportsbook, or team_name must be provided.")
        return []

    # Retrieve games from the database for the given date
    games = get_nhl_events_from_db(query_date, enable_logging=True)

    if not games:
        print(f"No games found in the game_info table for date {query_date}.")
        return []

    # Filter games to find the game_id involving the specified team
    game_ids = []
    for game in games:
        if team_name in (game['away_team'], game['home_team']):
            game_ids.append(game['id'])  # Note: using 'id' instead of 'game_id' to match the_odds schema

    if not game_ids:
        print(f"No games found for team {team_name} on {query_date}.")
        return []

    try:
        # Establish a database connection
        conn, cursor = get_db_connection('THE_ODDS_DB_')
        if not conn or not cursor:
            print("Database connection failed.")
            return []

        # If player_name is provided, first get all distinct player names from the database
        # for the specified game_ids to perform fuzzy matching
        if player_name:
            cursor.execute("""
                SELECT DISTINCT player_name 
                FROM player_sog_odds 
                WHERE game_id = ANY(%s)
            """, (game_ids,))
            db_players = [row[0] for row in cursor.fetchall()]
            
            # Perform fuzzy matching to find the best match
            best_match = None
            best_score = 0
            for db_player in db_players:
                score = fuzz.ratio(player_name.lower(), db_player.lower())
                if score > best_score and score >= fuzzy_threshold:
                    best_score = score
                    best_match = db_player
            
            if best_match:
                logging.info(f"Fuzzy matched '{player_name}' to '{best_match}' with score {best_score}")
                player_name = best_match
            else:
                logging.warning(f"No fuzzy match found for player '{player_name}' above threshold {fuzzy_threshold}")
                return []

        # Initialize the base query
        base_query = """
            SELECT 
                pso.game_id,
                pso.sportsbook,
                pso.player_name,
                pso.market_type,
                pso.handicap,
                pso.price,
                pso.last_update
            FROM player_sog_odds pso
            WHERE pso.game_id = ANY(%s)
        """

        # Initialize parameters list with the game_ids
        params = [game_ids]

        # Add filters based on provided arguments
        if player_name:
            base_query += " AND pso.player_name = %s"  # Use exact match since we've already done fuzzy matching
            params.append(player_name)
        
        if sportsbook:
            base_query += " AND pso.sportsbook ILIKE %s"
            params.append(sportsbook)

        cursor.execute(base_query, tuple(params))
        rows = cursor.fetchall()

        # Process the retrieved rows
        odds_dict = {}
        for row in rows:
            odds = {
                'game_id': row[0],
                'sportsbook': row[1],
                'player': row[2],
                'market_type': row[3],
                'handicap': row[4],
                'price': row[5],
                'timestamp': row[6]
            }
            key = (odds['sportsbook'], odds['market_type'])
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
            print(f"No odds found in player_sog_odds table for game_ids {game_ids} with filters: {filter_str}")
            return []

        # Filter for odds closest to +100 if the line flag is set
        result = filter_odds_closest_to_100(odds_dict) if line else [odds for odds_list in odds_dict.values() for odds in odds_list]
        logging.info("Completed retrieving player SOG odds.")
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

def process_sog_markets(game_id, query_date=None, enable_logging=False):
    """
    Fetches and processes player shots on goal markets from the_odds API for a specific game.
    Uses historical odds if query_date is not today.
    
    Args:
        game_id (str): The game ID to fetch markets for.
        query_date (datetime or str, optional): The datetime in UTC for historical odds, or date string in 'YYYY-MM-DD' format.
                                              If not provided or is today, uses live odds.
        enable_logging (bool): If True, enables logging. Defaults to False.
    """
    if enable_logging:
        logging.info(f"Processing SOG markets for game: {game_id}")

    # Get current timestamp without fractional seconds for scraped_at
    current_time = datetime.now().replace(microsecond=0)

    # Check if game exists in game_info and get its commence_time
    if query_date:
        # Extract date part if it's a datetime string with timezone
        if isinstance(query_date, str) and 'T' in query_date:
            query_date = query_date.split('T')[0]
    games = get_nhl_events_from_db(query_date, enable_logging=enable_logging)
    game_info = next((game for game in games if game['id'] == game_id), None)
    
    if game_info:
        # Convert commence_time to UTC ISO8601 string
        query_date = game_info['commence_time'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        if enable_logging:
            logging.info(f"Using commence_time from game_info: {query_date}")

    # Determine if we should use historical odds
    today = datetime.now().strftime('%Y-%m-%d')
    use_historical = query_date is not None and (
        isinstance(query_date, datetime) or 
        (isinstance(query_date, str) and query_date != today)
    )

    # Construct API URL for player shots on goal markets
    query_params = {
        'apiKey': API_KEY,
        'regions': 'us',
        'markets': 'player_shots_on_goal',
        'oddsFormat': 'american'
    }

    # Add date parameter for historical odds
    if use_historical:
        # If query_date is in ISO8601 format, use it directly
        if 'T' in query_date and 'Z' in query_date:
            query_params['date'] = query_date
        else:
            # Convert date string to timestamp format required by historical API
            query_date_obj = datetime.strptime(query_date, '%Y-%m-%d')
            query_params['date'] = query_date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        base_url = f"{API_HISTORICAL_URL}/events/{game_id}/odds"
        if enable_logging:
            logging.info(f"Using historical odds for date: {query_params['date']}")
    else:
        base_url = f"{API_BASE_URL}/events/{game_id}/odds"
        if enable_logging:
            logging.info("Using live odds")
    
    # Rest of the function remains unchanged
    query_string = urllib.parse.urlencode(query_params)
    url = f"{base_url}?{query_string}"
    
    # Get the markets data from the API
    response_data = get_request(url)
    
    # For historical odds, the actual odds data is nested in the 'data' field
    markets_data = response_data.get('data', response_data) if use_historical else response_data

    if not markets_data or 'bookmakers' not in markets_data:
        if enable_logging:
            logging.warning(f"No market data found for game {game_id}")
        return

    # Establish a database connection
    conn, cursor = get_db_connection('THE_ODDS_DB_')
    if not conn or not cursor:
        if enable_logging:
            logging.error("Failed to establish a database connection.")
        return

    try:
        # Define the insert query
        insert_query = """
            INSERT INTO player_sog_odds (
                game_id,
                sportsbook,
                player_name,
                market_type,
                handicap,
                price,
                last_update,
                scraped_at
            )
            VALUES %s
            ON CONFLICT (game_id, sportsbook, player_name, market_type, handicap) 
            DO UPDATE SET
                price = EXCLUDED.price,
                last_update = EXCLUDED.last_update,
                scraped_at = CURRENT_TIMESTAMP(0);
        """

        # Process bookmakers and their markets
        records_to_insert = []
        for bookmaker in markets_data['bookmakers']:
            sportsbook = bookmaker['key']
            last_update = bookmaker['last_update']  # Get bookmaker's last_update

            for market in bookmaker['markets']:
                if market['key'] != 'player_shots_on_goal':
                    continue

                for outcome in market['outcomes']:
                    player_name = outcome['description']  # Player name is in description
                    market_type = outcome['name'].lower()  # 'Over' or 'Under'
                    handicap = float(outcome['point'])  # Using point for handicap
                    price = int(outcome['price'])  # American odds format

                    record = (
                        game_id,
                        sportsbook,
                        player_name,
                        market_type,
                        handicap,
                        price,
                        last_update,
                        current_time  # Add scraped_at timestamp
                    )
                    records_to_insert.append(record)

        if records_to_insert:
            # Use execute_values for efficient bulk insertion
            execute_values(cursor, insert_query, records_to_insert)
            conn.commit()
            if enable_logging:
                logging.info(f"Inserted/Updated {len(records_to_insert)} records for game {game_id}")
        else:
            if enable_logging:
                logging.warning(f"No records to insert for game {game_id}")

    except Exception as e:
        if enable_logging:
            logging.error(f"An error occurred while processing game {game_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    if enable_logging:
        logging.info(f"Completed processing SOG markets for game {game_id}")

def process_all_sog_markets(query_date=None, enable_logging=False):
    """
    Process SOG markets for all games on a given date.
    
    Args:
        query_date (str, optional): The date to process in 'YYYY-MM-DD' format. Defaults to today.
        enable_logging (bool): If True, enables logging. Defaults to False.
    """
    if enable_logging:
        logging.info(f"Processing all SOG markets for date: {query_date}")

    # Get all games for the date
    games = get_nhl_events_from_db(query_date, enable_logging=enable_logging)
    
    # If no games found, try to fetch and store them first
    if not games:
        if enable_logging:
            logging.info(f"No games found for date {query_date}, attempting to fetch from API")
        fetch_and_store_nhl_games(query_date, enable_logging=enable_logging)
        # Try to get games again after fetching
        games = get_nhl_events_from_db(query_date, enable_logging=enable_logging)
        
    if not games:
        if enable_logging:
            logging.warning(f"No games found for date {query_date} even after fetching from API")
        return

    # Process markets for each game using its commence_time
    for game in games:
        # Convert commence_time to UTC ISO8601 string
        utc_time = game['commence_time'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        if enable_logging:
            logging.info(f"Processing game {game['id']} scheduled for {utc_time}")
        process_sog_markets(game['id'], query_date=utc_time, enable_logging=enable_logging)

    if enable_logging:
        logging.info(f"Completed processing all SOG markets for date {query_date}") 

def get_mismatched_game_ids_with_details(enable_logging=False):
    """
    Compare distinct game_ids in game_info and player_sog_odds tables
    and return game_ids that are not present in both tables, along with
    additional details for missing games in game_info.

    Args:
        enable_logging (bool): If True, enables logging. Defaults to False.

    Returns:
        dict: A dictionary with keys 'only_in_game_info' and 'only_in_player_sog_odds'
              containing lists of game_ids not present in both tables. 'only_in_game_info'
              includes additional details like away_team, home_team, and commence_time.
    """
    if enable_logging:
        logging.info("Comparing distinct game_ids in game_info and player_sog_odds tables.")
    try:
        # Establish a database connection
        conn, cursor = get_db_connection('THE_ODDS_DB_')
        if not conn or not cursor:
            if enable_logging:
                logging.error("Failed to establish a database connection.")
            return {'only_in_game_info': [], 'only_in_player_sog_odds': []}

        # Query distinct game_ids from both tables
        cursor.execute("SELECT DISTINCT id FROM game_info")
        game_info_ids = {row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT DISTINCT game_id FROM player_sog_odds")
        player_sog_odds_ids = {row[0] for row in cursor.fetchall()}

        # Find game_ids not present in both tables
        only_in_game_info_ids = game_info_ids - player_sog_odds_ids
        only_in_player_sog_odds = player_sog_odds_ids - game_info_ids

        # Fetch additional details for game_ids only in game_info
        only_in_game_info = []
        if only_in_game_info_ids:
            cursor.execute("""
                SELECT id, home_team, away_team, commence_time
                FROM game_info
                WHERE id = ANY(%s)
            """, (list(only_in_game_info_ids),))
            only_in_game_info = cursor.fetchall()

        if enable_logging:
            logging.info(f"Game IDs only in game_info: {only_in_game_info}")
            logging.info(f"Game IDs only in player_sog_odds: {only_in_player_sog_odds}")

        if enable_logging:
            logging.info("Completed comparison of game_ids.")
            
        return {
            'only_in_game_info': [
                {
                    'game_id': row[0],
                    'away_team': row[2],
                    'home_team': row[1],
                    'commence_time': row[3].strftime('%Y-%m-%d')
                }
                for row in only_in_game_info
            ],
            'only_in_player_sog_odds': list(only_in_player_sog_odds)
        }

    except Exception as e:
        if enable_logging:
            logging.error("An error occurred while comparing game_ids: %s", e)
        return {'only_in_game_info': [], 'only_in_player_sog_odds': []}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def filter_odds_closest_to_100(odds_dict):
    """
    Filters odds to find those closest to +100 in decimal format.
    For each sportsbook, finds the most recent handicap when Over/Under don't match,
    and returns both Over and Under for that handicap.

    Args:
        odds_dict (dict): A dictionary of odds categorized by sportsbook and market_type.

    Returns:
        list: A list of dictionaries containing the filtered odds.
    """
    # First, get the most recent lines for each unique combination
    most_recent_odds = {}
    for key, odds_list in odds_dict.items():
        for odd in odds_list:
            # Create a unique key for each combination
            unique_key = (odd['game_id'], odd['sportsbook'], odd['player'], odd['market_type'], odd['handicap'])
            
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
        
        side = 'over' if odd['market_type'].lower().startswith('over') else 'under'
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