import logging
import pandas as pd
import random
import time
from typing import Optional
from datetime import datetime, timedelta

from src.db.base_utils import connect_db, disconnect_db
from src.data_processing.nst_scraper import nst_on_ice_scraper, nst_team_on_ice_scraper
from src.data_processing.season_utils import get_season_for_date, NHL_SEASONS, get_season_end_date
from src.data_processing.team_utils import get_tricode_by_fullname

logger = logging.getLogger(__name__)

def insert_goalie_stats_df(df: pd.DataFrame, conn, cursor, table_name: str = "goalie_stats") -> None:
    """
    Insert goalie stats dataframe into database using psycopg2.
    
    This revised version cleans and maps the DataFrame's columns to match the new schema.
    It now expects the following columns (after cleaning):
      - player, team, gp, toi,
      - shots_against, saves, goals_against, sv_pct,
      - gaa, gsaa, xg_against, hd_shots_against, hd_saves,
      - hd_goals_against, hdsv_pct, hdgaa, hdgsaa,
      - md_shots_against, md_saves, md_goals_against, mdsv_pct,
      - mdgaa, mdgsaa, ld_shots_against, ld_saves, ld_goals_against,
      - ldsv_pct, ldgaa, ldgsaa, rush_attempts_against, rebound_attempts_against,
      - avg_shot_distance, avg_goal_distance,
      - date
      
    Args:
        df: DataFrame containing goalie stats
        conn: Database connection
        cursor: Database cursor
        table_name: Name of the table to insert into (default: "goalie_stats")
    """
    # Clean column names to match PostgreSQL table
    df.columns = (
        df.columns
        .str.replace('/', '_per_', regex=False)      # In case any '/' exists
        .str.replace('%', '_pct', regex=False)         # Replace % with _pct
        .str.replace(r'[^a-zA-Z0-9]', '_', regex=True)  # Replace other special characters
        .str.replace(r'_+', '_', regex=True)            # Collapse multiple underscores
        .str.strip('_')
        .str.lower()
    )
    
    # Replace placeholder '-' with None so that numeric columns are handled properly
    df.replace("-", None, inplace=True)
    
    # Convert expected numeric columns explicitly
    numeric_columns = [
        'gp', 'toi', 'shots_against', 'saves', 'goals_against', 'sv_pct',
        'gaa', 'gsaa', 'xg_against', 'hd_shots_against', 'hd_saves', 'hd_goals_against',
        'hdsv_pct', 'hdgaa', 'hdgsaa', 'md_shots_against', 'md_saves', 'md_goals_against',
        'mdsv_pct', 'mdgaa', 'mdgsaa', 'ld_shots_against', 'ld_saves', 'ld_goals_against',
        'ldsv_pct', 'ldgaa', 'ldgsaa', 'rush_attempts_against', 'rebound_attempts_against',
        'avg_shot_distance', 'avg_goal_distance'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Log cleaned columns for verification
    logger.info(f"Cleaned columns: {df.columns.tolist()}")
    
    # Ensure required columns are present
    required_columns = [
        'player', 'team', 'gp', 'toi',
        'shots_against', 'saves', 'goals_against', 'sv_pct',
        'gaa', 'gsaa', 'xg_against', 'hd_shots_against', 'hd_saves',
        'hd_goals_against', 'hdsv_pct', 'hdgaa', 'hdgsaa',
        'md_shots_against', 'md_saves', 'md_goals_against', 'mdsv_pct',
        'mdgaa', 'mdgsaa', 'ld_shots_against', 'ld_saves', 'ld_goals_against',
        'ldsv_pct', 'ldgaa', 'ldgsaa', 'rush_attempts_against', 'rebound_attempts_against',
        'avg_shot_distance', 'avg_goal_distance',
        'date'
    ]
    
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logger.error(f"Missing columns after cleaning: {missing}")
        raise KeyError(f"Missing columns: {missing}")
    
    # First, delete any existing records for the date we're inserting
    # This ensures we don't have duplicates
    delete_query = f"""
    DELETE FROM {table_name}
    WHERE date = %(date)s;
    """
    
    # Get unique dates from the DataFrame
    unique_dates = df['date'].unique()
    for date in unique_dates:
        cursor.execute(delete_query, {'date': date})
    
    # Prepare the INSERT statement with dynamic table name
    insert_query = f"""
    INSERT INTO {table_name} (
        player, team, gp, toi,
        shots_against, saves, goals_against, sv_pct,
        gaa, gsaa, xg_against, hd_shots_against, hd_saves,
        hd_goals_against, hdsv_pct, hdgaa, hdgsaa,
        md_shots_against, md_saves, md_goals_against, mdsv_pct,
        mdgaa, mdgsaa, ld_shots_against, ld_saves, ld_goals_against,
        ldsv_pct, ldgaa, ldgsaa, rush_attempts_against, rebound_attempts_against,
        avg_shot_distance, avg_goal_distance,
        date
    ) VALUES (
        %(player)s, %(team)s, %(gp)s, %(toi)s,
        %(shots_against)s, %(saves)s, %(goals_against)s, %(sv_pct)s,
        %(gaa)s, %(gsaa)s, %(xg_against)s, %(hd_shots_against)s, %(hd_saves)s,
        %(hd_goals_against)s, %(hdsv_pct)s, %(hdgaa)s, %(hdgsaa)s,
        %(md_shots_against)s, %(md_saves)s, %(md_goals_against)s, %(mdsv_pct)s,
        %(mdgaa)s, %(mdgsaa)s, %(ld_shots_against)s, %(ld_saves)s, %(ld_goals_against)s,
        %(ldsv_pct)s, %(ldgaa)s, %(ldgsaa)s, %(rush_attempts_against)s, %(rebound_attempts_against)s,
        %(avg_shot_distance)s, %(avg_goal_distance)s,
        %(date)s
    );
    """
    
    # Convert DataFrame to a list of dictionaries for batch insertion
    records = df.to_dict('records')
    cursor.executemany(insert_query, records)
    conn.commit()

def get_goalie_stats(
    goalie_name: Optional[str] = None,
    team: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    last_n: Optional[int] = None,
    db_prefix: str = "NST_DB_",
    table_name: str = "goalie_stats_all"
) -> pd.DataFrame:
    """
    Retrieve goalie stats from the database with optional filters.
    
    Args:
        goalie_name: Optional name of specific goalie to query
        team: Optional team name to filter by
        start_date: Optional start date for date range
        end_date: Optional end date for date range
        last_n: Optional number of days to look back from end_date, will span across seasons if needed
        db_prefix: Database environment variable prefix
        table_name: Name of the table to query (default: "goalie_stats_all")
    Returns:
        DataFrame containing goalie statistics
    """
    conditions = []
    params = []
    
    if goalie_name:
        conditions.append("player = %s")
        params.append(goalie_name)
    if team:
        conditions.append("team = %s")
        params.append(team)
        
    # Handle date filtering with last_n logic
    if last_n is not None:
        if end_date is None:
            # If no end_date provided, use current date
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        try:
            # Calculate start_date based on last_n days with season spanning logic
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            end_season = get_season_for_date(end_date)
            
            if end_season in NHL_SEASONS:
                season_start_obj = datetime.strptime(NHL_SEASONS[end_season]['start'], '%Y-%m-%d')
                
                # Calculate days since season start
                days_since_season_start = (end_date_obj - season_start_obj).days
                
                if days_since_season_start < last_n:
                    # Need to go into previous season
                    prev_season = end_season - 10001  # e.g., 20242025 -> 20232024
                    if prev_season in NHL_SEASONS:
                        # Calculate remaining days to look back in previous season
                        remaining_days = last_n - days_since_season_start
                        prev_season_end = get_season_end_date(prev_season, 2)  # Using stype=2 for regular season
                        prev_season_end_obj = datetime.strptime(prev_season_end, '%Y-%m-%d')
                        start_date_obj = prev_season_end_obj - timedelta(days=remaining_days)
                        start_date = start_date_obj.strftime('%Y-%m-%d')
                    else:
                        # If no previous season data, just go back from season start
                        start_date_obj = season_start_obj - timedelta(days=last_n)
                        start_date = start_date_obj.strftime('%Y-%m-%d')
                else:
                    # All dates within current season
                    start_date_obj = end_date_obj - timedelta(days=last_n)
                    start_date = start_date_obj.strftime('%Y-%m-%d')
            else:
                # If season not found, just do simple date arithmetic
                start_date_obj = end_date_obj - timedelta(days=last_n)
                start_date = start_date_obj.strftime('%Y-%m-%d')
                
        except ValueError as e:
            logger.error(f"Error calculating dates: {e}")
            # Fall back to simple date arithmetic if season logic fails
            start_date_obj = end_date_obj - timedelta(days=last_n)
            start_date = start_date_obj.strftime('%Y-%m-%d')
        
    if start_date:
        conditions.append("date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("date <= %s")
        params.append(end_date)
        
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
        SELECT 
            date,
            player,
            team,
            toi,
            shots_against,
            saves,
            goals_against,
            sv_pct,
            gaa,
            gsaa,
            xg_against,
            hd_shots_against,
            hd_saves,
            hd_goals_against,
            hdsv_pct,
            md_shots_against,
            md_saves,
            md_goals_against,
            mdsv_pct,
            ld_shots_against,
            ld_saves,
            ld_goals_against,
            ldsv_pct,
            rush_attempts_against,
            rebound_attempts_against,
            avg_shot_distance,
            avg_goal_distance
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY date DESC
    """
    
    conn = None
    try:
        conn = connect_db(db_prefix)
        cur = conn.cursor()
        
        # Execute query
        cur.execute(query, params)
        
        # Fetch column names from cursor description
        columns = [desc[0] for desc in cur.description]
        
        # Fetch all results and create DataFrame
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=columns)
        
        return df
        
    except Exception as e:
        logger.error(f"Error retrieving goalie stats: {e}")
        # Return an empty DataFrame with the expected columns if there's an error
        return pd.DataFrame(columns=[
            'date', 'player', 'team', 'toi', 'shots_against', 'saves',
            'goals_against', 'sv_pct', 'gaa', 'gsaa', 'xg_against',
            'hd_shots_against', 'hd_saves', 'hd_goals_against', 'hdsv_pct',
            'md_shots_against', 'md_saves', 'md_goals_against', 'mdsv_pct',
            'ld_shots_against', 'ld_saves', 'ld_goals_against', 'ldsv_pct',
            'rush_attempts_against', 'rebound_attempts_against',
            'avg_shot_distance', 'avg_goal_distance'
        ])
    finally:
        if conn:
            cur.close()
            disconnect_db(conn)

def get_goalie_rolling_stats(
    goalie_name: str,
    date: str,
    n_games: int = 5,
    db_prefix: str = "NST_DB_",
    table_name: str = "goalie_stats_all"
) -> pd.DataFrame:
    """
    Get rolling average stats for a specific goalie up to a given date.
    
    Args:
        goalie_name: Name of the goalie
        date: Date to get stats up to
        n_games: Number of games to look back
        db_prefix: Database environment variable prefix
        table_name: Name of the table to query (default: "goalie_stats_all")
    Returns:
        DataFrame with rolling average statistics
    """
    query = f"""
        WITH recent_games AS (
            SELECT *
            FROM {table_name}
            WHERE player = %s
            AND date <= %s
            ORDER BY date DESC
            LIMIT %s
        )
        SELECT 
            player,
            AVG(sv_pct) as avg_sv_pct,
            AVG(hdsv_pct) as avg_hd_sv_pct,
            AVG(mdsv_pct) as avg_md_sv_pct,
            AVG(ldsv_pct) as avg_ld_sv_pct,
            AVG(hd_shots_against) as avg_hd_shots,
            AVG(md_shots_against) as avg_md_shots,
            AVG(ld_shots_against) as avg_ld_shots,
            AVG(gsaa) as avg_gsaa
        FROM recent_games
        GROUP BY player
    """
    
    conn = None
    try:
        conn = connect_db(db_prefix)
        cur = conn.cursor()
        
        # Execute query
        cur.execute(query, [goalie_name, date, n_games])
        
        # Fetch column names and results
        columns = [desc[0] for desc in cur.description]
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=columns)
        
        return df
        
    except Exception as e:
        logger.error(f"Error retrieving rolling stats: {e}")
        raise
    finally:
        if conn:
            cur.close()
            disconnect_db(conn)

def get_goalie_comparison(
    date: str,
    n_games: int = 5,
    min_games: int = 3,
    db_prefix: str = "NST_DB_",
    table_name: str = "goalie_stats_all"
) -> pd.DataFrame:
    """
    Get comparison stats for all goalies with recent activity.
    
    Args:
        date: Date to get stats up to
        n_games: Number of games to look back
        min_games: Minimum number of games played to be included
        db_prefix: Database environment variable prefix
        table_name: Name of the table to query (default: "goalie_stats_all")
    Returns:
        DataFrame with comparison statistics for all active goalies
    """
    query = f"""
        WITH recent_games AS (
            SELECT 
                player,
                COUNT(*) as games_played,
                AVG(sv_pct) as avg_sv_pct,
                AVG(hdsv_pct) as avg_hd_sv_pct,
                AVG(mdsv_pct) as avg_md_sv_pct,
                AVG(ldsv_pct) as avg_ld_sv_pct,
                AVG(hd_shots_against) as avg_hd_shots,
                AVG(md_shots_against) as avg_md_shots,
                AVG(ld_shots_against) as avg_ld_shots,
                AVG(gsaa) as avg_gsaa
            FROM {table_name}
            WHERE date <= %s
            GROUP BY player
            HAVING COUNT(*) >= %s
            ORDER BY avg_sv_pct DESC
            LIMIT %s
        )
        SELECT *
        FROM recent_games
    """
    
    conn = None
    try:
        conn = connect_db(db_prefix)
        cur = conn.cursor()
        
        # Execute query
        cur.execute(query, [date, min_games, n_games])
        
        # Fetch column names and results
        columns = [desc[0] for desc in cur.description]
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=columns)
        
        return df
        
    except Exception as e:
        logger.error(f"Error retrieving goalie comparison: {e}")
        raise
    finally:
        if conn:
            cur.close()
            disconnect_db(conn)

def scrape_goalie_stats_range(
    start_date: str,
    end_date: str,
    db_prefix: str = "NST_DB_",
    delay_min: int = 3,
    delay_max: int = 7,
    situation: str = "all"
) -> None:
    """
    Scrape goalie stats across a date range and save to the database.
    
    This function iterates through each day in the provided date range.
    For each day, it calls the scraper to retrieve data by setting both startdate
    and enddate to the same value, then inserts that day's records into the database.
    The conflict target is (player, date).
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        db_prefix: Prefix for database environment variables
        delay_min: Minimum delay between requests (seconds)
        delay_max: Maximum delay between requests (seconds)
        situation: The game situation to scrape ('all', '5v5', or 'pk'). Determines which table to use.
    """
    # Determine table name based on situation
    if situation == "all":
        table_name = "goalie_stats_all"
    elif situation == "5v5":
        table_name = "goalie_stats_5v5"
    elif situation == "pk":
        table_name = "goalie_stats_pk"
    else:
        raise ValueError(f"Invalid situation: {situation}. Must be one of: 'all', '5v5', 'pk'")
    
    # Convert dates to datetime objects
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Initialize counters for logging purposes
    successful_scrapes = 0
    failed_scrapes = 0
    
    current_date = start
    while current_date <= end:
        conn = None
        cursor = None
        try:
            # Format the current day as a string
            current_date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"Scraping data for date: {current_date_str}")
            
            # Scrape data for the day
            goalie_stats_df = nst_on_ice_scraper(
                startdate=current_date_str,
                enddate=current_date_str,
                sit=situation,
                pos='G',
                rate='n',
                stdoi='g',
                lines='single'
            )
            
            # Add date information
            goalie_stats_df['date'] = current_date.date()
            
            # Add season information for logging
            year = current_date.year
            month = current_date.month
            season = f"{year-1}-{str(year)[2:]}" if month < 7 else f"{year}-{str(year+1)[2:]}"
            goalie_stats_df['season'] = season
            
            # Log details about the returned DataFrame
            logger.info(f"Goalie Stats DataFrame shape: {goalie_stats_df.shape}")
            logger.info(f"Goalie Stats DataFrame columns: {goalie_stats_df.columns.tolist()}")
            
            # Connect to the database
            conn = connect_db(db_prefix)
            if conn is None:
                raise Exception("Failed to establish database connection")
            cursor = conn.cursor()
            
            # Insert (or update) the day's data with the appropriate table name
            insert_goalie_stats_df(goalie_stats_df, conn, cursor, table_name)
            
            successful_scrapes += 1
            logger.info(f"Successfully saved data for {current_date_str}")
        except Exception as e:
            failed_scrapes += 1
            logger.error(f"Error processing data for {current_date_str}: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            # Close the database cursor and connection
            if cursor:
                cursor.close()
            if conn:
                disconnect_db(conn)
            
            # If not processing the last date, wait a random delay
            if current_date < end:
                delay = random.uniform(delay_min, delay_max)
                logger.info(f"Waiting {delay:.1f} seconds before next request...")
                time.sleep(delay)
            
            # Move on to the next day
            current_date += timedelta(days=1)
    
    # Log the final summary
    logger.info(f"""
    Scraping completed:
    - Successful scrapes: {successful_scrapes}
    - Failed scrapes: {failed_scrapes}
    - Date range: {start_date} to {end_date}
    - Table: {table_name}
    """)

def insert_team_stats_df(df: pd.DataFrame, conn, cursor, table_name: str = "team_stats") -> None:
    """
    Insert team stats dataframe into database using psycopg2.
    
    This function cleans and maps the DataFrame's columns to match the team stats schema.
    
    Args:
        df: DataFrame containing team stats
        conn: Database connection
        cursor: Database cursor
        table_name: Name of the table to insert into (default: "team_stats")
    """
    # Clean column names to match PostgreSQL table
    df.columns = (
        df.columns
        .str.replace('/', '_per_', regex=False)      # In case any '/' exists
        .str.replace('%', '_pct', regex=False)       # Replace % with _pct
        .str.replace(r'[^a-zA-Z0-9]', '_', regex=True)  # Replace other special characters
        .str.replace(r'_+', '_', regex=True)         # Collapse multiple underscores
        .str.strip('_')
        .str.lower()
    )
    
    # Replace placeholder '-' with None so that numeric columns are handled properly
    df.replace("-", None, inplace=True)
    
    # Convert all numeric columns explicitly
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert season format from string (e.g., "2023-24") to integer (e.g., 20232024)
    if 'season' in df.columns:
        def convert_season(season_str):
            if pd.isna(season_str) or season_str is None:
                return None
            try:
                # Handle string format like "2023-24"
                if isinstance(season_str, str) and '-' in season_str:
                    start_year, end_year = season_str.split('-')
                    if len(start_year) == 4 and len(end_year) == 2:
                        # Convert "2023-24" to 20232024
                        return int(start_year + '20' + end_year)
                    else:
                        # Try to handle other formats or return None if can't convert
                        logger.warning(f"Unexpected season format: {season_str}")
                        return None
                # If it's already an integer, return as is
                elif isinstance(season_str, (int, float)):
                    return int(season_str)
                else:
                    logger.warning(f"Unexpected season type: {type(season_str)}, value: {season_str}")
                    return None
            except Exception as e:
                logger.error(f"Error converting season '{season_str}': {e}")
                return None
        
        df['season'] = df['season'].apply(convert_season)
    
    # Log cleaned columns for verification
    logger.info(f"Cleaned columns: {df.columns.tolist()}")
    
    # First, delete any existing records for the date we're inserting
    # This ensures we don't have duplicates
    delete_query = f"""
    DELETE FROM {table_name}
    WHERE date = %(date)s;
    """
    
    # Get unique dates from the DataFrame
    unique_dates = df['date'].unique()
    for date in unique_dates:
        cursor.execute(delete_query, {'date': date})
    
    # Dynamically create the INSERT statement based on the DataFrame columns
    columns = df.columns.tolist()
    placeholders = [f"%({col})s" for col in columns]
    
    insert_query = f"""
    INSERT INTO {table_name} (
        {', '.join(columns)}
    ) VALUES (
        {', '.join(placeholders)}
    );
    """
    
    # Convert DataFrame to a list of dictionaries for batch insertion
    records = df.to_dict('records')
    cursor.executemany(insert_query, records)
    conn.commit()

def scrape_team_stats_range(
    start_date: str,
    end_date: str,
    db_prefix: str = "NST_DB_",
    delay_min: int = 3,
    delay_max: int = 7,
    situation: str = "all",
    stype: int = 2
) -> None:
    """
    Scrape team stats across a date range and save to the database.
    
    This function iterates through each day in the provided date range.
    For each day, it calls the team scraper to retrieve data by setting both startdate
    and enddate to the same value, then inserts that day's records into the database.
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        db_prefix: Prefix for database environment variables
        delay_min: Minimum delay between requests (seconds)
        delay_max: Maximum delay between requests (seconds)
        situation: The game situation to scrape ('all', '5v5', 'pk', or 'pp'). Determines which table to use.
        stype: Type of statistics to retrieve. Defaults to 2 for regular season.
    """
    # Determine table name based on situation
    if situation == "all":
        table_name = "team_stats_all"
    elif situation == "5v5":
        table_name = "team_stats_5v5"
    elif situation == "pk":
        table_name = "team_stats_pk"
    elif situation == "pp":
        table_name = "team_stats_pp"
    else:
        raise ValueError(f"Invalid situation: {situation}. Must be one of: 'all', '5v5', 'pk', 'pp'")
    
    # Convert dates to datetime objects
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Initialize counters for logging purposes
    successful_scrapes = 0
    failed_scrapes = 0
    
    current_date = start
    while current_date <= end:
        conn = None
        cursor = None
        try:
            # Format the current day as a string
            current_date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"Scraping team data for date: {current_date_str}")
            
            # Scrape data for the day using nst_team_on_ice_scraper
            # Set both startdate and enddate to the same value to ensure correct URL construction
            team_stats_df = nst_team_on_ice_scraper(
                startdate=current_date_str,
                enddate=current_date_str,
                sit=situation,
                stype=stype
            )
            
            # Add date information
            team_stats_df['date'] = current_date.date()
            
            # Add season information for logging
            year = current_date.year
            month = current_date.month
            season = f"{year-1}-{str(year)[2:]}" if month < 7 else f"{year}-{str(year+1)[2:]}"
            team_stats_df['season'] = season
            
            # Log details about the returned DataFrame
            logger.info(f"Team Stats DataFrame shape: {team_stats_df.shape}")
            logger.info(f"Team Stats DataFrame columns: {team_stats_df.columns.tolist()}")
            
            # Connect to the database
            conn = connect_db(db_prefix)
            if conn is None:
                raise Exception("Failed to establish database connection")
            cursor = conn.cursor()
            
            # Insert (or update) the day's data with the appropriate table name
            insert_team_stats_df(team_stats_df, conn, cursor, table_name)
            
            successful_scrapes += 1
            logger.info(f"Successfully saved team data for {current_date_str}")
        except Exception as e:
            failed_scrapes += 1
            logger.error(f"Error processing team data for {current_date_str}: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            # Close the database cursor and connection
            if cursor:
                cursor.close()
            if conn:
                disconnect_db(conn)
            
            # If not processing the last date, wait a random delay
            if current_date < end:
                delay = random.uniform(delay_min, delay_max)
                logger.info(f"Waiting {delay:.1f} seconds before next request...")
                time.sleep(delay)
            
            # Move on to the next day
            current_date += timedelta(days=1)
    
    # Log the final summary
    logger.info(f"""
    Team stats scraping completed:
    - Successful scrapes: {successful_scrapes}
    - Failed scrapes: {failed_scrapes}
    - Date range: {start_date} to {end_date}
    - Table: {table_name}
    """) 