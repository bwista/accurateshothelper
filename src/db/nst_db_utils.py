import logging
import pandas as pd
import random
import time
from typing import Optional
from datetime import datetime, timedelta

from src.db.base_utils import connect_db, disconnect_db
from src.data_processing.nst_scraper import nst_on_ice_scraper

logger = logging.getLogger(__name__)

def insert_goalie_stats_df(df: pd.DataFrame, conn, cursor) -> None:
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
      
    The INSERT uses (player, date) as the conflict target.
    
    Args:
        df: DataFrame containing goalie stats
        conn: Database connection
        cursor: Database cursor
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
    
    # Prepare the INSERT statement
    insert_query = """
    INSERT INTO goalie_stats (
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
    )
    ON CONFLICT (player, date) DO UPDATE SET
        team = EXCLUDED.team,
        gp = EXCLUDED.gp,
        toi = EXCLUDED.toi,
        shots_against = EXCLUDED.shots_against,
        saves = EXCLUDED.saves,
        goals_against = EXCLUDED.goals_against,
        sv_pct = EXCLUDED.sv_pct,
        gaa = EXCLUDED.gaa,
        gsaa = EXCLUDED.gsaa,
        xg_against = EXCLUDED.xg_against,
        hd_shots_against = EXCLUDED.hd_shots_against,
        hd_saves = EXCLUDED.hd_saves,
        hd_goals_against = EXCLUDED.hd_goals_against,
        hdsv_pct = EXCLUDED.hdsv_pct,
        hdgaa = EXCLUDED.hdgaa,
        hdgsaa = EXCLUDED.hdgsaa,
        md_shots_against = EXCLUDED.md_shots_against,
        md_saves = EXCLUDED.md_saves,
        md_goals_against = EXCLUDED.md_goals_against,
        mdsv_pct = EXCLUDED.mdsv_pct,
        mdgaa = EXCLUDED.mdgaa,
        mdgsaa = EXCLUDED.mdgsaa,
        ld_shots_against = EXCLUDED.ld_shots_against,
        ld_saves = EXCLUDED.ld_saves,
        ld_goals_against = EXCLUDED.ld_goals_against,
        ldsv_pct = EXCLUDED.ldsv_pct,
        ldgaa = EXCLUDED.ldgaa,
        ldgsaa = EXCLUDED.ldgsaa,
        rush_attempts_against = EXCLUDED.rush_attempts_against,
        rebound_attempts_against = EXCLUDED.rebound_attempts_against,
        avg_shot_distance = EXCLUDED.avg_shot_distance,
        avg_goal_distance = EXCLUDED.avg_goal_distance;
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
    db_prefix: str = "NST_DB_"
) -> pd.DataFrame:
    """
    Retrieve goalie stats from the database with optional filters.
    
    Args:
        goalie_name: Optional name of specific goalie to query
        team: Optional team name to filter by
        start_date: Optional start date for date range
        end_date: Optional end date for date range
        db_prefix: Database environment variable prefix
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
        FROM goalie_stats
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
        raise
    finally:
        if conn:
            cur.close()
            disconnect_db(conn)

def get_goalie_rolling_stats(
    goalie_name: str,
    date: str,
    n_games: int = 5,
    db_prefix: str = "NST_DB_"
) -> pd.DataFrame:
    """
    Get rolling average stats for a specific goalie up to a given date.
    
    Args:
        goalie_name: Name of the goalie
        date: Date to get stats up to
        n_games: Number of games to look back
        db_prefix: Database environment variable prefix
    Returns:
        DataFrame with rolling average statistics
    """
    query = """
        WITH recent_games AS (
            SELECT *
            FROM goalie_stats
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
    db_prefix: str = "NST_DB_"
) -> pd.DataFrame:
    """
    Get comparison stats for all goalies with recent activity.
    
    Args:
        date: Date to get stats up to
        n_games: Number of games to look back
        min_games: Minimum number of games played to be included
        db_prefix: Database environment variable prefix
    Returns:
        DataFrame with comparison statistics for all active goalies
    """
    query = """
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
            FROM goalie_stats
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
    delay_max: int = 7
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
    """
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
                sit='all',
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
            
            # Insert (or update) the day's data
            insert_goalie_stats_df(goalie_stats_df, conn, cursor)
            
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
    """) 