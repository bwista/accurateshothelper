import pandas as pd
import requests
from io import StringIO
from datetime import datetime, timedelta

from src.data_processing.team_utils import nst_to_nhl_tricode
from src.data_processing.season_utils import get_season_for_date, NHL_SEASONS, get_season_end_date

def nst_on_ice_scraper(fromseason=None, thruseason=None, startdate='', enddate=None, last_n=None, stype=2, sit='5v5', stdoi='std', pos='S', rate='n', loc='B', lines='multi'):
    """
    Extracts player on-ice statistics from Natural Stat Trick for specified seasons and filtering conditions.

    Parameters:
        fromseason (int, optional): The starting season in the format YYYYYYYY (e.g., 20242025). If not provided, calculated from dates.
        thruseason (int, optional): The ending season in the format YYYYYYYY (e.g., 20242025). If not provided, calculated from dates.
        startdate (str, optional): The start date in the format YYYY-MM-DD (e.g., 2024-10-08). Defaults to empty string.
        enddate (str): The end date in the format YYYY-MM-DD (e.g., 2024-10-14). Required.
        last_n (int, optional): Number of days to look back from enddate. If provided, overrides startdate.
        stype (int, optional): Type of statistics to retrieve. Defaults to 2 for regular season.
        sit (str, optional): Situation type to filter by (e.g., '5v5', 'all', 'pk'). Defaults to '5v5'.
        stdoi (str, optional): Statistic type of interest. Use 'std' for individual stats, 'oi' for on-ice stats, or 'g' for goalies. Defaults to 'std'.
        pos (str, optional): Type of player statistics to retrieve. Use 'S' for skaters or 'G' for goalies. Defaults to 'S'.
        rate (str, optional): Stat type, rate or count. Use 'n' or 'y'. Defaults to 'n'.
        loc (str, optional): Location filter. Use 'B' for both home and away, 'H' for home, or 'A' for away. Defaults to 'B'.
        lines (str, optional): Whether to split or combine results for multi-team players. Use 'multi' for split and 'single' for combined. Defaults to 'multi'.

    Returns:
        df: A DataFrame containing the player on-ice statistics.

    Raises:
        ValueError: If `stdoi` or `pos` or `loc` has an invalid value, or if enddate is not provided.
        requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
        Exception: For any other errors that occur during the scraping process.
    """
    if stdoi not in ['std', 'oi', 'g']:
        raise ValueError("stdoi must be either 'std' for individual stats, 'oi' for on-ice stats, or 'g' for goalies.")
    if pos not in ['S', 'G']:
        raise ValueError("pos must be either 'S' for skaters or 'G' for goalies.")
    if loc not in ['B', 'H', 'A']:
        raise ValueError("loc must be 'B' for both home and away, 'H' for home, or 'A' for away.")
    if lines not in ['multi', 'single']:
        raise ValueError("lines must be either 'multi' for split results or 'single' for combined results.")
    if sit not in ['5v5', 'all', 'pk']:
        raise ValueError("sit must be one of: '5v5', 'all', 'pk'")
        
    # Override pos to 'S' when requesting goalie stats as the API requires this
    if stdoi == 'g':
        pos = 'S'
        
    # Handle date calculations
    if enddate is None:
        raise ValueError("enddate must be provided")
    
    try:
        end_season = get_season_for_date(enddate)
    except ValueError:
        # If enddate is between seasons, find the next season
        end_date_obj = datetime.strptime(enddate, '%Y-%m-%d')
        for season in sorted(NHL_SEASONS.keys()):
            season_start = datetime.strptime(NHL_SEASONS[season]['start'], '%Y-%m-%d')
            if end_date_obj < season_start:
                end_season = season
                break
        else:
            raise ValueError(f"Could not determine season for end date {enddate}")

    if last_n is not None:
        end_date_obj = datetime.strptime(enddate, '%Y-%m-%d')
        season_start_obj = datetime.strptime(NHL_SEASONS[end_season]['start'], '%Y-%m-%d')
        
        # Calculate days since season start
        days_since_season_start = (end_date_obj - season_start_obj).days
        
        if days_since_season_start < last_n:
            # Need to go into previous season
            prev_season = end_season - 10001  # e.g., 20242025 -> 20232024
            if prev_season in NHL_SEASONS:
                # Calculate remaining days to look back in previous season
                remaining_days = last_n - days_since_season_start
                prev_season_end = get_season_end_date(prev_season, stype)
                prev_season_end_obj = datetime.strptime(prev_season_end, '%Y-%m-%d')
                start_date_obj = prev_season_end_obj - timedelta(days=remaining_days)
                startdate = start_date_obj.strftime('%Y-%m-%d')
                start_season = prev_season
            else:
                # If no previous season data, just go back from season start
                start_date_obj = season_start_obj - timedelta(days=last_n)
                startdate = start_date_obj.strftime('%Y-%m-%d')
                start_season = end_season
        else:
            # All dates within current season
            start_date_obj = end_date_obj - timedelta(days=last_n)
            startdate = start_date_obj.strftime('%Y-%m-%d')
            start_season = end_season
    else:
        # If no last_n provided and startdate is empty, use end_season as start_season
        if not startdate:
            start_season = end_season
        else:
            try:
                start_season = get_season_for_date(startdate)
            except ValueError:
                # If startdate falls between seasons, use the previous season
                start_date_obj = datetime.strptime(startdate, '%Y-%m-%d')
                for season in sorted(NHL_SEASONS.keys()):
                    season_start = datetime.strptime(NHL_SEASONS[season]['start'], '%Y-%m-%d')
                    if start_date_obj < season_start:
                        start_season = season - 10001
                        # Adjust startdate to use the end of the previous season based on stype
                        startdate = get_season_end_date(start_season, stype)
                        break
                else:
                    raise ValueError(f"Could not determine season for start date {startdate}")

    # Set seasons if not provided
    if fromseason is None:
        fromseason = min(start_season, end_season)
    if thruseason is None:
        thruseason = max(start_season, end_season)

    url = (
        f"https://www.naturalstattrick.com/playerteams.php?"
        f"fromseason={fromseason}&thruseason={thruseason}&stype={stype}&sit={sit}"
        f"&score=all&stdoi={stdoi}&rate={rate}&team=ALL&pos={pos}&loc={loc}&toi=0"
        f"&gpfilt=gpdate&fd={startdate}&td={enddate}"
        f"&tgp=410&lines={lines}&draftteam=ALL"
    )

    # Add browser-like headers
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.8',
        'cache-control': 'no-cache',
        'connection': 'keep-alive',
        'host': 'www.naturalstattrick.com',
        'pragma': 'no-cache',
        'referer': 'https://www.naturalstattrick.com/playerteams.php?stdoi=g',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Brave";v="132"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'sec-gpc': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
    }

    # First make a request to get a session cookie
    session = requests.Session()
    session.get('https://www.naturalstattrick.com/playerteams.php?stdoi=g')

    try:
        # Send a GET request to the URL with headers using the session
        response = session.get(url, headers=headers)
        response.raise_for_status()  # Raises HTTPError for bad responses

        # Wrap the response text in StringIO
        html_content = StringIO(response.text)

        # Parse all tables from the HTML content using 'lxml' parser
        tables = pd.read_html(html_content, flavor='lxml')

        if tables:
            # Assuming the first table is the one you need
            df = tables[0]
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            # Convert team names using nst_to_nhl_tricode, split by comma and apply to each team
            if 'team' in df.columns:
                df['team'] = df['team'].apply(lambda x: ', '.join(nst_to_nhl_tricode(team.strip()) or team.strip() for team in str(x).split(',')))
            
            # Format TOI and TOI/GP columns
            toi_columns = [col for col in df.columns if 'toi' in col.lower()]
            for col in toi_columns:
                # Convert time format (MM:SS) to decimal minutes
                df[col] = df[col].apply(lambda x: float(x.split(':')[0]) + float(x.split(':')[1])/60 if ':' in str(x) else x).round(2)
                
            return df
        else:
            print("No tables found on the webpage.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP error
    except Exception as err:
        print(f"An error occurred: {err}")  # Other errors
        
def nst_team_on_ice_scraper(fromseason=None, thruseason=None, startdate='', enddate=None, last_n=None, stype=2, sit='all'):
    """
    Extracts team on-ice statistics from Natural Stat Trick for specified seasons and filtering conditions.

    Parameters:
        fromseason (int, optional): The starting season in the format YYYYYYYY (e.g., 20242025). If not provided, calculated from dates.
        thruseason (int, optional): The ending season in the format YYYYYYYY (e.g., 20242025). If not provided, calculated from dates.
        startdate (str, optional): The start date in the format YYYY-MM-DD (e.g., 2024-10-08). Defaults to empty string.
        enddate (str): The end date in the format YYYY-MM-DD (e.g., 2024-10-14). Required.
        last_n (int, optional): Number of days to look back from enddate. If provided, overrides startdate.
        stype (int, optional): Type of statistics to retrieve. Defaults to 2 for regular season.
        sit (str, optional): Situation type to filter by (e.g., 'all'). Defaults to 'all'.

    Returns:
        DataFrame: A DataFrame containing the team on-ice statistics.

    Raises:
        ValueError: If enddate is not provided.
        requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
        Exception: For any other errors that occur during the scraping process.
    """
    # Handle date calculations
    if enddate is None:
        raise ValueError("enddate must be provided")
    
    try:
        end_season = get_season_for_date(enddate)
    except ValueError:
        # If enddate is between seasons, find the next season
        end_date_obj = datetime.strptime(enddate, '%Y-%m-%d')
        for season in sorted(NHL_SEASONS.keys()):
            season_start = datetime.strptime(NHL_SEASONS[season]['start'], '%Y-%m-%d')
            if end_date_obj < season_start:
                end_season = season
                break
        else:
            raise ValueError(f"Could not determine season for end date {enddate}")

    if last_n is not None:
        end_date_obj = datetime.strptime(enddate, '%Y-%m-%d')
        season_start_obj = datetime.strptime(NHL_SEASONS[end_season]['start'], '%Y-%m-%d')
        
        # Calculate days since season start
        days_since_season_start = (end_date_obj - season_start_obj).days
        
        if days_since_season_start < last_n:
            # Need to go into previous season
            prev_season = end_season - 10001  # e.g., 20242025 -> 20232024
            if prev_season in NHL_SEASONS:
                # Calculate remaining days to look back in previous season
                remaining_days = last_n - days_since_season_start
                prev_season_end = get_season_end_date(prev_season, stype)
                prev_season_end_obj = datetime.strptime(prev_season_end, '%Y-%m-%d')
                start_date_obj = prev_season_end_obj - timedelta(days=remaining_days)
                startdate = start_date_obj.strftime('%Y-%m-%d')
                start_season = prev_season
            else:
                # If no previous season data, just go back from season start
                start_date_obj = season_start_obj - timedelta(days=last_n)
                startdate = start_date_obj.strftime('%Y-%m-%d')
                start_season = end_season
        else:
            # All dates within current season
            start_date_obj = end_date_obj - timedelta(days=last_n)
            startdate = start_date_obj.strftime('%Y-%m-%d')
            start_season = end_season
    else:
        # If no last_n provided and startdate is empty, use end_season as start_season
        if not startdate:
            start_season = end_season
        else:
            try:
                start_season = get_season_for_date(startdate)
            except ValueError:
                # If startdate falls between seasons, use the previous season
                start_date_obj = datetime.strptime(startdate, '%Y-%m-%d')
                for season in sorted(NHL_SEASONS.keys()):
                    season_start = datetime.strptime(NHL_SEASONS[season]['start'], '%Y-%m-%d')
                    if start_date_obj < season_start:
                        start_season = season - 10001
                        # Adjust startdate to use the end of the previous season based on stype
                        startdate = get_season_end_date(start_season, stype)
                        break
                else:
                    raise ValueError(f"Could not determine season for start date {startdate}")

    # Set seasons if not provided
    if fromseason is None:
        fromseason = start_season
    if thruseason is None:
        thruseason = end_season

    # For single-day queries, use the same season for both fromseason and thruseason
    if startdate == enddate:
        fromseason = thruseason = end_season

    url = (
        f"https://www.naturalstattrick.com/teamtable.php?"
        f"fromseason={fromseason}&thruseason={thruseason}&stype={stype}&sit={sit}"
        f"&score=all&rate=n&team=all&loc=B"
        f"&gpfilt=gpdate&fd={startdate}&td={enddate}"
        f"&tgp=410"
    )

    # Add browser-like headers
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.8',
        'cache-control': 'no-cache',
        'connection': 'keep-alive',
        'host': 'www.naturalstattrick.com',
        'pragma': 'no-cache',
        'referer': 'https://www.naturalstattrick.com/teamtable.php',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Brave";v="132"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'sec-gpc': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
    }

    # First make a request to get a session cookie
    session = requests.Session()
    session.get('https://www.naturalstattrick.com/teamtable.php')

    try:
        # Send a GET request to the URL with headers using the session
        response = session.get(url, headers=headers)
        response.raise_for_status()  # Raises HTTPError for bad responses

        # Wrap the response text in StringIO
        html_content = StringIO(response.text)

        # Parse all tables from the HTML content using 'lxml' parser
        tables = pd.read_html(html_content, flavor='lxml')

        if tables:
            # Assuming the first table is the one you need
            df = tables[0]
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            
            # Format TOI and TOI/GP columns
            toi_columns = [col for col in df.columns if 'toi' in col.lower()]
            for col in toi_columns:
                # Convert time format (MM:SS) to decimal minutes
                df[col] = df[col].apply(lambda x: float(x.split(':')[0]) + float(x.split(':')[1])/60 if ':' in str(x) else x).round(2)
                
            return df
        else:
            print("No tables found on the webpage.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP error
    except Exception as err:
        print(f"An error occurred: {err}")  # Other errors
