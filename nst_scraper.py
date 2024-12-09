import pandas as pd
import requests
from io import StringIO

def nst_on_ice_scraper(fromseason, thruseason, startdate, enddate, stype=2, sit='5v5', stdoi='std', pos='std', rate='n', loc='B'):
    """
    Extracts player on-ice statistics from Natural Stat Trick for specified seasons and filtering conditions.

    Parameters:
        fromseason (int): The starting season in the format YYYYYYYY (e.g., 20242025).
        thruseason (int): The ending season in the format YYYYYYYY (e.g., 20242025).
        startdate (str): The start date in the format YYYY-MM-DD (e.g., 2024-10-08).
        enddate (str): The end date in the format YYYY-MM-DD (e.g., 2024-10-14).
        stype (int, optional): Type of statistics to retrieve. Defaults to 2 for regular season.
        sit (str, optional): Situation type to filter by (e.g., '5v5'). Defaults to '5v5'.
        stdoi (str, optional): Statistic type of interest. Use 'std' for individual stats, 'oi' for on-ice stats, or 'g' for goalies. Defaults to 'std'.
        pos (str, optional): Type of player statistics to retrieve. Use 'std' for standard players or 'g' for goalies. Defaults to 'std'.
        rate (str, optional): Stat type, rate or count. Use 'n' or 'y'. Defaults to 'n'.
        loc (str, optional): Location filter. Use 'B' for both home and away, 'H' for home, or 'A' for away. Defaults to 'B'.

    Returns:
        df: A DataFrame containing the player on-ice statistics.

    Raises:
        ValueError: If `stdoi` or `pos` or `loc` has an invalid value.
        requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
        Exception: For any other errors that occur during the scraping process.
    """
    if stdoi not in ['std', 'oi', 'g']:
        raise ValueError("stdoi must be either 'std' for individual stats, 'oi' for on-ice stats, or 'g' for goalies.")
    # Validate pos input, std(skaters) or g(goalies)
    if pos not in ['std', 'g']:
        raise ValueError("pos must be either 'std' for standard players or 'g' for goalies.")
    # Validate loc input
    if loc not in ['B', 'H', 'A']:
        raise ValueError("loc must be 'B' for both home and away, 'H' for home, or 'A' for away.")

    url = (
        f"https://www.naturalstattrick.com/playerteams.php?"
        f"fromseason={fromseason}&thruseason={thruseason}&stype={stype}&sit={sit}"
        f"&score=all&stdoi={stdoi}&rate={rate}&team=ALL&pos={pos}&loc={loc}&toi=0"
        f"&gpfilt=none&fd={startdate}&td={enddate}&tgp=410&lines=single&draftteam=ALL"
    )

    try:
        # Send a GET request to the URL
        response = requests.get(url)
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
            return df
        else:
            print("No tables found on the webpage.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP error
    except Exception as err:
        print(f"An error occurred: {err}")  # Other errors
        
def nst_team_on_ice_scraper(fromseason, thruseason, startdate, enddate, stype=2, sit='all'):
    """
    Extracts team on-ice statistics from Natural Stat Trick for specified seasons and filtering conditions.

    Parameters:
        fromseason (int): The starting season in the format YYYYYYYY (e.g., 20242025).
        thruseason (int): The ending season in the format YYYYYYYY (e.g., 20242025).
        startdate (str): The start date in the format YYYY-MM-DD (e.g., 2024-10-08).
        enddate (str): The end date in the format YYYY-MM-DD (e.g., 2024-10-14).
        stype (int, optional): Type of statistics to retrieve. Defaults to 2 for regular season.
        sit (str, optional): Situation type to filter by (e.g., 'all'). Defaults to 'all'.

    Returns:
        DataFrame: A DataFrame containing the team on-ice statistics.

    Raises:
        requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
        Exception: For any other errors that occur during the scraping process.
    """
    url = (
        f"https://www.naturalstattrick.com/teamtable.php?"
        f"fromseason={fromseason}&thruseason={thruseason}&stype={stype}&sit={sit}"
        f"&score=all&rate=n&team=all&loc=B&gpf=410&fd={startdate}&td={enddate}"
    )

    try:
        # Send a GET request to the URL
        response = requests.get(url)
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
            return df
        else:
            print("No tables found on the webpage.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP error
    except Exception as err:
        print(f"An error occurred: {err}")  # Other errors
