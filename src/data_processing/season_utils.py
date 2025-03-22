from datetime import datetime

# Dictionary containing season start and end dates
NHL_SEASONS = {
    20242025: {
        'start': '2024-10-04',
        'regular_end': '2025-04-18',  # Estimated
        'playoff_end': '2025-06-30'  # Estimated
    },
    20232024: {
        'start': '2023-10-10',
        'regular_end': '2024-04-18',
        'playoff_end': '2024-06-24'
    },
    20222023: {
        'start': '2022-10-07',
        'regular_end': '2023-04-14',
        'playoff_end': '2023-06-13'
    },
    20212022: {
        'start': '2021-10-12',
        'regular_end': '2022-04-29',
        'playoff_end': '2022-06-26'
    },
    20202021: {
        'start': '2021-01-13',
        'regular_end': '2021-05-19',
        'playoff_end': '2021-07-07'  # Covid-shortened season
    },
    20192020: {
        'start': '2019-10-02',
        'regular_end': '2020-03-10',
        'playoff_end': '2020-09-28'
    },
    20182019: {
        'start': '2018-10-03',
        'regular_end': '2019-04-06',
        'playoff_end': '2019-06-12'
    },
    20172018: {
        'start': '2017-10-04',
        'regular_end': '2018-04-08',
        'playoff_end': '2018-06-07'
    },
    20162017: {
        'start': '2016-10-12',
        'regular_end': '2017-04-09',
        'playoff_end': '2017-06-11'
    },
    20152016: {
        'start': '2015-10-07',
        'regular_end': '2016-04-10',
        'playoff_end': '2016-06-12'
    },
    20142015: {
        'start': '2014-10-08',
        'regular_end': '2015-04-12',
        'playoff_end': '2015-06-15'
    },     
    20132014: {
        'start': '2013-10-01',
        'regular_end': '2014-04-13',
        'playoff_end': '2014-06-15'
    },     
}

def get_season_end_date(season, stype=2):
    """
    Get the end date for a season based on the season type.
    
    Args:
        season (int): Season in YYYYYYYY format
        stype (int): Season type (2 for regular season, 3 for playoffs). Defaults to 2.
        
    Returns:
        str: End date in YYYY-MM-DD format
        
    Raises:
        ValueError: If season is not found or invalid stype
    """
    if season not in NHL_SEASONS:
        raise ValueError(f"Season {season} not found in NHL_SEASONS")
    
    if stype == 2:
        return NHL_SEASONS[season]['regular_end']
    elif stype == 3:
        return NHL_SEASONS[season]['playoff_end']
    else:
        raise ValueError(f"Invalid stype {stype}. Must be 2 for regular season or 3 for playoffs.")

def get_season_start_date(season):
    """
    Get the start date for a season.
    
    Args:
        season (int): Season in YYYYYYYY format
        
    Returns:
        str: Start date in YYYY-MM-DD format
        
    Raises:
        ValueError: If season is not found
    """
    if season not in NHL_SEASONS:
        raise ValueError(f"Season {season} not found in NHL_SEASONS")
    
    return NHL_SEASONS[season]['start']

def get_season_for_date(date_str):
    """
    Determines which NHL season a given date falls into.
    
    Args:
        date_str (str): Date string in YYYY-MM-DD format
        
    Returns:
        int: Season in YYYYYYYY format (e.g., 20232024)
        
    Raises:
        ValueError: If date doesn't fall within any known season
    """
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    for season in sorted(NHL_SEASONS.keys(), reverse=True):
        season_start = datetime.strptime(NHL_SEASONS[season]['start'], '%Y-%m-%d')
        season_end = datetime.strptime(NHL_SEASONS[season]['playoff_end'], '%Y-%m-%d')
        
        if season_start <= date_obj <= season_end:
            return season
            
    raise ValueError(f"Date {date_str} does not fall within any known NHL season") 