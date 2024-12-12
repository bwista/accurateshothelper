# db_utils/prop_odds_db_utils.py
from .base_utils import get_db_connection

DB_PREFIX = 'PROP_ODDS_DB_'

def get_prop_odds_db_connection():
    return get_db_connection(DB_PREFIX)

# Add Prop Odds specific functions...