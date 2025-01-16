def american_to_decimal(american_odds):
    """
    Converts American odds to Decimal odds.

    Args:
        american_odds (float or int): The American odds to convert.

    Returns:
        float: The converted Decimal odds.
    """
    if american_odds > 0:
        return 1 + (american_odds / 100)
    else:
        return 1 + (100 / abs(american_odds))