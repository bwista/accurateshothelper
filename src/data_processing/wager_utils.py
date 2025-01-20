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


def american_to_probability(american_odds):
    """
    Converts American odds to implied probability.

    Args:
        american_odds (float or int): The American odds to convert.

    Returns:
        float: The implied probability.
    """
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return -american_odds / (-american_odds + 100)


def decimal_to_probability(decimal_odds):
    """
    Converts Decimal odds to implied probability.

    Args:
        decimal_odds (float or int): The Decimal odds to convert.

    Returns:
        float: The implied probability.
    """
    if decimal_odds <= 0:
        raise ValueError("Decimal odds must be positive.")
    return 1 / decimal_odds