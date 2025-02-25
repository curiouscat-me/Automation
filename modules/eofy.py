from datetime import datetime

"""
    Determines the End of Financial Year (EOFY) date based on the current date.
    
    Returns:
    datetime: The EOFY date, set to September 1st of the current year if the current month is September or earlier, 
              otherwise set to September 1st of the next year.
    
    Process:
    1. Get the current date.
    2. Extract the current year from the current date.
    3. Check if the current month is past September.
       - If true, set EOFY to September 1st of the next year.
       - If false, set EOFY to September 1st of the current year.
    4. Return the EOFY date.
    """

def get_eofy():
    today = datetime.now()
    current_year = today.year

    # Check if the current month is past September
    if today.month > 9:
        eofy = datetime(current_year + 1, 9, 1)  # Set EOFY to September 1 of the next year
    else:
        eofy = datetime(current_year, 9, 1)  # Set EOFY to September 1 of the current year
    return eofy