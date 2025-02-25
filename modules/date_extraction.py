from datetime import datetime, timedelta
import re

def extract_date_from_filename(filename):
    """
    Extracts the date from a filename and calculates the last day of the current month and the first day of the next month.
    
    Parameters:
    filename (str): The filename containing a date in 'YYMMDD' format.
    
    Returns:
    tuple: A tuple containing two strings:
           - The last day of the current month in '%b-%y' format.
           - The first day of the next month in '%b-%y' format.
           If no date is found in the filename, returns None.
    
    Process:
    1. Use a regular expression to search for a 'YYMMDD' date pattern in the filename.
    2. If no date pattern is found, return None.
    3. Extract the date string from the match.
    4. Parse the date string assuming the format 'YYMMDD'.
    5. Adjust the date to the first day of the next month.
    6. Subtract one day to get the last day of the current month.
    7. Return the formatted dates for the last day of the current month and the first day of the next month.
    """
    # Regular expression to find a date pattern in the filename
    match = re.search(r'\d{6}', filename)
    if not match:
        return None  # Return None if no date is found

    # Extract the date string from the match
    date_str = match.group()
    # Parse the date assuming the format 'YYMMDD'
    date_obj = datetime.strptime(date_str, '%y%m%d')
    
    # Adjust to the first day of the month and then subtract one day to get the current month's end
    first_day_of_static_month = date_obj.replace(day=1) # Change date to first of next month 
    last_day_of_op_month = first_day_of_static_month - timedelta(days=1) # Take first day next month - 1 day for last day current month
    
    return last_day_of_op_month.strftime('%b-%y'), first_day_of_static_month.strftime('%b-%y')