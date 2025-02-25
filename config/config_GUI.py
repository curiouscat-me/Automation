from datetime import datetime, timedelta
import re

def extract_date_from_filename(filename):
    """
    Extracts the current and next month's date strings from a filename containing a date in 'YYMMDD' format.

    Parameters:
    filename (str): The filename containing a date in 'YYMMDD' format.

    Returns:
    tuple: A tuple containing two strings:
           - The current month's date string in 'MMYY' format.
           - The next month's date string in 'MMYY' format.
           If no date is found in the filename, returns None.

    Process:
    1. Use a regular expression to search for a 'YYMMDD' date pattern in the filename.
       - If no date pattern is found, return None.
    2. Extract the date string from the match.
    3. Parse the date string assuming the format 'YYMMDD'.
    4. Calculate the current month's date string by setting the date to the first of the month and subtracting one day.
    5. Format the current month's date string in 'MMYY' format.
    6. Format the next month's date string in 'MMYY' format.
    7. Return the current and next month's date strings as a tuple.
    """
    # Regular expression to find a date pattern in the filename
    match = re.search(r'\d{6}', filename)
    if not match:
        return None
    
    # Extract the date string from the match
    date_str = match.group()
    # Parse the date assuming the format 'YYMMDD'
    date_obj = datetime.strptime(date_str, '%y%m%d')

    # Get the current month and next month for sheet names
    current_month = (date_obj.replace(day=1) - timedelta(days=1)).strftime('%m%y')
    next_month = date_obj.strftime('%m%y')
    
    return current_month, next_month

def update_sheets_name(static_file):
    """
    Updates the sheet names in the CONFIG based on the dates extracted from the given static file's filename.

    Parameters:
    static_file (str): The filename of the static report from which to extract dates.

    Raises:
    ValueError: If the date extraction from the filename fails.

    Process:
    1. Extract dates from the filename in 'MMYY' format using the extract_date_from_filename function.
       - If date extraction fails, raise a ValueError.
    2. Unpack the extracted dates into current and next month's date strings.
    3. Update the CONFIG dictionary with the new sheet names based on the extracted dates:
       a. Set the current month's sheet name in the format 'ex_StaticFTE_MMYY'.
       b. Set the next month's sheet name in the format 'ex_StaticFTE_MMYY'.
    """
    dates = extract_date_from_filename(static_file)  # Extract dates from the filename in 'MMYY' format
    if not dates:
        raise ValueError("Failed to extract date from Static Report filename.")
    
    # Unpack the current and next month's date strings
    current_month_str, next_month_str = dates

    # Update CONFIG with the new sheet names based on the extracted dates
    CONFIG['sheets']['current_month']['sheet_name'] = f"ex_StaticFTE_{current_month_str}"  # Update the current sheet with ex_StaticFTE_MMYY format
    CONFIG['sheets']['next_month']['sheet_name'] = f"ex_StaticFTE_{next_month_str}"  # Update the next sheet with ex_StaticFTE_MMYY format

# Define configurations for the project.
CONFIG = {
    """
    Configuration settings for the project, defining file paths, sheet structures, column mappings, and default values.

    Structure:
    1. "OP_FILE" (str): Path to the op plan file.
    2. "STATIC_FILE" (str): Path to the static report file.
    3. "GLOBAL_STAFF_LIST" (str): Path to the global staff list file.

    4. "sheets" (dict): Contains configurations for various Excel sheets used in the project.
    - "current_month" (dict): Configuration for the current month's sheet. Includes:
        - "sheet_name" (str): The sheet name, dynamically updated.
        - "usecols" (list): List of column names to use from the sheet.
    - "next_month" (dict): Similar to "current_month", but for the next month's sheet.
    - "global" (dict): Configuration for the global staff list sheet.
    - "FTE" (dict): Configuration for the "FTE" sheet. If you want to use specific columns within FTE sheet, then update the "usecols" list.
    - "MS" (dict): Configuration for the "MS" (Managed Services) sheet. If you want to use specific columns within MS sheet, then update the "usecols" list.

    5. "OP_FTE_COLUMNS" (list): Defines the columns for the output op plan related to FTE data, ensuring correct order and structure.

    6. "OP_MS_COLUMNS" (list): Defines the columns for the output op plan related to MS data.

    7. "COLUMN_MAPPING_FTE" (dict): Maps columns from the static report to corresponding columns in the operational FTE plan.

    8. "COLUMN_MAPPING_MS" (dict): Maps columns from the static report to corresponding columns in the operational MS plan.

    9. "COLUMN_MAPPING_GLOBAL" (dict): Maps columns from the global staff list to corresponding columns in the op plan.

    10. "MERGE_KEY_COLUMNS_OP" (list): Key columns used for merging data in the op plan (e.g., 'Employee ID', 'Tech Area').

    11. "MERGE_KEY_COLUMNS_STATIC" (list): Key columns used for merging data from the static report (e.g., 'Employee ID', 'ORG_HIER3_NAME').

    12. "COLUMN_VALUES_FTE" (dict): Default values for specific columns in the FTE op plan when creating new entries.

    13. "COLUMN_VALUES_MS" (dict): Default values for specific columns in the MS op plan when creating new entries.

    Usage:
    This configuration is used throughout the project to define how data is read, processed, and written across multiple Excel sheets, ensuring consistency in data handling.
    """
    "OP_FILE": "",
    "STATIC_FILE": "", 
    "GLOBAL_STAFF_LIST":"",

    "sheets": {
        "current_month": {
            "sheet_name": "", # Will be updated dynamically
            "usecols": [
                "Employee ID",
                "Legal First Name",
                "Legal Surname",
                "Employee Group (Name)",
                "Supervisor Employee ID",
                "Supervisor Legal First Name",
                "Supervisor Legal Surname",
                "Position Title",
                "(Pay Grade) Pay Group Level2",
                "FTE",
                "FTE Category",
                "Username",
                "Country (Label)",
                "ORG_HIER2_NAME",
                "ORG_HIER3_NAME",
            ]
        },
        "next_month": {
            "sheet_name": "", # Will be updated dynamically
            "usecols": [
                "Employee ID",
                "Legal First Name",
                "Legal Surname",
                "Employee Group (Name)",
                "Supervisor Employee ID",
                "Supervisor Legal First Name",
                "Supervisor Legal Surname",
                "Position Title",
                "(Pay Grade) Pay Group Level2",
                "FTE",
                "FTE Category",
                "Username",
                "Country (Label)",
                "ORG_HIER2_NAME",
                "ORG_HIER3_NAME",
            ]
        },

        "global": {
            "sheet_name": "Global",
            "usecols": [
                "Employee ID",
                "Vendor Name",
                "Operational Division (Label)",
            ]
        },

        "FTE": {
            "sheet_name": "FTE"
        },

        "MS": {
            "sheet_name": "MS"
        },
        # Add other sheets if necessary
    },

    # Define the columns for the operational plan (for maintaining order or additional processing)
    "OP_FTE_COLUMNS": [
        'Resource Type',
        'Input Annualised Stretch $ \n(if applicable)',
        'Employee ID',
        'FTE Name',
        'LANID',
        'Role Type',
        'Job Grade',
        'FTE based Country\n(drives FTE rates calc)',
        'Domain',
        'Tech Area',
        'Planning Unit Country',
        'Squad',
        'Service',
        'Asset',
        'Product',
        'Tech Financial Reform',
        'FTET approval Ref',
        'FTE #',
        'Headcount',
        'Start Date',
        'End Date',
        'Comments',
        'Free Input',
        'Run %',
        'Divisional Change %',
        'Tech Projects %',
        'Total %',
        'Role Status',
        'Modified'
    ],
    
    "OP_MS_COLUMNS": [
        'Resource Type',
        'Vendor Name',
        'MS Daily Rate',
        'Annualised FY24 Cost or (Stretch) $',
        'Resource Name',
        'Employee ID',
        'LANID',
        'Role Type',
        'Domain',
        'Tech Area',
        'Planning Unit Country',
        'Physical Location',
        'Squad',
        'Service',
        'Asset',
        'Product',
        'VEP Approval reference',
        'Tech Financial Reform',
        'T&M / FP',
        'Start Date',
        'End Date',
        'Comments',
        'Free Input',
        'Run %',
        'Divisional Change %',
        'Tech Projects %',
        'Total %',
    ],
    
    # Column Mapping - Static: Op
    "COLUMN_MAPPING_FTE": {
        "Employee ID": "Employee ID",
        "Employee Group (Name)": "Resource Type",
        "Username": "LANID",
        "Position Title": "Role Type",
        "(Pay Grade) Pay Group Level2": "Job Grade",
        "ORG_HIER2_NAME": "Domain",
        "ORG_HIER3_NAME": "Tech Area",
        "FTE": "FTE #",
        "Country (Label)": "Planning Unit Country"
        # Include other mappings as necessary
    },

    "COLUMN_MAPPING_MS": {
        "Employee ID": "Employee ID",
        "Employee Group (Name)": "Resource Type",
        "Username": "LANID",
        "Position Title": "Role Type",
        "ORG_HIER2_NAME": "Domain",
        "ORG_HIER3_NAME": "Tech Area",
        "Country (Label)": "Planning Unit Country",
        # Add more as nesccessary
    },

    # Column mapping - Global - Op
    "COLUMN_MAPPING_GLOBAL":{
        "Employee ID": "Employee ID",
        "Vendor Name": "Vendor Name",
    },

    "MERGE_KEY_COLUMNS_OP": ['Employee ID', 'Tech Area'], 
    "MERGE_KEY_COLUMNS_STATIC": ['Employee ID', 'ORG_HIER3_NAME'],

    # Define default values for specific columns in FTE
    "COLUMN_VALUES_FTE": {
        'Squad': '-',
        'Service': '-',
        'Asset': '-',
        'Product':'-',
        'Tech Financial Reform': 'Select Dropdown',
        'FTET approval Ref': '-',
        'Headcount': int('1'),
        'Comments': '-',
        'Free Input': '-',
        'Run %': '-',
        'Divisional Change %': '-',
        'Tech Projects %': '-',
        'Total %': '-',
        # Add other default values as necessary
    },

    # Define default values for specific columns in MS
    "COLUMN_VALUES_MS": {
        "MS Daily Rate": '-',
        "Annualised FY23 Cost or (Stretch) $": '-',
        "Squad": '-',
        "Service": '-',
        'Asset': '-',
        'Product':'-',
        'Tech Financial Reform': 'Select Dropdown',
        "T&M / FP": '-',
        'Comments': '-',
        'Free Input': '-',
        'Run %': '-',
        'Divisional Change %': '-',
        'Tech Projects %': '-',
        'Total %': '-',
    },
}

def set_op_plan_path(path):
    CONFIG["OP_FILE"] = path

def set_static_report_path(path):
    CONFIG["STATIC_FILE"] = path
    update_sheets_name(path)

def set_global_staff_list_path(path):
    CONFIG["GLOBAL_STAFF_LIST"] = path