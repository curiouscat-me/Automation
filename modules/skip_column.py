from config.config_GUI import CONFIG
import pandas as pd
from datetime import datetime
from modules.date_extraction import extract_date_from_filename
from modules.logger import data_logger

def initiate_skip_column_fte(op_fte_df):
    """
    Initializes the 'Skip' column in the Op Plan data based on the 'End Date' column.
    Skips updating employees whose 'End Date' has already passed, compared to the Static Report date.

    Returns:
    DataFrame: Updated DataFrame with 'Skip' column initialized.

    Process:
    1. Extract the static report date from the filename and convert it to a datetime object.
    2. Initialize the 'Skip' column in the op_fte_df.
    3. Iterate over each row in the op_fte_df:
       a. Parse the 'End Date' column, handling multiple data types (e.g., datetime, string, int).
       b. Convert numeric 'End Date' values to strings if needed.
       c. Skip rows with invalid, NaT, or None values in 'End Date'.
       d. If the 'End Date' is before the static report date, mark the 'Skip' column as 'Past'.
    4. Log the number of records skipped due to past 'End Date'.
    5. Return the updated op_fte_df.
    """
    _, static_report_date_str = extract_date_from_filename(CONFIG['STATIC_FILE'])
    static_report_date = datetime.strptime(static_report_date_str, '%b-%y').date()

    skipped_past = 0
    op_fte_df['Skip'] = ''  # Initialize the 'Skip' column to track skipped records

    for index, row in op_fte_df.iterrows():
        end_date_str = row['End Date']

        # Ensure end_date_str is a string
        if pd.notna(end_date_str) and isinstance(end_date_str, (float, int)):
            end_date_str = str(int(end_date_str))  # Convert numeric types to strings

        if isinstance(end_date_str, datetime):
            end_date = end_date_str.date()
        elif isinstance(end_date_str, str):
            try:
                end_date = datetime.strptime(end_date_str, '%b-%y').date() if pd.notna(end_date_str) else None
            except ValueError:
                continue  # Ignore parsing errors and proceed with the update
        else:
            end_date = None

        # Skip rows with NaT or None values in 'End Date'
        if end_date is None or pd.isna(end_date):
            continue

        if end_date < static_report_date:
            skipped_past += 1
            op_fte_df.at[index, 'Skip'] = 'Past'

    if skipped_past > 0:
        data_logger.info(f"Skipped {skipped_past} records due to past End Date.")

    return op_fte_df

def initiate_skip_column_ms(op_ms_df):
    """
    Initializes the 'Skip' column in the MS Op Plan data based on the 'End Date' column.
    Skips updating employees whose 'End Date' has already passed, compared to the Static Report date.

    Returns:
    DataFrame: Updated DataFrame with 'Skip' column initialized.

    Process:
    1. Extract the static report date from the filename and convert it to a datetime object.
    2. Initialize the 'Skip' column in the op_ms_df.
    3. Iterate over each row in the op_ms_df:
       a. Parse the 'End Date' column, handling multiple data types (e.g., datetime, string, int).
       b. Convert numeric 'End Date' values to strings if needed.
       c. Skip rows with invalid, NaT, or None values in 'End Date'.
       d. If the 'End Date' is before the static report date, mark the 'Skip' column as 'Past'.
    4. Log the number of records skipped due to past 'End Date'.
    5. Return the updated op_ms_df.
    """
    _, static_report_date_str = extract_date_from_filename(CONFIG['STATIC_FILE'])
    static_report_date = datetime.strptime(static_report_date_str, '%b-%y').date()

    skipped_past = 0
    op_ms_df['Skip'] = ''  # Initialize the 'Skip' column to track skipped records

    for index, row in op_ms_df.iterrows():
        end_date_str = row['End Date']

        # Ensure end_date_str is a string
        if pd.notna(end_date_str) and isinstance(end_date_str, (float, int)):
            end_date_str = str(int(end_date_str))  # Convert numeric types to strings

        if isinstance(end_date_str, datetime):
            end_date = end_date_str.date()
        elif isinstance(end_date_str, str):
            try:
                end_date = datetime.strptime(end_date_str, '%b-%y').date() if pd.notna(end_date_str) else None
            except ValueError:
                continue  # Ignore parsing errors and proceed with the update
        else:
            end_date = None

        # Skip rows with NaT or None values in 'End Date'
        if end_date is None or pd.isna(end_date):
            continue

        if end_date < static_report_date:
            skipped_past += 1
            op_ms_df.at[index, 'Skip'] = 'Past'

    if skipped_past > 0:
        data_logger.info(f"Skipped {skipped_past} records due to past End Date.")

    return op_ms_df