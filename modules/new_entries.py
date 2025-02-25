import pandas as pd

def add_new_entries_fte(op_fte_df, new_entries):
    """
    Adds new entries to the operational plan DataFrame for FTE.
    
    Parameters:
    op_fte_df (DataFrame): The DataFrame containing the current operational plan data for FTE.
    new_entries (list): A list of new entries to add to the operational plan DataFrame.
    
    Returns:
    DataFrame: The updated operational plan DataFrame with the new entries added.
    
    Process:
    1. Check if there are new entries to add.
    2. If new entries exist:
       a. Convert the list of new entries into a DataFrame and reset its index to avoid duplicates.
       b. Reset the index of op_fte_df to ensure it has a unique index.
       c. Concatenate the new entries DataFrame to the existing op_fte_df.
    3. Return the updated operational plan DataFrame.
    """
    if new_entries:
        new_entries_df = pd.DataFrame(new_entries).reset_index(drop=True)  # Reset index to avoid duplicates
        op_fte_df = op_fte_df.reset_index(drop=True)  # Ensure op_fte_df also has unique index
        op_fte_df = pd.concat([op_fte_df, new_entries_df], ignore_index=True)
    return op_fte_df

def add_new_entries_ms(op_ms_df, new_entries):
    """
    Adds new entries to the operational plan DataFrame for MS.
    
    Parameters:
    op_ms_df (DataFrame): The DataFrame containing the current operational plan data for FTE.
    new_entries (list): A list of new entries to add to the operational plan DataFrame.
    
    Returns:
    DataFrame: The updated operational plan DataFrame with the new entries added.
    
    Process:
    1. Check if there are new entries to add.
    2. If new entries exist:
       a. Convert the list of new entries into a DataFrame and reset its index to avoid duplicates.
       b. Reset the index of op_ms_df to ensure it has a unique index.
       c. Concatenate the new entries DataFrame to the existing op_ms_df.
    3. Return the updated operational plan DataFrame.
    """
    if new_entries:
        new_entries_df = pd.DataFrame(new_entries).reset_index(drop=True)  # Reset index to avoid duplicates
        op_ms_df = op_ms_df.reset_index(drop=True)  # Ensure op_ms_df also has unique index
        op_ms_df = pd.concat([op_ms_df, new_entries_df], ignore_index=True)
    return op_ms_df