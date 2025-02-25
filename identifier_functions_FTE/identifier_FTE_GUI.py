from config.config_GUI import CONFIG
import pandas as pd
from datetime import datetime
from modules.logger import data_logger
from modules.formatting import *
from modules.eofy import get_eofy
from modules.new_entries import add_new_entries_fte
from modules.employee_filtering_conditions import employee_filtering_condition_fte, employee_security_fte, shorten_filtering_condition_fte

"""
    Global Parameters:
    current_df (DataFrame): The DataFrame containing the current month data from the static report.
    next_df (DataFrame): The DataFrame containing the next month data from the static report.
    op_fte_df (DataFrame): The DataFrame containing the operational plan data.
    file_date (tuple): A tuple containing the end date string and start date string in '%b-%y' format.
    Returns:
    DataFrame: Updated operational plan DataFrame with identified movements marked.
"""

def identify_exits_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have exited the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the end date string from file_date to a datetime object.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the current month's DataFrame for Security and FTE employees.
    3. Merge the filtered current month data with next month's data to identify exits.
       - Exits are employees present in the current month but not in the next month.
    4. Log the number of identified exits.
    5. For each identified exit:
       a. Find matching entries in the operational plan DataFrame.
       b. Update the 'End Date', 'Role Status' to "Exit", and mark as 'Modified'.
       c. Log each updated exit.
    6. Return the updated operational plan DataFrame.
    """
    end_date_str, _ = file_date
    if not end_date_str:
        data_logger.error("Failed to extract date from Static Report")
        return op_fte_df
    
    # Convert the date str back to datetime object to perform date operations
    try:
        file_date = datetime.strptime(end_date_str, '%b-%y').date()
    except ValueError as e:
        data_logger.error(f"Date conversion error for Exits: {e}")
        return op_fte_df

    # Use the reusable function to filter for Security and FTE
    current_security_fte = employee_security_fte(current_df)

    # Filter to find employees who are no longer in the static file and were in Security Domain
    merged_df = pd.merge(current_security_fte, next_df[['Employee ID']], on='Employee ID', how='left', indicator=True)

    # Filter for exits specially from Security who were FTE and not CWR
    exits = merged_df[
        (merged_df['_merge'] == 'left_only')] # Exist entries in Static report current month but NOT in next month data

    data_logger.info(f"Identified {len(exits)} exits in Security Domain for {file_date}...")
    if not exits.empty:
        data_logger.info(f"Processing Exits...")
        for index, row in exits.iterrows():
            emp_indices = shorten_filtering_condition_fte(op_fte_df, row['Employee ID'])
            
            if not emp_indices.empty:
                for emp_index in emp_indices:
                    op_fte_df.at[emp_index, 'End Date'] = file_date
                    op_fte_df.at[emp_index, 'Role Status'] = "Exit"
                    op_fte_df.at[emp_index, 'Modified'] = True
                    data_logger.info(f"Exit updated: {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']})")

    return op_fte_df

def identify_new_joiners_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies new joiners in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the start date string from file_date to a datetime object.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the next month's DataFrame for Security and FTE employees.
    3. Identify new joiners not in the current month but in the next month's Security domain.
    4. Identify employees who were CWR in another domain and move to FTE and Security domain next month.
    5. Combine the results from both conditions to get all new joiners.
    6. Log the number of identified new joiners.
    7. For each identified new joiner:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found, update the 'Role Status' to "New Hire" and mark as 'Modified'.
       c. If not found, create a new entry with the new joiner's information and mark as 'Modified'.
       d. Log each processed new hire.
    8. Add the new entries to the operational plan DataFrame.
    9. Return the updated operational plan DataFrame.
    """
    _, start_date_str = file_date # Extract Start Date. '_' would determine if the start/end date to be current month or next month.
    
    try:
        eofy = get_eofy().strftime('%b-%y')
        first_day_of_static_month = datetime.strptime(start_date_str, '%b-%y').date().strftime('%b-%y')
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df

    # Use the reusable function to filter for Security and FTE
    next_security_fte = employee_security_fte(next_df)
    
    # Condition 1: New joiners not in current data but in next month's Security domain
    new_joiners_condition1 = next_security_fte[
        (~next_security_fte['Employee ID'].isin(current_df['Employee ID']))] # Not in current month

    # Sub-conditions for Condition 2:
    current_cwr_not_security = current_df[
        (current_df['Domain'] != 'Security') & # Currently not in Security
        (current_df['FTE Category'] == 'Non-FTE') # Is NOT FTE current month 
    ]
    # Condition 2: Used to be CWR in another domain, move to FTE and Security Domain next month
    new_joiners_condition2 = next_security_fte[
        (next_security_fte['Employee ID'].isin(current_cwr_not_security['Employee ID']))] # In the list that currently CWR and not in Security

    # Combine results from both conditions
    new_joiners = pd.concat([new_joiners_condition1, new_joiners_condition2]) # .drop_duplicates()

    data_logger.info(f"Identified {len(new_joiners)} new joiners into the Security domain for {file_date}...")
    if not new_joiners.empty:
        data_logger.info(f"Processing New Joiners...")
        new_entries = []
        for index, row in new_joiners.iterrows():
            emp_indices = employee_filtering_condition_fte(op_fte_df, row['Employee ID'])

            if not emp_indices.empty:
                for emp_index in emp_indices:
                    op_fte_df.at[emp_index, 'Role Status'] = "New Hire"
                    op_fte_df.at[emp_index, 'Modified'] = True
                    data_logger.info(f"New Hire processed for {op_fte_df.at[emp_index,'FTE Name']} (Employee ID: {row['Employee ID']}) joining {row['Tech Area']}")
            else:
                new_entry = pd.Series(CONFIG['COLUMN_VALUES_FTE'], index=CONFIG['OP_FTE_COLUMNS'])
                for static_col, op_col in CONFIG['COLUMN_MAPPING_FTE'].items():
                    col_name = f"{static_col}"
                    if col_name in row:
                        new_entry[op_col] = row[col_name]
                first_name = str(row['Legal First Name']) if pd.notna(row['Legal First Name']) else ''
                last_name = str(row['Legal Surname']) if pd.notna(row['Legal Surname']) else ''

                new_entry['Resource Type'] = row['Resource Type']
                new_entry['FTE Name'] = first_name + " " + last_name
                new_entry['Employee ID'] = row['Employee ID']
                new_entry['LANID'] = row['LANID']
                new_entry['Role Type'] = row['Role Type']
                new_entry['Job Grade'] = row['Job Grade']
                new_entry['FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country'])
                new_entry['Domain'] = format_domain(row['Domain'])
                new_entry['Tech Area'] = format_tech_area(row['Tech Area'])
                new_entry['Planning Unit Country'] = row['Planning Unit Country']
                new_entry['FTE #'] = row['FTE #']
                new_entry['Start Date'] = first_day_of_static_month
                new_entry['End Date'] = eofy
                new_entry['Role Status'] = "New Hire"
                new_entry['Modified'] = True

                new_entries.append(new_entry)
                data_logger.info(f"New Hire processed for {new_entry['FTE Name']} (Employee ID: {row['Employee ID']}) joining {row['Tech Area']}")

        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)

    return op_fte_df

def identify_transfers_in_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have transferred into the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the start date string from file_date to a datetime object.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the next month's DataFrame for Security and FTE employees.
    3. Merge the current month's DataFrame with the next month's filtered data on 'Employee ID'.
    4. Identify transfers into Security domain based on domain and resource type conditions.
    5. Log the number of identified transfers in.
    6. For each identified transfer in:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found and matches the criteria, update the 'Role Status' to "Transfer In" and mark as 'Modified'.
       c. If not found, create a new entry with the transfer in's information and mark as 'Modified'.
       d. Log each processed transfer in.
    7. Add the new entries to the operational plan DataFrame.
    8. Return the updated operational plan DataFrame.
    """
    _, start_date_str = file_date
    try:
        eofy = get_eofy().strftime('%b-%y')
        first_day_of_static_month = datetime.strptime(start_date_str, '%b-%y').date().strftime('%b-%y')
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df
    
    # Filter for employees who are in Security and FTE in the current month
    next_security_fte = employee_security_fte(next_df)
    
    # Check for transfer in into Security Domain for existing FTE
    merged_df = pd.merge(current_df, next_security_fte, on='Employee ID', suffixes=('_current', '_next'))

    # Filter conditions
    transfers_in = merged_df[
        (merged_df['Domain_current'] != 'Security') & # Is NOT in Security Domain current month
        (merged_df['FTE Category_current'] == 'FTE') & # Is FTE current month
        (merged_df['Resource Type_current'] == merged_df['Resource Type_next'])  # Have the same Resource Type in both months
    ]

    data_logger.info(f"Identified {len(transfers_in)} transfers into the Security domain for {file_date}...")
    if not transfers_in.empty:
        data_logger.info(f"Processing Transfers In...")
        new_entries = []
        for index, row in transfers_in.iterrows():
            emp_indices = employee_filtering_condition_fte(op_fte_df, row['Employee ID'])
            
            if not emp_indices.empty:
                for emp_index in emp_indices:
                    # Check for existing entries in the Op Plan that match the criteria
                    existing_entries = op_fte_df[
                        (op_fte_df['Employee ID'] == row['Employee ID']) & 
                        (op_fte_df['Domain'] == row['Domain_next']) &
                        (op_fte_df['End Date'] == eofy)
                    ]
                    # If such entries exist, log and skip further processing for this entry
                    if not existing_entries.empty:
                        data_logger.info(f"Transfer In existed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}). Skipping.")
                        continue

                    op_fte_df.at[emp_index, 'Role Status'] = "Transfer In"
                    op_fte_df.at[emp_index, 'Modified'] = True
                    data_logger.info(f"Transfer In processed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Domain_current']} to {row['Domain_next']}")
            else:
                new_entry = pd.Series(CONFIG['COLUMN_VALUES_FTE'], index=CONFIG['OP_FTE_COLUMNS'])
                for static_col, op_col in CONFIG['COLUMN_MAPPING_FTE'].items():
                    col_name = f"{static_col}"
                    if col_name in row:
                        new_entry[op_col] = row[col_name]
                first_name = str(row['Legal First Name_next']) if pd.notna(row['Legal First Name_next']) else ''
                last_name = str(row['Legal Surname_next']) if pd.notna(row['Legal Surname_next']) else ''

                new_entry['Resource Type'] = row['Resource Type_next']
                new_entry['FTE Name'] = first_name + " " + last_name
                new_entry['Employee ID'] = row['Employee ID']                
                new_entry['LANID'] = row['LANID_next']
                new_entry['Role Type'] = row['Role Type_next']
                new_entry['Job Grade'] = row['Job Grade_next']
                new_entry['FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country_next'])
                new_entry['Domain'] = format_domain(row['Domain_next'])
                new_entry['Tech Area'] = format_tech_area(row['Tech Area_next'])
                new_entry['Planning Unit Country'] = row['Planning Unit Country_next']
                new_entry['FTE #'] = row['FTE #_next']
                new_entry['Start Date'] = first_day_of_static_month
                new_entry['End Date'] = eofy
                new_entry['Role Status'] = "Transfer In"
                new_entry['Modified'] = True

                new_entries.append(new_entry)
                data_logger.info(f"Transfer In processed for {new_entry['FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Domain_current']} to {row['Domain_next']}")

        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)

    return op_fte_df

def identify_transfers_out_fte(current_df, next_df, op_fte_df, file_date):   
    """
    Identifies employees who have transferred out of the Security domain and updates the operational plan DataFrame accordingly.
    Process:
    1. Extract and convert the end date string from file_date to a datetime object.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the current month's DataFrame for Security and FTE employees.
    3. Merge the current month's DataFrame with the next month's data on 'Employee ID'.
    4. Identify transfers out of Security domain based on domain and FTE category conditions.
    5. Log the number of identified transfers out.
    6. For each identified transfer out:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found, update the 'End Date' to the last day of the operational month, 'Role Status' to "Transfer Out", and mark as 'Modified'.
       c. Log each processed transfer out.
    7. Return the updated operational plan DataFrame.
    """
    end_date_str, _ = file_date
    if not file_date:
        data_logger.error("Failed to extract date from Static Report")
        return op_fte_df
    
    # Convert the date str back to datetime object to perform date operations
    try:
        last_day_of_op_month = datetime.strptime(end_date_str, '%b-%y').date().strftime('%b-%y')
    except ValueError as e:
        data_logger.error(f"Date conversion error for Transfers Out: {e}")
        return op_fte_df
    
    # Filter for employees who are in Security and FTE in the current month
    current_security_fte = employee_security_fte(current_df)
    
    # Merge current and next df to track changes
    merged_df = pd.merge(current_security_fte, next_df, on=['Employee ID'], suffixes=('_current', '_next'), how='left', indicator=True)

    # Filter for Transfer Out: Was in Security Domain current month, but in different domain next month, still FTE, not contains 'CWR'
    transfers_out = merged_df[
        (merged_df['Domain_next'] != 'Security') &  # Not in Security next month
        (merged_df['FTE Category_next'] == 'FTE') &  # Is FTE Next month
        (merged_df['_merge'] == 'both')  # Still exists in the next month's data
    ]

    data_logger.info(f"Identified {len(transfers_out)} transfers out of the Security Domain for {file_date}")
    if not transfers_out.empty:
        data_logger.info(f"Processing Transfers Out...")
        for index, row in transfers_out.iterrows():
            emp_indices = shorten_filtering_condition_fte(op_fte_df, row['Employee ID'])
            
            if not emp_indices.empty:
                for emp_index in emp_indices:
                    op_fte_df.at[emp_index, 'End Date'] = last_day_of_op_month
                    op_fte_df.at[emp_index, 'Role Status'] = "Transfer Out"
                    op_fte_df.at[emp_index, 'Modified'] = True
                    data_logger.info(f"Transfer out processed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Domain_current']} to {row['Domain_next']}")
    
    return op_fte_df

def identify_grade_changes_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have grade changes in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the end date and start date strings from file_date to datetime objects.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the current and next month's DataFrames for Security and FTE employees.
    3. Merge the filtered DataFrames on 'Employee ID'.
    4. Identify grade changes based on resource type, tech area, and job grade conditions.
    5. Log the number of identified grade changes.
    6. For each identified grade change:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found and matches the criteria, update the existing entry to mark it as not current.
       c. Create a new entry with the new job grade based on the existing entry and mark as 'Grade Change' and 'Modified'.
       d. Log each processed grade change.
    7. Add the new entries to the operational plan DataFrame.
    8. Return the updated operational plan DataFrame.
    """
    end_date_str, start_date_str = file_date
    
    try:
        eofy = get_eofy().strftime('%b-%y')
        last_day_of_op_month = datetime.strptime(end_date_str, '%b-%y').date().strftime('%b-%y')
        first_day_of_static_month = datetime.strptime(start_date_str, '%b-%y').date().strftime('%b-%y')
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df

    # Filter for employees who are in Security and FTE in the current month
    current_security_fte = employee_security_fte(current_df)
    next_security_fte = employee_security_fte(next_df)

    merged_df = pd.merge(current_security_fte, next_security_fte, on='Employee ID', suffixes=('_current', '_next'))

    # Filter conditions for grade changes
    grade_changes = merged_df[
        (merged_df['Resource Type_current'] == merged_df['Resource Type_next']) &  # Has the same Resource Type
        (merged_df['Tech Area_current'] == merged_df['Tech Area_next']) &  # Same Tech Area
        (merged_df['Job Grade_current'] != merged_df['Job Grade_next']) # Different Grade
    ]

    data_logger.info(f"Identified {len(grade_changes)} grade changes in Security Domain for {file_date}")
    if not grade_changes.empty:
        data_logger.info(f"Processing Grade Changes...")
        new_entries = []
        for index, row in grade_changes.iterrows():
            # Get indices of existing entries for the employee in op_fte_df
            emp_indices = employee_filtering_condition_fte(op_fte_df, row['Employee ID'])
            
            if not emp_indices.empty:
                for emp_index in emp_indices:
                    # Check if the grade change already exists in op_fte_df
                    existing_entries = op_fte_df[
                        (op_fte_df['Employee ID'] == row['Employee ID']) & 
                        (op_fte_df['Job Grade'] == row['Job Grade_next']) &
                        (op_fte_df['End Date'] == eofy)
                    ]
                    if not existing_entries.empty:
                        data_logger.info(f"Grade Change existed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}). Skipping.")
                        continue
                    
                    # Update the existing entry to mark it as not current
                    op_fte_df.at[emp_index, 'Job Grade'] = row['Job Grade_current']
                    op_fte_df.at[emp_index, 'End Date'] = last_day_of_op_month
                    op_fte_df.at[emp_index, 'Role Status'] = "Not Current"
                    op_fte_df.at[emp_index, 'Modified'] = True

                    # Create a new entry with the new job grade based on the existing entry
                    new_entry = op_fte_df.loc[emp_index].copy()
                    new_entry['Resource Type'] = row['Resource Type_next']
                    new_entry['Job Grade'] = row['Job Grade_next']
                    new_entry['FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country_next'])
                    new_entry['Domain'] = format_domain(row['Domain_next'])
                    new_entry['Tech Area'] = format_tech_area(row['Tech Area_next'])
                    new_entry['Start Date'] = first_day_of_static_month
                    new_entry['End Date'] = eofy
                    new_entry['Role Status'] = "Grade Change"
                    new_entry['Modified'] = True
                    new_entries.append(new_entry)
                    data_logger.info(f"Grade change processed for {new_entry['FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Job Grade_current']} to {row['Job Grade_next']}")
        
        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)

    return op_fte_df

def identify_internal_mobility_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identify internal mobility for employees within the Security domain and update the operational FTE DataFrame accordingly.
    
    Process:
    1. Convert the end and start dates from string to date format.
    2. Filter employees who are in Security and FTE for both current and next month.
    3. Merge the filtered current month data with next month's data on 'Employee ID'.
    4. Identify employees with internal mobility based on changes in 'Tech Area' while maintaining the same 'Resource Type'.
    5. For each identified internal mobility:
       a. Check if the change already exists in the operational FTE DataFrame.
       b. Update the existing entry's 'Tech Area', 'End Date', 'Role Status', and mark it as modified.
       c. Create a new entry for the employee with updated details reflecting the internal mobility and mark it as modified.
    6. Add the new entries to the operational FTE DataFrame.
    7. Return the updated operational FTE DataFrame.
    """
    end_date_str, start_date_str = file_date
    try:
        # Identify EOFY, last day of op month to be End Date, and first day of static month to be Start Date
        eofy = get_eofy().strftime('%b-%y')
        last_day_of_op_month = datetime.strptime(end_date_str, '%b-%y').date().strftime('%b-%y')
        first_day_of_static_month = datetime.strptime(start_date_str, '%b-%y').date().strftime('%b-%y')
        
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df
    
    # Filter for employees who are in Security and FTE in the current month
    current_security_fte = employee_security_fte(current_df)
    next_security_fte = employee_security_fte(next_df)

    # Merge the filtered current month data with next month's data on Employee ID
    merged_df = pd.merge(current_security_fte, next_security_fte, on='Employee ID', suffixes=('_current', '_next'))

    # Filter for internal mobility conditions
    internal_mobility = merged_df[
        (merged_df['Resource Type_current'] == merged_df['Resource Type_next']) &  # Have the same Resource Type in both months
        (merged_df['Tech Area_current'] != merged_df['Tech Area_next'])  # Tech Area has changed
    ]
    
    # Print out all headers after merging
    print("Headers after merging:", merged_df.columns.tolist())

    data_logger.info(f"Identified {len(internal_mobility)} Internal Mobility in Security Domain for {file_date}")
    if not internal_mobility.empty:
        data_logger.info('Processing Internal Mobility...')
        new_entries = []
        for index, row in internal_mobility.iterrows():
            emp_indices = employee_filtering_condition_fte(op_fte_df, row['Employee ID'])

            if not emp_indices.empty:
                processed_internal_mobility = False
                for emp_index in emp_indices:
                    # Check if the change already exists
                    existing_entries = op_fte_df[
                        (op_fte_df['Employee ID'] == row['Employee ID']) & 
                        (op_fte_df['Tech Area'] == row['Tech Area_next']) &
                        (op_fte_df['End Date'] == eofy)
                    ]
                    if not existing_entries.empty:
                        data_logger.info(f"Internal Mobility already exists for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}). Skipping.")
                        processed_internal_mobility = True
                        break

                if not processed_internal_mobility:
                    op_fte_df.at[emp_index, 'Tech Area'] = format_tech_area(row['Tech Area_current'])
                    op_fte_df.at[emp_index, 'End Date'] = last_day_of_op_month
                    op_fte_df.at[emp_index, 'Role Status'] = "Not Current"
                    op_fte_df.at[emp_index, 'Modified'] = True

                    new_entry = op_fte_df.loc[emp_index].copy()
                    new_entry['Role Type'] = row['Role Type_next']
                    new_entry['Job Grade'] = row['Job Grade_next']
                    new_entry['Tech Area'] = format_tech_area(row['Tech Area_next'])
                    new_entry['Start Date'] = first_day_of_static_month
                    new_entry['End Date'] = eofy
                    new_entry['FTE #'] = row['FTE #_next']
                    new_entry['Role Status'] = "Internal Mobility"
                    new_entry['Modified'] = True

                    new_entries.append(new_entry)
                    data_logger.info(f"Internal Mobility processed for {new_entry['FTE Name']} (Employee ID: {row['Employee ID']}) to {row['Tech Area_next']}")

        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)

    return op_fte_df

def indetify_conversions_within_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have converted within the FTE categories (e.g., from Fixed Term to Permanent) in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the end date and start date strings from file_date to datetime objects.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the current and next month's DataFrames for Security and FTE employees.
    3. Merge the filtered DataFrames on 'Employee ID'.
    4. Identify conversions within FTE based on resource type conditions.
    5. Log the number of identified conversions within FTE.
    6. For each identified conversion within FTE:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found, update the existing entry to mark it as not current.
       c. Create a new entry with the new resource type based on the existing entry and mark as 'Modified'.
       d. Log each processed conversion within FTE.
    7. Add the new entries to the operational plan DataFrame.
    8. Return the updated operational plan DataFrame.
    """
    end_date_str, start_date_str = file_date
    try:
        eofy = get_eofy().strftime('%b-%y')
        last_day_of_op_month = datetime.strptime(end_date_str, '%b-%y').date().strftime('%b-%y')
        first_day_of_static_month = datetime.strptime(start_date_str, '%b-%y').date().strftime('%b-%y')
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df
    
    # Filter for employees who are in Security and FTE in the current month
    current_security_fte = employee_security_fte(current_df)
    next_security_fte = employee_security_fte(next_df)

    # Merge current and next df for comparison
    merged_df = pd.merge(current_security_fte, next_security_fte, on='Employee ID', suffixes=('_current', '_next'))

    # Filter conditions
    conversions_fixed_perm = merged_df[(merged_df['Resource Type_current'] != merged_df['Resource Type_next'])] # Has different resource type
    
    data_logger.info(f"Identified {len(conversions_fixed_perm)} conversions from Fixed to Perm in Security Domain for {file_date}...")
    if not conversions_fixed_perm.empty:
        data_logger.info(f"Processing Conversions within Security FTE...")
        new_entries = []
        for index, row in conversions_fixed_perm.iterrows():
            emp_indices = employee_filtering_condition_fte(op_fte_df, row['Employee ID'])

            if not emp_indices.empty:
                for emp_index in emp_indices:
                    op_fte_df.at[emp_index, 'Resource Type'] = row['Resource Type_current']
                    op_fte_df.at[emp_index, 'End Date'] = last_day_of_op_month
                    op_fte_df.at[emp_index, 'Role Status'] = f"Conversion from {row['Resource Type_current']} to {row['Resource Type_next']}"
                    op_fte_df.at[emp_index, 'Modified'] = True

                    new_entry = op_fte_df.loc[emp_index].copy()
                    new_entry['Resource Type'] = row['Resource Type_next']
                    new_entry['Role Type'] = row['Role Type_next']
                    new_entry['Job Grade'] = row ['Job Grade_next']
                    new_entry['Tech Area'] = row['Tech Area_next']
                    new_entry['Start Date'] = first_day_of_static_month
                    new_entry['End Date'] = eofy
                    new_entry['Role Status'] = f"Conversion from {row['Resource Type_current']} to {row['Resource Type_next']}"
                    new_entry['Modified'] = True

                    new_entries.append(new_entry)
                    data_logger.info(f"Conversion processed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Resource Type_current']} to {row['Resource Type_next']}")

            else:
                new_entry = pd.Series(CONFIG['COLUMN_VALUES_FTE'], index=CONFIG['OP_FTE_COLUMNS'])
                for static_col, op_col in CONFIG['COLUMN_MAPPING_FTE'].items():
                    col_name = f"{static_col}"
                    if col_name in row:
                        new_entry[op_col] = row[col_name]
                first_name = str(row['Legal First Name_next']) if pd.notna(row['Legal First Name_next']) else ''
                last_name = str(row['Legal Surname_next']) if pd.notna(row['Legal Surname_next']) else ''

                new_entry['Resource Type'] = row['Resource Type_next']
                new_entry['FTE Name'] = first_name + " " + last_name
                new_entry['Employee ID'] = row['Employee ID']
                new_entry['LANID'] = row['LANID_next']
                new_entry['Role Type'] = row['Role Type_next']
                new_entry['Job Grade'] = row['Job Grade_next']
                new_entry['FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country_next'])
                new_entry['Domain'] = format_domain(row['Domain_next'])
                new_entry['Tech Area'] = format_tech_area(row['Tech Area_next'])
                new_entry['Planning Unit Country'] = row['Planning Unit Country_next']
                new_entry['FTE #'] = row['FTE #_next']
                new_entry['Start Date'] = first_day_of_static_month
                new_entry['End Date'] = eofy
                new_entry['Role Status'] = f"Conversion from {row['Resource Type_current']} to {row['Resource Type_next']}"
                new_entry['Modified'] = True

                new_entries.append(new_entry)
                data_logger.info(f"Conversion processed for {new_entry['FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Resource Type_current']} to {row['Resource Type_next']}")
    
        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)

    return op_fte_df

def identify_conversions_cwr_to_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have converted from CWR to FTE in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the start date string from file_date to a datetime object.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the next month's DataFrame for Security and FTE employees.
    3. Merge the current month's DataFrame with the next month's filtered data on 'Employee ID'.
    4. Identify conversions from CWR to FTE based on domain and FTE category conditions.
    5. Log the number of identified conversions from CWR to FTE.
    6. For each identified conversion from CWR to FTE:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found, update the existing entry to mark it as not current.
       c. Create a new entry with the new resource type based on the existing entry and mark as 'Modified'.
       d. Log each processed conversion from CWR to FTE.
    7. Add the new entries to the operational plan DataFrame.
    8. Return the updated operational plan DataFrame.
    """
    _, start_date_str = file_date
    try:
        eofy = get_eofy().strftime('%b-%y')
        first_day_of_static_month = datetime.strptime(start_date_str, '%b-%y').date().strftime('%b-%y')
    
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df
    
    # Filter for employees who are in Security and FTE in the current month
    next_security_fte = employee_security_fte(next_df)
    
    # Merge current and next df for comparison
    merged_df = pd.merge(current_df, next_security_fte, on='Employee ID', suffixes=('_current', '_next'))

    # Filter conditions
    conversions_cwr_fte = merged_df[
        (merged_df['Domain_current'] == 'Security') & # Is in Security current month
        (merged_df['FTE Category_current'] == 'Non-FTE') # Is NOT FTE current month
    ]

    data_logger.info(f"Identified {len(conversions_cwr_fte)} conversions from CWR to FTE in Security Domain for {file_date}...")
    if not conversions_cwr_fte.empty:
        data_logger.info(f"Processing Conversions from CWR to FTE...")
        new_entries = []
        for index, row in conversions_cwr_fte.iterrows():
            emp_indices = employee_filtering_condition_fte(op_fte_df, row['Employee ID'])

            if not emp_indices.empty:
                for emp_index in emp_indices:
                    # Check if the change already exists
                    existing_entries = op_fte_df[
                        (op_fte_df['Employee ID'] == row['Employee ID']) & 
                        (op_fte_df['Resource Type'] == row['Resource Type_next']) &
                        (op_fte_df['End Date'] == eofy)
                    ]
                    if not existing_entries.empty:
                        data_logger.info(f"Conversion already exists for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}). Skipping.")
                        continue
            else:
                new_entry = pd.Series(CONFIG['COLUMN_VALUES_FTE'], index=CONFIG['OP_FTE_COLUMNS'])
                for static_col, op_col in CONFIG['COLUMN_MAPPING_FTE'].items():
                    col_name = f"{static_col}"
                    if col_name in row:
                        new_entry[op_col] = row[col_name]
                first_name = str(row['Legal First Name_next']) if pd.notna(row['Legal First Name_next']) else ''
                last_name = str(row['Legal Surname_next']) if pd.notna(row['Legal Surname_next']) else ''

                new_entry['Resource Type'] = row['Resource Type_next']
                new_entry['FTE Name'] = first_name + " " + last_name
                new_entry['Employee ID'] = row['Employee ID']
                new_entry['LANID'] = row['LANID_next']
                new_entry['Role Type'] = row['Role Type_next']
                new_entry['Job Grade'] = row['Job Grade_next']
                new_entry['FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country_next'])
                new_entry['Domain'] = format_domain(row['Domain_next'])
                new_entry['Tech Area'] = format_tech_area(row['Tech Area_next'])
                new_entry['Planning Unit Country'] = row['Planning Unit Country_next']
                new_entry['FTE #'] = row['FTE #_next']
                new_entry['Start Date'] = first_day_of_static_month
                new_entry['End Date'] = eofy
                new_entry['Role Status'] = "Conversion to FTE"
                new_entry['Modified'] = True

                new_entries.append(new_entry)
                data_logger.info(f"Conversion processed for {new_entry['FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Resource Type_current']} to {row['Resource Type_next']}")
    
        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)

    return op_fte_df

def identify_conversions_fte_to_cwr(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have converted from FTE to CWR in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the end date string from file_date to a datetime object.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the current month's DataFrame for Security and FTE employees.
    3. Merge the current month's DataFrame with the next month's data on 'Employee ID'.
    4. Identify conversions from FTE to CWR based on domain and FTE category conditions.
    5. Log the number of identified conversions from FTE to CWR.
    6. For each identified conversion from FTE to CWR:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found, update the existing entry to mark it as not current and set the 'End Date' to the last day of the operational month.
       c. Log each processed conversion from FTE to CWR.
    7. Return the updated operational plan DataFrame.
    """
    end_date_str, _ = file_date
    try:
        last_day_of_op_month = datetime.strptime(end_date_str, '%b-%y').date().strftime('%b-%y')
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df
    
    # Filter for employees who are in Security and FTE in the current month
    current_security_fte = employee_security_fte(current_df)

    # Merge current and next df for comparison
    merged_df = pd.merge(current_security_fte, next_df, on='Employee ID', suffixes=('_current', '_next'))
 
    # Filter conditions
    conversions_from_fte = merged_df[
        (merged_df['Domain_next'] == 'Security') & # Is in Security Domain next month
        (merged_df['FTE Category_next'] == 'Non-FTE')  # Is FTE next month
    ]

    data_logger.info(f"Identified {len(conversions_from_fte)} conversions from FTE in Security Domain for {file_date}...")
    if not conversions_from_fte.empty:
        data_logger.info(f"Processing Conversions from FTE...")
        for index, row in conversions_from_fte.iterrows():
            emp_indices = employee_filtering_condition_fte(op_fte_df, row['Employee ID'])
            
            if not emp_indices.empty:
                for emp_index in emp_indices:
                    op_fte_df.at[emp_index, 'Resource Type'] = row['Resource Type_current']
                    op_fte_df.at[emp_index,'Employee ID'] = row['Employee ID']
                    op_fte_df.at[emp_index, 'Job Grade'] = row['Job Grade_current']
                    op_fte_df.at[emp_index, 'End Date'] = last_day_of_op_month
                    op_fte_df.at[emp_index, 'Role Status'] = "Conversion from FTE"
                    op_fte_df.at[emp_index, 'Modified'] = True
                    data_logger.info(f"Conversion processed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Resource Type_current']} to {row['Resource Type_next']}")
    
    return op_fte_df

def identify_line_manager_changes_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have experienced a line manager change in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Filter the current and next month's DataFrames for Security and FTE employees.
    2. Merge the filtered DataFrames on 'Employee ID'.
    3. Identify line manager changes based on 'Supervisor Employee ID' differences between the current and next month.
    4. Log the number of identified line manager changes.
    5. For each identified line manager change:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found, update the 'Line Manager' and mark the entry as 'Modified'.
       c. Log each processed line manager change.
    6. Return the updated operational plan DataFrame.
    """
    # Filter for employees who are in Security and FTE in the current month
    current_security_fte = employee_security_fte(current_df)
    next_security_fte = employee_security_fte(next_df)

    merged_df = pd.merge(current_security_fte, next_security_fte, on='Employee ID', suffixes=('_current', '_next'))
    # Filter conditions
    line_manager_changes = merged_df[(merged_df['Supervisor Employee ID_current'] != merged_df['Supervisor Employee ID_next'])] # Have different line manager

    data_logger.info(f"Identified {len(line_manager_changes)} Line Manager Changes in Security Domain for {file_date}")
    if not line_manager_changes.empty:
        data_logger.info('Processing Line Manager Changes...')
        for index, row in line_manager_changes.iterrows():
            emp_indices = shorten_filtering_condition_fte(op_fte_df, row['Employee ID'])
            if not emp_indices.empty:
                for emp_index in emp_indices:
                    op_fte_df.at[emp_index, 'Modified'] = True
                    op_fte_df.at[emp_index, 'Line Manager'] = f"{row['Supervisor Legal First Name_next']} {row['Supervisor Legal Surname_next']}"
                    data_logger.info(f"Line Manager Change processed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}) from {int(row['Supervisor Employee ID_current'])} to {int(row['Supervisor Employee ID_next'])}")
    
    return op_fte_df

def identify_location_changes_fte(current_df, next_df, op_fte_df, file_date):
    """
    Identifies employees who have experienced a location change in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Extract and convert the end date and start date strings from file_date to datetime objects.
       - If date extraction or conversion fails, log an error and return the original op_fte_df.
    2. Filter the current and next month's DataFrames for Security and FTE employees.
    3. Merge the filtered DataFrames on 'Employee ID'.
    4. Identify location changes based on resource type and location conditions.
    5. Log the number of identified location changes.
    6. For each identified location change:
       a. Check if the employee already exists in the operational plan DataFrame.
       b. If found, update the existing entry to mark it as not current and set the 'End Date' to the last day of the operational month.
       c. Create a new entry with the new location based on the existing entry and mark as 'Location Change' and 'Modified'.
       d. Log each processed location change.
    7. Add the new entries to the operational plan DataFrame.
    8. Return the updated operational plan DataFrame.
    """
    end_date_str, start_date_str = file_date
    
    try:
        eofy = get_eofy().strftime('%b-%y')
        last_day_of_op_month = datetime.strptime(end_date_str, '%b-%y').date().strftime('%b-%y')
        first_day_of_static_month = datetime.strptime(start_date_str, '%b-%y').date().strftime('%b-%y')
    
    except ValueError as e:
        data_logger.error(f"Date conversion error: {e}")
        return op_fte_df
    
    # Filter for employees who are in Security and FTE in the current month
    current_security_fte = employee_security_fte(current_df)
    next_security_fte = employee_security_fte(next_df)

    # Merge the filtered current month data with next month's data on Employee ID
    merged_df = pd.merge(current_security_fte, next_security_fte, on='Employee ID', suffixes=('_current', '_next'))

    # Filter conditions
    location_changes = merged_df[
        (merged_df['Resource Type_current'] == merged_df['Resource Type_next']) & # Have the same Resource Type in both months
        (merged_df['Planning Unit Country_current'] != merged_df['Planning Unit Country_next']) # Different location comparing to current month
    ]

    data_logger.info(f"Identified {len(location_changes)} location changes in Security Domain for {file_date}")
    if not location_changes.empty:
        data_logger.info('Processing Location Changes...')
        new_entries = []
        for index, row in location_changes.iterrows():
            emp_indices = shorten_filtering_condition_fte(op_fte_df, row['Employee ID'])
            
            if not emp_indices.empty:
                for emp_index in emp_indices:
                    op_fte_df.at[emp_index, 'FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country_current'])
                    op_fte_df.at[emp_index, 'Planning Unit Country'] = row['Planning Unit Country_current']
                    op_fte_df.at[emp_index, 'End Date'] = last_day_of_op_month
                    op_fte_df.at[emp_index, 'Role Status'] = "Not Current"
                    op_fte_df.at[emp_index, 'Modified'] = True

                    new_entry = op_fte_df.loc[emp_index].copy()
                    new_entry['FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country_next'])
                    new_entry['Planning Unit Country'] = row['Planning Unit Country_next']
                    new_entry['Start Date'] = first_day_of_static_month
                    new_entry['FTE #'] = row['FTE #_next']
                    new_entry['End Date'] = eofy
                    new_entry['Role Status'] = "Location Change"
                    new_entry['Modified'] = True

                    new_entries.append(new_entry)
                    data_logger.info(f"Location change processed for {op_fte_df.at[emp_index, 'FTE Name']} (Employee ID: {row['Employee ID']}) from {row['Planning Unit Country_current']} to {row['Planning Unit Country_next']}")
        
        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)

    return op_fte_df