import pandas as pd
from config.config_GUI import CONFIG
from modules.logger import data_logger
from modules.formatting import *
from modules.new_entries import *
from modules.employee_filtering_conditions import *
      
"""
Global Parameters:
    current_df (DataFrame): The DataFrame containing the current month data from the static report.
    next_df (DataFrame): The DataFrame containing the next month data from the static report.
    op_ms_df (DataFrame): The DataFrame containing the operational plan data for MS.
    Returns:
    DataFrame: Updated operational plan DataFrame with missing employees added.
"""

def identify_missing_employees_fte(current_df, next_df, op_fte_df):
    """
    Identifies employees missing from the operational plan in the Security domain and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Filter the current and next month's DataFrames for Security and FTE employees.
    2. Merge the filtered DataFrames on 'Employee ID' using an outer join.
    3. Ensure relevant columns are strings for the contains check.
    4. Identify missing employees based on the following conditions:
       a. Condition 1: Appear in both current and next month static reports but not in the operational plan.
       b. Condition 2: Appear only in the current month static report but not in the operational plan.
    5. Log the number of identified missing employees.
    6. For each identified missing employee:
       a. Create a new entry with the missing employee's information.
       b. Ensure values are strings before concatenation.
       c. Mark the new entry as 'Missing from Op FTE' and 'Modified'.
       d. Log each added missing employee.
    7. Add the new entries to the operational plan DataFrame.
    8. Return the updated operational plan DataFrame.
    """
    # Use the reusable function to filter for Security and FTE
    current_security_fte = employee_security_fte(current_df)
    next_security_fte = employee_security_fte(next_df)

    # Combine current and next df
    merged_df = pd.merge(current_security_fte, next_security_fte, on='Employee ID', how='outer', suffixes=('_current', '_next'), indicator=True)

    # Ensure relevant columns are strings for the contains check
    merged_df['Resource Type_current'] = merged_df['Resource Type_current'].astype(str)
    merged_df['Resource Type_next'] = merged_df['Resource Type_next'].astype(str)
 
    # Filtering for Missing Employee ID from the Op Plan
    missing_entries = merged_df[
        (   # Condition 1: Appear in Both Current and Next Month Static Reports but Not in the Op Plan
            (~merged_df['Resource Type_current'].str.contains('CWR', na=False)) & # Is not CWR current month
            (~merged_df['Resource Type_next'].str.contains('CWR', na=False)) & # Is not CWR current month
            (~merged_df['Employee ID'].isin(op_fte_df['Employee ID'])) & # Not in Op Plan
            (merged_df['_merge'] != 'right_only') # Not only in the next month
        ) | (# Condition 2: Appear Only in the Current Month Static Report but Not in the Op Plan
            (~merged_df['Resource Type_current'].str.contains('CWR', na=False)) & # Is not CWR current month
            (~merged_df['Employee ID'].isin(op_fte_df['Employee ID'])) & # Not in Op Plan
            (merged_df['_merge'] == 'left_only') # Only in the current month
        )
    ]

    data_logger.info(f"Identified {len(missing_entries)} missing employees in the Security domain.")
    if not missing_entries.empty:
        new_entries = []
        for index, row in missing_entries.iterrows():
            new_entry = pd.Series(CONFIG['COLUMN_VALUES_FTE'], index=CONFIG['OP_FTE_COLUMNS'])
            for static_col, op_col in CONFIG['COLUMN_MAPPING_FTE'].items():
                col_name = static_col
                if col_name in row:
                    new_entry[op_col] = row[col_name]

            # Ensure values are strings before concatenation
            first_name = str(row['Legal First Name_current']) if pd.notna(row['Legal First Name_current']) else ''
            last_name = str(row['Legal Surname_current']) if pd.notna(row['Legal Surname_current']) else ''

            new_entry['Resource Type'] = row['Resource Type_current']
            new_entry['FTE Name'] = first_name + " " + last_name
            new_entry['Employee ID'] = row['Employee ID']
            new_entry['LANID'] = row['LANID_current']
            new_entry['Role Type'] = row['Role Type_current']
            new_entry['Job Grade'] = row['Job Grade_current']
            new_entry['FTE based Country\n(drives FTE rates calc)'] = map_to_hub_FTE(row['Planning Unit Country_current'])
            new_entry['Domain'] = format_domain(row['Domain_current'])
            new_entry['Tech Area'] = format_tech_area(row['Tech Area_current'])
            new_entry['Planning Unit Country'] = row['Planning Unit Country_current']
            new_entry['FTE #'] = row['FTE #_current']
            new_entry['Start Date'] = '-'
            new_entry['End Date'] = '-'
            new_entry['Role Status'] = "Missing from Op FTE"
            new_entry['Modified'] = True

            new_entries.append(new_entry)
            data_logger.info(f"Missing Employee added: {new_entry['FTE Name']} (Employee ID: {row['Employee ID']})")

        op_fte_df = add_new_entries_fte(op_fte_df, new_entries)
        
    return op_fte_df

def identify_missing_employees_ms(current_df, next_df, op_ms_df):
    """
    Identifies employees missing from the operational plan in the Security domain for MS and updates the operational plan DataFrame accordingly.
    
    Process:
    1. Filter the current and next month's DataFrames for Security and MS employees.
    2. Load the global staff list.
    3. Merge the filtered DataFrames on 'Employee ID' using an outer join.
    4. Ensure relevant columns are strings for the contains check.
    5. Identify missing employees based on the following conditions:
       a. Condition 1: Appear in both current and next month static reports but not in the operational plan.
       b. Condition 2: Appear only in the current month static report but not in the operational plan.
    6. Merge the identified missing entries with the global staff list to get vendor names.
    7. Log the number of identified missing employees.
    8. For each identified missing employee:
       a. Create a new entry with the missing employee's information.
       b. Ensure values are strings before concatenation.
       c. Mark the new entry as 'Missing from Op MS' and 'Modified'.
       d. Log each added missing employee.
    9. Add the new entries to the operational plan DataFrame for Managed Services.
    10. Return the updated operational plan DataFrame.
    """
    # Use the reusable function to filter for Security and FTE
    current_security_ms = employee_security_ms(current_df)
    next_security_ms = employee_security_ms(next_df)

    global_staff_df = pd.read_excel(CONFIG['GLOBAL_STAFF_LIST'])
    
    # Combine current and next df
    merged_df = pd.merge(current_security_ms, next_security_ms, on='Employee ID', how='outer', suffixes=('_current', '_next'), indicator=True)

    # Ensure relevant columns are strings for the contains check
    merged_df['Resource Type_current'] = merged_df['Resource Type_current'].astype(str)
    merged_df['Resource Type_next'] = merged_df['Resource Type_next'].astype(str)

    missing_entries = merged_df[
        (   # Condition 1: Appear in Both Current and Next Month Static Reports but Not in the Op Plan
            (merged_df['_merge'] != 'right_only') & # Not only in the next month
            (~merged_df['Employee ID'].isin(op_ms_df['Employee ID'])) # Not in Op Plan
        ) | (# Condition 2: Appear Only in the Current Month Static Report but Not in the Op Plan
            (merged_df['_merge'] == 'left_only') &  # Only in the current month
            (~merged_df['Employee ID'].isin(op_ms_df['Employee ID']))  # Not in Op Plan
        )
    ]

    missing_entries = pd.merge(missing_entries, global_staff_df[['Employee ID', 'Vendor Name']], on='Employee ID', how='left')

    data_logger.info(f"Identified {len(missing_entries)} Missing from Op MS in the Security domain.")
    if not missing_entries.empty:
        new_entries = []
        for index, row in missing_entries.iterrows():
            new_entry = pd.Series(CONFIG['COLUMN_VALUES_MS'], index=CONFIG['OP_MS_COLUMNS'])
            for static_col, op_col in CONFIG['COLUMN_MAPPING_MS'].items():
                col_name = static_col
                if col_name in row:
                    new_entry[op_col] = row[col_name]

            # Ensure values are strings before concatenation
            first_name = str(row['Legal First Name_current']) if pd.notna(row['Legal First Name_current']) else ''
            last_name = str(row['Legal Surname_current']) if pd.notna(row['Legal Surname_current']) else ''

            new_entry['Resource Type'] = row['Resource Type_current']
            new_entry['Resource Name'] = first_name + " " + last_name
            new_entry['Vendor Name'] = row['Vendor Name']
            new_entry['Employee ID'] = row['Employee ID']
            new_entry['LANID'] = row['LANID_current']
            new_entry['Role Type'] = row['Role Type_current']
            new_entry['Domain'] = format_domain(row['Domain_current'])
            new_entry['Tech Area'] = format_tech_area(row['Tech Area_current'])
            new_entry['Planning Unit Country'] = row['Planning Unit Country_current']
            new_entry['Start Date'] = '-'
            new_entry['End Date'] = '-'
            new_entry['Role Status'] = "Missing from Op MS"
            new_entry['Modified'] = True

            new_entries.append(new_entry)
            data_logger.info(f"Missing from Op MS added: {new_entry['Resource Name']} (Employee ID: {row['Employee ID']})")

        op_ms_df = add_new_entries_ms(op_ms_df, new_entries)

    return op_ms_df