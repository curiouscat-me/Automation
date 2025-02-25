import pandas as pd
from modules.logger import data_logger
from modules.formatting import format_domain, format_tech_area

def sanity_checks_fte(current_df, next_df, op_fte_df):
    """
    Perform sanity checks on the existing Op Plan data and update it with the correct information from next month data.
    Skip rows where 'Skip' column is marked as 'Past', except for formatting the 'Domain' and 'Tech Area'.

    Returns:
    DataFrame: Updated DataFrame after performing sanity checks and updates.
    
    Process:
    1. Iterate over each row in the op_fte_df.
    2. For each employee:
       a. Check if the employee ID exists in next_df.
       b. If the employee's domain in next_df is not 'Security', check current_df.
       c. If neither next_df nor current_df has the employee in the 'Security' domain, skip the row.
       d. If the row's 'Skip' status is 'Past', only format the 'Domain' and 'Tech Area' and skip further updates.
       e. Log how many rows are skipped due to no information in the domain.
       f. Update op_fte_df columns with values from current_row or next_row, prioritizing next_df.
       g. Ensure 'Domain' and 'Tech Area' are formatted properly.
    3. Return the updated op_fte_df.
    """
    no_info_skip = 0 # Counter for rows skipped due to no information in 'Security' domain
    for index, row in op_fte_df.iterrows():
        employee_id = row['Employee ID']
        employee_name = row['FTE Name']
        skip_status = row['Skip']

        # Skip most updates if the row is marked as 'Past', except for formatting 'Domain' and 'Tech Area'
        if skip_status == 'Past':
            op_fte_df.at[index, 'Tech Area'] = format_tech_area(row['Tech Area'])
            op_fte_df.at[index, 'Domain'] = format_domain(row['Domain'])
            continue

        # Skip if employee doesn't exist in next_df
        if pd.isna(employee_id) or employee_id not in next_df['Employee ID'].values:
            continue

        # Retrieve the corresponding next month's row
        next_row = next_df[next_df['Employee ID'] == employee_id].iloc[0]

        # Check if the information in next_df is not in Security Domain
        if next_row['Domain'] != 'Security':
            current_row = current_df[current_df['Employee ID'] == employee_id]
            if current_row.empty or current_row.iloc[0]['Domain'] != 'Security':
                no_info_skip += 1  # Increment the counter for no information rows
                data_logger.info(f"Skipping Employee {employee_name} (Employee ID: {employee_id}) - No information in Security domain.")
                continue
            current_row = current_row.iloc[0]
        else:
            current_row = next_row

        # Update the necessary fields
        for column in next_df.columns:
            if column in op_fte_df.columns and column != 'Planning Unit Country':
                original_value = row[column]
                new_value = current_row[column]
                if pd.notna(new_value) and original_value != new_value:
                    op_fte_df.at[index, column] = new_value

        # Always format 'Tech Area' and 'Domain' properly
        op_fte_df.at[index, 'Tech Area'] = format_tech_area(next_row['Tech Area'])
        op_fte_df.at[index, 'Domain'] = format_domain(next_row['Domain'])

    if no_info_skip > 0:
        data_logger.info(f"Skipped {no_info_skip} rows due to no information in Security domain.")
    
    return op_fte_df

def sanity_checks_ms(current_df, next_df, op_ms_df):
    """
    Perform sanity checks on the existing Op Plan data and update it with the correct information from next_df.
    Skip updating employees whose End Date has already passed, except for formatting the 'Domain' and 'Tech Area'.
    
    Returns:
    DataFrame: Updated DataFrame after performing sanity checks and updates.
    
    Process:
    1. Iterate over each row in the op_ms_df.
    2. For each employee:
       a. Check if the employee ID exists in next_df.
       b. If the employee's domain in next_df is not 'Security', check current_df.
       c. If neither next_df nor current_df has the employee in the 'Security' domain, skip the row.
       d. If the row's 'Skip' status is 'Past', only format the 'Domain' and 'Tech Area' and skip further updates.
       e. Log how many rows are skipped due to no information in the domain.
       f. Update op_fte_df columns with values from current_row or next_row, prioritizing next_df.
       g. Ensure 'Domain' and 'Tech Area' are formatted properly.
    3. Return the updated op_ms_df.
    """
    no_info_skip = 0  # Counter for rows skipped due to no information in 'Security' domain
    for index, row in op_ms_df.iterrows():
        employee_id = row['Employee ID']
        employee_name = row['Resource Name']
        skip_status = row['Skip']

        # Skip most updates if the row is marked as 'Past', except for formatting 'Domain' and 'Tech Area'
        if skip_status == 'Past':
            op_ms_df.at[index, 'Tech Area'] = format_tech_area(row['Tech Area'])
            op_ms_df.at[index, 'Domain'] = format_domain(row['Domain'])
            continue

        # Skip if employee doesn't exist in next_df
        if pd.isna(employee_id) or employee_id not in next_df['Employee ID'].values:
            continue

        # Retrieve the corresponding next month's row
        next_row = next_df[next_df['Employee ID'] == employee_id].iloc[0]

        # Check if the information in next_df is not in Security Domain
        if next_row['Domain'] != 'Security':
            current_row = current_df[current_df['Employee ID'] == employee_id]
            if current_row.empty or current_row.iloc[0]['Domain'] != 'Security':
                no_info_skip += 1  # Increment the counter for no information rows
                data_logger.info(f"Skipping Employee {employee_name} (Employee ID: {employee_id}) - No information in Security domain.")
                continue
            current_row = current_row.iloc[0]
        else:
            current_row = next_row

        # Update the necessary fields
        for column in next_df.columns:
            if column in op_ms_df.columns and column != 'Planning Unit Country':
                original_value = row[column]
                new_value = current_row[column]
                if pd.notna(new_value) and original_value != new_value:
                    op_ms_df.at[index, column] = new_value

        # Always format 'Tech Area' and 'Domain' properly
        op_ms_df.at[index, 'Tech Area'] = format_tech_area(next_row['Tech Area'])
        op_ms_df.at[index, 'Domain'] = format_domain(next_row['Domain'])

    if no_info_skip > 0:
        data_logger.info(f"Skipped {no_info_skip} rows due to no information in Security domain Static Report.")
    
    return op_ms_df