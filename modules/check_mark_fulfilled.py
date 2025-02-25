import pandas as pd

def check_and_mark_fulfilled_fte(current_df, next_df, op_fte_df):
    """
    Checks for employees with complete information and marks them as 'Fulfilled'.
    
    Returns:
    DataFrame: Updated DataFrame with 'Fulfilled' column marked.
    
    Process:
    1. Ensure that the 'Start Date' and 'End Date' columns are of datetime type.
    2. Add an empty 'Fulfilled' column to the op_fte_df.
    3. Group the op_fte_df by 'Employee ID'.
    4. For each employee:
       a. Skip rows with specific statuses defined in 'skip_statuses'.
       b. Check if there are multiple roles with non-overlapping dates and mark 'Fulfilled' as 'Yes' if true.
       c. Check if the employee's information remains unchanged between the current and next month and mark 'Fulfilled' as 'Yes' if true.
    5. Return the updated op_fte_df.
    """
    # Ensure 'Start Date' and 'End Date' are of datetime type, and handle errors
    op_fte_df['Start Date'] = pd.to_datetime(op_fte_df['Start Date'], errors='coerce')
    op_fte_df['End Date'] = pd.to_datetime(op_fte_df['End Date'], errors='coerce')

    op_fte_df['Fulfilled'] = ''
    employee_id = op_fte_df['Employee ID']
    skip_statuses = ['New Hire', 'Transfer In', 'Conversion', 'Internal Mobility', 'Location Change', 'Missing from Op FTE']

    # Group by Employee ID to check for completeness
    grouped_employee_id = op_fte_df.groupby(employee_id)

    for employee_id, employee in grouped_employee_id:
        # Skip rows with specific statuses
        if any(employee['Role Status'].isin(skip_statuses)):
            continue

        # Check if there are multiple roles with non-overlapping dates
        if len(employee) > 1:
            fulfilled = True
            sorted_group = employee.sort_values(by='Start Date')
            for i in range(len(sorted_group) - 1):
                # Check if the end date of the current entry is before the start date of the next entry
                if pd.isna(sorted_group.iloc[i]['End Date']) or pd.isna(sorted_group.iloc[i + 1]['Start Date']):
                    fulfilled = False
                    break
                if sorted_group.iloc[i]['End Date'] >= sorted_group.iloc[i + 1]['Start Date']:
                    fulfilled = False
                    break
            if fulfilled:
                op_fte_df.loc[employee.index, 'Fulfilled'] = 'Yes'
 
        # Check if the employee's information remains unchanged between current and next month
        current_info = current_df[current_df['Employee ID'] == employee_id]
        next_info = next_df[next_df['Employee ID'] == employee_id]

        if not current_info.empty and not next_info.empty:
            columns_to_check = ['Employee ID', 'Tech Area', 'Domain', 'Resource Type', 'Job Grade']
            current_values = current_info[columns_to_check].values[0]
            next_values = next_info[columns_to_check].values[0]
            if (current_values == next_values).all():
                op_fte_df.loc[employee.index, 'Fulfilled'] = 'Yes'
    
    return op_fte_df

def check_and_mark_fulfilled_ms(current_df, next_df, op_ms_df):
    """
    Checks for employees with complete information and marks them as 'Fulfilled'.
    
    Returns:
    DataFrame: Updated DataFrame with 'Fulfilled' column marked.
    Process:
    1. Ensure that the 'Start Date' column is of datetime type.
    2. Add an empty 'Fulfilled' column to the op_ms_df.
    3. Group the op_ms_df by 'Employee ID'.
    4. For each employee:
       a. Skip rows with specific statuses defined in 'skip_statuses'.
       b. Check if there are multiple roles with non-overlapping dates and mark 'Fulfilled' as 'Yes' if true.
       c. Check if the employee's information remains unchanged between the current and next month and mark 'Fulfilled' as 'Yes' if true.
    5. Return the updated op_ms_df.
    """
    # Ensure 'Start Date' and 'End Date' are of datetime type, and handle errors
    op_ms_df['Start Date'] = pd.to_datetime(op_ms_df['Start Date'], errors='coerce')
    op_ms_df['End Date'] = pd.to_datetime(op_ms_df['End Date'], errors='coerce')

    op_ms_df['Fulfilled'] = ''
    employee_id = op_ms_df['Employee ID']
    skip_statuses = ['New Hire', 'Transfer In', 'Conversion', 'Internal Mobility', 'Location Change', 'Missing from Op MS']

    # Group by Employee ID to check for completeness
    grouped_employee_id = op_ms_df.groupby('Employee ID')

    for employee_id, employee in grouped_employee_id:
        # Skip rows with specific statuses
        if any(employee['Role Status'].isin(skip_statuses)):
            continue

        # Check if there are multiple roles with non-overlapping dates
        if len(employee) > 1:
            fulfilled = True
            sorted_group = employee.sort_values(by='Start Date')
            for i in range(len(sorted_group) - 1):
                # Check if the end date of the current entry is before the start date of the next entry
                if pd.isna(sorted_group.iloc[i]['End Date']) or pd.isna(sorted_group.iloc[i + 1]['Start Date']):
                    fulfilled = False
                    break
                if sorted_group.iloc[i]['End Date'] >= sorted_group.iloc[i + 1]['Start Date']:
                    fulfilled = False
                    break
            if fulfilled:
                op_ms_df.loc[employee.index, 'Fulfilled'] = 'Yes'
 
        # Check if the employee's information remains unchanged between current and next month
        current_info = current_df[current_df['Employee ID'] == employee_id]
        next_info = next_df[next_df['Employee ID'] == employee_id]

        if not current_info.empty and not next_info.empty:
            columns_to_check = ['Tech Area', 'Domain', 'FTE Category', 'Resource Type']
            current_values = current_info[columns_to_check].values[0]
            next_values = next_info[columns_to_check].values[0]
            if (current_values == next_values).all():
                op_ms_df.loc[employee.index, 'Fulfilled'] = 'Yes'
    
    return op_ms_df