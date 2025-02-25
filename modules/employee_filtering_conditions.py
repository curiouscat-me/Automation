"""
Global Parameters:
    df (DataFrame): The DataFrame to filter.
    employee_id (str): The employee ID to filter for.
    domain (str, optional): The domain to filter for. Defaults to 'Security'.
    fte_category (str, optional): The FTE category to filter for. Defaults to 'FTE' and 'Non-FTE' accordingly
"""

def employee_filtering_condition_fte(df, employee_id):
    """
    Filters the DataFrame to identify relevant entries for a specific employee ID based on various conditions.
    
    Returns:
    Index: The index of the filtered DataFrame rows that match the given conditions.
    
    Process:
    1. Exclude entries with 'Stretch' in the 'Resource Type' column.
    2. Exclude entries with 'Vacant' in the 'FTE Name' column.
    3. Include entries that match the given employee ID.
    4. Include entries where 'Employee ID' and 'LANID' are not null.
    5. Exclude entries with 'Missing from Op FTE' in the 'Role Status' column.
    6. Exclude entries with 'Yes' in the 'Fulfilled' column.
    7. Exclude entries with 'Past' in the 'Skip' column.
    """
    return df[
        (df['Resource Type'] != 'Stretch') &
        (~df['FTE Name'].str.contains('Vacant', na=False)) &
        (df['Employee ID'] == employee_id) &
        (df['Employee ID'].notna()) &
        (df['LANID'].notna()) &
        (df['Role Status'] != 'Missing from Op FTE') &
        (df['Fulfilled'] != 'Yes') &
        (df['Skip'] != 'Past')
    ].index

def shorten_filtering_condition_fte(df, employee_id):
    """
    A shorten version of employee_filtering_condition_fte for a wider range of employees
    
    Returns:
    Index: The index of the filtered DataFrame rows that match the given conditions.
    
    Process:
    1. Exclude entries with 'Stretch' in the 'Resource Type' column.
    2. Exclude entries with 'Vacant' in the 'FTE Name' column.
    3. Include entries that match the given employee ID.
    4. Include entries where 'Employee ID' and 'LANID' are not null.
    5. Exclude entries with 'Missing from Op FTE' in the 'Role Status' column.
    """
    return df[
        (df['Resource Type'] != 'Stretch') &
        (~df['FTE Name'].str.contains('Vacant', na=False)) &
        (df['Employee ID'] == employee_id) &
        (df['Employee ID'].notna()) &
        (df['LANID'].notna()) &
        (df['Role Status'] != 'Missing from Op FTE')
        ].index

def employee_filtering_condition_ms(df, employee_id):
    """
    Filters the DataFrame to identify relevant entries for a specific employee ID based on various conditions.
    
    Returns:
    Index: The index of the filtered DataFrame rows that match the given conditions.
    
    Process:
    1. Include entries that match the given employee ID.
    2. Include entries where 'Employee ID' and 'LANID' are not null.
    3. Exclude entries with 'Yes' in the 'Fulfilled' column.
    4. Exclude entries with 'Missing from Op FTE' in the 'Role Status' column.
    5. Exclude entries with 'Past' in the 'Skip' column.
    """
    return df[
        (df['Employee ID'] == employee_id) & 
        (df['Employee ID'].notna()) &  # Ensure Employee ID is not blank
        (df['LANID'].notna()) & # Ensure LANID is not blank
        (df['Fulfilled'] != 'Yes') &  # Skip rows marked as Fulfilled
        (df['Role Status'] != 'Missing from Op MS') &
        (df['Skip'] != 'Past')  # Ensure Employee ID is not blank
    ].index

def shorten_filtering_condition_ms(df, employee_id):
    """
    A shorten version of employee_filtering_condition_ms for a wider range of employees
    
    Returns:
    Index: The index of the filtered DataFrame rows that match the given conditions.
    
    Process:
    1. Exclude entries with 'Stretch' in the 'Resource Type' column.
    2. Exclude entries with 'Vacant' in the 'FTE Name' column.
    3. Include entries that match the given employee ID.
    4. Include entries where 'Employee ID' and 'LANID' are not null.
    5. Exclude entries with 'Missing from Op FTE' in the 'Role Status' column.
    """
    return df[
        (df['Resource Type'] != 'Stretch') &
        (~df['Resource Name'].str.contains('Vacant', na=False)) &
        (df['Employee ID'] == employee_id) &
        (df['Employee ID'].notna()) &
        (df['LANID'].notna()) &
        (df['Role Status'] != 'Missing from Op MS')
        ].index

def employee_security_fte(df, domain='Security', fte_category='FTE'):
    """
    Filters the DataFrame to include only employees in the specified domain and FTE category.
    
    Returns:
    DataFrame: The filtered DataFrame containing only employees in the specified domain and FTE category.
    
    Process:
    1. Filter the DataFrame to include only rows where the 'Domain' column matches the specified domain.
    2. Filter the DataFrame to include only rows where the 'FTE Category' column matches the specified FTE category.
    """
    return df[
        (df['Domain'] == domain) & 
        (df['FTE Category'] == fte_category)
    ]

def employee_security_ms(df, domain='Security', fte_category='Non-FTE'):
    """
    Filters the DataFrame to include only employees in the specified domain and FTE category.
    
    Returns:
    DataFrame: The filtered DataFrame containing only employees in the specified domain and FTE category.
    
    Process:
    1. Filter the DataFrame to include only rows where the 'Domain' column matches the specified domain.
    2. Filter the DataFrame to include only rows where the 'FTE Category' column matches the specified FTE category.
    """
    return df[
        (df['Domain'] == domain) & 
        (df['FTE Category'] == fte_category)
    ]