def get_column_index(ws, column_name):
    """
    Finds the index of a specified column in the given worksheet.
    
    Parameters:
    ws (Worksheet): The worksheet to search.
    column_name (str): The name of the column to find.
    
    Returns:
    int: The 1-based index of the column if found, otherwise returns None.
    
    Process:
    1. Iterate through the cells in the first row (header) of the worksheet.
    2. Check if the cell's value matches the specified column name.
    3. If a match is found, return the 1-based index of the column.
    4. If no match is found after checking all cells, return None.
    """
    for idx, cell in enumerate(ws[1]): # Header is in row 1
        if cell.value == column_name:
            return idx + 1 # +1 to convert from 0-based to 1-based indexing
    return None