from lstore.table import Table, Record
#from lstore.index import Index

class Query:

    """
    Initializes a Query Object
    """
    def __init__(self, table):
        self.table = table


    """
    Deletes a Record from the Table
    """
    def delete(self, primary_key):
        
        # Delete the Record from the Table
        self.table.delete(primary_key)
      
      
    """
    Inserts a Record in the Table
    """
    def insert(self, *record):
        
        # Check if the correct number of columns were passed
        if len(record) != self.table.num_columns:
            print("Error: Incorrect Number of Values Passed")
            return
            
        # Insert the Record into the Table
        self.table.write(record)
        

    """
    Returns Columns Matching the Selection Criteria
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        
        # Check if Column Indexes are within Bounds
        if search_key_index < 0 or search_key_index >= self.table.num_columns:
            print("Error: Invalid Column Range (Search Column)")
            return
        if len(projected_columns_index) != self.table.num_columns:
            print("Error: Invalid Column Range (Selected Columns)")
            return
            
        # Return the Records that Match the Search Criteria
        return self.table.select(search_key, search_key_index, projected_columns_index)


    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass


    """
    Updates a Record with a new Record
    """
    def update(self, primary_key, *record):
        
        # Check if the correct number of columns were passed
        if len(record) != self.table.num_columns:
            print("Error: Incorrect Number of Values Passed")
            return
            
        # Insert the Record into the Table
        self.table.update(primary_key, record)
        

    """
    Returns Sum of a Range of Values
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Returns the summation of the given range upon success.

        Parameters:
        - start_range: Start of the key range to aggregate.
        - end_range: End of the key range to aggregate.
        - aggregate_column_index: Index of the desired column to aggregate.

        Returns:
        - The summation of the given range upon success.
        - False if no record exists in the given range.
        """

        # Check if the Column Index is within Bounds
        if aggregate_column_index < 0 or aggregate_column_index >= self.table.num_columns:
            print("Error: Invalid Column Index")
            return
            
        # Return the Sum of the Records in the Column
        return self.table.sum(start_range, end_range, aggregate_column_index)


    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass
        

    def increment(self, key, column):
        pass
