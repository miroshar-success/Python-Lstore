from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = {}

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
    
        # Check if the Table Already Exists
        if name in self.tables:
            print(f"Table {name} already exists.")
            return self.tables[name]
        
        # Otherwise Create the Table
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
    
        # Delete the Table if it Exists
        if name in self.tables:
            del self.tables[name]
            return
            
        # Otherwise Print an Error Message
        print(f"Table {name} does not exist.")
        

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
    
        # Return the Table if it Exists
        if name in self.tables:
            return self.tables[name]
        
        # Otherwsise Print an Error Message
        print(f"Table {name} does not exist.")
        return None
