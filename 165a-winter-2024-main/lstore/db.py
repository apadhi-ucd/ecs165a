from lstore.table import Table
import lstore.config as config

class Database():

    def __init__(self):
        self.tables = {} # changed from list to dictionary to allow for O(1) lookup time

    # Not required for milestone1
    def open(self, path):
        pass

    # Not required for milestone1
    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # Check if table name already exists
        if name in self.tables:
            raise Exception("ERROR: Table already exists, Table name must be unique") # Table name must be unique

        # Create a new local table
        tb = Table(name, num_columns, key_index)
        self.tables[name] = tb  # pushing the new local table in the database

        # Return the created table
        return tb
    


    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
       # Check if table name exists
        if name in self.tables:
            del self.tables[name]
            return True
            
        return False
    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
