from lstore.table import Table

class Database:
    """
    Database class that manages all tables
    """
    def __init__(self):
        self.tables = {}        # Dictionary mapping table names to Table objects

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

        # If table exists, return existing table
        if name in self.tables: 
            print("Table already exists")
            self.tables[name] = table
            return table
        
        # Create table
        table = Table(name, num_columns, key_index)
        self.tables[name] = table

        # Return table
        return table

    """
    # Deletes the specified table
    :param name: string         #Table name
    """
    def drop_table(self, name):
        
        # If table does not exist, return
        if name not in self.tables:
            print("Table does not exist")
            return
 
        del self.tables[name]


    
    """
    # Returns table with the passed name
    :param name: string         #Table name
    """
    def get_table(self, name):

        return self.tables[name]
