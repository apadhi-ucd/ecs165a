from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

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
        # Check if table name already exists
        for table in self.tables:
            if table.name == name:
                return None  # Table name must be unique

        table = Table(name, num_columns, key_index)
        self.tables.append(table)  # Append table to list
        return table


    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if table.name == name:
                del self.tables[i]  # Remove table from the list
                return True
        return False  # Table not found

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table  # Return matching table
        return None  # Table not found