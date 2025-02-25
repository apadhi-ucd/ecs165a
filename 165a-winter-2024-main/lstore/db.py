from lstore.table import Table
import msgpack
from BTrees.OOBTree import OOBTree

class Database:
    """
    Database class that manages all tables
    """
    def __init__(self):
        self.tables = OOBTree()  # Dictionary mapping table names to Table objects
        self.path = None  # File prefix for persistence

    # Not required for milestone1
    def open(self, path):
        """Loads database metadata from a file if it exists, otherwise initializes a new one."""
        self.path = path

        try:
            with open(path + "_index.msgpack", "rb") as file:
                self.tables = msgpack.unpackb(file.read(), raw=False)
        except FileNotFoundError:
            self.tables = OOBTree()  # Start fresh if no file exists

    def close(self):
        """Saves the database index (metadata) to disk using msgpack."""
        if self.path:
            with open(self.path + "_index.msgpack", "wb") as file:
                file.write(msgpack.packb(self.tables, use_bin_type=True))


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
        self.close()  # Save metadata changes

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
        Table.delete_data(self.path, name)  # Delete stored table data
        del self.tables[name]
        self.close()  # Save metadata changes


    
    """
    # Returns table with the passed name
    :param name: string         #Table name
    """
    def get_table(self, name):

        return self.tables[name]
