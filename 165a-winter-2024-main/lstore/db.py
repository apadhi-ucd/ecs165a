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
        """Loads database metadata and record data from disk; reconstructs tables."""
        self.path = path
        try:
            with open(path + "_index.msgpack", "rb") as file:
                data = file.read()
                if not data:
                    self.tables = OOBTree()
                else:
                    metadata = msgpack.unpackb(data, raw=False)
                    # Handle old list format by ignoring metadata.
                    if isinstance(metadata, dict):
                        self.tables = OOBTree()
                        from lstore.table import Table
                        for name, info in metadata.items():
                            table = Table(name, info[0], info[1])
                            # NEW: Load table records from disk
                            table.load_records(path)
                            # Removed table.reset() to retain loaded records.
                            self.tables[name] = table
                    else:
                        self.tables = OOBTree()
        except FileNotFoundError:
            self.tables = OOBTree()

    def close(self):
        """Saves the database metadata and record data to disk using msgpack."""
        if self.path:
            # Persist table metadata instead of entire Table objects.
            metadata = {name: [table.num_columns, table.key] for name, table in self.tables.items()}
            with open(self.path + "_index.msgpack", "wb") as file:
                file.write(msgpack.packb(metadata, use_bin_type=True))
            # NEW: Persist each table’s record data
            for table in self.tables.values():
                table.persist_records(self.path)

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
            return self.tables[name]
        
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
