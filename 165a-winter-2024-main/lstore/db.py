import os
import json
from lstore.table import Table
from lstore.index import Index
from BTrees.OOBTree import OOBTree
import atexit
import shutil

class Database():

    def __init__(self, path="ECS165 DB"):
        self.tables:dict = {}
        self.path = path
        self.no_path_set = True
        atexit.register(self.__remove_db_path)
        

    def open(self, path):
        """Loads database from disk and restores tables, indexes, and pages"""
        self.path = path
        self.no_path_set = False

        # create new path if database path doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)

        atexit.unregister(self.__remove_db_path)
        
        # grab table metadata
        # this logic will be skipped if opening for the first time -- close() aggregates all table metadata into tables.json path
        tables_metadata_path = os.path.join(path, "tables.json")
        if os.path.exists(tables_metadata_path):
            with open(tables_metadata_path, "r") as file:
                tables_metadata = json.load(file)

                # loops through tables and adds them to the self.tables dictionary
                for table_name, table_info in tables_metadata.items():
                    table = Table(table_name, table_info["num_columns"], table_info["key_index"], self.path)
                    self.tables[table_name] = table

                    # restore table metadata
                    table.deserialize(table_info)
                    

    def close(self):
        """Flushes dirty pages back to disk, saves table metadata and shuts down"""
        # if self.no_path_set:
        #     raise ValueError("Database path is not set. Use open() before closing.")
        
        tables_metadata = {}

        for table_name, table in self.tables.items():
            # flush dirty pages from table's bufferpool
            table.bufferpool.unload_all_frames()

            # serialize table metadata
            tables_metadata[table_name] = table.serialize()

            # clear each table's bufferpool to free memory
            table.bufferpool = None

        # save all tables' metadata to disk
        tables_metadata_path = os.path.join(self.path, "tables.json")
        with open(tables_metadata_path, "w", encoding="utf-8") as file:
            json.dump(tables_metadata, file, indent=4)

        # clear memory references
        self.tables = {}


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if self.tables.get(name) is not None:
            raise NameError(f"Error creating Table! Following table already exists: {name}")

        self.tables[name] = Table(name, num_columns, key_index, self.path)
        return self.tables[name]

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Error dropping Table! Following table does not exist: {name}")
        
        del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Error getting Table! Following table does not exist: {name}")
        
        return self.tables[name]
    
    def __remove_db_path(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

