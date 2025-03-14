import os
import json
import atexit
import shutil
from lstore.table import Table
from lstore.index import Index
from BTrees.OOBTree import OOBTree
from lstore.lock import LockManager

class Database():

    def __init__(self, path="ECS165 DB"):
        self.tables:dict = {}
        self.path = path
        self.no_path_set = True
        atexit.register(self.__cleanup_db_directory)
        self.lock = LockManager()
    
    """
    # Fetches an existing table from the database
    """
    def get_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Cannot retrieve table! The requested table does not exist: {name}")
        
        return self.tables[name]
    
    """
    # Establishes a new table in the database
    :param name: string         #Identifier for the table
    :param num_columns: int     #Total column count (integer values only)
    :param key_index: int       #Position of primary key column
    """
    def create_table(self, name, num_columns, key_index):
        if self.tables.get(name) is not None:
            raise NameError(f"Cannot create table! A table with this name already exists: {name}")

        self.tables[name] = Table(name, num_columns, key_index, self.path, self.lock)
        return self.tables[name]
    
    """
    # Removes the specified table from the database
    """
    def drop_table(self, name):
        if self.tables.get(name) is None:
            raise NameError(f"Cannot drop table! The specified table does not exist: {name}")
        
        del self.tables[name]

    def open(self, path):
        """Initializes database from storage and reconstructs tables with their data"""
        self.path = path
        self.no_path_set = False

        # ensure database directory exists
        if not os.path.exists(path):
            os.makedirs(path)

        atexit.unregister(self.__cleanup_db_directory)
        
        # retrieve stored table configurations
        # skipped during first-time initialization - close() will create tables.json
        db_config_file = os.path.join(path, "tables.json")
        if os.path.exists(db_config_file):
            with open(db_config_file, "r") as config_file:
                stored_tables_data = json.load(config_file)

                # reconstruct each table from stored configuration
                for tbl_name, tbl_config in stored_tables_data.items():
                    tbl_instance = Table(tbl_name, tbl_config["num_columns"], tbl_config["key_index"], self.path, self.lock)
                    self.tables[tbl_name] = tbl_instance

                    # populate table with stored metadata
                    tbl_instance.deserialize(tbl_config)

    def close(self):
        """Persists database state to disk and releases resources"""
        # if self.no_path_set:
        #     raise ValueError("Database path is not set. Use open() before closing.")
        
        db_config = {}

        for tbl_name, tbl_instance in self.tables.items():
            # persist any pending changes to storage
            tbl_instance.bufferpool.unload_all_frames()

            # capture table configuration
            db_config[tbl_name] = tbl_instance.serialize()

            # release memory resources
            tbl_instance.bufferpool = None

        # write consolidated database configuration
        db_config_file = os.path.join(self.path, "tables.json")
        with open(db_config_file, "w", encoding="utf-8") as config_file:
            json.dump(db_config, config_file, indent=4)

        # clear in-memory table references
        self.tables = {}
    
    def __cleanup_db_directory(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

