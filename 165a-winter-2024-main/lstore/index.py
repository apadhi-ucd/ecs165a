from BTrees.OOBTree import OOBTree
from lstore.config import *
import json
from typing import List, Set, Dict, Union, Any

class Index:

    def __init__(self, table):
        self.table = table
        self.indices = {}
        '''Maps column_number to a BTree'''
        self.value_mapper = {}
        '''Maps column_number to a dictionary that maps values to a set of rids'''
        self.create_index(table.key)
        self.__generate_primary_index()

    def locate(self, column, value):
        '''
        Returns the location of all records with the given value on column "column"
        '''
        if column not in self.indices:
            return None
        
        if value not in self.value_mapper[column]:
            return None
        
        return self.value_mapper[column][value]

    def locate_range(self, begin, end, column):
        '''
        Returns the RIDs of all records with values in column "column" between "begin" and "end"
        '''
        if column not in self.indices:
            return None
        
        rids = set()
        for key in self.indices[column].keys(begin, end):
            rids.update(self.value_mapper[column].get(key, set()))
        
        return rids if rids else None

    def create_index(self, column_number):
        '''
        Creates an index on the specified column
        '''
        if column_number in self.indices:
            return False
        
        self.indices[column_number] = OOBTree()
        self.value_mapper[column_number] = {}
        return True

    def drop_index(self, column_number):
        '''
        Drops the index on the specified column
        '''
        if column_number not in self.indices:
            return False
        
        del self.indices[column_number]
        del self.value_mapper[column_number]
        return True

    def delete_from_index(self, column_index, column_value):
        '''
        Deletes a value from the index
        '''
        if column_index not in self.indices:
            return
        
        if column_value in self.value_mapper[column_index]:
            del self.value_mapper[column_index][column_value]
        
        if column_value in self.indices[column_index]:
            del self.indices[column_index][column_value]

    def insert_to_index(self, column_index, column_value, rid):
        '''
        Inserts a value into the index
        '''
        if column_index not in self.indices:
            return
        
        self.indices[column_index][column_value] = rid
        
        if column_value not in self.value_mapper[column_index]:
            self.value_mapper[column_index][column_value] = set()
        
        self.value_mapper[column_index][column_value].add(rid)

    def insert_in_all_indices(self, columns):
        '''
        Inserts a record into all indices
        '''
        rid = columns[RID_COLUMN]
        
        for column_index in self.indices:
            if column_index < len(columns) - NUM_HIDDEN_COLUMNS:
                column_value = columns[column_index + NUM_HIDDEN_COLUMNS]
                self.insert_to_index(column_index, column_value, rid)

    def delete_from_all_indices(self, primary_key):
        '''
        Deletes a record from all indices
        '''
        # Get the RID for the primary key
        primary_key_column = self.table.key
        
        if primary_key_column not in self.indices:
            return
        
        if primary_key not in self.value_mapper[primary_key_column]:
            return
        
        rids = self.value_mapper[primary_key_column][primary_key]
        
        if not rids:
            return
        
        rid = next(iter(rids))
        
        # Get the record's values for each indexed column
        page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
        
        if page_range_index >= len(self.table.page_ranges):
            return
        
        page_range = self.table.page_ranges[page_range_index]
        
        # Delete from each index
        for column_index in self.indices:
            if column_index < self.table.num_columns:
                column_value = page_range.bufferpool.read_page_slot(
                    page_range_index, 
                    column_index + NUM_HIDDEN_COLUMNS, 
                    page_index, 
                    page_slot
                )
                
                if column_value is not None:
                    if column_value in self.value_mapper[column_index] and rid in self.value_mapper[column_index][column_value]:
                        self.value_mapper[column_index][column_value].remove(rid)
                        
                        if not self.value_mapper[column_index][column_value]:
                            del self.value_mapper[column_index][column_value]
                            del self.indices[column_index][column_value]

    def update_all_indices(self, primary_key, columns):
        '''
        Updates a record in all indices
        '''
        # Get rid from primary key
        primary_key_column = self.table.key
        
        if primary_key_column not in self.indices:
            return
        
        if primary_key not in self.value_mapper[primary_key_column]:
            return
        
        rids = self.value_mapper[primary_key_column][primary_key]
        
        if not rids:
            return
        
        rid = next(iter(rids))
        
        # Get the record's values for each indexed column
        page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
        
        if page_range_index >= len(self.table.page_ranges):
            return
        
        page_range = self.table.page_ranges[page_range_index]
        
        # Update each index
        for column_index in self.indices:
            if column_index < self.table.num_columns:
                # Only update if the column is in the update
                if columns[column_index + NUM_HIDDEN_COLUMNS] is not None:
                    # Get the old value
                    old_value = page_range.bufferpool.read_page_slot(
                        page_range_index, 
                        column_index + NUM_HIDDEN_COLUMNS, 
                        page_index, 
                        page_slot
                    )
                    
                    # Get the new value
                    new_value = columns[column_index + NUM_HIDDEN_COLUMNS]
                    
                    # Update the index
                    if old_value != new_value:
                        # Remove the old value
                        if old_value in self.value_mapper[column_index] and rid in self.value_mapper[column_index][old_value]:
                            self.value_mapper[column_index][old_value].remove(rid)
                            
                            if not self.value_mapper[column_index][old_value]:
                                del self.value_mapper[column_index][old_value]
                                del self.indices[column_index][old_value]
                        
                        # Add the new value
                        self.insert_to_index(column_index, new_value, rid)

    def grab_all(self):
        '''
        Returns all RIDs in the index
        '''
        all_rids = set()
        
        for column_index in self.indices:
            for value in self.value_mapper[column_index]:
                all_rids.update(self.value_mapper[column_index][value])
        
        return all_rids

    def get(self, key_column_number, value_column_number, key):
        '''Returns the value of the value_column for the given key'''
        return self.__search_value_mapper(key_column_number, value_column_number, key, key)

    def get_range(self, key_column_number, value_column_number, begin, end):
        '''Returns the values of the value_column for the given key range'''
        return self.__search_value_mapper(key_column_number, value_column_number, begin, end)

    def __search_value_mapper(self, key_column_number, value_column_number, begin, end):
        '''Helper function for get and get_range'''
        if key_column_number not in self.indices:
            return None
        
        result = {}
        
        for key in self.indices[key_column_number].keys(begin, end):
            rids = self.value_mapper[key_column_number][key]
            
            for rid in rids:
                page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
                
                if page_range_index < len(self.table.page_ranges):
                    page_range = self.table.page_ranges[page_range_index]
                    value = page_range.bufferpool.read_page_slot(
                        page_range_index, 
                        value_column_number + NUM_HIDDEN_COLUMNS, 
                        page_index, 
                        page_slot
                    )
                    
                    result[key] = value
        
        return result

    def __generate_primary_index(self):
        '''
        Generates the primary index for the table
        '''
        # Initialize a BTree
        primary_key_column = self.table.key
        self.indices[primary_key_column] = OOBTree()
        self.value_mapper[primary_key_column] = {}
        
        # Get all RIDs
        all_rids = self.table.grab_all_base_rids()
        
        # Add each RID to the index
        for rid in all_rids:
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            
            if page_range_index < len(self.table.page_ranges):
                page_range = self.table.page_ranges[page_range_index]
                primary_key_value = page_range.bufferpool.read_page_slot(
                    page_range_index, 
                    primary_key_column + NUM_HIDDEN_COLUMNS, 
                    page_index, 
                    page_slot
                )
                
                if primary_key_value is not None:
                    self.insert_to_index(primary_key_column, primary_key_value, rid)

    def serialize(self):
        '''
        Serializes the index for storage
        '''
        serialized_indices = {}
        
        for column_index in self.indices:
            serialized_indices[str(column_index)] = {
                "keys": list(self.indices[column_index].keys()),
                "values": {}
            }
            
            for value in self.value_mapper[column_index]:
                serialized_indices[str(column_index)]["values"][str(value)] = list(self.value_mapper[column_index][value])
        
        return serialized_indices

    def deserialize(self, data):
        '''
        Deserializes the index from storage
        '''
        self.indices = {}
        self.value_mapper = {}
        
        for column_index_str, index_data in data.items():
            column_index = int(column_index_str)
            self.indices[column_index] = OOBTree()
            self.value_mapper[column_index] = {}
            
            for key in index_data["keys"]:
                self.indices[column_index][key] = None
            
            for value_str, rids in index_data["values"].items():
                value = int(value_str)
                self.value_mapper[column_index][value] = set(rids)
                
                for rid in rids:
                    self.indices[column_index][value] = rid
