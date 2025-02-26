from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from lstore.page_range import PageRange
from time import time
import threading

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        self.frames_used = []
        self.frames_lock = threading.Lock()
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # Locate the RID associated with the primary key
        key_column = self.table.key
        rids = self.table.index.locate(key_column, primary_key)
        
        if not rids:
            return False
        
        # Get the first RID (should be only one for a primary key)
        rid = next(iter(rids))
        
        # Delete from all indices
        self.table.index.delete_from_all_indices(primary_key)
        
        # Add the RID to the deallocation queue for recycling
        self.table.deallocation_base_rid_queue.put(rid)
        
        return True

    """
    # Insert a record with the specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for any reason
    """
    def insert(self, *columns):
        # Check if the primary key already exists
        key_column = self.table.key
        primary_key = columns[key_column]
        
        if self.table.index.locate(key_column, primary_key) is not None:
            return False
        
        # Create a new record
        record = Record(0, primary_key, [0] * (self.table.total_num_columns))
        
        # Assign a RID to the record
        self.table.assign_rid_to_record(record)
        
        # Set the hidden columns
        record.columns[RID_COLUMN] = record.rid
        record.columns[INDIRECTION_COLUMN] = record.rid
        record.columns[SCHEMA_ENCODING_COLUMN] = 0
        
        # Set the data columns
        for i, value in enumerate(columns):
            record.columns[i + NUM_HIDDEN_COLUMNS] = value
        
        # Insert the record into the table
        self.table.insert_record(record)
        
        # Update the index
        self.table.index.insert_in_all_indices(record.columns)
        
        return True

    """
    # Read a record with specified key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # retrieve a list of RIDs that contain the "search_key" value within the column as defined by "search_key_index"
        rids = self.table.index.locate(search_key_index, search_key)
        
        if not rids:
            return []
        
        records = []
        
        for rid in rids:
            # Get the record location
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            
            if page_range_index >= len(self.table.page_ranges):
                continue
            
            page_range = self.table.page_ranges[page_range_index]
            
            # Read the record columns
            record_columns = [None] * (self.table.total_num_columns)
            
            # Read the indirection column to find the latest version
            indirection_rid = page_range.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
            
            # If the indirection points to a tail record, read from there
            if indirection_rid >= MAX_RECORD_PER_PAGE_RANGE:
                # Read from tail record
                for i in range(NUM_HIDDEN_COLUMNS):
                    record_columns[i] = page_range.read_tail_record_column(indirection_rid, i)
                
                # Read data columns based on projected_columns_index
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        record_columns[i + NUM_HIDDEN_COLUMNS] = page_range.read_tail_record_column(indirection_rid, i + NUM_HIDDEN_COLUMNS)
            else:
                # Read from base record
                for i in range(NUM_HIDDEN_COLUMNS):
                    record_columns[i] = page_range.bufferpool.read_page_slot(page_range_index, i, page_index, page_slot)
                
                # Read data columns based on projected_columns_index
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        record_columns[i + NUM_HIDDEN_COLUMNS] = page_range.bufferpool.read_page_slot(page_range_index, i + NUM_HIDDEN_COLUMNS, page_index, page_slot)
            
            # Create a record object
            record = Record(record_columns[RID_COLUMN], record_columns[NUM_HIDDEN_COLUMNS + self.table.key], record_columns)
            records.append(record)
        
        return records

    """
    # Read a record with specified key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # Get the RIDs for the search key
        rids = self.table.index.locate(search_key_index, search_key)
        
        if not rids:
            return []
        
        records = []
        
        for rid in rids:
            # Get the record location
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            
            if page_range_index >= len(self.table.page_ranges):
                continue
            
            page_range = self.table.page_ranges[page_range_index]
            
            # Start with the base record's indirection
            indirection_rid = page_range.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
            
            # Navigate to the desired version
            current_version = 0
            target_rid = indirection_rid
            
            # If we want an older version, traverse the indirection chain
            while current_version < relative_version and target_rid >= MAX_RECORD_PER_PAGE_RANGE:
                # Get the indirection of the current tail record
                next_rid = page_range.read_tail_record_column(target_rid, INDIRECTION_COLUMN)
                
                # If we've reached the base record, stop
                if next_rid < MAX_RECORD_PER_PAGE_RANGE:
                    break
                
                target_rid = next_rid
                current_version += 1
            
            # If we couldn't find the requested version, skip this record
            if current_version < relative_version:
                continue
            
            # Read the record columns
            record_columns = [None] * (self.table.total_num_columns)
            
            # If target_rid points to a tail record, read from there
            if target_rid >= MAX_RECORD_PER_PAGE_RANGE:
                # Read from tail record
                for i in range(NUM_HIDDEN_COLUMNS):
                    record_columns[i] = page_range.read_tail_record_column(target_rid, i)
                
                # Read data columns based on projected_columns_index
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        record_columns[i + NUM_HIDDEN_COLUMNS] = page_range.read_tail_record_column(target_rid, i + NUM_HIDDEN_COLUMNS)
            else:
                # Read from base record
                for i in range(NUM_HIDDEN_COLUMNS):
                    record_columns[i] = page_range.bufferpool.read_page_slot(page_range_index, i, page_index, page_slot)
                
                # Read data columns based on projected_columns_index
                for i in range(self.table.num_columns):
                    if projected_columns_index[i]:
                        record_columns[i + NUM_HIDDEN_COLUMNS] = page_range.bufferpool.read_page_slot(page_range_index, i + NUM_HIDDEN_COLUMNS, page_index, page_slot)
            
            # Create a record object
            record = Record(record_columns[RID_COLUMN], record_columns[NUM_HIDDEN_COLUMNS + self.table.key], record_columns)
            records.append(record)
        
        return records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # Get the RIDs for the primary key
        key_column = self.table.key
        rids = self.table.index.locate(key_column, primary_key)
        
        if not rids:
            return False
        
        # Get the first RID (should be only one for a primary key)
        rid = next(iter(rids))
        
        # Get the record location
        page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
        
        if page_range_index >= len(self.table.page_ranges):
            return False
        
        page_range = self.table.page_ranges[page_range_index]
        
        # Read the current record
        base_record_columns = [None] * (self.table.total_num_columns)
        
        for i in range(self.table.total_num_columns):
            base_record_columns[i] = page_range.bufferpool.read_page_slot(page_range_index, i, page_index, page_slot)
        
        # Create a new tail record
        tail_record_columns = [None] * (self.table.total_num_columns)
        
        # Set the hidden columns
        logical_rid = page_range.assign_logical_rid()
        tail_record_columns[RID_COLUMN] = logical_rid
        tail_record_columns[INDIRECTION_COLUMN] = base_record_columns[INDIRECTION_COLUMN]
        
        # Calculate schema encoding
        schema_encoding = 0
        for i, value in enumerate(columns):
            if value is not None:
                schema_encoding |= (1 << i)
        
        tail_record_columns[SCHEMA_ENCODING_COLUMN] = schema_encoding
        
        # Set the data columns
        for i, value in enumerate(columns):
            if value is not None:
                tail_record_columns[i + NUM_HIDDEN_COLUMNS] = value
            else:
                tail_record_columns[i + NUM_HIDDEN_COLUMNS] = base_record_columns[i + NUM_HIDDEN_COLUMNS]
        
        # Update the base record's indirection to point to the new tail record
        page_range.bufferpool.write_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot, logical_rid)
        
        # Write the tail record
        update_success = self.table.update_record(rid, tail_record_columns)
        
        if update_success:
            # Update the index
            self.table.index.update_all_indices(primary_key, tail_record_columns)
        
        return update_success

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # Get the RIDs in the range
        key_column = self.table.key
        rids = self.table.index.locate_range(start_range, end_range, key_column)
        
        if not rids:
            return False
        
        # Calculate the sum
        column_sum = 0
        
        for rid in rids:
            # Get the record location
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            
            if page_range_index >= len(self.table.page_ranges):
                continue
            
            page_range = self.table.page_ranges[page_range_index]
            
            # Read the indirection column to find the latest version
            indirection_rid = page_range.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
            
            # If the indirection points to a tail record, read from there
            if indirection_rid >= MAX_RECORD_PER_PAGE_RANGE:
                # Read from tail record
                value = page_range.read_tail_record_column(indirection_rid, aggregate_column_index + NUM_HIDDEN_COLUMNS)
            else:
                # Read from base record
                value = page_range.bufferpool.read_page_slot(page_range_index, aggregate_column_index + NUM_HIDDEN_COLUMNS, page_index, page_slot)
            
            column_sum += value
        
        return column_sum

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        # Get all RIDs within the specified range
        key_column = self.table.key
        rids = self.table.index.locate_range(start_range, end_range, key_column)
        
        if not rids:
            return False
        
        # Calculate the sum
        column_sum = 0
        
        for rid in rids:
            # Get the record location
            page_range_index, page_index, page_slot = self.table.get_base_record_location(rid)
            
            if page_range_index >= len(self.table.page_ranges):
                continue
            
            page_range = self.table.page_ranges[page_range_index]
            
            # Start with the base record's indirection
            indirection_rid = page_range.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
            
            # Navigate to the desired version
            current_version = 0
            target_rid = indirection_rid
            
            # If we want an older version, traverse the indirection chain
            while current_version < relative_version and target_rid >= MAX_RECORD_PER_PAGE_RANGE:
                # Get the indirection of the current tail record
                next_rid = page_range.read_tail_record_column(target_rid, INDIRECTION_COLUMN)
                
                # If we've reached the base record, stop
                if next_rid < MAX_RECORD_PER_PAGE_RANGE:
                    break
                
                target_rid = next_rid
                current_version += 1
            
            # If we couldn't find the requested version, skip this record
            if current_version < relative_version:
                continue
            
            # Read the value from the appropriate record
            if target_rid >= MAX_RECORD_PER_PAGE_RANGE:
                # Read from tail record
                value = page_range.read_tail_record_column(target_rid, aggregate_column_index + NUM_HIDDEN_COLUMNS)
            else:
                # Read from base record
                value = page_range.bufferpool.read_page_slot(page_range_index, aggregate_column_index + NUM_HIDDEN_COLUMNS, page_index, page_slot)
            
            column_sum += value
        
        return column_sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def __readAndMarkSlot(self, page_range_index, column, page_index, page_slot):
        '''Reads a slot from a page and marks the frame as used'''
        value = self.table.bufferpool.read_page_slot(page_range_index, column, page_index, page_slot)
        frame_num = self.table.bufferpool.get_page_frame_num(page_range_index, column, page_index)
        if frame_num is not None:
            self.frames_used.append(frame_num)
        return value

    def __readAndTrack(self, page_range_index, column, page_index, slot, frames_used):
        '''Reads a slot from a page and tracks the frame used'''
        value = self.table.bufferpool.read_page_slot(page_range_index, column, page_index, slot)
        frame_num = self.table.bufferpool.get_page_frame_num(page_range_index, column, page_index)
        if frame_num is not None:
            frames_used.append(frame_num)
        return value
