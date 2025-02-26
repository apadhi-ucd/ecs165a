from lstore.table import Table, Record
from lstore.config import *

from time import time
from queue import Queue

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table:Table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    # Delete was simplified to just locate rid and put it on the queue, then deleting indices. Actual process will occur in merge function
    def delete(self, primary_key):

        # Locate the RID associated with the primary key
        base_rid = self.table.index.locate(self.table.key, primary_key)
        if(base_rid is None):
            return False  # Record does not exist

        self.table.deallocation_base_rid_queue.put(base_rid[0])
        self.table.index.delete_from_all_indices(primary_key)

        # Deletion successful
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):

        if (self.table.index.locate(self.table.key, columns[self.table.key])):
            return False

        # check for invalid number of columns
        if len(columns) != self.table.num_columns:
            return False
  
        record = Record(rid = None, key = self.table.key, columns = None)
        self.table.assign_rid_to_record(record)

        hidden_columns = [None] * NUM_HIDDEN_COLUMNS
        hidden_columns[INDIRECTION_COLUMN] = record.rid
        hidden_columns[RID_COLUMN] = record.rid
        hidden_columns[UPDATE_TIMESTAMP_COLUMN] = RECORD_NONE_VALUE
        #hidden_columns[TIMESTAMP_COLUMN] = int(time())
        hidden_columns[SCHEMA_ENCODING_COLUMN] = 0
        #hidden_columns[BASE_PAGE_ID_COLUMN] = record.rid
        record.columns = hidden_columns + list(columns)
    
        self.table.insert_record(record)
        self.table.index.insert_in_all_indices(record.columns)

        return True

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # retrieve a list of RIDs that contain the "search_key" value within the column as defined by "search_key_index"
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):

        rid_list = self.table.index.locate(search_key_index, search_key)
        if not rid_list:
            raise ValueError("No records found with the given key")
        
        record_objs = []

        for rid in rid_list:
            record_columns = [None] * self.table.num_columns
            page_range_index, base_page_index, base_page_slot = self.table.get_base_record_location(rid)
            
            projected_columns_schema = 0
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    projected_columns_schema |= (1 << i)

            if (projected_columns_schema >> self.table.key) & 1 == 1:
                record_columns[self.table.key] = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + self.table.key, base_page_index, base_page_slot)
                projected_columns_schema &= ~(1 << self.table.key)

            base_schema = self.__readAndMarkSlot(page_range_index, SCHEMA_ENCODING_COLUMN, base_page_index, base_page_slot)
            base_timestamp = self.__readAndMarkSlot(page_range_index, TIMESTAMP_COLUMN, base_page_index, base_page_slot)
            current_tail_rid = self.__readAndMarkSlot(page_range_index, INDIRECTION_COLUMN, base_page_index, base_page_slot)

            if current_tail_rid == rid:
                
                # Current RID = base RID, read all columns from the base page
                for i in range(self.table.num_columns):
                    if (projected_columns_schema >> i) & 1:
                        record_columns[i] = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + i, base_page_index, base_page_slot)
            
            else:
                current_version = 0
                
                for i in range(self.table.num_columns):
                    if (projected_columns_schema >> i) & 1:
                        if (base_schema >> i) & 1 == 0:
                            record_columns[i] = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + i, base_page_index, base_page_slot)
                            continue

                        temp_tail_rid = current_tail_rid
                        found_value = False
                        
                        while temp_tail_rid != rid and current_version <= relative_version:
                            tail_schema = self.table.page_ranges[page_range_index].read_tail_record_column(temp_tail_rid, SCHEMA_ENCODING_COLUMN)
                            tail_timestamp = self.table.page_ranges[page_range_index].read_tail_record_column(temp_tail_rid, TIMESTAMP_COLUMN)
                            
                            if (tail_schema >> i) & 1:

                                # Tail_timestamp should be greater than the base_timestamp for current version
                                if tail_timestamp >= base_timestamp:
                                    
                                    if relative_version == 0:
                                        tail_page_index, tail_slot = self.table.page_ranges[page_range_index].get_column_location(temp_tail_rid, NUM_HIDDEN_COLUMNS + i)
                                        record_columns[i] = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + i, tail_page_index, tail_slot)
                                        found_value = True
                                        break
                                
                                 # Reading from an older version of the record
                                else:
                                    current_version += 1

                                    if current_version == relative_version:
                                        tail_page_index, tail_slot = self.table.page_ranges[page_range_index].get_column_location(temp_tail_rid, NUM_HIDDEN_COLUMNS + i)
                                        record_columns[i] = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + i, tail_page_index, tail_slot)
                                        found_value = True
                                        break

                            temp_tail_rid = self.table.page_ranges[page_range_index].read_tail_record_column(temp_tail_rid, INDIRECTION_COLUMN)
                        
                        if not found_value:
                            record_columns[i] = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + i, base_page_index, base_page_slot)
            
            record_objs.append(Record(rid, record_columns[self.table.key], record_columns))
        
        return record_objs


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking (Ignore this for now)
    """
    def update(self, primary_key, *columns):
        rid_location = self.table.index.locate(self.table.key, primary_key)
        if rid_location is None:
            print("Update Error: Record does not exist")
            return False
        
        new_columns = [None] * self.table.total_num_columns
        schema_encoding = 0
        
        for i, value in enumerate(columns):

            # If we're modifying the primary_key then this update should be stopped since we can't change the primary_key column
            if i == self.table.key and value is not None:
                if (self.table.index.locate(self.table.key, value) is not None):
                    print("Update Error: Primary Key already exists")
                    return False
            
            if value is not None:
                schema_encoding |= (1 << i)
            
            new_columns[NUM_HIDDEN_COLUMNS + i] = value

        page_range_index, page_index, page_slot = self.table.get_base_record_location(rid_location[0])
        
        prev_tail_rid = self.table.bufferpool.read_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
        base_schema = self.table.bufferpool.read_page_slot(page_range_index, SCHEMA_ENCODING_COLUMN, page_index, page_slot)
        
        updated_base_schema = base_schema | schema_encoding

        new_columns[INDIRECTION_COLUMN] = prev_tail_rid
        new_columns[SCHEMA_ENCODING_COLUMN] = schema_encoding
        #new_columns[TIMESTAMP_COLUMN] = int(time())

        new_record = Record(rid = self.table.page_ranges[page_range_index].assign_logical_rid(), key = primary_key, columns = new_columns)

        new_columns[RID_COLUMN] = new_record.rid

        self.table.update_record(rid_location[0], new_columns)
        
        self.table.bufferpool.write_page_slot(page_range_index, INDIRECTION_COLUMN, page_index, page_slot, new_record.rid)
        self.table.bufferpool.write_page_slot(page_range_index, SCHEMA_ENCODING_COLUMN, page_index, page_slot, updated_base_schema)

        indirFrame_num = self.table.bufferpool.get_page_frame_num(page_range_index, INDIRECTION_COLUMN, page_index)
        schemaFrame_num = self.table.bufferpool.get_page_frame_num(page_range_index, SCHEMA_ENCODING_COLUMN, page_index)
        self.table.bufferpool.mark_frame_used(indirFrame_num)
        self.table.bufferpool.mark_frame_used(schemaFrame_num)

        # Update successful
        self.table.index.update_all_indices(primary_key, new_columns)
        # print("Update Successful\n")

        return True


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    
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
        records_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        
        if not records_list:
            return False

        sum_total = 0
        for rid in records_list:
            page_range_index, base_page_index, base_page_slot = self.table.get_base_record_location(rid)

            # Step 3: Get Base Record Details
            base_schema = self.__readAndMarkSlot(page_range_index, SCHEMA_ENCODING_COLUMN, base_page_index, base_page_slot)
            base_timestamp = self.__readAndMarkSlot(page_range_index, TIMESTAMP_COLUMN, base_page_index, base_page_slot)
            
            # Get the current tail RID from the base record
            current_tail_rid = self.__readAndMarkSlot(page_range_index, INDIRECTION_COLUMN, base_page_index, base_page_slot)

            # Step 4: Check if the RID points to the base record
            if current_tail_rid == (rid % MAX_RECORD_PER_PAGE_RANGE):
                # Base RID, read directly from the base page
                aggregate_value = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + aggregate_column_index, base_page_index, base_page_slot)
                sum_total += aggregate_value
                continue
            
            # Traverse Tail Records by Version
            current_version = 0
            found_value = False
            
            current_version_rid = current_tail_rid
            while current_version_rid != (rid % MAX_RECORD_PER_PAGE_RANGE) and current_version <= relative_version:
                # Read schema and timestamp from the tail record
                tail_schema = self.table.page_ranges[page_range_index].read_tail_record_column(current_version_rid, SCHEMA_ENCODING_COLUMN)
                tail_timestamp = self.table.page_ranges[page_range_index].read_tail_record_column(current_version_rid, TIMESTAMP_COLUMN)

                # Check if the column was updated in this version
                if (tail_schema >> aggregate_column_index) & 1:
                    
                    # Tail_timestamp should be greater than the base_timestamp for current version
                    if tail_timestamp >= base_timestamp:
                        # If looking for the latest version
                        if relative_version == 0:
                            tail_page_index, tail_slot = self.table.page_ranges[page_range_index].get_column_location(current_version_rid, NUM_HIDDEN_COLUMNS + aggregate_column_index)
                            aggregate_value = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + aggregate_column_index, tail_page_index, tail_slot)
                            sum_total += aggregate_value
                            found_value = True
                            break

                    # Reading from an older version of the record
                    else:
                        current_version += 1
                        if current_version == relative_version:
                            tail_page_index, tail_slot = self.table.page_ranges[page_range_index].get_column_location(current_version_rid, NUM_HIDDEN_COLUMNS + aggregate_column_index)
                            aggregate_value = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + aggregate_column_index, tail_page_index, tail_slot)
                            sum_total += aggregate_value
                            found_value = True
                            break

                # Move to the previous version
                current_version_rid = self.table.page_ranges[page_range_index].read_tail_record_column(current_version_rid, INDIRECTION_COLUMN)

            # If no value found in tail records, read from base page
            if not found_value:
                aggregate_value = self.__readAndMarkSlot(page_range_index, NUM_HIDDEN_COLUMNS + aggregate_column_index, base_page_index, base_page_slot)
                sum_total += aggregate_value

        return sum_total

    
    """
    Increments one column of the record
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
        value = self.table.bufferpool.read_page_slot(page_range_index, column, page_index, page_slot)
        frame_num = self.table.bufferpool.get_page_frame_num(page_range_index, column, page_index)
        self.table.bufferpool.mark_frame_used(frame_num)

        return value
    
    def __readAndTrack(self, page_range_index, column, page_index, slot, frames_used):
        value = self.table.bufferpool.read_page_slot(page_range_index, column, page_index, slot)
        frame_num = self.table.bufferpool.get_page_frame_num(page_range_index, column, page_index)
        frames_used.put(frame_num)
        return value