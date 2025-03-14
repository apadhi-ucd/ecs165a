from lstore.table import Table, Record
from lstore.config import *

from time import time
from queue import Queue

class Query:
    """
    # Constructs a Query instance capable of executing various operations on the specified table 
    Operations that fail must return False
    Operations that succeed should return the result or True
    Any operation that encounters exceptions should return False
    """
    def __init__(self, table):
        self.table:Table = table
        pass

    """
    # Insert data with specified columns
    # Return True when insertion succeeds
    # Returns False if insertion fails for any reason
    """
    def insert(self, *columns):
        if (self.table.index.locate(self.table.key, columns[self.table.key])):
            return False

        # verify column count matches expected
        if len(columns) != self.table.num_columns:
            return False
  
        entry = Record(rid = None, key = self.table.key, columns = None)
        self.table.assign_rid_to_record(entry)

        metadata_cols = [None] * NUM_HIDDEN_COLUMNS
        metadata_cols[INDIRECTION_COLUMN] = entry.rid
        metadata_cols[RID_COLUMN] = entry.rid
        metadata_cols[UPDATE_TIMESTAMP_COLUMN] = RECORD_NONE_VALUE
        #metadata_cols[TIMESTAMP_COLUMN] = int(time())
        metadata_cols[SCHEMA_ENCODING_COLUMN] = 0
        #metadata_cols[BASE_PAGE_ID_COLUMN] = entry.rid
        entry.columns = metadata_cols + list(columns)
    
        self.table.insert_record(entry)
        self.table.index.insert_in_all_indices(entry.columns)

        return True

    """
    # Fetch matching data with specified search criteria
    # :param lookup_val: the value to search for
    # :param lookup_col_idx: the column index to search in
    # :param output_cols_mask: which columns to include in results. array of 1 or 0 values.
    # Returns a list of Record objects on success
    # Returns False if data locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, lookup_val, lookup_col_idx, output_cols_mask):
        # retrieve results matching the "lookup_val" within the column specified by "lookup_col_idx"
        return self.select_version(lookup_val, lookup_col_idx, output_cols_mask, 0)
    
    """
    # Fetch matching data with specified search criteria for a particular version
    # :param lookup_val: the value to search for
    # :param lookup_col_idx: the column index to search in
    # :param output_cols_mask: which columns to include in results. array of 1 or 0 values.
    # :param ver_offset: the relative version of the data to retrieve.
    # Returns a list of Record objects on success
    # Returns False if data locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, lookup_val, lookup_col_idx, output_cols_mask, ver_offset):
        rid_collection = self.table.index.locate(lookup_col_idx, lookup_val)
        if not rid_collection:
            raise ValueError("No records found with the given key")
        
        result_entries = []

        for curr_rid in rid_collection:
            result_data = [None] * self.table.num_columns
            page_range_idx, base_page_idx, base_slot_idx = self.table.get_base_record_location(curr_rid)
            
            output_schema = 0
            for i in range(len(output_cols_mask)):
                if output_cols_mask[i] == 1:
                    output_schema |= (1 << i)

            if (output_schema >> self.table.key) & 1 == 1:
                result_data[self.table.key] = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + self.table.key, base_page_idx, base_slot_idx)
                output_schema &= ~(1 << self.table.key)

            base_schema = self.__readAndMarkSlot(page_range_idx, SCHEMA_ENCODING_COLUMN, base_page_idx, base_slot_idx)
            base_timestamp = self.__readAndMarkSlot(page_range_idx, TIMESTAMP_COLUMN, base_page_idx, base_slot_idx)
            current_tail_rid = self.__readAndMarkSlot(page_range_idx, INDIRECTION_COLUMN, base_page_idx, base_slot_idx)

            if current_tail_rid == curr_rid:
                # Current RID = base RID, read all columns from the base page
                for i in range(self.table.num_columns):
                    if (output_schema >> i) & 1:
                        result_data[i] = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + i, base_page_idx, base_slot_idx)
            
            else:
                version_counter = 0
                
                for i in range(self.table.num_columns):
                    if (output_schema >> i) & 1:
                        if (base_schema >> i) & 1 == 0:
                            result_data[i] = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + i, base_page_idx, base_slot_idx)
                            continue

                        temp_tail = current_tail_rid
                        value_retrieved = False
                        
                        while temp_tail != curr_rid and version_counter <= ver_offset:
                            tail_schema = self.table.page_ranges[page_range_idx].read_tail_record_column(temp_tail, SCHEMA_ENCODING_COLUMN)
                            tail_timestamp = self.table.page_ranges[page_range_idx].read_tail_record_column(temp_tail, TIMESTAMP_COLUMN)
                            
                            if (tail_schema >> i) & 1:
                                # Tail timestamp should exceed base timestamp for current version
                                if tail_timestamp >= base_timestamp:
                                    if ver_offset == 0:
                                        tail_pg_idx, tail_slot_pos = self.table.page_ranges[page_range_idx].get_column_location(temp_tail, NUM_HIDDEN_COLUMNS + i)
                                        result_data[i] = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + i, tail_pg_idx, tail_slot_pos)
                                        value_retrieved = True
                                        break
                                
                                # Retrieving an older version of the record
                                else:
                                    version_counter += 1
                                    if version_counter == ver_offset:
                                        tail_pg_idx, tail_slot_pos = self.table.page_ranges[page_range_idx].get_column_location(temp_tail, NUM_HIDDEN_COLUMNS + i)
                                        result_data[i] = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + i, tail_pg_idx, tail_slot_pos)
                                        value_retrieved = True
                                        break

                            temp_tail = self.table.page_ranges[page_range_idx].read_tail_record_column(temp_tail, INDIRECTION_COLUMN)
                        
                        if not value_retrieved:
                            result_data[i] = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + i, base_page_idx, base_slot_idx)
            
            result_entries.append(Record(curr_rid, result_data[self.table.key], result_data))
        
        return result_entries

    """
    # Modify a record with specified key and new column values
    # Returns True if update succeeds
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        record_loc = self.table.index.locate(self.table.key, primary_key)
        if record_loc is None:
            print("Update Error: Record does not exist")
            return False
        
        modified_cols = [None] * self.table.total_num_columns
        schema_bits = 0
        
        for i, new_val in enumerate(columns):
            # Prevent modification of primary key to an existing value
            if i == self.table.key and new_val is not None:
                if (self.table.index.locate(self.table.key, new_val) is not None):
                    print("Update Error: Primary Key already exists")
                    return False
            
            if new_val is not None:
                schema_bits |= (1 << i)
            
            modified_cols[NUM_HIDDEN_COLUMNS + i] = new_val

        pg_range_idx, page_idx, slot_idx = self.table.get_base_record_location(record_loc[0])
        
        old_tail_rid = self.table.bufferpool.read_page_slot(pg_range_idx, INDIRECTION_COLUMN, page_idx, slot_idx)
        existing_schema = self.table.bufferpool.read_page_slot(pg_range_idx, SCHEMA_ENCODING_COLUMN, page_idx, slot_idx)
        
        combined_schema = existing_schema | schema_bits

        modified_cols[INDIRECTION_COLUMN] = old_tail_rid
        modified_cols[SCHEMA_ENCODING_COLUMN] = schema_bits
        #modified_cols[TIMESTAMP_COLUMN] = int(time())

        new_entry = Record(rid = self.table.page_ranges[pg_range_idx].assign_logical_rid(), key = primary_key, columns = modified_cols)

        modified_cols[RID_COLUMN] = new_entry.rid

        self.table.update_record(record_loc[0], modified_cols)
        
        self.table.bufferpool.write_page_slot(pg_range_idx, INDIRECTION_COLUMN, page_idx, slot_idx, new_entry.rid)
        self.table.bufferpool.write_page_slot(pg_range_idx, SCHEMA_ENCODING_COLUMN, page_idx, slot_idx, combined_schema)

        indir_frame = self.table.bufferpool.get_page_frame_num(pg_range_idx, INDIRECTION_COLUMN, page_idx)
        schema_frame = self.table.bufferpool.get_page_frame_num(pg_range_idx, SCHEMA_ENCODING_COLUMN, page_idx)
        self.table.bufferpool.mark_frame_used(indir_frame)
        self.table.bufferpool.mark_frame_used(schema_frame)

        # Update indices with new values
        self.table.index.update_all_indices(primary_key, modified_cols)

        return True

    """
    # Internal Method
    # Locate and queue record with specified RID for removal
    # Returns True when record is successfully queued for deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # Find the RID linked to the primary key
        base_loc = self.table.index.locate(self.table.key, primary_key)
        if(base_loc is None):
            return False  # Record not found

        self.table.deallocation_base_rid_queue.put(base_loc[0])
        self.table.index.delete_from_all_indices(primary_key)

        # Record successfully queued for deletion
        return True
    
    """
    :param range_start: int         # Beginning of the key range to calculate 
    :param range_end: int           # End of the key range to calculate 
    :param target_col: int          # Index of column to aggregate
    # this operation only applies to the primary key.
    # Returns the summation of values in the given range
    # Returns False if no record exists in the given range
    """
    def sum(self, range_start, range_end, target_col):
        return self.sum_version(range_start, range_end, target_col, 0)

    
    """
    :param range_start: int         # Beginning of the key range to calculate 
    :param range_end: int           # End of the key range to calculate 
    :param target_col: int          # Index of column to aggregate
    :param ver_offset: int          # The relative version of records to process
    # this operation only applies to the primary key.
    # Returns the summation of values in the given range
    # Returns False if no record exists in the given range
    """
    def sum_version(self, range_start, range_end, target_col, ver_offset):
        # Retrieve all RIDs in the specified key range
        matching_rids = self.table.index.locate_range(range_start, range_end, self.table.key)
        
        if not matching_rids:
            return False

        running_total = 0
        for curr_rid in matching_rids:
            page_range_idx, base_page_idx, base_slot_idx = self.table.get_base_record_location(curr_rid)

            # Get base record metadata
            base_schema = self.__readAndMarkSlot(page_range_idx, SCHEMA_ENCODING_COLUMN, base_page_idx, base_slot_idx)
            base_timestamp = self.__readAndMarkSlot(page_range_idx, TIMESTAMP_COLUMN, base_page_idx, base_slot_idx)
            
            # Get current tail RID from base record
            current_tail_rid = self.__readAndMarkSlot(page_range_idx, INDIRECTION_COLUMN, base_page_idx, base_slot_idx)

            # Check if we're dealing with the base record
            if current_tail_rid == (curr_rid % MAX_RECORD_PER_PAGE_RANGE):
                # Direct read from base page
                column_value = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + target_col, base_page_idx, base_slot_idx)
                running_total += column_value
                continue
            
            # Navigate through tail records to find the right version
            version_counter = 0
            value_found = False
            
            active_tail_rid = current_tail_rid
            while active_tail_rid != (curr_rid % MAX_RECORD_PER_PAGE_RANGE) and version_counter <= ver_offset:
                # Get metadata from tail record
                tail_schema = self.table.page_ranges[page_range_idx].read_tail_record_column(active_tail_rid, SCHEMA_ENCODING_COLUMN)
                tail_timestamp = self.table.page_ranges[page_range_idx].read_tail_record_column(active_tail_rid, TIMESTAMP_COLUMN)

                # Check if our target column was modified in this version
                if (tail_schema >> target_col) & 1:
                    
                    # For current version, tail timestamp must be newer than base
                    if tail_timestamp >= base_timestamp:
                        # When targeting the latest version
                        if ver_offset == 0:
                            tail_pg_idx, tail_slot_pos = self.table.page_ranges[page_range_idx].get_column_location(active_tail_rid, NUM_HIDDEN_COLUMNS + target_col)
                            column_value = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + target_col, tail_pg_idx, tail_slot_pos)
                            running_total += column_value
                            value_found = True
                            break

                    # For historical versions
                    else:
                        version_counter += 1
                        if version_counter == ver_offset:
                            tail_pg_idx, tail_slot_pos = self.table.page_ranges[page_range_idx].get_column_location(active_tail_rid, NUM_HIDDEN_COLUMNS + target_col)
                            column_value = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + target_col, tail_pg_idx, tail_slot_pos)
                            running_total += column_value
                            value_found = True
                            break

                # Move to previous version
                active_tail_rid = self.table.page_ranges[page_range_idx].read_tail_record_column(active_tail_rid, INDIRECTION_COLUMN)

            # Fall back to base record if needed
            if not value_found:
                column_value = self.__readAndMarkSlot(page_range_idx, NUM_HIDDEN_COLUMNS + target_col, base_page_idx, base_slot_idx)
                running_total += column_value

        return running_total
    
    """
    Adds one to a specific column value of a record
    this implementation relies on select and update operations
    :param key: the primary key of the record to modify
    :param column: the column to increment
    # Returns True when increment succeeds
    # Returns False if record not found or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        matching_record = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if matching_record is not False:
            incremented_values = [None] * self.table.num_columns
            incremented_values[column] = matching_record[column] + 1
            update_result = self.update(key, *incremented_values)
            return update_result
        return False
    
    def __readAndMarkSlot(self, page_range_idx, column, page_idx, slot_pos):
        data_val = self.table.bufferpool.read_page_slot(page_range_idx, column, page_idx, slot_pos)
        frame_idx = self.table.bufferpool.get_page_frame_num(page_range_idx, column, page_idx)
        self.table.bufferpool.mark_frame_used(frame_idx)

        return data_val
    
    def __readAndTrack(self, page_range_idx, column, page_idx, slot_pos, tracked_frames):
        data_val = self.table.bufferpool.read_page_slot(page_range_idx, column, page_idx, slot_pos)
        frame_idx = self.table.bufferpool.get_page_frame_num(page_range_idx, column, page_idx)
        tracked_frames.put(frame_idx)
        return data_val
