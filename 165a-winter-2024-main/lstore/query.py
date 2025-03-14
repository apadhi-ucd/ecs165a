from lstore.table import Table, Record
from lstore.config import *

import threading

from time import time, sleep
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
    def insert(self, *columns, log_entry=None):
        if (self.table.index.locate(self.table.key, columns[self.table.key])):
            return False

        # verify column count matches expected
        if len(columns) != self.table.num_columns:
            return False
  
        entry = Record(rid = None, key = self.table.key, columns = None)
        self.table.assign_rid_to_record(entry)

        # look over?
        metadata_cols = [None] * NUM_HIDDEN_COLUMNS
        metadata_cols[INDIRECTION_COLUMN] = entry.rid
        metadata_cols[RID_COLUMN] = entry.rid
        metadata_cols[UPDATE_TIMESTAMP_COLUMN] = RECORD_NONE_VALUE
        metadata_cols[SCHEMA_ENCODING_COLUMN] = 0
        entry.columns = metadata_cols + list(columns)
    
        self.table.insert_record(entry)
        self.table.index.insert_in_all_indices(entry.columns)


        if log_entry:
            # Track changes to each indexed column
            index_modifications = []
            
            # Identify which columns had indices and record their values
            for col_idx, has_index in enumerate(self.table.index.indices):
                if has_index:
                    # Record: column index, original value, new value (None for deletion)
                    index_modifications.append((col_idx, None, columns[col_idx]))
            
            # Add comprehensive deletion entry to the log
            log_entry["changes"].append({
                "type": "insert",
                "rid": entry.rid,
                "table": self.table.name,
                "columns": columns,  # No new column values for deletion
                "prev_columns": None,
                "page_range": entry.rid // MAX_RECORD_PER_PAGE_RANGE,
                "index_changes": index_modifications
            })
        
        # Operation completed successfully
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
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Retrieves records matching a search criteria at a specific historical version.
        
        This method performs a multi-step retrieval process:
        1. Locates record IDs matching the search criteria
        2. For each record, determines which version to retrieve based on version offset
        3. Assembles complete records by fetching only the requested columns
        4. Handles the complexity of retrieving data from either base or tail pages
        
        Parameters:
            search_key (any): The value to search for
            search_key_index (int): Column index to search within
            projected_columns_index (list): Bitmap of columns to include (1=include, 0=exclude)
            relative_version (int): Historical version offset (0=latest)
            
        Returns:
            list: Collection of Record objects containing the requested data
                  Empty list if no matching records found
        """
        # Step 1: Find matching record IDs, creating temporary index if needed
        matching_rids = None
        
        # Create temporary index if one doesn't exist for this column
        if not self.table.index.exist_index(search_key_index):
            # Create temporary index, use it, then drop it
            self.table.index.create_index(search_key_index)
            matching_rids = self.table.index.locate(search_key_index, search_key)
            self.table.index.drop_index(search_key_index)
        else:
            # Use existing index
            matching_rids = self.table.index.locate(search_key_index, search_key)
        
        # Early return if no matches found
        if matching_rids is None:
            return []
        
        # Collection to hold our result records
        result_records = []

        # Step 2: Process each matching record
        for record_id in matching_rids:
            # Initialize storage for column values
            column_values = [None] * self.table.num_columns
            
            # Locate the base record's physical position
            page_range_id, base_page_id, slot_position = self.table.get_base_record_location(record_id)
            
            # Convert column projection list to bitwise schema for efficient checking
            column_projection_bitmap = 0
            for col_idx, include_flag in enumerate(projected_columns_index):
                if include_flag == 1:
                    column_projection_bitmap |= (1 << col_idx)

            # Special handling for primary key (always fetch from base record)
            if (column_projection_bitmap >> self.table.key) & 1 == 1:
                column_values[self.table.key] = self.__readAndMarkSlot(
                    page_range_id, 
                    NUM_HIDDEN_COLUMNS + self.table.key, 
                    base_page_id, 
                    slot_position
                )
                # Remove primary key from projection bitmap since we've handled it
                column_projection_bitmap &= ~(1 << self.table.key)

            # Fetch metadata from base record
            base_record_schema = self.__readAndMarkSlot(page_range_id, SCHEMA_ENCODING_COLUMN, base_page_id, slot_position)
            base_record_timestamp = self.__readAndMarkSlot(page_range_id, TIMESTAMP_COLUMN, base_page_id, slot_position)
            latest_update_rid = self.__readAndMarkSlot(page_range_id, INDIRECTION_COLUMN, base_page_id, slot_position)

            # Case 1: No updates exist (base record is the only version)
            if latest_update_rid < MAX_RECORD_PER_PAGE_RANGE:
                # Read all projected columns directly from base record
                for col_idx in range(self.table.num_columns):
                    if (column_projection_bitmap >> col_idx) & 1:
                        column_values[col_idx] = self.__readAndMarkSlot(
                            page_range_id, 
                            NUM_HIDDEN_COLUMNS + col_idx, 
                            base_page_id, 
                            slot_position
                        )
            
            # Case 2: Updates exist (need to navigate update chain)
            else:
                version_counter = 0
                
                # Process each requested column
                for col_idx in range(self.table.num_columns):
                    if (column_projection_bitmap >> col_idx) & 1:
                        # If column was never updated, read from base record
                        if (base_record_schema >> col_idx) & 1 == 0:
                            column_values[col_idx] = self.__readAndMarkSlot(
                                page_range_id, 
                                NUM_HIDDEN_COLUMNS + col_idx, 
                                base_page_id, 
                                slot_position
                            )
                            continue

                        # Column was updated - traverse update chain
                        current_tail_id = latest_update_rid
                        value_retrieved = False
                        
                        # Follow update chain until we find the right version
                        while current_tail_id >= MAX_RECORD_PER_PAGE_RANGE and version_counter <= relative_version:
                            # Get metadata from tail record
                            update_schema = self.table.page_ranges[page_range_id].read_tail_record_column(
                                current_tail_id, SCHEMA_ENCODING_COLUMN
                            )
                            update_timestamp = self.table.page_ranges[page_range_id].read_tail_record_column(
                                current_tail_id, TIMESTAMP_COLUMN
                            )

                            # Check if this update modified our target column
                            if (update_schema >> col_idx) & 1:
                                # For latest version (relative_version=0)
                                if update_timestamp >= base_record_timestamp:
                                    if relative_version == 0:
                                        # Get physical location of column in tail page
                                        tail_page_id, tail_slot = self.table.page_ranges[page_range_id].get_column_location(
                                            current_tail_id, NUM_HIDDEN_COLUMNS + col_idx
                                        )
                                        # Read value and mark as found
                                        column_values[col_idx] = self.__readAndMarkSlot(
                                            page_range_id, 
                                            NUM_HIDDEN_COLUMNS + col_idx, 
                                            tail_page_id, 
                                            tail_slot
                                        )
                                        value_retrieved = True
                                        break
                                
                                # For historical versions
                                else:
                                    version_counter += 1
                                    if version_counter == relative_version:
                                        # Found the specific historical version requested
                                        tail_page_id, tail_slot = self.table.page_ranges[page_range_id].get_column_location(
                                            current_tail_id, NUM_HIDDEN_COLUMNS + col_idx
                                        )
                                        column_values[col_idx] = self.__readAndMarkSlot(
                                            page_range_id, 
                                            NUM_HIDDEN_COLUMNS + col_idx, 
                                            tail_page_id, 
                                            tail_slot
                                        )
                                        value_retrieved = True
                                        break

                            # Move to previous update in chain
                            current_tail_id = self.table.page_ranges[page_range_id].read_tail_record_column(
                                current_tail_id, INDIRECTION_COLUMN
                            )
                        
                        # If no suitable update found, fall back to base record
                        if not value_retrieved:
                            column_values[col_idx] = self.__readAndMarkSlot(
                                page_range_id, 
                                NUM_HIDDEN_COLUMNS + col_idx, 
                                base_page_id, 
                                slot_position
                            )
            
            # Create record object and add to results
            result_records.append(Record(record_id, column_values[self.table.key], column_values))
        
        return result_records


    """
    # Modify a record with specified key and new column values
    # Returns True if update succeeds
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, log_entry=None):
        """
        Modifies an existing record identified by its primary key.
        
        This method implements the update operation through a multi-version approach:
        1. Validates the record exists and any primary key changes are valid
        2. Creates a new tail record with the updated values
        3. Updates the base record's metadata to point to the new tail record
        4. Updates all relevant indices to reflect the changes
        5. Logs the update operation if logging is enabled
        
        Parameters:
            primary_key: The primary key of the record to update
            *columns: Variable length argument containing new values for columns
                      (None for columns that should remain unchanged)
            log_entry: Optional logging structure to record the update operation
            
        Returns:
            bool: True if update succeeds, False if record doesn't exist or 
                  primary key constraint is violated
        """
        # Step 1: Locate the record to be updated
        target_record_location = self.table.index.locate(self.table.key, primary_key)
        if target_record_location is None:
            return False  # Record not found
        
        # Extract the record ID
        target_rid = target_record_location[0]
        
        # Step 2: Prepare the update data
        # Initialize array for the full record (metadata + data columns)
        updated_values = [None] * self.table.total_num_columns
        
        # Track which columns are being modified using a bitmap
        modification_bitmap = 0
        
        # Retrieve current column values (needed for index updates)
        original_values = self.__get_prev_columns(target_rid, *columns)
        
        # Process each column value in the update request
        for col_idx, new_value in enumerate(columns):
            # Special handling for primary key updates
            if col_idx == self.table.key and new_value is not None:
                # Check if the new primary key already exists
                if self.table.index.locate(self.table.key, new_value) is not None:
                    print("Update Error: Primary Key already exists")
                    return False
            
            # Mark column as modified in the bitmap if value is provided
            if new_value is not None:
                modification_bitmap |= (1 << col_idx)
            
            # Store the new value in the appropriate position
            updated_values[NUM_HIDDEN_COLUMNS + col_idx] = new_value
        
        # Step 3: Locate the physical position of the base record
        page_range_id, base_page_id, slot_id = self.table.get_base_record_location(target_rid)
        
        # Step 4: Retrieve metadata from the base record
        current_tail_rid = self.table.bufferpool.read_page_slot(
            page_range_id, INDIRECTION_COLUMN, base_page_id, slot_id
        )
        current_schema = self.table.bufferpool.read_page_slot(
            page_range_id, SCHEMA_ENCODING_COLUMN, base_page_id, slot_id
        )
        
        # Compute the combined schema (existing + new modifications)
        merged_schema = current_schema | modification_bitmap
        
        # Step 5: Set up metadata for the new tail record
        updated_values[INDIRECTION_COLUMN] = current_tail_rid  # Link to previous version
        updated_values[SCHEMA_ENCODING_COLUMN] = modification_bitmap  # Only track columns modified in this update
        # Timestamp is handled by the lower layers
        
        # Step 6: Create a new tail record
        tail_record = Record(
            rid=self.table.page_ranges[page_range_id].assign_logical_rid(),
            key=primary_key,
            columns=updated_values
        )
        
        # Store the new tail record's RID in the record
        updated_values[RID_COLUMN] = tail_record.rid
        
        # Step 7: Persist the update to storage
        self.table.update_record(target_rid, updated_values)
        
        # Step 8: Update the base record's metadata to point to the new tail record
        self.table.bufferpool.write_page_slot(
            page_range_id, INDIRECTION_COLUMN, base_page_id, slot_id, tail_record.rid
        )
        self.table.bufferpool.write_page_slot(
            page_range_id, SCHEMA_ENCODING_COLUMN, base_page_id, slot_id, merged_schema
        )
        
        # Step 9: Mark the affected buffer frames as recently used
        indirection_frame = self.table.bufferpool.get_page_frame_num(
            page_range_id, INDIRECTION_COLUMN, base_page_id
        )
        schema_frame = self.table.bufferpool.get_page_frame_num(
            page_range_id, SCHEMA_ENCODING_COLUMN, base_page_id
        )
        self.table.bufferpool.mark_frame_used(indirection_frame)
        self.table.bufferpool.mark_frame_used(schema_frame)
        
        # Step 10: Update all indices to reflect the changes
        self.table.index.update_all_indices(primary_key, updated_values, original_values)
        
        # Step 11: Log the update operation if logging is enabled
        if log_entry:
            # Track changes to indexed columns
            index_modifications = []
            
            # Record only the columns that have both an index and a changed value
            for col_idx, has_index in enumerate(self.table.index.indices):
                if has_index and original_values[col_idx] != columns[col_idx]:
                    # Store column index, old value, and new value
                    index_modifications.append((col_idx, original_values[col_idx], columns[col_idx]))
            
            # Add detailed update entry to the log
            log_entry["changes"].append({
                "type": "update",
                "rid": target_rid,
                "prev_tail_rid": current_tail_rid,
                "table": self.table.name,
                "columns": updated_values.copy(),
                "prev_columns": original_values.copy(),
                "page_range": page_range_id,
                "index_changes": index_modifications
            })
        
        # Update completed successfully
        return True

    """
    # Internal Method
    # Locate and queue record with specified RID for removal
    # Returns True when record is successfully queued for deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key, log_entry=None):
        """
        Removes a record with the specified primary key from the database.
        
        This method performs the following operations:
        1. Locates the record's RID using the primary key
        2. Retrieves previous column values for logging and index updates
        3. Logs the deletion operation if logging is enabled
        4. Queues the record for physical deletion
        5. Removes the record from all indices
        
        Args:
            primary_key: The primary key of the record to delete
            
        Returns:
            bool: True if record was successfully queued for deletion,
                  False if record doesn't exist or is locked
        """
        # Step 1: Attempt to locate the record by primary key
        record_location = self.table.index.locate(self.table.key, primary_key)
        
        # Verify record exists before proceeding
        if record_location is None:
            return False  # Early exit - nothing to delete
        
        # Extract the RID from the location result
        target_rid = record_location[0]
        
        # Step 2: Retrieve column values before deletion (needed for index updates)
        column_values_before_deletion = self.__get_prev_columns(target_rid, *self.table.index.indices)
        
        # Step 3: Create deletion log if logging is enabled
        if log_entry:
            # Track changes to each indexed column
            index_modifications = []
            
            # Identify which columns had indices and record their values
            for col_idx, has_index in enumerate(self.table.index.indices):
                if has_index:
                    # Record: column index, original value, new value (None for deletion)
                    index_modifications.append((col_idx, column_values_before_deletion[col_idx], None))
            
            # Add comprehensive deletion entry to the log
            log_entry["changes"].append({
                "type": "delete",
                "rid": target_rid,
                "table": self.table.name,
                "columns": None,  # No new column values for deletion
                "prev_columns": column_values_before_deletion.copy(),
                "page_range": target_rid // MAX_RECORD_PER_PAGE_RANGE,
                "index_changes": index_modifications
            })
        
        # Step 4: Queue record for physical deletion
        self.table.deallocation_base_rid_queue.put(target_rid)
        
        # Step 5: Remove record from all indices
        self.table.index.delete_from_all_indices(primary_key, column_values_before_deletion)
        
        # Operation completed successfully
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
    # MIGHT NEED UPDATING
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
    
    def __get_prev_columns(self, rid, *columns):
        """
        Retrieves the previous values of specified columns for a given record.
        
        This internal helper method navigates the complex update chain structure to find
        the most recent values for columns that are about to be modified. It handles
        both base records and various tail record scenarios, including edge cases.
        
        Args:
            rid: The record ID to retrieve column values for
            *columns: Variable length argument specifying which columns to retrieve
                     (typically contains booleans or None values)
        
        Returns:
            list: Array of column values representing the record's current state
                  before modification
        """
        # Initialize storage for retrieved column values
        original_values = [None] * self.table.num_columns
        found_in_tail_record = False
        
        # Locate the physical position of the base record
        page_range_num, base_page_num, slot_position = self.table.get_base_record_location(rid)
        
        # Retrieve metadata from base record
        latest_update_pointer = self.__readAndMarkSlot(page_range_num, INDIRECTION_COLUMN, base_page_num, slot_position)
        base_record_time = self.__readAndMarkSlot(page_range_num, TIMESTAMP_COLUMN, base_page_num, slot_position)
        update_bitmap = self.__readAndMarkSlot(page_range_num, SCHEMA_ENCODING_COLUMN, base_page_num, slot_position)
        
        # Get timestamp from the latest update (if any)
        latest_update_time = self.table.page_ranges[page_range_num].read_tail_record_column(
            latest_update_pointer, TIMESTAMP_COLUMN
        )
        
        # Process each column that needs to be retrieved
        for col_idx in range(self.table.num_columns):
            # Only process columns that are marked for retrieval
            if columns[col_idx] is not None:
                # Re-read the indirection pointer (it might have changed)
                current_pointer = self.__readAndMarkSlot(page_range_num, INDIRECTION_COLUMN, base_page_num, slot_position)
                
                # Case 1: No updates exist for this record
                if current_pointer == (rid % MAX_RECORD_PER_PAGE_RANGE):
                    # Read directly from base record
                    original_values[col_idx] = self.__readAndMarkSlot(
                        page_range_num, 
                        NUM_HIDDEN_COLUMNS + col_idx, 
                        base_page_num, 
                        slot_position
                    )
                else:
                    # Case 2: Updates exist - check if this column was updated
                    tail_page_num = None
                    tail_slot = None
                    
                    # If column has been updated according to schema bitmap
                    if (update_bitmap >> col_idx) & 1:
                        # Try to locate the column in the update chain
                        update_pointer = current_pointer
                        
                        # Loop until we find the right update or handle exceptions
                        while True:
                            try:
                                # Attempt to locate the column in the current tail record
                                tail_page_num, tail_slot = self.table.page_ranges[page_range_num].get_column_location(
                                    update_pointer, col_idx + NUM_HIDDEN_COLUMNS
                                )
                                
                                # Get timestamp of this update
                                latest_update_time = self.table.page_ranges[page_range_num].read_tail_record_column(
                                    update_pointer, TIMESTAMP_COLUMN
                                )
                                
                                # Mark that we found the column in a tail record
                                found_in_tail_record = True
                                break
                                
                            except:
                                # Store current pointer before moving to previous update
                                previous_pointer = update_pointer
                                
                                # Follow the update chain backward
                                update_pointer = self.table.page_ranges[page_range_num].read_tail_record_column(
                                    update_pointer, INDIRECTION_COLUMN
                                )
                                
                                # Handle edge case: latest update is in first tail record
                                if update_pointer == rid:
                                    # Get RID of the least recently updated tail record
                                    oldest_tail_rid = self.table.page_ranges[page_range_num].read_tail_record_column(
                                        previous_pointer, RID_COLUMN
                                    )
                                    
                                    # Locate the column in this tail record
                                    tail_page_num, tail_slot = self.table.page_ranges[page_range_num].get_column_location(
                                        oldest_tail_rid, col_idx + NUM_HIDDEN_COLUMNS
                                    )
                                    
                                    # Get timestamp of this update
                                    latest_update_time = self.table.page_ranges[page_range_num].read_tail_record_column(
                                        oldest_tail_rid, TIMESTAMP_COLUMN
                                    )
                                    
                                    found_in_tail_record = True
                                    break
                    
                    # Determine where to read the value from
                    if (update_bitmap >> col_idx) & 1 and latest_update_time >= base_record_time and found_in_tail_record:
                        # Read from tail record if column was updated and update is newer than base
                        original_values[col_idx] = self.__readAndMarkSlot(
                            page_range_num, 
                            NUM_HIDDEN_COLUMNS + col_idx, 
                            tail_page_num, 
                            tail_slot
                        )
                    else:
                        # Fall back to base record if column wasn't updated or update is older
                        original_values[col_idx] = self.__readAndMarkSlot(
                            page_range_num, 
                            NUM_HIDDEN_COLUMNS + col_idx, 
                            base_page_num, 
                            slot_position
                        )
        
        # Return the retrieved column values
        return original_values
    
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
