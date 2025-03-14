"""
Container for managing table column indices. Primary key column gets indexed by default, while additional columns can be indexed as needed. Typically uses B-Trees for indexing, though alternative structures are supported.
"""
import base64
from lstore.config import *
from BTrees.OOBTree import OOBTree
import pickle
import threading

class Index:

    def __init__(self, table):
        self.table = table
        self.num_columns = table.num_columns
        self.key = table.key
        
        # Initialize index array with None values
        self.indices = [None] * self.num_columns
        # Set up primary key index
        self.indices[self.key] = OOBTree()
        # Create lock for thread safety
        self.index_lock = threading.Lock()

    """
    # Extract all RIDs from primary index, useful for merge operations
    """
    def grab_all(self):
        result_rids = []
        for _, rid_dict in self.indices[self.key].items():
            for single_rid in rid_dict:
                result_rids.append(single_rid)
        return result_rids

    """
    # Add value to specified index
    """
    def insert_to_index(self, idx, val, record_id):
        '''Inserts a record ID into the appropriate index'''
        current_index = self.indices[idx]

        if (current_index is None):
            return False
        
        if (not current_index.get(val)):
            current_index[val] = {}
        
        current_index[val][record_id] = True

        return True

    """
    # Remove a single value from specified index
    """
    def delete_from_index(self, idx, val):
        '''Removes a specific value from an index'''
        current_index = self.indices[idx]

        if (current_index is None):
            return False
        
        if (current_index.get(val)):
            del current_index[val]
        else:
            return False

    """
    # Find all record locations with given value in specified column
    # Returns None if no records found
    """
    def locate(self, column, value):
        with self.index_lock:
            if self.indices[column] == None:
                return False
            
            found_records = list(self.indices[column].get(value, [])) or None
            return found_records
    
    """
    # Find all record IDs with values between start and end in specified column
    # Returns None if no records found
    """
    def locate_range(self, start, end, column):
        with self.index_lock:
            if self.indices[column] == None:
                return False

            matching_rids = [record_id for record_set in self.indices[column].values(min=start, max=end) for record_id in record_set.keys()] or None
            return matching_rids

    """
    # Check if index exists for given column
    """
    def exist_index(self, col_num):
        return self.indices[col_num] != None

    """
    # Process a new record and add it to all relevant indices
    """
    def insert_in_all_indices(self, columns):
        with self.index_lock:
            pk_value = columns[self.key + NUM_HIDDEN_COLUMNS]
            if self.indices[self.key].get(pk_value):
                return False

            # Insert into each active index
            for col_idx in range(self.num_columns):
                if self.indices[col_idx] != None:
                    col_value = columns[col_idx + NUM_HIDDEN_COLUMNS]
                    record_id = columns[RID_COLUMN]
                    self.insert_to_index(col_idx, col_value, record_id)

            return True

    """
    # Update values across all indices
    """
    def update_all_indices(self, pk_value, updated_columns, original_columns):
        with self.index_lock:
            # Retrieve record ID using primary key
            if not self.indices[self.key].get(pk_value):
                return False
            
            record_id = list(self.indices[self.key][pk_value].keys())[0]

            # Update primary key index if needed
            if (updated_columns[NUM_HIDDEN_COLUMNS + self.key] != None) and (self.indices[self.key] != None) and (original_columns[self.key] != None):
                if self.indices[self.key].get(pk_value) and self.indices[self.key][pk_value].get(record_id, []):
                    del self.indices[self.key][pk_value][record_id]

                    if self.indices[self.key][pk_value] == {}:
                        del self.indices[self.key][pk_value]
                    self.insert_to_index(self.key, updated_columns[self.key + NUM_HIDDEN_COLUMNS], record_id)
                        
                pk_value = updated_columns[self.key + NUM_HIDDEN_COLUMNS]
            
            # Update secondary indices
            for col_idx in range(0, self.num_columns):
                if (updated_columns[NUM_HIDDEN_COLUMNS + col_idx] != None) and (self.indices[col_idx] != None) and (original_columns[col_idx] != None) and (col_idx != self.key):
                    old_value = original_columns[col_idx]

                    if self.indices[col_idx].get(old_value) and self.indices[col_idx][old_value].get(record_id, []):
                        del self.indices[col_idx][old_value][record_id]

                        if self.indices[col_idx][old_value] == {}:
                            del self.indices[col_idx][old_value]

                        self.insert_to_index(col_idx, updated_columns[col_idx + NUM_HIDDEN_COLUMNS], record_id)

            return True

    """
    # Remove record entries from all indices
    """
    def delete_from_all_indices(self, pk_value, original_columns):
        with self.index_lock:
            if not self.indices[self.key].get(pk_value):
                return False
            
            record_id = list(self.indices[self.key][pk_value].keys())[0]

            # Clean up all relevant indices
            for col_idx in range(self.num_columns):
                if (self.indices[col_idx] != None) and (original_columns[col_idx] != None):
                    if self.indices[col_idx].get(original_columns[col_idx]) and self.indices[col_idx][original_columns[col_idx]].get(record_id, []):
                        del self.indices[col_idx][original_columns[col_idx]][record_id]

                        if self.indices[col_idx][original_columns[col_idx]] == {}:
                            del self.indices[col_idx][original_columns[col_idx]]

            return True

    """
    # Create secondary index by scanning through page ranges and buffer pool
    """
    def create_index(self, col_num):
        if col_num >= self.table.num_columns:
            return False

        if self.indices[col_num] == None:
            # Initialize new index
            self.indices[col_num] = OOBTree()
            # Get all base record IDs to index
            base_records = self.grab_all()

            for record_id in base_records:
                # Locate base record
                page_range_idx, page_idx, slot_idx = self.table.get_base_record_location(record_id)

                # Read indirection column
                indirection_rid = self.table.bufferpool.read_page_slot(page_range_idx, INDIRECTION_COLUMN, page_idx, slot_idx)
                frame_id = self.table.bufferpool.get_page_frame_num(page_range_idx, INDIRECTION_COLUMN, page_idx)
                self.table.bufferpool.mark_frame_used(frame_id)

                # Read schema encoding
                schema_code = self.table.bufferpool.read_page_slot(page_range_idx, SCHEMA_ENCODING_COLUMN, page_idx, slot_idx)
                frame_id = self.table.bufferpool.get_page_frame_num(page_range_idx, SCHEMA_ENCODING_COLUMN, page_idx)
                self.table.bufferpool.mark_frame_used(frame_id)

                # Check for updates
                column_data = None
                is_tail = False
                tail_time = 0
                
                if indirection_rid == (record_id % MAX_RECORD_PER_PAGE_RANGE): # No updates
                    # Read directly from base record
                    column_data = self.table.bufferpool.read_page_slot(page_range_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx, slot_idx)
                    frame_id = self.table.bufferpool.get_page_frame_num(page_range_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx)
                    self.table.bufferpool.mark_frame_used(frame_id)

                else: # Updates exist
                    # Get base timestamp
                    base_time = self.table.bufferpool.read_page_slot(page_range_idx, TIMESTAMP_COLUMN, page_idx, slot_idx)
                    frame_id = self.table.bufferpool.get_page_frame_num(page_range_idx, TIMESTAMP_COLUMN, page_idx)
                    self.table.bufferpool.mark_frame_used(frame_id)

                    # Check if this column has been updated
                    if (schema_code >> col_num) & 1:
                        while True:
                            try:
                                # Try to find the tail record for this column
                                tail_page_idx, tail_slot_idx = self.table.page_ranges[page_range_idx].get_column_location(indirection_rid, col_num + NUM_HIDDEN_COLUMNS)
                                tail_time = self.table.page_ranges[page_range_idx].read_tail_record_column(indirection_rid, TIMESTAMP_COLUMN)
                                is_tail = True
                                break
                            except:
                                # Move to previous update
                                prior_rid = indirection_rid
                                indirection_rid = self.table.page_ranges[page_range_idx].read_tail_record_column(indirection_rid, INDIRECTION_COLUMN)

                                # Edge case: latest update is in first tail record
                                if indirection_rid == record_id:
                                    first_tail_rid = self.table.page_ranges[page_range_idx].read_tail_record_column(prior_rid, RID_COLUMN)
                                    tail_page_idx, tail_slot_idx = self.table.page_ranges[page_range_idx].get_column_location(first_tail_rid, col_num + NUM_HIDDEN_COLUMNS)
                                    tail_time = self.table.page_ranges[page_range_idx].read_tail_record_column(first_tail_rid, TIMESTAMP_COLUMN)
                                    is_tail = True
                                    break

                    # Determine whether to use tail or base record value
                    if (schema_code >> col_num) & 1 and tail_time >= base_time and is_tail:
                        # Use tail record (most recent update)
                        column_data = self.table.bufferpool.read_page_slot(page_range_idx, col_num + NUM_HIDDEN_COLUMNS, tail_page_idx, tail_slot_idx)
                        frame_id = self.table.bufferpool.get_page_frame_num(page_range_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx)
                        self.table.bufferpool.mark_frame_used(frame_id)
                    else:
                        # Use base record (merged value)
                        column_data = self.table.bufferpool.read_page_slot(page_range_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx, slot_idx)
                        frame_id = self.table.bufferpool.get_page_frame_num(page_range_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx)
                        self.table.bufferpool.mark_frame_used(frame_id)
                
                # Add to index
                self.insert_to_index(col_num, column_data, record_id)

            return True
        else:
            return False

    """
    # Remove index for specified column
    """
    def drop_index(self, col_num):
        if col_num >= self.table.num_columns:
            return False

        if self.indices[col_num] != None:
            # Clear and remove the index
            self.indices[col_num].clear()
            self.indices[col_num] = None
            return True
        else:
            return False

    """
    # Convert indices to serialized format for persistence
    """
    def serialize(self):
        serialized = {}

        for col_idx in range(self.num_columns):
            if self.indices[col_idx] != None:
                pickled_data = pickle.dumps(self.indices[col_idx])
                encoded_data = base64.b64encode(pickled_data).decode('utf-8')
                serialized[f"index[{col_idx}]"] = encoded_data

        return serialized
    
    """
    # Restore indices from serialized data
    """
    def deserialize(self, serialized_data):
        for col_idx in range(self.num_columns):
            index_key = f"index[{col_idx}]"
            if index_key in serialized_data:
                decoded_data = base64.b64decode(serialized_data[index_key])
                self.indices[col_idx] = pickle.loads(decoded_data)
