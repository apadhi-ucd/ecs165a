"""
A structure that maintains indices for table columns. Primary key column gets indexed by default,
while other columns can be indexed on demand. Indices typically use B-Trees though other
data structures are possible.
"""
import base64
import re
from lstore.config import *
from BTrees.OOBTree import OOBTree
import pickle
import os

class Index:

    def __init__(self, table):
        self.indices = [None] * table.num_columns
        
        """
        # Maps primary key to most recent column values
        # self.key_value_dict[primary_key] = [col1_val, col2_val...]
        """
        self.key_value_dict = OOBTree()
        
        self.column_count = table.num_columns
        self.key = table.key
        self.table = table
        
        self.create_index(self.key)
        self.__build_primary_index()
    
    def __build_primary_index(self):
        # Initialize a BTree for primary index
        self.indices[self.key] = OOBTree()

        # Get all unique base record IDs
        all_rids = self.table.grab_all_base_rids()
        used_frames = []
        
        # Populate primary index by reading through buffer pool
        for record_id in all_rids:
            pr_idx, pg_idx, slot_idx = self.table.get_base_record_location(record_id)
            pk_val = self.table.bufferpool.read_page_slot(pr_idx, self.key + NUM_HIDDEN_COLUMNS, pg_idx, slot_idx)
            frame_num = self.table.bufferpool.get_page_frame_num(pr_idx, self.key + NUM_HIDDEN_COLUMNS, pg_idx)
            used_frames.append(frame_num)
            
            # Insert primary key mapping in BTree: {pk_val: {record_id: True}}
            self.add_to_index(self.key, pk_val, record_id)
        
        # Mark all accessed frames as used
        for frame in used_frames:
            self.table.bufferpool.mark_frame_used(frame)
    
    def add_to_index(self, col_idx, col_val, record_id):
        """Add a record ID to an index under the given column value"""
        idx_tree = self.indices[col_idx]

        if (idx_tree is None):
            return False
        
        if (not idx_tree.get(col_val)):
            idx_tree[col_val] = {}

        idx_tree[col_val][record_id] = True
    
    def remove_from_index(self, col_idx, col_val):
        """Remove a specific value from an index"""
        idx_tree = self.indices[col_idx]

        if (idx_tree is None):
            return False
        
        if (idx_tree.get(col_val)):
            del idx_tree[col_val]
        else:
            return False
    
    def locate(self, column, value):
        """
        # Find locations of all records with given value in specified column
        # Returns list of record IDs or None if nothing found
        """
        return list(self.indices[column].get(value, [])) or None

    def locate_range(self, start_val, end_val, column):
        """
        # Find record IDs with values between start_val and end_val in the specified column
        # Returns list of record IDs or None if nothing found
        """
        return [rid for sublist in self.indices[column].values(min=start_val, max=end_val) for rid in sublist.keys()] or None
    
    def create_index(self, col_num):
        """Create an index on a specific column using page range and buffer pool data"""
        if col_num >= self.table.num_columns:
            return False

        if self.indices[col_num] is None:
            self.indices[col_num] = OOBTree()
            cache_frames = []
            
            # Retrieve all base record IDs
            all_records = self.grab_all()

            # Read from buffer pool to find latest record values
            for record_id in all_records:
                pr_idx, page_idx, slot_pos = self.table.get_base_record_location(record_id)

                indir_ptr = self.table.bufferpool.read_page_slot(pr_idx, INDIRECTION_COLUMN, page_idx, slot_pos)
                frame_id = self.table.bufferpool.get_page_frame_num(pr_idx, INDIRECTION_COLUMN, page_idx)
                cache_frames.append(frame_id)

                # Determine whether to read from base or tail record
                if indir_ptr == (record_id % MAX_RECORD_PER_PAGE_RANGE): # No updates exist
                    column_data = self.table.bufferpool.read_page_slot(pr_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx, slot_pos)
                    frame_id = self.table.bufferpool.get_page_frame_num(pr_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx)
                    cache_frames.append(frame_id)

                else: # Updates exist - need to check recency
                    base_ts = self.table.bufferpool.read_page_slot(pr_idx, TIMESTAMP_COLUMN, page_idx, slot_pos)
                    frame_id = self.table.bufferpool.get_page_frame_num(pr_idx, TIMESTAMP_COLUMN, page_idx)
                    cache_frames.append(frame_id)

                    tail_schema = self.table.page_ranges[pr_idx].read_tail_record_column(indir_ptr, SCHEMA_ENCODING_COLUMN)
                    tail_ts = self.table.page_ranges[pr_idx].read_tail_record_column(indir_ptr, TIMESTAMP_COLUMN)

                    # Check if tail record has most recent update for this column
                    if (tail_schema >> col_num) & 1 and tail_ts >= base_ts:
                        tail_pg_idx, tail_slot = self.table.page_ranges[pr_idx].get_column_location(indir_ptr, col_num + NUM_HIDDEN_COLUMNS)

                        column_data = self.table.bufferpool.read_page_slot(pr_idx, col_num + NUM_HIDDEN_COLUMNS, tail_pg_idx, tail_slot)
                        frame_id = self.table.bufferpool.get_page_frame_num(pr_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx)
                        cache_frames.append(frame_id)

                    else: # Base record has most recent value (after merge)
                        column_data = self.table.bufferpool.read_page_slot(pr_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx, slot_pos)
                        frame_id = self.table.bufferpool.get_page_frame_num(pr_idx, col_num + NUM_HIDDEN_COLUMNS, page_idx)
                        cache_frames.append(frame_id)

                # Add to index
                self.add_to_index(col_num, column_data, record_id)

                # Mark frames as used
                for frame in cache_frames:
                    self.table.bufferpool.mark_frame_used(frame)

            return True
        else:
            return False
    
    def drop_index(self, col_num):
        """Remove index for a specific column"""
        if col_num >= self.table.num_columns:
            return False

        # Clear and remove index
        if self.indices[col_num] is not None:
            self.indices[col_num].clear()
            self.indices[col_num] = None
            return True
        else:
            return False
    
    def insert_in_all_indices(self, columns):
        """Insert record into all active indices and value mapper"""
        pk_val = columns[self.key + NUM_HIDDEN_COLUMNS]
        if self.indices[self.key].get(pk_val):
            return False
        
        # Add to value mapper
        self.key_value_dict[pk_val] = columns[NUM_HIDDEN_COLUMNS:]

        # Add to all existing indices
        for i in range(self.column_count):
            if self.indices[i] is not None:
                col_val = columns[i + NUM_HIDDEN_COLUMNS]
                record_id = columns[RID_COLUMN]
                self.add_to_index(i, col_val, record_id)

        return True
    
    def delete_from_all_indices(self, pk_val):
        """Remove record from all indices based on primary key"""
        if not self.indices[self.key].get(pk_val):
            return False
        
        # Remove from secondary indices
        for i in range(self.column_count):
            if self.indices[i] is not None and i != self.key:
                idx_key = self.key_value_dict[pk_val][i]
                record_id = list(self.indices[self.key][pk_val].keys())[0]

                del self.indices[i][idx_key][record_id]

                # Clean up empty entries
                if self.indices[i][idx_key] == {}:
                    del self.indices[i][idx_key]

        # Remove from value mapper
        del self.key_value_dict[pk_val]
        return True
    
    def update_all_indices(self, pk_val, columns):
        """Update indices and value mapper with new column values"""
        if not self.indices[self.key].get(pk_val):
            return False
        
        # Update affected columns
        for i in range(0, self.column_count):
            if columns[NUM_HIDDEN_COLUMNS + i] is not None:
                if self.indices[i] is not None:
                    old_val = self.key_value_dict[pk_val][i]
                    record_id = list(self.indices[self.key][pk_val].keys())[0]
                    new_val = columns[i + NUM_HIDDEN_COLUMNS]

                    # Handle updates in indexed column
                    if self.indices[i].get(new_val):
                        self.add_to_index(i, new_val, record_id)
                        del self.indices[i][old_val][record_id]
                    else:
                        self.add_to_index(i, new_val, record_id)
                        del self.indices[i][old_val][record_id]
                        
                        # Clean up empty entries
                        if self.indices[i][old_val] == {}:
                            del self.indices[i][old_val]

                # Update value mapper regardless of index existence
                self.key_value_dict[pk_val][i] = columns[i + NUM_HIDDEN_COLUMNS]

        return True
    
    def grab_all(self):
        """Retrieve all record IDs from primary index"""
        record_ids = []
        for _, rid_dict in self.indices[self.key].items():
            for rid in rid_dict:
                record_ids.append(rid)
        return record_ids
    
    def get(self, key_col_num, val_col_num, key_val):
        """Retrieve column values using key-based lookup"""
        return self.__lookup_in_value_map(key_col_num, val_col_num, key_val, key_val)

    def get_range(self, key_col_num, val_col_num, start_val, end_val):
        """Retrieve column values for records with keys in a specified range"""
        return self.__lookup_in_value_map(key_col_num, val_col_num, start_val, end_val)

    def __lookup_in_value_map(self, key_col_num, val_col_num, start_val, end_val):
        """Helper method to extract values from value mapper within a range"""
        result_values = []
        
        for _, column_data in self.key_value_dict.items():
            # Extract key and target values from value mapper
            map_key = column_data[key_col_num]
            map_val = column_data[val_col_num]

            if start_val <= map_key <= end_val:
                result_values.append(map_val)
            
        return result_values if result_values else None
    
    def serialize(self):
        """Serialize index data to base64-encoded pickled representation"""
        serialized_data = {}

        # Serialize each non-empty index
        for i in range(self.column_count):
            if self.indices[i] is not None:
                pickled_data = pickle.dumps(self.indices[i])
                encoded_data = base64.b64encode(pickled_data).decode('utf-8')
                serialized_data[f"index[{i}]"] = encoded_data
        
        # Serialize value mapper
        pickled_mapper = pickle.dumps(self.key_value_dict)
        encoded_mapper = base64.b64encode(pickled_mapper).decode('utf-8')
        serialized_data["value_mapper"] = encoded_mapper

        return serialized_data
    
    def deserialize(self, data):
        """Restore index data from serialized representation"""
        # Restore value mapper
        if "value_mapper" in data:
            decoded_mapper = base64.b64decode(data["value_mapper"])
            self.key_value_dict = pickle.loads(decoded_mapper)

        # Restore individual indices
        for i in range(self.column_count):
            index_key = f"index[{i}]"
            if index_key in data:
                decoded_index = base64.b64decode(data[index_key])
                self.indices[i] = pickle.loads(decoded_index)
