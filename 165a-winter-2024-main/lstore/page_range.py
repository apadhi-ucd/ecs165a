import threading
from lstore.config import *
from lstore.bufferpool import BufferPool
import json
from typing import Type
import queue

class MergeRequest:
    '''Holds information about tail pages scheduled for merging'''
    def __init__(self, range_idx, terminate=False):
        self.terminate = terminate
        self.range_idx = range_idx

class PageRange:
    '''
    Manages a range of pages containing all columns of records.
    Base page indirection columns contain logical record IDs pointing to corresponding tail records.
    '''

    def __init__(self, range_idx, col_count, bufferpool:BufferPool):
        self.bufferpool = bufferpool
        self.record_locations = {}
        '''Maps logical record IDs to physical locations for each user column'''
        self.next_logical_id = MAX_RECORD_PER_PAGE_RANGE
        '''Counter for assigning logical IDs to updates'''

        self.total_columns = col_count + NUM_HIDDEN_COLUMNS
        '''Total column count including hidden system columns'''

        self.tail_page_counters = [MAX_PAGE_RANGE] * self.total_columns
        '''Current tail page index for each column'''

        self.update_counter = 0
        '''Tail page sequence count'''

        self.access_lock = threading.Lock()
        self.range_idx = range_idx

        '''Queue for recycling logical record IDs'''
        self.logical_id_queue = queue.Queue()

    def write_base_record(self, page_idx, slot_idx, column_values) -> bool:
        '''Write a complete base record across all column pages'''
        column_values[INDIRECTION_COLUMN] = self.__normalize_id(column_values[RID_COLUMN])
        for (col_idx, value) in enumerate(column_values):
            self.bufferpool.write_page_slot(self.range_idx, col_idx, page_idx, slot_idx, value)
        with self.access_lock:
            self.update_counter += 1
        return True
    
    def copy_base_record(self, page_idx, slot_idx) -> list:
        '''Retrieve all column values for a base record - mainly used during merge operations'''
        record_data = [None] * self.total_columns
        
        # Read all column values from buffer pool
        for col_idx in range(self.total_columns):
            record_data[col_idx] = self.bufferpool.read_page_slot(self.range_idx, col_idx, page_idx, slot_idx)

        # Mark frames as recently used
        for col_idx in range(self.total_columns):
            frame_idx = self.bufferpool.get_page_frame_num(self.range_idx, col_idx, page_idx)
            self.bufferpool.mark_frame_used(frame_idx)

        return record_data
    
    def find_records_last_logical_id(self, logical_id):
        '''Follow indirection pointers to find the most recent logical ID for a record chain'''
        latest_id = logical_id
        while (logical_id >= MAX_RECORD_PER_PAGE_RANGE):
            latest_id = logical_id
            page_idx, slot_pos = self.get_column_location(logical_id, INDIRECTION_COLUMN)
            logical_id = self.bufferpool.read_page_slot(self.range_idx, INDIRECTION_COLUMN, page_idx, slot_pos)
            frame_idx = self.bufferpool.get_page_frame_num(self.range_idx, INDIRECTION_COLUMN, page_idx)
            if (frame_idx):
                self.bufferpool.mark_frame_used(frame_idx)

        return latest_id

    def write_tail_record(self, logical_id, *column_values) -> bool:
        '''Store updated column values in tail pages and return success status'''

        self.record_locations[logical_id] = [None] * (self.total_columns - NUM_HIDDEN_COLUMNS)

        for (col_idx, value) in enumerate(column_values):
            if (value is None):
                continue
            
            # Check if current tail page has room or move to next page
            has_space = self.bufferpool.get_page_has_capacity(self.range_idx, col_idx, self.tail_page_counters[col_idx])

            if not has_space:
                self.tail_page_counters[col_idx] += 1
            elif has_space is None:
                return False
                
            slot_pos = self.bufferpool.write_page_next(self.range_idx, col_idx, self.tail_page_counters[col_idx], value)

            # Only map user columns (skip hidden system columns)
            if (col_idx >= NUM_HIDDEN_COLUMNS):
                physical_pos = (self.tail_page_counters[col_idx] * MAX_RECORD_PER_PAGE) + slot_pos
                self.record_locations[logical_id][col_idx - NUM_HIDDEN_COLUMNS] = physical_pos

        with self.access_lock:
            self.update_counter += 1
        return True
    
    def read_tail_record_column(self, logical_id, col_idx) -> int:
        '''Retrieve a specific column value from tail records given a logical ID'''
        page_idx, slot_pos = self.get_column_location(logical_id, col_idx)
        value = self.bufferpool.read_page_slot(self.range_idx, col_idx, page_idx, slot_pos)
        frame_idx = self.bufferpool.get_page_frame_num(self.range_idx, col_idx, page_idx)
        self.bufferpool.mark_frame_used(frame_idx)
        return value
    
    def get_column_location(self, logical_id, col_idx) -> tuple[int, int]:
        '''Find the physical location of a column value for a given logical ID'''
        if (col_idx < NUM_HIDDEN_COLUMNS):
            return self.__locate_system_column(logical_id)
        else:
            return self.__locate_user_column(logical_id, col_idx)
    
    def __locate_system_column(self, logical_id) -> tuple[int, int]:
        '''Calculate location for system (hidden) columns which use fixed layout'''
        page_idx = logical_id // MAX_RECORD_PER_PAGE
        slot_pos = logical_id % MAX_RECORD_PER_PAGE
        return page_idx, slot_pos
    
    def __locate_user_column(self, logical_id, col_idx) -> tuple[int, int]:
        '''Look up location for user-defined columns using the location directory'''
        physical_pos = self.record_locations[logical_id][col_idx - NUM_HIDDEN_COLUMNS]
        page_idx = physical_pos // MAX_RECORD_PER_PAGE
        slot_pos = physical_pos % MAX_RECORD_PER_PAGE
        return page_idx, slot_pos
    
    def __normalize_id(self, record_id) -> int:
        '''Calculate a page-range relative ID from a global record ID'''
        return record_id % MAX_RECORD_PER_PAGE_RANGE

    def has_capacity(self, record_id) -> bool:
        '''Check if this page range can accommodate a record with the given ID'''
        return record_id < (self.range_idx * MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE) 

    def assign_logical_id(self) -> int:
        '''Allocate a logical ID for a new tail record, reusing IDs when possible'''
        if not self.logical_id_queue.empty():
            return self.logical_id_queue.get()
        else:
            self.next_logical_id += 1
            return self.next_logical_id - 1
    
    def serialize(self):
        '''Convert page range metadata to JSON-compatible dictionary format'''
        return {
            "logical_directory": self.record_locations,
            "tail_page_index": self.tail_page_counters,
            "logical_rid_index": self.next_logical_id,
            "tps": self.update_counter
        }
    
    def deserialize(self, stored_data):
        '''Restore page range state from serialized data'''
        self.record_locations = {int(k): v for k, v in stored_data["logical_directory"].items()}
        self.tail_page_counters = stored_data["tail_page_index"]
        self.next_logical_id = stored_data["logical_rid_index"]
        self.update_counter = stored_data["tps"]

    def __hash__(self):
        return self.range_idx
    
    def __eq__(self, other:Type['PageRange']):
        return self.range_idx == other.range_idx
    
    def __str__(self):
        return json.dumps(self.serialize())

    def __repr__(self):
        return json.dumps(self.serialize())
