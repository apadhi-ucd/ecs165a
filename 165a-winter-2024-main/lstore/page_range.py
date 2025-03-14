import threading
from lstore.config import *
from lstore.bufferpool import BufferPool
import json
from typing import Type
import queue

class MergeRequest:
    '''Stores metadata about tail pages scheduled for merging operations'''
    def __init__(self, page_range_index, turn_off=False):
        self.turn_off = turn_off
        self.page_range_index = page_range_index

class PageRange:
    '''
    PageRange maintains column data for records
    Base page indirection column points to corresponding tail record via logical_rid
    '''

    def __init__(self, page_range_index, num_columns, bufferpool:BufferPool):
        self.bufferpool = bufferpool
        self.page_range_index = page_range_index
        self.page_range_lock = threading.Lock()
        
        # Mapping between logical rids and physical storage locations
        self.logical_directory = {}
        
        # Counter for assigning logical rids to record updates
        self.logical_rid_index = MAX_RECORD_PER_PAGE_RANGE
        
        # Total columns including metadata columns
        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS
        
        # Current tail page index for each column
        self.tail_page_index = [MAX_PAGE_RANGE] * self.total_num_columns
        
        # Tail page sequence counter
        self.tps = 0
        
        # Queue for recycling logical rids
        self.allocation_logical_rid_queue = queue.Queue()

    def __normalize_rid(self, rid) -> int:
        '''Normalizes a record ID to fit within page range boundaries'''
        return rid % MAX_RECORD_PER_PAGE_RANGE

    def __get_hidden_column_location(self, logical_rid) -> tuple[int, int]:
        '''Calculates physical location for metadata columns given a logical rid'''
        pg_idx = logical_rid // MAX_RECORD_PER_PAGE
        pg_pos = logical_rid % MAX_RECORD_PER_PAGE
        return pg_idx, pg_pos
    
    def __get_known_column_location(self, logical_rid, column) -> tuple[int, int]:
        '''Retrieves physical location for data columns from logical directory'''
        phys_location = self.logical_directory[logical_rid][column - NUM_HIDDEN_COLUMNS]
        pg_idx = phys_location // MAX_RECORD_PER_PAGE
        pg_pos = phys_location % MAX_RECORD_PER_PAGE
        return pg_idx, pg_pos
    
    # API methods
    
    def get_column_location(self, logical_rid, column) -> tuple[int, int]:
        '''Determines physical storage location for a column based on logical rid'''
        if (column < NUM_HIDDEN_COLUMNS):
            return self.__get_hidden_column_location(logical_rid)
        else:
            return self.__get_known_column_location(logical_rid, column)
    
    def has_capacity(self, rid) -> bool:
        '''Checks if base pages can accommodate the given record ID'''
        return rid < (self.page_range_index * MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE) 

    def assign_logical_rid(self) -> int:
        '''Allocates a logical rid either from recycled queue or by incrementing counter'''
        if not self.allocation_logical_rid_queue.empty():
            return self.allocation_logical_rid_queue.get()
        else:
            self.logical_rid_index += 1
            return self.logical_rid_index - 1
    
    def write_base_record(self, page_index, page_slot, columns) -> bool:
        '''Stores a new record in base pages and updates sequence counter'''
        columns[INDIRECTION_COLUMN] = self.__normalize_rid(columns[RID_COLUMN])
        for (col_idx, col_val) in enumerate(columns):
            self.bufferpool.write_page_slot(self.page_range_index, col_idx, page_index, page_slot, col_val)
        with self.page_range_lock:
            self.tps += 1
        return True
    
    def write_tail_record(self, logical_rid, *columns) -> bool:
        '''Appends update record to tail pages and updates directory mapping'''

        self.logical_directory[logical_rid] = [None] * (self.total_num_columns - NUM_HIDDEN_COLUMNS)

        for (col_idx, col_val) in enumerate(columns):
            if (col_val is None):
                continue
            
            # Check if current tail page has space available
            space_available = self.bufferpool.get_page_has_capacity(self.page_range_index, col_idx, self.tail_page_index[col_idx])

            if not space_available:
                self.tail_page_index[col_idx] += 1

            elif space_available is None:
                return False
                
            slot_position = self.bufferpool.write_page_next(self.page_range_index, col_idx, self.tail_page_index[col_idx], col_val)

            # Only map non-metadata columns in the directory
            if (col_idx >= NUM_HIDDEN_COLUMNS):
                self.logical_directory[logical_rid][col_idx - NUM_HIDDEN_COLUMNS] = (self.tail_page_index[col_idx] * MAX_RECORD_PER_PAGE) + slot_position

        with self.page_range_lock:
            self.tps += 1
        return True
    
    def read_tail_record_column(self, logical_rid, column) -> int:
        '''Retrieves column value from tail pages and marks buffer frame as accessed'''
        pg_idx, pg_pos = self.get_column_location(logical_rid, column)
        value = self.bufferpool.read_page_slot(self.page_range_index, column, pg_idx, pg_pos)
        frame_id = self.bufferpool.get_page_frame_num(self.page_range_index, column, pg_idx)
        self.bufferpool.mark_frame_used(frame_id)
        return value
    
    def copy_base_record(self, page_index, page_slot) -> list:
        '''Retrieves complete record data from base pages for merge operations'''
        record_data = [None] * self.total_num_columns
        
        # Fetch all column values
        for col_idx in range(self.total_num_columns):
            record_data[col_idx] = self.bufferpool.read_page_slot(self.page_range_index, col_idx, page_index, page_slot)

        # Update frame access metadata
        for col_idx in range(self.total_num_columns):
            frame_id = self.bufferpool.get_page_frame_num(self.page_range_index, col_idx, page_index)
            self.bufferpool.mark_frame_used(frame_id)

        return record_data
    
    def find_records_last_logical_rid(self, logical_rid):
        '''Traverses update chain to find most recent version of a record'''
        latest_rid = logical_rid
        while (logical_rid >= MAX_RECORD_PER_PAGE_RANGE):
            latest_rid = logical_rid
            pg_idx, pg_pos = self.get_column_location(logical_rid, INDIRECTION_COLUMN)
            logical_rid = self.bufferpool.read_page_slot(self.page_range_index, INDIRECTION_COLUMN, pg_idx, pg_pos)
            frame_id = self.bufferpool.get_page_frame_num(self.page_range_index, INDIRECTION_COLUMN, pg_idx)
            if (frame_id):
                self.bufferpool.mark_frame_used(frame_id)

        return latest_rid

    # Serialization methods
    
    def serialize(self):
        '''Converts page range state to serializable dictionary format'''
        return {
            "logical_directory": self.logical_directory,
            "tail_page_index": self.tail_page_index,
            "logical_rid_index": self.logical_rid_index,
            "tps": self.tps
        }
    
    def deserialize(self, json_data):
        '''Reconstructs page range state from serialized data'''
        self.logical_directory = {int(k): v for k, v in json_data["logical_directory"].items()}
        self.tail_page_index = json_data["tail_page_index"]
        self.logical_rid_index = json_data["logical_rid_index"]
        self.tps = json_data["tps"]

    # Magic methods
    
    def __hash__(self):
        return self.page_range_index
    
    def __eq__(self, other:Type['PageRange']):
        return self.page_range_index == other.page_range_index
    
    def __str__(self):
        return json.dumps(self.serialize())

    def __repr__(self):
        return json.dumps(self.serialize())
    
    
