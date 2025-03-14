import threading
from lstore.config import *
from lstore.bufferpool import BufferPool
import json
from typing import Type
import queue

class MergeRequest:
    def __init__(self, page_range_idx, turn_off=False):
        self.turn_off = turn_off
        self.page_range_idx = page_range_idx

class PageRange:
    def __init__(self, range_idx, col_count, buffer_pool:BufferPool):
        self.bufferpool = buffer_pool
        self.logical_directory = {}
        self.logical_rid_index = MAX_RECORD_PER_PAGE_RANGE
        self.total_num_columns = col_count + NUM_HIDDEN_COLUMNS
        self.tail_page_index = [MAX_PAGE_RANGE] * self.total_num_columns
        self.tps = 0
        self.page_range_lock = threading.Lock()
        self.page_range_index = range_idx
        self.allocation_logical_rid_queue = queue.Queue()

    def write_base_record(self, pg_idx, slot_idx, col_values) -> bool:
        col_values[INDIRECTION_COLUMN] = self.__normalize_rid(col_values[RID_COLUMN])
        for (idx, value) in enumerate(col_values):
            self.bufferpool.write_page_slot(self.page_range_index, idx, pg_idx, slot_idx, value)
        with self.page_range_lock:
            self.tps += 1
        return True
    
    def copy_base_record(self, pg_idx, slot_idx) -> list:
        base_columns = [None] * self.total_num_columns
        for idx in range(self.total_num_columns):
            base_columns[idx] = self.bufferpool.read_page_slot(self.page_range_index, idx, pg_idx, slot_idx)

        for idx in range(self.total_num_columns):
            frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, idx, pg_idx)
            self.bufferpool.mark_frame_used(frame_num)

        return base_columns
    
    def find_records_last_logical_rid(self, logical_id):
        last_record_id = logical_id
        while (logical_id >= MAX_RECORD_PER_PAGE_RANGE):
            last_record_id = logical_id
            page_index, slot_index = self.get_column_location(logical_id, INDIRECTION_COLUMN)
            logical_id = self.bufferpool.read_page_slot(self.page_range_index, INDIRECTION_COLUMN, page_index, slot_index)
            frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, INDIRECTION_COLUMN, page_index)
            if (frame_num):
                self.bufferpool.mark_frame_used(frame_num)

        return last_record_id

    def write_tail_record(self, logical_id, *col_values) -> bool:
        self.logical_directory[logical_id] = [None] * (self.total_num_columns - NUM_HIDDEN_COLUMNS)

        for (idx, value) in enumerate(col_values):
            if (value is None):
                continue
            
            if (idx < NUM_HIDDEN_COLUMNS):
                page_index, slot_index = self.__get_hidden_column_location(logical_id)
                self.bufferpool.write_page_slot(self.page_range_index, idx, page_index, slot_index, value)
            else:
                with self.page_range_lock:
                    space_available = self.bufferpool.get_page_has_capacity(self.page_range_index, idx, self.tail_page_index[idx])

                    if not space_available:
                        self.tail_page_index[idx] += 1
                    elif space_available is None:
                        return False
                        
                    position = self.bufferpool.write_page_next(self.page_range_index, idx, self.tail_page_index[idx], value)
                    
                    self.logical_directory[logical_id][idx - NUM_HIDDEN_COLUMNS] = (self.tail_page_index[idx] * MAX_RECORD_PER_PAGE) + position

        with self.page_range_lock:
            self.tps += 1
        return True
    
    def read_tail_record_column(self, logical_id, col_idx) -> int:
        page_index, slot_index = self.get_column_location(logical_id, col_idx)
        value = self.bufferpool.read_page_slot(self.page_range_index, col_idx, page_index, slot_index)
        frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, col_idx, page_index)
        self.bufferpool.mark_frame_used(frame_num)
        return value
    
    def get_column_location(self, logical_id, col_idx) -> tuple[int, int]:
        if (col_idx < NUM_HIDDEN_COLUMNS):
            return self.__get_hidden_column_location(logical_id)
        else:
            return self.__get_known_column_location(logical_id, col_idx)
    
    def __get_hidden_column_location(self, logical_id) -> tuple[int, int]:
        page_index = logical_id // MAX_RECORD_PER_PAGE
        slot_index = logical_id % MAX_RECORD_PER_PAGE
        return page_index, slot_index
    
    def __get_known_column_location(self, logical_id, col_idx) -> tuple[int, int]:
        physical_id = self.logical_directory[logical_id][col_idx - NUM_HIDDEN_COLUMNS]
        page_index = physical_id // MAX_RECORD_PER_PAGE
        slot_index = physical_id % MAX_RECORD_PER_PAGE
        return page_index, slot_index
    
    def __normalize_rid(self, rid) -> int:
        return rid % MAX_RECORD_PER_PAGE_RANGE

    def has_capacity(self, rid) -> bool:
        return rid < (self.page_range_index * MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE) 

    def assign_logical_rid(self) -> int:
        if not self.allocation_logical_rid_queue.empty():
            return self.allocation_logical_rid_queue.get()
        else:
            self.logical_rid_index += 1
            return self.logical_rid_index - 1
    
    def serialize(self):
        return {
            "logical_directory": self.logical_directory,
            "tail_page_index": self.tail_page_index,
            "logical_rid_index": self.logical_rid_index,
            "tps": self.tps
        }
    
    def deserialize(self, data):
        self.logical_directory = {int(k): v for k, v in data["logical_directory"].items()}
        self.tail_page_index = data["tail_page_index"]
        self.logical_rid_index = data["logical_rid_index"]
        self.tps = data["tps"]

    def __hash__(self):
        return self.page_range_index
    
    def __eq__(self, other:Type['PageRange']):
        return self.page_range_index == other.page_range_index
    
    def __str__(self):
        return json.dumps(self.serialize())

    def __repr__(self):
        return json.dumps(self.serialize())
    
    