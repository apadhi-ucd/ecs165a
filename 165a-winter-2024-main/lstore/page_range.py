import threading
from lstore.config import *
from lstore.page import Page
from lstore.bufferpool import BufferPool
import json
from typing import Type
import queue

class MergeRequest:
    '''Contains information about the tail pages to be merged'''
    def __init__(self, page_range_index, turn_off=False):
        self.turn_off = turn_off
        self.page_range_index = page_range_index

class PageRange:
    '''
    Each PageRange contains all columns of a record
    Indirection column of a base page would contain the logical_rid of its corresponding tail record
    '''

    def __init__(self, page_range_index, num_columns, bufferpool:BufferPool):
        self.bufferpool = bufferpool
        self.logical_directory = {}
        '''Maps logical rid's to physical locations in page for each column (except hidden columns)'''
        self.logical_rid_index = MAX_RECORD_PER_PAGE_RANGE
        '''Used to assign logical rids to updates'''

        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS
        '''Total number of columns in a record'''

        self.tail_page_index = [MAX_PAGE_RANGE] * self.total_num_columns
        '''Tail page index for each column'''

        self.tps = 0
        '''Tail page sequence number'''

        self.page_range_lock = threading.Lock()

        self.page_range_index = page_range_index

        '''setup queue for logical rid allocation'''
        self.allocation_logical_rid_queue = queue.Queue()

        # Initialize base pages for backward compatibility
        self.base_pages = []
        self.tail_pages = []
        self.num_base_records = 0
        self.num_tail_records = 0
        
        for _ in range(num_columns + METADATA_COLUMNS):
            self.base_pages.append([Page()])
            self.tail_pages.append([Page()])

    def write_base_record(self, page_index, page_slot, columns) -> bool:
        columns[INDIRECTION_COLUMN] = self.__normalize_rid(columns[RID_COLUMN])
        for (i, column) in enumerate(columns):
            self.bufferpool.write_page_slot(self.page_range_index, i, page_index, page_slot, column)
        with self.page_range_lock:
            self.tps += 1
        return True
    
    def copy_base_record(self, page_index, page_slot) -> list:
        '''Costly function mostly used by table merge on the background'''
        base_record_columns = [None] * self.total_num_columns
        # Read buffer pool frames
        for i in range(self.total_num_columns):
            base_record_columns[i] = self.bufferpool.read_page_slot(self.page_range_index, i, page_index, page_slot)

        # mark the frame used after reading
        for i in range(self.total_num_columns):
            frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, i, page_index)
            if frame_num is not None:
                self.bufferpool.mark_frame_used(frame_num)

        return base_record_columns
    
    def find_records_last_logical_rid(self, logical_rid):
        '''Merge Helper API Call: Returns the last logical rid of a record given a starting logical rid'''
        last_logical_rid = logical_rid
        while (logical_rid >= MAX_RECORD_PER_PAGE_RANGE):
            last_logical_rid = logical_rid
            page_index, page_slot = self.get_column_location(logical_rid, INDIRECTION_COLUMN)
            logical_rid = self.bufferpool.read_page_slot(self.page_range_index, INDIRECTION_COLUMN, page_index, page_slot)
            frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, INDIRECTION_COLUMN, page_index)
            if (frame_num):
                self.bufferpool.mark_frame_used(frame_num)

        return last_logical_rid

    def write_tail_record(self, logical_rid, *columns) -> bool:
        '''Writes a set of columns to the tail pages returns true on success'''

        self.logical_directory[logical_rid] = [None] * (self.total_num_columns - NUM_HIDDEN_COLUMNS)

        for (i, column) in enumerate(columns):
            if (column is None):
                continue
            
            # If current tail page doesn't have capacity move to the next tail page
            has_capacity = self.bufferpool.get_page_has_capacity(self.page_range_index, i, self.tail_page_index[i])

            if not has_capacity:
                self.tail_page_index[i] += 1

            elif has_capacity is None:
                return False
                
            page_slot = self.bufferpool.write_page_next(self.page_range_index, i, self.tail_page_index[i], column)

            # we can skip mapping hidden columns since they are partitioned equally
            if (i >= NUM_HIDDEN_COLUMNS):
                self.logical_directory[logical_rid][i - NUM_HIDDEN_COLUMNS] = (self.tail_page_index[i] * MAX_RECORD_PER_PAGE) + page_slot

        with self.page_range_lock:
            self.tps += 1
        return True
    
    def read_tail_record_column(self, logical_rid, column) -> int:
        '''Reads a column from the tail pages given a logical rid'''
        page_index, page_slot = self.get_column_location(logical_rid, column)
        column_value = self.bufferpool.read_page_slot(self.page_range_index, column, page_index, page_slot)
        frame_num = self.bufferpool.get_page_frame_num(self.page_range_index, column, page_index)
        if frame_num is not None:
            self.bufferpool.mark_frame_used(frame_num)
        return column_value
    
    # Only use this function for API calls
    def get_column_location(self, logical_rid, column) -> tuple[int, int]:
        '''Returns the location of a column within tail pages given a logical rid'''
        if (column < NUM_HIDDEN_COLUMNS):
            return self.__get_hidden_column_location(logical_rid)
        else:
            return self.__get_known_column_location(logical_rid, column)
    
    def __get_hidden_column_location(self, logical_rid) -> tuple[int, int]:
        '''Returns the location of the hidden columns for a logical rid'''
        page_index = logical_rid // MAX_RECORD_PER_PAGE
        page_slot = logical_rid % MAX_RECORD_PER_PAGE
        return page_index, page_slot
    
    def __get_known_column_location(self, logical_rid, column) -> tuple[int, int]:
        '''Returns the location of a column within tail pages given a logical rid'''
        physical_rid = self.logical_directory[logical_rid][column - NUM_HIDDEN_COLUMNS]
        page_index = physical_rid // MAX_RECORD_PER_PAGE
        page_slot = physical_rid % MAX_RECORD_PER_PAGE
        return page_index, page_slot
    
    def __normalize_rid(self, rid) -> int:
        '''Returns the normalized rid for a given rid'''
        return rid % MAX_RECORD_PER_PAGE_RANGE

    def has_capacity(self, rid) -> bool:
        '''returns true if there is capacity in the base pages for the given rid '''
        return rid < (self.page_range_index * MAX_PAGE_RANGE * MAX_RECORD_PER_PAGE) 

    def assign_logical_rid(self) -> int:
        '''returns logical rid to be assigned to a column'''
        if not self.allocation_logical_rid_queue.empty():
            return self.allocation_logical_rid_queue.get()
        else:
            self.logical_rid_index += 1
            return self.logical_rid_index - 1
    
    # For backward compatibility with existing code
    def has_page_capacity(self, is_base):
        """Returns true if the page range can store another record"""
        pages = self.base_pages if is_base else self.tail_pages
        return pages[0][-1].has_capacity()
    
    def add_page(self, is_base):
        """Adds a new page for each column when current ones are full"""
        pages = self.base_pages if is_base else self.tail_pages
        for column in pages:
            column.append(Page())
    
    def write_record(self, record, is_base):
        """Write a record to the page"""
        if len(record) != self.total_num_columns:
            print("Unexpected value in write_record")
            return
        
        # Write record to page
        pages = self.base_pages if is_base else self.tail_pages
        if not self.has_page_capacity(is_base):
            self.add_page(is_base)
            
        page_index = len(pages[0]) - 1
        slot = pages[0][page_index].num_records
        
        # Write metadata
        for i, value in enumerate(record):
            pages[i][page_index].write(value)
            
        # Update metadata
        if is_base:
            self.num_base_records += 1
        else:
            self.num_tail_records += 1
        return (page_index, slot)
    
    def read_record(self, page_index, slot, projected_columns_index, is_base):
        """Read a record from the page"""
        pages = self.base_pages if is_base else self.tail_pages
        # Check if the index is valid
        if page_index >= len(pages[0]) or slot >= pages[0][page_index].num_records:
            print("Invalid index")
            return None
            
        record = []

        # Read metadata
        for i in range(METADATA_COLUMNS):
            record.append(pages[i][page_index].read(slot))
            
        # Read data
        for i in range(self.total_num_columns - METADATA_COLUMNS):
            if projected_columns_index[i]:
                value = pages[i + METADATA_COLUMNS][page_index].read(slot)
                record.append(value)
            else:
                record.append(None)
                
        return record
    
    def update_record_column(self, page_index, slot, column, value, is_base):
        """Update a record in the page"""
        pages = self.base_pages if is_base else self.tail_pages
        # Check if the index is valid
        if page_index >= len(pages[0]) or slot >= pages[0][page_index].num_records:
            print("Invalid index")
            return
        
        # Update metadata
        if column < 0 or column >= self.total_num_columns:
            print("Invalid column")
            return
            
        # Update data
        page = pages[column][page_index]
        page.write_precise(slot, value)
    
    def serialize(self):
        '''Returns page metadata as a JSON-compatible dictionary'''
        return {
            "logical_directory": self.logical_directory,
            "tail_page_index": self.tail_page_index,
            "logical_rid_index": self.logical_rid_index,
            "tps": self.tps,
            "num_base_records": self.num_base_records,
            "num_tail_records": self.num_tail_records
        }
    
    def deserialize(self, json_data):
        '''Loads a page from serialized data'''
        self.logical_directory = {int(k): v for k, v in json_data.get("logical_directory", {}).items()}
        self.tail_page_index = json_data.get("tail_page_index", [MAX_PAGE_RANGE] * self.total_num_columns)
        self.logical_rid_index = json_data.get("logical_rid_index", MAX_RECORD_PER_PAGE_RANGE)
        self.tps = json_data.get("tps", 0)
        self.num_base_records = json_data.get("num_base_records", 0)
        self.num_tail_records = json_data.get("num_tail_records", 0)

    def __hash__(self):
        return self.page_range_index
    
    def __eq__(self, other:Type['PageRange']):
        return self.page_range_index == other.page_range_index
    
    def __str__(self):
        return json.dumps(self.serialize())

    def __repr__(self):
        return json.dumps(self.serialize())

    
