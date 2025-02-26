from lstore.index import Index
from lstore.page_range import PageRange, MergeRequest
from time import time
from lstore.config import *
from lstore.bufferpool import BufferPool
import json
import os
import threading
import queue
from typing import List

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        '''Each record contains both the hidden columns and the given columns [...HIDDEN_COLUMNS, ...GIVEN_COLUMNS]'''

    def __str__(self):
        return f"RID: {self.rid} Key: {self.key} \nColumns: {self.columns}"

class Table:

    """
    Represents a database table with data stored across page ranges
    
    :param name: string         # Table name
    :param num_columns: int     # Number of user-visible columns
    :param key: int             # Index of primary key column
    :db_path: string            # Path to database storage directory
    """
    def __init__(self, name, num_columns, key, db_path):
        if (key < 0 or key >= num_columns):
            raise ValueError("Error Creating Table! Primary Key must be within the columns of the table")

        self.name = name
        self.key = key
        self.db_path = db_path
        self.table_path = os.path.join(db_path, name)
        self.num_columns = num_columns
        self.total_num_columns = num_columns + NUM_HIDDEN_COLUMNS

        self.page_dir = {}
        '''
        Page direcotry will map rids to consolidated rids
        page_directory[rid] = consolidated_rid
        consolidated_rid is treated the same as a base_rid
        Table_merge should be the only function that modifies the page_directory
        All others can access the page_dierctory
        '''
        self.pd_lock = threading.Lock()


        # initialize bufferpool in table, not DB
        self.bufferpool = BufferPool(self.table_path, self.num_columns)
        self.page_ranges:List[PageRange] = []

        # setup queues for base rid allocation/deallocation
        self.dealloc_rid_queue = queue.Queue()
        self.alloc_rid_queue = queue.Queue()

        self.merge_queue = queue.Queue()
        '''stores the base rid of a record to be merged'''

        # The table should handle assigning RIDs
        self.rid_index = 0
        
        self.index = Index(self)
        # Start the merge thread
        # Note: This thread will stop running when the main program terminates
        self.merge_thread = threading.Thread(target=self.__merge, daemon=True)
        self.merge_thread.start()

        # start the deallocation thread
        self.dealloc_thread = threading.Thread(target=self.__delete_worker, daemon=True)
        self.dealloc_thread.start()

    def assign_rid_to_record(self, record: Record):
        '''Use this function to assign a record's RID'''
        with self.pd_lock:
            
            # recycle unused RIDs
            if not self.alloc_rid_queue.empty():
                record.rid = self.alloc_rid_queue.get()
            else:
                record.rid = self.rid_index
                self.rid_index += 1

    def get_base_record_location(self, rid) -> tuple[int, int, int]:
        '''Returns the location of a record within base pages given a rid'''
        pr_idx = rid // (MAX_RECORD_PER_PAGE_RANGE)
        page_idx = rid % (MAX_RECORD_PER_PAGE_RANGE) // MAX_RECORD_PER_PAGE
        slot = rid % MAX_RECORD_PER_PAGE
        return (pr_idx, page_idx, slot)

    def insert_record(self, record: Record):
        pr_idx, page_idx, slot = self.get_base_record_location(record.rid)

        if (pr_idx >= len(self.page_ranges)):
            self.page_ranges.append(PageRange(pr_idx, self.num_columns, self.bufferpool))
        
        curr_pr = self.page_ranges[pr_idx]

        with curr_pr.page_range_lock:
            record.columns[TIMESTAMP_COLUMN] = curr_pr.tps
        curr_pr.write_base_record(page_idx, slot, record.columns)   

    def update_record(self, rid, columns) -> bool:
        '''Updates a record given its RID'''
        pr_idx = rid // MAX_RECORD_PER_PAGE_RANGE
        curr_pr = self.page_ranges[pr_idx]

        with curr_pr.page_range_lock:
            columns[TIMESTAMP_COLUMN] = curr_pr.tps
            
        update_success = curr_pr.write_tail_record(columns[RID_COLUMN], *columns)

        if (curr_pr.tps % (MAX_TAIL_PAGES_BEFORE_MERGING * MAX_RECORD_PER_PAGE) == 0):
            self.merge_queue.put(MergeRequest(curr_pr.page_range_index)) 
            # if (self.merge_thread.is_alive() == False):
            #     self.merge_thread = threading.Thread(target=self.__merge)
            #     self.merge_thread.start()

        return update_success

    
    def __merge(self):
        # print("Merge is happening")

        while True:
            # Block ensures that we wait for a record to be added to the queue first
            # before we continue merging a record
            merge_req = self.merge_queue.get()

            # make a copy of the base page for the recieved rid
            start_rid = merge_req.page_range_index * MAX_RECORD_PER_PAGE_RANGE
            end_rid = min(start_rid + MAX_RECORD_PER_PAGE_RANGE, self.rid_index)

            curr_pr = self.page_ranges[merge_req.page_range_index]

            for rid in range(start_rid, end_rid):
                _, page_idx, slot = self.get_base_record_location(rid)

                base_cols = curr_pr.copy_base_record(page_idx, slot)
                base_merge_time = base_cols[UPDATE_TIMESTAMP_COLUMN]

                if (base_merge_time is None):
                    base_merge_time = 0
                    if (self.__insert_base_copy_to_tail_pages(curr_pr, base_cols) is False):
                        continue

                # Get the latest record
                curr_rid = base_cols[INDIRECTION_COLUMN]
                latest_schema = base_cols[SCHEMA_ENCODING_COLUMN]
                latest_ts = curr_pr.read_tail_record_column(curr_rid, TIMESTAMP_COLUMN)
                curr_ts = latest_ts

                # if current rid < MAX_RECORD_PER_PAGE_RANGE, then we are at the base record
                while curr_rid >= MAX_RECORD_PER_PAGE_RANGE and latest_schema != 0 and curr_ts > base_merge_time:
                    indir_col = curr_pr.read_tail_record_column(curr_rid, INDIRECTION_COLUMN)
                    schema = curr_pr.read_tail_record_column(curr_rid, SCHEMA_ENCODING_COLUMN)
                    curr_ts = curr_pr.read_tail_record_column(curr_rid, TIMESTAMP_COLUMN)
                    
                    for col_idx in range(self.num_columns):
                        if (latest_schema & (1 << col_idx)) and (schema & (1 << col_idx)):
                            latest_schema ^= (1 << col_idx)
                            base_cols[col_idx + NUM_HIDDEN_COLUMNS] = curr_pr.read_tail_record_column(curr_rid, col_idx + NUM_HIDDEN_COLUMNS)

                    curr_rid = indir_col
                
                base_cols[UPDATE_TIMESTAMP_COLUMN] = latest_ts

                base_cols[UPDATE_TIMESTAMP_COLUMN] = int(time())
                self.bufferpool.write_page_slot(merge_req.page_range_index, UPDATE_TIMESTAMP_COLUMN, page_idx, slot, base_cols[UPDATE_TIMESTAMP_COLUMN])

                # consolidate base page columns
                for i in range(self.num_columns):
                    self.bufferpool.write_page_slot(merge_req.page_range_index, NUM_HIDDEN_COLUMNS + i, page_idx, slot, base_cols[i + NUM_HIDDEN_COLUMNS])
            
            self.merge_queue.task_done()


    def __insert_base_copy_to_tail_pages(self, page_range:PageRange, base_cols):
        '''Inserts a copy of the base record to the last tail page of the record'''
        logical_rid = page_range.assign_logical_rid()
        indir_rid = base_cols[INDIRECTION_COLUMN]
        last_indir_rid = page_range.find_records_last_logical_rid(indir_rid)

        # if no tail record exist return false
        if (last_indir_rid == indir_rid):
            return False
        
        page_idx, slot = page_range.get_column_location(last_indir_rid, INDIRECTION_COLUMN)
        page_range.write_tail_record(logical_rid, *base_cols)

        # edit the last page's indirection column to point to the new copied base record
        self.bufferpool.write_page_slot(page_range.page_range_index, INDIRECTION_COLUMN, page_idx, slot, logical_rid)

        return True
    
    def grab_all_base_rids(self):
        '''Returns a list of all base rids'''
        return list(range(self.rid_index))
        
    def serialize(self):
        """Returns table metadata as a JSON-compatible dictionary"""
        return {
            "table_name": self.name,
            "num_columns": self.num_columns,
            "key_index": self.key,
            "page_directory": self.serialize_page_directory(),
            "rid_index": self.rid_index,
            "index": self.index.serialize(),
            "page_ranges": [pr.serialize() for pr in self.page_ranges]
        }
        
        
    def serialize_page_directory(self):
        """Serializes the Page Directory for JSON compatibility"""
        ser_dir = {}
        for rid, location in self.page_dir.items():
            # Location is (Page Range ID, Page Index, Slot Index)
            ser_dir[rid] = {
                "page_range_id": location[0],
                "page_index": location[1],
                "slot_index": location[2]
            }
        return ser_dir

    def deserialize(self, data):
        """Restores the Table state from a JSON-compatible dictionary"""
        # Restore basic table metadata
        self.name = data['table_name']
        self.num_columns = data['num_columns']
        self.key = data['key_index']
        self.rid_index = data['rid_index']
        
        # Recreate Page Directory
        self.page_dir = self.deserialize_page_directory(data['page_directory'])

        # Recreate Index
        self.index.deserialize(data['index'])

        for idx, pr_data in enumerate(data['page_ranges']):
        # Fix: Pass required arguments for PageRange
            page_range = PageRange(idx, self.num_columns, self.bufferpool)
            page_range.deserialize(pr_data)
            self.page_ranges.append(page_range)
            

    def deserialize_page_directory(self, serialized_directory):
        """Deserializes the Page Directory from JSON-compatible format"""
        deser_dir = {}

        for rid_str, location in serialized_directory.items():
            # Convert RID key from string to integer
            rid = int(rid_str)

            # Reconstruct the location tuple: (Page Range ID, Page Index, Slot Index)
            deser_dir[rid] = (
                int(location['page_range_id']),  # Convert to int
                int(location['page_index']),     # Convert to int
                int(location['slot_index'])      # Convert to int
            )

        return deser_dir

    def __delete_worker(self):
        '''
        1. Grabs a RID from `dealloc_rid_queue`. 
        2. Moves the base RID to `alloc_rid_queue` for reuse.
        3. Traverses all tail records and moves their logical RIDs to `allocation_logical_rid_queue` in the corresponding PageRange
        '''
        while True:
                # process base rid deletions (retrieve rid from base deallocation queue)
                rid = self.dealloc_rid_queue.get(block=True)

                # locate page range given rid
                pr_idx, page_idx, slot = self.get_base_record_location(rid)
                page_range = self.page_ranges[pr_idx]

                self.alloc_rid_queue.put(rid)

                logical_rid = page_range.bufferpool.read_page_slot(pr_idx, INDIRECTION_COLUMN, page_idx, slot)   

                # traverse 
                while logical_rid >= MAX_RECORD_PER_PAGE_RANGE:
                    page_range.allocation_logical_rid_queue.put(logical_rid)
                    log_page_idx, log_slot = page_range.get_column_location(logical_rid, INDIRECTION_COLUMN)
                    logical_rid = page_range.bufferpool.read_page_slot(pr_idx, INDIRECTION_COLUMN, log_page_idx, log_slot)
            
                self.dealloc_rid_queue.task_done()
