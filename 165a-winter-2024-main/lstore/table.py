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
        # Contains both hidden and visible columns

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
        # Maps RIDs to consolidated RIDs - modified only during merges
        self.pd_lock = threading.Lock()

        # Initialize buffer pool for this table
        self.bufferpool = BufferPool(self.table_path, self.num_columns)
        self.page_ranges: List[PageRange] = []

        # Queues for RID management
        self.dealloc_rid_queue = queue.Queue()
        self.alloc_rid_queue = queue.Queue()
        self.merge_queue = queue.Queue()  # Stores base RIDs ready for merging

        # RID counter for assigning unique IDs
        self.rid_index = 0
        
        # Initialize indexing
        self.index = Index(self)
        
        # Start background worker threads
        self.merge_thread = threading.Thread(target=self.__merge, daemon=True)
        self.merge_thread.start()
        
        self.dealloc_thread = threading.Thread(target=self.__delete_worker, daemon=True)
        self.dealloc_thread.start()

    # Record Management Methods
    
    def assign_rid_to_record(self, record: Record):
        """Assigns a unique or recycled RID to a record"""
        with self.pd_lock:
            # Recycle unused RIDs when available
            if not self.alloc_rid_queue.empty():
                record.rid = self.alloc_rid_queue.get()
            else:
                record.rid = self.rid_index
                self.rid_index += 1

    def get_base_record_location(self, rid) -> tuple[int, int, int]:
        """Converts RID to physical location coordinates"""
        pr_idx = rid // (MAX_RECORD_PER_PAGE_RANGE)
        page_idx = rid % (MAX_RECORD_PER_PAGE_RANGE) // MAX_RECORD_PER_PAGE
        slot = rid % MAX_RECORD_PER_PAGE
        return (pr_idx, page_idx, slot)

    def insert_record(self, record: Record):
        """Writes a new record to the appropriate base page"""
        pr_idx, page_idx, slot = self.get_base_record_location(record.rid)

        # Create new page range if needed
        if (pr_idx >= len(self.page_ranges)):
            self.page_ranges.append(PageRange(pr_idx, self.num_columns, self.bufferpool))
        
        curr_pr = self.page_ranges[pr_idx]

        # Set timestamp and write the record
        with curr_pr.page_range_lock:
            record.columns[TIMESTAMP_COLUMN] = curr_pr.tps
        curr_pr.write_base_record(page_idx, slot, record.columns)   

    def update_record(self, rid, columns) -> bool:
        """Updates a record and returns success status"""
        pr_idx = rid // MAX_RECORD_PER_PAGE_RANGE
        curr_pr = self.page_ranges[pr_idx]

        # Set timestamp for this update
        with curr_pr.page_range_lock:
            columns[TIMESTAMP_COLUMN] = curr_pr.tps
            
        success = curr_pr.write_tail_record(columns[RID_COLUMN], *columns)

        # Check if it's time to merge
        if (curr_pr.tps % (MAX_TAIL_PAGES_BEFORE_MERGING * MAX_RECORD_PER_PAGE) == 0):
            self.merge_queue.put(MergeRequest(curr_pr.page_range_index))

        return success

    # Background Processing Methods
    
    def __merge(self):
        """Background thread that merges tail records into base pages"""
        while True:
            # Wait for merge requests
            merge_req = self.merge_queue.get()

            # Process all records in the affected page range
            start_rid = merge_req.page_range_index * MAX_RECORD_PER_PAGE_RANGE
            end_rid = min(start_rid + MAX_RECORD_PER_PAGE_RANGE, self.rid_index)
            curr_pr = self.page_ranges[merge_req.page_range_index]

            for rid in range(start_rid, end_rid):
                _, page_idx, slot = self.get_base_record_location(rid)

                # Get base record data
                base_cols = curr_pr.copy_base_record(page_idx, slot)
                base_merge_time = base_cols[UPDATE_TIMESTAMP_COLUMN]

                # Handle records that haven't been merged before
                if base_merge_time is None:
                    base_merge_time = 0
                    if not self.__insert_base_copy_to_tail_pages(curr_pr, base_cols):
                        continue

                # Navigate update chain to build consolidated record
                curr_rid = base_cols[INDIRECTION_COLUMN]
                latest_schema = base_cols[SCHEMA_ENCODING_COLUMN]
                latest_ts = curr_pr.read_tail_record_column(curr_rid, TIMESTAMP_COLUMN)
                curr_ts = latest_ts

                # Process all updates newer than last merge
                while (curr_rid >= MAX_RECORD_PER_PAGE_RANGE and 
                       latest_schema != 0 and 
                       curr_ts > base_merge_time):
                    
                    indir_col = curr_pr.read_tail_record_column(curr_rid, INDIRECTION_COLUMN)
                    schema = curr_pr.read_tail_record_column(curr_rid, SCHEMA_ENCODING_COLUMN)
                    curr_ts = curr_pr.read_tail_record_column(curr_rid, TIMESTAMP_COLUMN)
                    
                    # Apply updates from each column if needed
                    for col_idx in range(self.num_columns):
                        if ((latest_schema & (1 << col_idx)) and 
                            (schema & (1 << col_idx))):
                            latest_schema ^= (1 << col_idx)
                            base_cols[col_idx + NUM_HIDDEN_COLUMNS] = curr_pr.read_tail_record_column(
                                curr_rid, col_idx + NUM_HIDDEN_COLUMNS)

                    curr_rid = indir_col
                
                # Update the merge timestamp
                base_cols[UPDATE_TIMESTAMP_COLUMN] = int(time())
                self.bufferpool.write_page_slot(
                    merge_req.page_range_index, 
                    UPDATE_TIMESTAMP_COLUMN, 
                    page_idx, 
                    slot, 
                    base_cols[UPDATE_TIMESTAMP_COLUMN]
                )

                # Write consolidated values to base page
                for i in range(self.num_columns):
                    self.bufferpool.write_page_slot(
                        merge_req.page_range_index, 
                        NUM_HIDDEN_COLUMNS + i, 
                        page_idx, 
                        slot, 
                        base_cols[i + NUM_HIDDEN_COLUMNS]
                    )
            
            self.merge_queue.task_done()

    def __insert_base_copy_to_tail_pages(self, page_range: PageRange, base_cols):
        """Creates a tail record copy of base record for merge tracking"""
        logical_rid = page_range.assign_logical_rid()
        indir_rid = base_cols[INDIRECTION_COLUMN]
        last_indir_rid = page_range.find_records_last_logical_rid(indir_rid)

        # Skip if no tail records exist yet
        if last_indir_rid == indir_rid:
            return False
        
        # Create copy and update pointers
        page_idx, slot = page_range.get_column_location(last_indir_rid, INDIRECTION_COLUMN)
        page_range.write_tail_record(logical_rid, *base_cols)

        # Update indirection in the previous tail record
        self.bufferpool.write_page_slot(
            page_range.page_range_index, 
            INDIRECTION_COLUMN, 
            page_idx, 
            slot, 
            logical_rid
        )

        return True
    
    def grab_all_base_rids(self):
        """Returns list of all allocated base RIDs"""
        return list(range(self.rid_index))
    
    def __delete_worker(self):
        """Handles RID recycling for deleted records"""
        while True:
            # Get a RID that needs to be deleted
            rid = self.dealloc_rid_queue.get(block=True)

            # Find record location and make RID available for reuse
            pr_idx, page_idx, slot = self.get_base_record_location(rid)
            page_range = self.page_ranges[pr_idx]
            self.alloc_rid_queue.put(rid)

            # Find all associated tail records
            logical_rid = page_range.bufferpool.read_page_slot(
                pr_idx, INDIRECTION_COLUMN, page_idx, slot
            )   

            # Recycle all tail record RIDs
            while logical_rid >= MAX_RECORD_PER_PAGE_RANGE:
                page_range.allocation_logical_rid_queue.put(logical_rid)
                log_page_idx, log_slot = page_range.get_column_location(logical_rid, INDIRECTION_COLUMN)
                logical_rid = page_range.bufferpool.read_page_slot(
                    pr_idx, INDIRECTION_COLUMN, log_page_idx, log_slot
                )
            
            self.dealloc_rid_queue.task_done()
    
    # Serialization Methods
    
    def serialize(self):
        """Exports table metadata as a JSON-compatible dictionary"""
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
        """Converts page directory to JSON-compatible format"""
        serialized = {}
        for rid, location in self.page_dir.items():
            serialized[rid] = {
                "page_range_id": location[0],
                "page_index": location[1],
                "slot_index": location[2]
            }
        return serialized

    def deserialize(self, data):
        """Restores table state from serialized data"""
        # Restore basic properties
        self.name = data['table_name']
        self.num_columns = data['num_columns']
        self.key = data['key_index']
        self.rid_index = data['rid_index']
        
        # Restore page directory
        self.page_dir = self.deserialize_page_directory(data['page_directory'])

        # Restore index
        self.index.deserialize(data['index'])

        # Restore page ranges
        for idx, pr_data in enumerate(data['page_ranges']):
            page_range = PageRange(idx, self.num_columns, self.bufferpool)
            page_range.deserialize(pr_data)
            self.page_ranges.append(page_range)
            
    def deserialize_page_directory(self, serialized_directory):
        """Converts JSON page directory back to internal format"""
        deserialized = {}

        for rid_str, location in serialized_directory.items():
            rid = int(rid_str)
            deserialized[rid] = (
                int(location['page_range_id']),
                int(location['page_index']),
                int(location['slot_index'])
            )

        return deserialized
