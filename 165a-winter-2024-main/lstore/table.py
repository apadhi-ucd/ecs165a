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
        '''Record structure combines hidden metadata columns and user-provided data columns [HIDDEN_COLUMNS..., USER_COLUMNS...]'''

    def __str__(self):
        return f"RID: {self.rid} Key: {self.key} \nColumns: {self.columns}"

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table(primary) key in column
    :db_path: string            #Path to the database directory where the table's data will be stored.
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

        self.page_directory = {}
        '''
        Page directory maps original RIDs to consolidated RIDs
        page_directory[original_rid] = consolidated_rid
        Consolidated RID functions similarly to base_rid
        Only table_merge should modify the page_directory
        Other functions have read-only access
        '''
        self.page_directory_lock = threading.Lock()

        # Initialize buffer pool at table level, not DB level
        self.bufferpool = BufferPool(self.table_path, self.num_columns)
        self.page_ranges:List[PageRange] = []

        # Set up queues for RID management
        self.deallocation_base_rid_queue = queue.Queue()
        self.allocation_base_rid_queue = queue.Queue()

        self.merge_queue = queue.Queue()
        '''Stores base RIDs of records scheduled for merging'''

        # RID management - table handles assignment
        self.rid_index = 0
        
        self.index = Index(self)
        
        # Launch background worker threads
        self.merge_thread = threading.Thread(target=self.__merge, daemon=True)
        self.merge_thread.start()

        self.deallocation_thread = threading.Thread(target=self.__delete_worker, daemon=True)
        self.deallocation_thread.start()

    def assign_rid_to_record(self, record: Record):
        '''Assigns a unique or recycled RID to a record'''
        with self.page_directory_lock:
            # Prioritize reusing deallocated RIDs
            if not self.allocation_base_rid_queue.empty():
                record.rid = self.allocation_base_rid_queue.get()
            else:
                record.rid = self.rid_index
                self.rid_index += 1

    def get_base_record_location(self, rid) -> tuple[int, int, int]:
        '''Calculates record storage location (page_range_idx, page_idx, slot) from RID'''
        pr_idx = rid // (MAX_RECORD_PER_PAGE_RANGE)
        pg_idx = rid % (MAX_RECORD_PER_PAGE_RANGE) // MAX_RECORD_PER_PAGE
        slot_pos = rid % MAX_RECORD_PER_PAGE
        return (pr_idx, pg_idx, slot_pos)

    def grab_all_base_rids(self):
        '''Retrieves all currently assigned base RIDs'''
        return list(range(self.rid_index))

    def insert_record(self, record: Record):
        '''Writes a new record to the appropriate base page location'''
        pr_idx, pg_idx, slot_pos = self.get_base_record_location(record.rid)

        # Create new page range if needed
        if (pr_idx >= len(self.page_ranges)):
            self.page_ranges.append(PageRange(pr_idx, self.num_columns, self.bufferpool))
        
        current_range = self.page_ranges[pr_idx]

        # Assign timestamp and write record
        with current_range.page_range_lock:
            record.columns[TIMESTAMP_COLUMN] = current_range.tps
        current_range.write_base_record(pg_idx, slot_pos, record.columns)   

    def update_record(self, rid, columns) -> bool:
        '''Creates a tail record for an updated version of the record'''
        pr_idx = rid // MAX_RECORD_PER_PAGE_RANGE
        current_range = self.page_ranges[pr_idx]

        # Assign timestamp atomically
        with current_range.page_range_lock:
            columns[TIMESTAMP_COLUMN] = current_range.tps
            
        # Write the update to tail pages
        update_result = current_range.write_tail_record(columns[RID_COLUMN], *columns)

        # Schedule merge if tail section getting large
        if (current_range.tps % (MAX_TAIL_PAGES_BEFORE_MERGING * MAX_RECORD_PER_PAGE) == 0):
            self.merge_queue.put(MergeRequest(current_range.page_range_index))

        return update_result

    def __insert_base_copy_to_tail_pages(self, page_range:PageRange, base_columns):
        '''Creates a tail record copy of a base record for merge operations'''
        logical_id = page_range.assign_logical_rid()
        indirection_id = base_columns[INDIRECTION_COLUMN]
        last_tail_id = page_range.find_records_last_logical_rid(indirection_id)

        # Skip if no tail records exist
        if (last_tail_id == indirection_id):
            return False
        
        pg_idx, slot_pos = page_range.get_column_location(last_tail_id, INDIRECTION_COLUMN)
        page_range.write_tail_record(logical_id, *base_columns)

        # Update previous tail record to point to new consolidated record
        self.bufferpool.write_page_slot(page_range.page_range_index, INDIRECTION_COLUMN, pg_idx, slot_pos, logical_id)

        return True
    
    def __merge(self):
        '''Background process that consolidates base and tail records'''
        while True:
            # Wait for merge requests
            merge_req = self.merge_queue.get()

            # Calculate range of RIDs to process
            start_id = merge_req.page_range_index * MAX_RECORD_PER_PAGE_RANGE
            end_id = min(start_id + MAX_RECORD_PER_PAGE_RANGE, self.rid_index)

            target_range = self.page_ranges[merge_req.page_range_index]

            # Process each record in the page range
            for current_rid in range(start_id, end_id):
                _, pg_idx, slot_pos = self.get_base_record_location(current_rid)

                # Copy the base record to work with
                base_data = target_range.copy_base_record(pg_idx, slot_pos)
                base_merge_timestamp = base_data[UPDATE_TIMESTAMP_COLUMN]

                # Initialize if this is first merge
                if base_merge_timestamp is None:
                    base_merge_timestamp = 0
                    if not self.__insert_base_copy_to_tail_pages(target_range, base_data):
                        continue

                # Get latest record
                latest_rid = base_data[INDIRECTION_COLUMN]
                latest_schema = base_data[SCHEMA_ENCODING_COLUMN]
                latest_ts = target_range.read_tail_record_column(latest_rid, TIMESTAMP_COLUMN)
                current_ts = latest_ts

                # Traverse update chain to consolidate all changes since last merge
                while latest_rid >= MAX_RECORD_PER_PAGE_RANGE and latest_schema != 0 and current_ts > base_merge_timestamp:
                    next_rid = target_range.read_tail_record_column(latest_rid, INDIRECTION_COLUMN)
                    update_schema = target_range.read_tail_record_column(latest_rid, SCHEMA_ENCODING_COLUMN)
                    current_ts = target_range.read_tail_record_column(latest_rid, TIMESTAMP_COLUMN)
                    
                    # Apply updates column by column based on schema encoding
                    for col_idx in range(self.num_columns):
                        if (latest_schema & (1 << col_idx)) and (update_schema & (1 << col_idx)):
                            latest_schema ^= (1 << col_idx)
                            base_data[col_idx + NUM_HIDDEN_COLUMNS] = target_range.read_tail_record_column(
                                latest_rid, col_idx + NUM_HIDDEN_COLUMNS)

                    latest_rid = next_rid
                
                # Update timestamp to mark merge point
                base_data[UPDATE_TIMESTAMP_COLUMN] = latest_ts
                base_data[UPDATE_TIMESTAMP_COLUMN] = int(time())
                
                # Write merged timestamp back to base record
                self.bufferpool.write_page_slot(
                    merge_req.page_range_index, 
                    UPDATE_TIMESTAMP_COLUMN, 
                    pg_idx, 
                    slot_pos, 
                    base_data[UPDATE_TIMESTAMP_COLUMN]
                )

                # Write all consolidated columns back to base pages
                for col_idx in range(self.num_columns):
                    self.bufferpool.write_page_slot(
                        merge_req.page_range_index, 
                        NUM_HIDDEN_COLUMNS + col_idx, 
                        pg_idx, 
                        slot_pos, 
                        base_data[col_idx + NUM_HIDDEN_COLUMNS]
                    )
            
            self.merge_queue.task_done()

    def __delete_worker(self):
        '''
        Reclaims deleted record space:
        1. Takes RIDs from deallocation queue
        2. Adds base RIDs to allocation queue for reuse
        3. Reclaims all associated tail record RIDs
        '''
        while True:
            # Get next RID to process
            deleted_rid = self.deallocation_base_rid_queue.get(block=True)

            # Locate record in storage
            pr_idx, pg_idx, slot_pos = self.get_base_record_location(deleted_rid)
            target_range = self.page_ranges[pr_idx]

            # Make RID available for reuse
            self.allocation_base_rid_queue.put(deleted_rid)

            # Get first tail record RID from indirection column
            tail_rid = target_range.bufferpool.read_page_slot(
                pr_idx, INDIRECTION_COLUMN, pg_idx, slot_pos
            )   

            # Traverse and reclaim all tail records
            while tail_rid >= MAX_RECORD_PER_PAGE_RANGE:
                target_range.allocation_logical_rid_queue.put(tail_rid)
                tail_pg_idx, tail_slot = target_range.get_column_location(tail_rid, INDIRECTION_COLUMN)
                tail_rid = target_range.bufferpool.read_page_slot(
                    pr_idx, INDIRECTION_COLUMN, tail_pg_idx, tail_slot
                )
            
            self.deallocation_base_rid_queue.task_done()
    
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
        """Converts Page Directory to JSON-compatible format"""
        serialized = {}
        for rid, location in self.page_directory.items():
            # Location tuple: (Page Range ID, Page Index, Slot Index)
            serialized[rid] = {
                "page_range_id": location[0],
                "page_index": location[1],
                "slot_index": location[2]
            }
        return serialized

    def deserialize(self, data):
        """Rebuilds Table state from serialized data"""
        # Restore core attributes
        self.name = data['table_name']
        self.num_columns = data['num_columns']
        self.key = data['key_index']
        self.rid_index = data['rid_index']
        
        # Rebuild page directory
        self.page_directory = self.deserialize_page_directory(data['page_directory'])

        # Restore index structures
        self.index.deserialize(data['index'])

        # Recreate page ranges
        for idx, pr_data in enumerate(data['page_ranges']):
            page_range = PageRange(idx, self.num_columns, self.bufferpool)
            page_range.deserialize(pr_data)
            self.page_ranges.append(page_range)
            
    def deserialize_page_directory(self, serialized_directory):
        """Converts JSON page directory back to internal format"""
        rebuild_directory = {}

        for rid_str, location in serialized_directory.items():
            # Convert string keys to integers
            rid = int(rid_str)

            # Rebuild location tuple
            rebuild_directory[rid] = (
                int(location['page_range_id']),
                int(location['page_index']),
                int(location['slot_index'])
            )

        return rebuild_directory
