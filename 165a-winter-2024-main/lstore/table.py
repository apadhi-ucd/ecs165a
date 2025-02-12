from lstore.index import Index
from lstore.page_range  import PageRange
from time import time
import bisect

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns, schema_encoding):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.schema_encoding = schema_encoding

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns 
        self.lineage = {}                        # Maps record IDs to their corresponding page ranges
        self.index = Index(self)                        # Index structure for fast lookups
        self.page_ranges = [PageRange(num_columns)]     # List of page ranges
        self.sorted_keys = []                           # Sorted list of keys for efficient searching





    # Essentially this merge function combines tail pages into a base page to optimize space --> it gets
    # the most recent tail pages and combines it with the base page
    # I don't think we need this for milestone 1
    def merge(self): 
        print("merge is happening")
        pass




    
    def insert(self, *values):
        record_id = values[self.key]    # Assuming the key is the first value in the tuple
        if record_id in self.lineage:
            return False     # Record already exists
        
        # Generate a unique RID for the new record
        rid = len(self.sorted_keys)  # Using length as a simple RID generator
        
        # Attempt to insert into an existing page range
        for page_range in self.page_ranges:
            if page_range.insert(values):
                # Update the lineage
                self.lineage[record_id] = page_range
                bisect.insort(self.sorted_keys, record_id)
                
                # Update indices for all indexed columns
                for col_index in range(self.num_columns):
                    if self.index.indices[col_index] is not None:
                        self.index.insert_entry(col_index, values[col_index], rid)
                
                return True
        
        # If no existing page range has space, create a new one
        new_range = PageRange(self.num_columns)
        if new_range.insert(values):
            self.page_ranges.append(new_range)
            self.lineage[record_id] = new_range
            bisect.insort(self.sorted_keys, record_id)
            
            # Update indices for the new page range
            for col_index in range(self.num_columns):
                if self.index.indices[col_index] is not None:
                    self.index.insert_entry(col_index, values[col_index], rid)
            
            return True
        
        return False  # Insert failed





    # Handles any updates from the base page to the tail page and be able to track lineage
    # Can add another def or include another function to keep track of lineage def __get_record -->
    # finds latest version using lineage (lineage) --> return latest updated version 
    # Purpose: Finds latest version --> create new tail page if needed --> updates lineage in lineage
    """
    Updates an existing record by adding a new version in the tail page.
    - Looks up the record in lineage.
    - Creates a new version and updates metadata.
        
    :param record_id: ID of the record to update.
    :param new_values: Updated values for the record.
    :return: True if update is successful, False if record is not found.
    """
    def update(self, record_id, *new_values): 
        if record_id not in self.lineage:
            return False
        
        page_range = self.lineage[record_id]
        version = len(page_range.tail_pages[-1].version_metadata)  # Assign version number
        original_record = self.select_version(record_id, 0, [1] * self.num_columns, -1)  # Retrieve latest record
        page_range.update(original_record, new_values, version)
        return True
    
    """
    Finds page range that record is in, and sends request to the page range to delete it.
        
    :param record_id: The index of the record to delete.
    """ 
    def delete(self, record_id):
        if record_id not in self.lineage:
            return False
        
        page_range = self.lineage[record_id]
        return page_range.delete(self.sorted_keys.index(record_id))
    

    """
    Retrieves a specific version of a record based on its ID.

    - Uses lineage to find the correct page range.
    - Uses versioning to retrieve the correct record.

    :param record_id: ID of the record to retrieve.
    :param projected_columns: Columns to project.
    :param version: Version of the record to retrieve.
    """    
    def select_version(self, record_id, projected_columns, version=-1):
        if record_id not in self.lineage:
            return None
        
        page_range = self.lineage[record_id]
        page = page_range.tail_pages[version] if version < 0 else page_range.base_pages[0]
        return page.get_record(record_id, projected_columns)
    
    """
    Retrieves a range of records based on their keys.
    - Uses binary search to find the correct range.
    - Iterates through the range to retrieve records.

    :param start_key: Starting key of the range.
    :param end_key: Ending key of the range.
    :param column: Column to project.
    :param version: Version of the records to retrieve.
    """    
    def sum_version(self, start_key, end_key, column, version=-1):
        start_index = bisect.bisect_left(self.sorted_keys, start_key)
        end_index = bisect.bisect_right(self.sorted_keys, end_key)
        keys_in_range = self.sorted_keys[start_index:end_index]
        
        total = 0
        for key in keys_in_range:
            record = self.select_version(key, column, [1] * self.num_columns, version)
            total += record[column] if record and record[column] is not None else 0
        
        return total
    

"""

Notes about Tables 

Here's the link for the gpt I used https://chatgpt.com/share/67aa91a6-4ef0-8009-8bad-cb069d267768

Purpose of a table:
    * Store records (Base Page) --> Original 
    * Get Updated information (Tail Pages) --> When records are updated it goes to tail pages
    * Retrieve latest version efficiently 
    * Support Multiple pages
    * Track Original (Base Page) to Updated Versions (Tail Pages)

Implementation:
    * The skeleton already provides the table and records so from there we just need to implement base pages and 
      tail pages and we can add this under the table class
    * 
    
 
"""
 
