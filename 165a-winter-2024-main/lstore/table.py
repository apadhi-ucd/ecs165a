from lstore.index import Index
from lstore.page_range import PageRange
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
        self.page_directory = {}                        # Maps record IDs to their corresponding page ranges
        self.index = Index(self)                        # Index structure for fast lookups
        self.page_ranges = [PageRange(num_columns)]     # List of page ranges
        self.sorted_keys = []                           # Sorted list of keys for efficient searching





    # Essentially this merge function combines tail pages into a base page to optimize space --> it gets
    # the most recent tail pages and combines it with the base page
    # I don't think we need this for milestone 1
    def __merge(self): 
        print("merge is happening")
        pass





    # Be able to insert new records and not sure what parameters would 
    # Idea is to first find a base page with space, if not create a new base page
    # Can track records using page_directory
        """
        Inserts a new record into the table.
        - Finds a base page with available space.
        - If no space is available, creates a new base page.
        - Updates page_directory and sorted_keys.
        - Bisect is used to keep keys sorted for efficient searching.
        - bisect.insort keeps keys sorted when a new record is inserted
        
        :param values: Values of the record to be inserted.
        :return: True if insertion is successful, False if the record already exists.
        """
    def __insert(self, *values): 
        record_id = values[self.key]    # Assuming the key is the first value in the tuple
        if record_id in self.page_directory:
            return False     # Record already exists
        
        # Attempt to insert into an existing page range
        for page_range in self.page_ranges:
            if page_range.insert(values):
                self.page_directory[record_id] = page_range
                bisect.insort(self.sorted_keys, record_id)     # Keep keys sorted
                return True
        
        new_range = PageRange(self.num_columns)
        new_range.insert(values)
        self.page_ranges.append(new_range)
        self.page_directory[record_id] = new_range
        bisect.insort(self.sorted_keys, record_id)    # Keep keys sorted
        return True






    # Handles any updates from the base page to the tail page and be able to track page_directory
    # Can add another def or include another function to keep track of page_directory def __get_record -->
    # finds latest version using page_directory (page_directory) --> return latest updated version 
    # Purpose: Finds latest version --> create new tail page if needed --> updates page_directory in page_directory
    """
    Updates an existing record by adding a new version in the tail page.
    - Looks up the record in page_directory.
    - Creates a new version and updates metadata.
        
    :param record_id: ID of the record to update.
    :param new_values: Updated values for the record.
    :return: True if update is successful, False if record is not found.
    """
    def __update(self, record_id, *new_values): 
        if record_id not in self.page_directory:
            return False
        
        page_range = self.page_directory[record_id]
        version = len(page_range.tail_pages[-1].version_metadata)  # Assign version number
        page_range.update(new_values, version)
        return True
    
    
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
 
