from lstore.index import Index
from time import time

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
        self.page_directory = {}
        self.index = Index(self)
        pass

    def __merge(self): # Essentially this merge function combines tail pages into a base page to optimize space --> it gets
                       # the most recent tail pages and combines it with the base page
        print("merge is happening")
        pass

    def __insert(self) # Be able to insert new records and not sure what parameters would 
        pass           # Idea is to first find a base page with space, if not create a new base page
                       # Can track records using page_directory

    def __update(self) # Handles any updates from the base page to the tail page and be able to track lineage
        pass           # Can add another def or include another function to keep track of lineage def __get_record -->
                       # finds latest version using lineage (page_directory) --> return latest updated version 

                       # Purpose: Finds latest version --> create new tail page if needed --> updates lineage in page_directory

    
    
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
 
