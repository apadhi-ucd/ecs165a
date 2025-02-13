from lstore.index import Index
from lstore.page_range import PageRange
from time import time

class Record:
    """
    Represents a single record with data values.
    """
    def __init__(self, indirection, rid, timestamp, schema_encoding, key, columns):
        self.indirection = indirection  
        self.rid = rid                  
        self.timestamp = timestamp     
        self.schema_encoding = schema_encoding 
        self.key = key                  
        self.columns = columns         

    def __getitem__(self, column):
        """
        Enables accessing record values like an array.
        """
        return self.columns[column]

    def __str__(self):
        """
        Returns a string representation of the record for debugging.
        """
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

class Table:
    """
    Manages records and pages for a table.
    :param name: str - Table name
    :param num_columns: int - Number of columns (all are integers)
    :param key: int - Index of the primary key column
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_ranges = [PageRange(num_columns)]  # Stores table's page ranges
        self.page_ranges_index = 0  # Tracks the current page range index
        self.page_directory = {}  # Maps RID to record location
        self.index = Index(self)  # Index for quick lookup
        self.rid = 1  # Next available record ID

    def new_rid(self):
        """
        Generates a new unique record ID (RID).
        """
        self.rid += 1
        return self.rid - 1

    def add_new_page_range(self):
        """
        Adds a new page range when the current one is full.
        """
        if self.page_ranges[-1].has_base_page_capacity():
            return
        
        self.page_ranges.append(PageRange(self.num_columns))
        self.page_ranges_index += 1

    def __merge(self):
        """
        Merges updated records into base records for efficiency.
        TO BE IMPLEMENTED IN MILESTONE 2
        """
        print("Merge is happening")
        pass
