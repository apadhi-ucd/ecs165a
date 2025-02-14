from lstore.index import Index
from lstore.page_range import PageRange

class Record:

    def __init__(self, indirection, rid, timestamp, schema_encoding, key, columns):
        self.rid = rid                  
        self.key = key                  
        self.columns = columns         
        self.indirection = indirection  
        self.timestamp = timestamp     
        self.schema_encoding = schema_encoding 


    def __getitem__(self, column):
        return self.columns[column]

    def __str__(self):

        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

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
        self.rid = 1                                                     
        self.page_range = [PageRange(num_columns)]  
        self.page_range_index = 0                   # Current page range index
        self.page_directory = {}                     # Maps RID to record location



    """
    # Inserts a record into the table
    """
    def new_rid(self):
        self.rid += 1
        return self.rid - 1

    """
    # Creates a page range 
    """
    def create_page_range(self):
        if self.page_range[-1].has_page_capacity(True): return;
    
        self.page_range.append(PageRange(self.num_columns))
        self.page_range_index += 1

    def __merge(self):
        print("merge is happening")
        pass