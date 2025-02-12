
class Page:
    PAGE_SIZE = 4096*16 # 64KB
    RECORDS_PER_PAGE = 4000 # Each base page holds 4k records
    DELETE_FLAG = -1  # Special value to indicate deletion

    """
    Represents a single page that stores records in a columnar format.
    """
    def __init__(self, num_columns, page_type = "base"):
        self.num_columns = num_columns+1      # Number of columns in the table for each page, extra +1 for schema
        self.page_type = page_type  # Type of page: 'base' or 'tail'
        self.pages = [bytearray(self.PAGE_SIZE) for _ in range(self.num_columns)] # Column data storage
        self.num_records = 0  # Number of records in the page
        self.offsets = [0] * self.num_columns # Track offsets for each column (used for writing data)
        self.invalid_records = set()  # Track deleted records

        # Do we need this??
        self.version_metadata = [] if page_type == 'tail' else None  # Track versions for tail pages
        


    """
    Checks if the page has space to store more records.
    """
    def has_capacity(self):
        return self.num_records < self.RECORDS_PER_PAGE


    """
    Writes a new record to the page if space is available.
        
    :param record: The record to write.
    :param schema_encoding: The schema encoding for the record.
    :param version: The version of the record (for tail pages).
    :return: True if the record is successfully written, False otherwise.
    """
    def write(self, record, schema_encoding, version = None):
        if self.has_capacity():

            # Write the schema encoding to the first column
            schema_encoding_bytes = str(schema_encoding).encode('utf-8')
            self.physical_pages[0][self.offsets[0]:self.offsets[0] + len(schema_encoding_bytes)] = schema_encoding_bytes
            self.offsets[0] += len(schema_encoding_bytes)

            # Write the record to the pages
            for i, value in enumerate(record):
                encoded_value = str(value).encode('utf-8') # Encode the value as bytes
                record_size = len(encoded_value) # Get the size of the encoded value
            
                if self.offsets[i] + record_size > self.PAGE_SIZE:
                    return False  # Not enough space in the column's page
            
                self.pages[i][self.offsets[i]:self.offsets[i] + record_size] = encoded_value # Write the encoded value
                self.offsets[i] += record_size # Update the offset for the column

            if self.page_type == 'tail':
                self.version_metadata.append(version)

            self.num_records += 1
            return True
        return False
    

    """
    Flags a record to delete by setting its value to DELETE_FLAG.
        
    :param record_id: The index of the record to delete.
    """
    def invalidate(self, record_id):
        self.invalid_records.add(record_id)  
        
    """
    Retrieves a record from the page based on its index and projected columns.

    :param record_index: The index of the record to retrieve.
    :param projected_columns: A list indicating which columns to project.
    """    
    def get_record(self, record_index, projected_columns):
        if record_index in self.invalid_records:
            return None
        
        record = []
        for i in range(self.num_columns):
            if projected_columns[i]:
                record.append(self.physical_pages[i][record_index])
        return record


