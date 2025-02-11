
class Page:
    PAGE_SIZE = 4096*16 # 64KB
    RECORDS_PER_PAGE = 4000 # Each base page holds 4k records

    """
    Represents a single page that stores records in a columnar format.
    """
    def __init__(self, num_columns, page_type = "base"):
        self.num_columns = num_columns      # Number of columns in the table for each page
        self.page_type = page_type  # Type of page: 'base' or 'tail'
        self.pages = [bytearray(self.PAGE_SIZE) for _ in range(num_columns)] # Column data storage
        self.num_records = 0  # Number of records in the page
        self.offsets = [0] * num_columns # Track offsets for each column (used for writing data)


    """
    Checks if the page has space to store more records.
    """
    def has_capacity(self):
        return self.num_records < self.RECORDS_PER_PAGE


    """
    Writes a new record to the page if space is available.
        
    :param record: The record to write.
    :return: True if the record is successfully written, False otherwise.
    """
    def write(self, record):
        if self.has_capacity():
            for i, value in enumerate(record):
                encoded_value = str(value).encode('utf-8') # Encode the value as bytes
                record_size = len(encoded_value) # Get the size of the encoded value
            
                if self.offsets[i] + record_size > self.PAGE_SIZE:
                    return False  # Not enough space in the column's physical page
            
                self.pages[i][self.offsets[i]:self.offsets[i] + record_size] = encoded_value # Write the encoded value
                self.offsets[i] += record_size # Update the offset for the column
        
            self.num_records += 1
            return True
        return False


