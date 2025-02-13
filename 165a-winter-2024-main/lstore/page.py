import lstore.config as config
from time import time

class Page:
    def __init__(self):
        self.num_records = 0                    # Number of records currently stored
        self.data = bytearray(config.PAGE_SIZE) # Physical memory allocated for this page

    def has_capacity(self):
        """
        Returns true if the page has capacity to store another record
        """
        return self.num_records < config.RECORDS_PER_PAGE

    def write(self, value):
        """
        Writes an integer value to the page
        """
        if not self.has_capacity():
            return False
        
        # Calculate offset for new record
        offset = self.num_records * 8
        
        # Convert integer to bytes and write to page
        value_bytes = value.to_bytes(8, byteorder='big', signed=True)
        self.data[offset:offset+8] = value_bytes
        self.num_records += 1
        return True

    def read(self, index):
        """
        Reads an integer value from the page at the given index
        """
        if index >= self.num_records:
            return None
            
        # Calculate offset and convert bytes back to integer
        offset = index * 8
        value_bytes = self.data[offset:offset+8]
        return int.from_bytes(value_bytes, byteorder='big', signed=True)