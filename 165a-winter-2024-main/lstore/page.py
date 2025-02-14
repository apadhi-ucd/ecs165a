import lstore.config as config

class Page:

    def __init__(self):
        self.num_records = 0                    
        self.data = bytearray(config.PAGE_SIZE) 

    """
    Returns true if the page has capacity to store another record
    """
    def has_capacity(self):
        return self.num_records < config.PAGE_CAPACITY

    """
    Writes an integer value to the page
    :param value: int - Integer value to write
    """
    def write(self, value):
        # Write the value to the page
        if not self.has_capacity():
            return False
        
        # Calculate the offset
        offset = self.num_records * 8
        
        # Write the value to the page
        value_bytes = value.to_bytes(8, byteorder='big', signed=True)
        self.data[offset:offset+8] = value_bytes
        self.num_records += 1
        return True
    
    """
    Reads an integer value from the page
    :param index: int - Index of the record to read
    """
    def read(self, index):
        # Read the value from the page
        if index >= self.num_records:
            return None
            
        # Calculate the offset
        offset = index * 8

        # Read the value from the page
        value_bytes = self.data[offset:offset+8]
        return int.from_bytes(value_bytes, byteorder='big', signed=True)
