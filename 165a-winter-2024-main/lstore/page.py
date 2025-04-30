from lstore.config import *
import struct
import base64
import zlib

class Page:
    def __init__(self):
        self.entry_count = 0
        self.content = bytearray(PAGE_SIZE)
    
    def has_capacity(self):
        """Check if page can store additional records"""
        return self.entry_count < MAX_RECORD_PER_PAGE
    
    def write(self, value):
        """Append a new integer value to the page and return its index"""
        struct.pack_into("i", self.content, self.entry_count * INTEGER_BYTE_SIZE, value)
        self.entry_count += 1
        return (self.entry_count - 1)
    
    def write_precise(self, position, value):
        """
        Write an integer value at a specific position in the page.
        Used for updating values like indirection pointers in base pages.
        """
        struct.pack_into("i", self.content, position * INTEGER_BYTE_SIZE, value)
    
    def get(self, position):
        """Retrieve the integer value stored at the specified position"""
        return struct.unpack_from("i", self.content, position * INTEGER_BYTE_SIZE)[0]
    
    def serialize(self):
        """
        Convert page data to a serializable format.
        Returns a dictionary with compressed page data in base64 encoding.
        """
        compressed_content = zlib.compress(self.content)
        return {
            "entry_count": self.entry_count,
            "content": base64.b64encode(compressed_content).decode('utf-8')
        }
    
    def deserialize(self, serialized_data):
        """
        Restore page from serialized data.
        Unpacks the compressed base64-encoded content back into page.
        """
        self.entry_count = serialized_data["entry_count"]
        compressed_content = base64.b64decode(serialized_data["content"])
        self.content = bytearray(zlib.decompress(compressed_content))
