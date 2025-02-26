from lstore.config import *
import struct
import base64
import zlib

class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < MAX_RECORD_PER_PAGE

    def write(self, value):
        struct.pack_into("i", self.data, self.num_records * INTEGER_BYTE_SIZE, value)
        self.num_records += 1
        return (self.num_records - 1)

    def write_precise(self, index, value):
        '''
        This function should be able to write data on a precise index inside the table.
        Useful for changing the indirection column on the base page
        '''
        struct.pack_into("i", self.data, index * INTEGER_BYTE_SIZE, value)

    def get(self, index):
        '''This funciton should be able to grab a data located at a certain index in the page'''
        return struct.unpack_from("i", self.data, index * INTEGER_BYTE_SIZE)[0]
    
    def serialize(self):
        '''Returns page metadata as a JSON-compatible dictionary'''
        compressed_data = zlib.compress(self.data)
        return {
            "num_records": self.num_records,

            # converts bytearray into b64, which is then converted into a string (JSON compatible)
            "data": base64.b64encode(compressed_data).decode('utf-8')
        }

    def deserialize(self, json_data):
        '''Loads a page from serialized data'''
        self.num_records = json_data["num_records"]
        compressed_data = base64.b64decode(json_data["data"])
        self.data = bytearray(zlib.decompress(compressed_data))
