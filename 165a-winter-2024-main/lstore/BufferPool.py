# Buffer Pool Class: 
# - Notes: Least frequently used out first
# - Instance variabels: dirty page count (might need page count, or might just consistently have the same # of pages loaded)
# - evict(): checks if page is clean or dirty, writes to disk if clean, otherwise discards
# - pull(): pull full page from disk based off of needed data
# - read(): should return wanted data and update the dirty/clean and page visits variables in the page

import config


# POLICY: Least used first out
class BufferPool:
    def __init__(self):
        self.dirty_page_count = 0
    

    def evict(self, page_to_evict):
        # check if page is dirty or clean
        # if clean, discard
        # if dirty, write to memeory then discard
        pass

    def pull(self, desired_rid): # parameter(s) may be incorrect here
        # search disk to find desired page within table
        # determine least used page in the pool
        # call evict() on least used page
        # then pull desired page into pool
        pass

    def read(self, desired_rid):
        # check if desired data is already in Buffer Pool
        # if not, call pull()
        # read data
        # return data
        pass

